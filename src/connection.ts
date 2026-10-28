import { EventEmitter } from 'node:events';
import { types } from 'node:util';
import type { EventEmitter as EventEmitterType } from 'node:events';
import * as net from 'node:net';
import { PassThrough } from 'node:stream';
import type { Readable, Writable } from 'node:stream';
import { StringDecoder } from 'node:string_decoder';

import type {
  JsonRpcId,
  JsonRpcRequest,
  JsonRpcResponse,
  JsonRpcNotification,
} from './types.js';
import { makeRequest, makeResponse, makeErrorResponse, makeNotification } from './types.js';
import { ConnectionClosedError, ConnectionTimeoutError, RpcError } from './errors.js';

const NOT_SENT =
  'Nothing was sent: this connection can no longer send (it has closed, or its output has ended), ' +
  "so the call didn't run. This connection doesn't reopen.";
const UNANSWERED =
  'The connection closed before the call got an answer, so whether it ran is unknown: ' +
  "check its effect before calling again. A closed connection doesn't reopen.";
const NO_MORE_MESSAGES = 'The connection has closed, so no more messages will come.';
/** The longest delay setTimeout keeps; it runs a longer one after 1 ms. */
const MAX_TIMER_MS = 2 ** 31 - 1;

/**
 * A value the other end sent, as JSON. JSON.parse reads 1e999 as Infinity,
 * which JSON would write as null, so a number that isn't finite is shown as
 * itself when it is the whole value; inside an array or object it reads as
 * null, as JSON writes it. JSON.stringify recurses, so a value nested deeper
 * than the stack allows, which JSON.parse accepts, is named instead of shown.
 */
function shown(value: unknown): string {
  if (typeof value === 'number' && !Number.isFinite(value)) return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return '(a value nested too deeply to show)';
  }
}

function errorResponseText(error: unknown): string {
  if (typeof error !== 'object' || error === null || Array.isArray(error)) {
    return `The other end returned an error that isn't a JSON-RPC error object: ${shown(error)}`;
  }
  const { code, message } = error as { code?: unknown; message?: unknown };
  if (typeof message === 'string' && message !== '') return message;
  const named =
    typeof code === 'number'
      ? `error ${code} with`
      : code === undefined
        ? 'an error with no code and'
        : `an error with the code ${shown(code)} and`;
  if (message == null || message === '') return `The other end returned ${named} no message.`;
  return `The other end returned ${named} a message that isn't text: ${shown(message)}`;
}

export type IncomingMessage =
  | { type: 'request'; request: JsonRpcRequest }
  | { type: 'notification'; notification: JsonRpcNotification };

export interface McplConnectionEvents {
  request: [request: JsonRpcRequest];
  notification: [notification: JsonRpcNotification];
  close: [];
  error: [error: Error];
}

type TypedEmitter = {
  on<K extends keyof McplConnectionEvents>(event: K, listener: (...args: McplConnectionEvents[K]) => void): TypedEmitter;
  emit<K extends keyof McplConnectionEvents>(event: K, ...args: McplConnectionEvents[K]): boolean;
  once<K extends keyof McplConnectionEvents>(event: K, listener: (...args: McplConnectionEvents[K]) => void): TypedEmitter;
  removeListener<K extends keyof McplConnectionEvents>(event: K, listener: (...args: McplConnectionEvents[K]) => void): TypedEmitter;
} & EventEmitterType;

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timer?: ReturnType<typeof setTimeout>;
}

type Framing = 'lines' | 'messages';

const LINE_SEPARATORS = /[\u2028\u2029]/g;

/**
 * JSON text with U+2028 and U+2029 escaped. JSON.stringify leaves them raw,
 * and they can only be inside strings there, where an escape means the same.
 * For the wire alone: a digest over canonical JSON wants them as they are.
 */
function escapeLineSeparators(json: string): string {
  return json.replace(LINE_SEPARATORS, (c) => (c === '\u2028' ? '\\u2028' : '\\u2029'));
}

/**
 * Bidirectional async JSON-RPC 2.0 connection for MCPL.
 *
 * Transport-agnostic: works over TCP, stdio, or any Node.js Readable/Writable pair.
 * Over a stream, messages are newline-delimited JSON: one JSON object per
 * line, ended by `\n` (a `\r` before it is whitespace), and the last line
 * also by the end of the input. Nothing else ends a line, so a U+2028 or
 * U+2029 inside a JSON string, which JSON allows raw, stays in its message.
 * Over a WebSocket, each text message is one JSON object (MCPL §4.2.2), not
 * split into lines at all. Outgoing JSON escapes U+2028 and U+2029
 * (`\u2028`, `\u2029`; the value is the same), so a peer that reads lines
 * with Node's readline still reads one message: readline splits at them on
 * Node 24.
 *
 * Dual API:
 *   - Pull-based: `nextMessage()` returns the next incoming request/notification
 *   - Event-based: `.on('request', ...)` and `.on('notification', ...)`
 *
 * Responses to pending `sendRequest()` calls are routed internally and never
 * surfaced through either API.
 *
 * The connection closes, once, when either stream fails (a TCP reset, a
 * WebSocket error, a child process's broken pipe), when its input ends or is
 * destroyed, when its output is destroyed before it finished, or when
 * `close()` is called. An output ended on purpose leaves the input to
 * finish: the connection still reads, and sends nothing more
 * (`sendRequest()` rejects with `ConnectionClosedError`, and the other send
 * methods do nothing). Closing takes effect before anything is reported:
 * nothing more is read or sent, pending requests reject with
 * `ConnectionClosedError`, and `nextMessage()` returns what was already
 * queued and then rejects. Then the stream error that closed it, if any, is
 * emitted as `'error'`, and `'close'` follows. So a listener of either sees a
 * closed connection: `sendRequest()` rejects and the other send methods do
 * nothing, and a request taken from the queue after the close can't be
 * answered.
 *
 * A connection built on a stream that was already done is closed from the
 * start: `isClosed` is true, the send methods refuse, and `nextMessage()`
 * rejects. That is an input that had ended, failed or been destroyed, or an
 * output that had failed or been destroyed before it finished. The close is
 * reported on a later turn of the event loop, so the code that built the
 * connection, or that awaited the promise that gave it, can listen first if
 * it adds its listeners before it awaits anything else: the error such a
 * stream failed with, if any, and then `'close'`. An output that had
 * finished, and hadn't failed since, leaves the input to finish, as one
 * ended later does.
 *
 * Only the stream error that closed the connection is emitted, once, before
 * `'close'`, however many of the streams fail and whatever a listener does
 * meanwhile. A malformed line is emitted as `'error'` too, and leaves the
 * connection open, unless it is the last line of an input that has ended,
 * which closes the connection anyway. An exception thrown by a `'request'`
 * or `'notification'` listener is emitted as `'error'` too, as it was
 * thrown (a thrown value that isn't an Error is the `cause` of the Error
 * emitted). It doesn't close the connection, and it's reported even when
 * the listener closed the connection first. Each message is handed to
 * `nextMessage()` before its listeners hear it, so a listener that throws
 * doesn't take it from `nextMessage()`; as with any EventEmitter, the
 * listeners after the one that threw don't hear it. An async listener's
 * rejection isn't caught, unless `EventEmitter.captureRejections` was set
 * before the connection was made; the connection doesn't set it. A JSON
 * value that isn't an object is not a JSON-RPC message, and is ignored.
 * Like any EventEmitter, a connection with no `'error'` listener throws
 * them; it still closes when the error is a stream's. If reporting a
 * malformed line or a listener's exception throws, the messages behind it
 * are taken on a later turn while the input is open; at the end of the
 * input, the reading ends there, and the connection closes without taking
 * the rest. Once the connection has closed, nothing more is read from the
 * input, and a stream error that arrives later isn't reported, even before
 * `'close'` is. A message already read when a listener closes the connection
 * finishes its dispatch: the rest of its listeners hear it, and
 * `nextMessage()` returns it, to a call already waiting or to a later one.
 * Messages read behind it and not yet taken are dropped.
 *
 * `close()` also releases the connection's output, once: it destroys the
 * output stream (for `fromTcp`, the socket), and closes the WebSocket for
 * `fromWebSocket`, even when a `'close'` listener throws or calls `close()`
 * itself. The input given to `fromStreams` stays its caller's: the
 * connection only stops reading it. A connection that closed on its own
 * releases nothing, so call `close()` after it too, as you would after
 * closing it yourself.
 */
export class McplConnection extends (EventEmitter as new () => TypedEmitter) {
  private writer: Writable;
  private stopReading: () => void;
  private nextId = 1;
  private pending = new Map<string, PendingRequest>();
  private incomingQueue: IncomingMessage[] = [];
  private incomingWaiters: Array<{
    resolve: (msg: IncomingMessage) => void;
    reject: (err: Error) => void;
  }> = [];
  private closed = false;
  private released = false;
  private releaseTransport?: () => void;

  private constructor(
    readable: Readable,
    writable: Writable,
    { framing = 'lines', releaseTransport }: { framing?: Framing; releaseTransport?: () => void } = {},
  ) {
    super();
    this.writer = writable;
    this.releaseTransport = releaseTransport;

    const take = (text: string): void => {
      if (this.closed) return;
      const trimmed = text.trim();
      if (!trimmed) return;
      let msg: unknown;
      try {
        msg = JSON.parse(trimmed);
      } catch (e) {
        this.emit('error', new Error(`Malformed JSON-RPC message: ${(e as Error).message}`));
        return;
      }
      // An Error from another realm is still an Error; any other thrown value
      // is kept as the cause of one.
      try {
        this.handleParsedMessage(msg);
      } catch (err) {
        this.emit('error', types.isNativeError(err) ? err : new Error('A listener threw a value that is not an Error', { cause: err }));
      }
    };

    // Messages are taken one at a time, in the order they arrived. A
    // listener can push more input while a message is being taken (a
    // stream's 'data' then arrives synchronously): that input joins the
    // queue behind what came before it, and only the outermost call takes
    // messages from the queue. Each message leaves the queue before its
    // listeners run.
    let taking = false;
    const takeQueued = (next: () => string | undefined): void => {
      if (taking) return;
      taking = true;
      try {
        let message: string | undefined;
        while (!this.closed && (message = next()) !== undefined) take(message);
      } catch (err) {
        // Reporting a message threw (an 'error' with no listener): the
        // messages behind it are taken on a later turn rather than left
        // waiting for more input.
        setImmediate(() => takeQueued(next));
        throw err;
      } finally {
        taking = false;
      }
    };

    let onData: (chunk: string | Buffer) => void;
    let onEnd: () => void;
    let discardQueued: () => void;
    if (framing === 'messages') {
      const messages: string[] = [];
      discardQueued = () => { messages.length = 0; };
      onData = (chunk) => {
        messages.push(String(chunk));
        takeQueued(() => messages.shift());
      };
      onEnd = () => {
        try {
          takeQueued(() => messages.shift());
        } finally {
          this.handleClose();
        }
      };
    } else {
      // Lines end at '\n' and nowhere else (not at a lone '\r', nor at
      // U+2028 or U+2029 as readline does on Node 24). A stream of bytes is
      // decoded as UTF-8 across chunk boundaries.
      const decoder = new StringDecoder('utf8');
      const lines: string[] = [];
      let partial: string[] = [];
      discardQueued = () => {
        lines.length = 0;
        partial = [];
      };
      onData = (chunk) => {
        const text = typeof chunk === 'string' ? chunk : decoder.write(chunk);
        let start = 0;
        let newline: number;
        while ((newline = text.indexOf('\n', start)) !== -1) {
          partial.push(text.slice(start, newline));
          lines.push(partial.join(''));
          partial = [];
          start = newline + 1;
        }
        if (start < text.length) partial.push(text.slice(start));
        takeQueued(() => lines.shift());
      };
      // The last line needn't end with '\n'. ('end' doesn't arrive inside a
      // 'data' listener, so no message is being taken then.) The connection
      // closes even if reporting a malformed last line throws, as an 'error'
      // with no listener does.
      onEnd = () => {
        try {
          partial.push(decoder.end());
          takeQueued(() => lines.shift());
          const last = partial.join('');
          partial = [];
          take(last);
        } finally {
          this.handleClose();
        }
      };
    }
    this.stopReading = () => {
      readable.removeListener('data', onData);
      readable.removeListener('end', onEnd);
      readable.pause();
      discardQueued();
    };

    // A stream that fails closes the connection, whichever side it is: a
    // stream that errors or is destroyed (a TCP reset, a WebSocket error, a
    // child's broken pipe) closes without ending. The connection closes
    // before the error is reported (handleClose). A closed connection
    // reports no more stream errors: these listeners stay attached, so a late error
    // is ignored here rather than thrown by the stream, and an error
    // reaching both listeners (a TCP socket is both streams), a second
    // stream failing from the same cause, or one an 'error' listener sets
    // off is reported once.
    const fail = (err: Error): void => this.handleClose(err);
    readable.on('error', fail);
    readable.on('close', () => this.handleClose());
    writable.on('error', fail);
    // An output ended on purpose (end(), then 'finish') leaves the input to
    // finish, and the send methods refuse from its end() on (canSend); one
    // destroyed before it finished can't send any more.
    writable.on('close', () => {
      if (!writable.writableFinished) this.handleClose();
    });

    // A stream that was already done before it was handed in sends none of
    // those events again, or only those already on their way: an input that
    // had ended, failed or been destroyed, or an output that had failed or
    // been destroyed before it finished (one that finished without failing
    // was ended on purpose). A stream that doesn't destroy itself on an error
    // (autoDestroy off) has failed without being destroyed, so its error
    // counts on its own. The connection is closed from the start, so an event
    // on its way meets the closed check, and nothing can be sent. The close is
    // reported on a later turn of the event loop rather than the next tick,
    // so a caller that gets the connection through a promise settled in an
    // event's callback (acceptTcp), and listens before it awaits anything
    // else, has listened first: the error such a stream failed with, as for
    // any stream error, and then 'close'.
    const outputBroken = writable.errored != null || (writable.destroyed && !writable.writableFinished);
    if (readable.destroyed || readable.readableEnded || readable.errored != null || outputBroken) {
      const error = readable.errored ?? (outputBroken ? writable.errored : null) ?? undefined;
      this.latch();
      setImmediate(() => this.announce(error));
    } else {
      // Reading starts now, as readline started it, even for an input paused
      // before it was handed in. A connection closed from the start never
      // listens for its input's data: adding a 'data' listener starts an
      // input that isn't paused, and Node's resume then reads from the
      // source on the next tick, after the latch has paused it.
      readable.on('data', onData);
      readable.on('end', onEnd);
      readable.resume();
    }
  }

  /**
   * Create from a TCP socket. It sets the socket's encoding to UTF-8 and turns
   * off Nagle's algorithm (`setNoDelay(true)`), so a message this end writes
   * isn't held back while what it sent before is unacknowledged.
   */
  static fromTcp(socket: net.Socket): McplConnection {
    socket.setEncoding('utf-8');
    socket.setNoDelay(true);
    return new McplConnection(socket, socket);
  }

  static fromStreams(readable: Readable, writable: Writable): McplConnection {
    return new McplConnection(readable, writable);
  }

  /**
   * Accept a single TCP connection from a server and return an McplConnection.
   *
   * It listens on the server only while it is pending. If the server emits
   * 'error' first, it rejects with that error. Either way it then leaves no
   * listener of its own on the server, and never closes it.
   *
   * What a rejection leaves depends on the error:
   * - **A failed accept** (such as EMFILE) on a listening server leaves it
   *   listening, so the caller can call acceptTcp again to keep accepting,
   *   or close the server.
   * - **A failure to listen** (such as EADDRINUSE), when an accept is
   *   pending, rejects that accept, and the server isn't listening: listen
   *   again before accepting. With no accept pending, it goes to the
   *   caller's own listeners, as below.
   *
   * acceptTcp has no listener on the server between accepts:
   * - **An 'error'** the server emits then goes to the caller's own 'error'
   *   listeners. With none, Node throws it from the server's emit, not
   *   through any acceptTcp promise, so a try/catch around the await doesn't
   *   catch it, and in a server process it ends the process. A caller that
   *   keeps the server listening after an accept (an accept loop, say)
   *   attaches its own 'error' listener for as long as it does. Attaching it
   *   once 'listening' has fired keeps a failure to listen, with no accept
   *   pending, ending the process.
   * - **A connection** that arrives then isn't taken by acceptTcp. Node
   *   accepts it, and nothing of acceptTcp's reads or closes it, so a caller
   *   that serves one connection at a time leaves such a client open and
   *   unanswered: the next accept takes the next connection to arrive, not
   *   that one.
   *
   * Await each accept before starting the next: two pending at once would
   * both resolve with the same first socket. A pending accept can't be
   * cancelled: racing its promise against a timeout leaves its listeners on
   * the server, and the next connection or error still settles it.
   */
  static acceptTcp(server: net.Server): Promise<McplConnection> {
    return new Promise((resolve, reject) => {
      const onConnection = (socket: net.Socket): void => {
        server.off('error', onError);
        resolve(McplConnection.fromTcp(socket));
      };
      const onError = (err: Error): void => {
        server.off('connection', onConnection);
        reject(err);
      };
      server.once('connection', onConnection);
      server.once('error', onError);
    });
  }

  /**
   * Create from a WebSocket. Each text message is one JSON-RPC message
   * (MCPL §4.2.2), parsed whole, and each message sent is one text message.
   * Works with any WebSocket implementation that has `on('message')`,
   * `on('close')`, `on('error')`, `send()` and `close()`. A `readyState`, when
   * the socket has one, is read too: a socket already closed (3) when it's
   * wrapped closes the connection, and sends go out only while it's open (1).
   * A socket without one is taken to be open until its `'close'`.
   */
  static fromWebSocket(ws: {
    on(event: 'message', cb: (data: unknown) => void): void;
    on(event: 'close', cb: () => void): void;
    on(event: 'error', cb: (err: Error) => void): void;
    send(data: string): void;
    close(): void;
    readyState?: number;
  }): McplConnection {
    // One chunk per WebSocket message, so a message is never split or joined.
    const readable = new PassThrough({ objectMode: true });
    const writable = new PassThrough();

    // The input ends when the WebSocket closes: nothing follows it. A message
    // that comes after the connection has closed is ignored, rather than held
    // in the paused input until the socket closes.
    let ended = false;
    let connection: McplConnection | undefined;
    const end = (): void => {
      ended = true;
      readable.push(null);
    };

    ws.on('message', (data: unknown) => {
      if (ended || connection?.isClosed) return;
      readable.push(typeof data === 'string' ? data : String(data));
    });

    ws.on('close', end);

    ws.on('error', (err: Error) => {
      readable.destroy(err);
    });

    // A WebSocket that is already closed (readyState 3) sends no 'close'
    // again. Its input is done before the connection is built, so the
    // connection is closed from the start, as for any input already done.
    if (ws.readyState === 3) {
      ended = true;
      readable.destroy();
    }

    // Writable stream → WS message (strip trailing newline). Only an open
    // socket sends (readyState 1, OPEN); one without a readyState sends.
    writable.on('data', (chunk: Buffer) => {
      const text = chunk.toString().replace(/\n$/, '');
      if (ws.readyState === undefined || ws.readyState === 1) {
        ws.send(text);
      }
    });

    connection = new McplConnection(readable, writable, { framing: 'messages', releaseTransport: () => ws.close() });
    return connection;
  }

  /**
   * Whether the connection has closed: nothing more is read or sent. An
   * output ended on purpose doesn't close it, since the input may still bring
   * answers, but the send methods refuse from its `end()` on.
   */
  get isClosed(): boolean {
    return this.closed;
  }

  /** Default timeout for sendRequest (ms). 0 = no timeout, as is a negative value or NaN. */
  requestTimeout = 30_000;

  /**
   * Send a JSON-RPC request and wait for the response.
   * Responses are matched by ID; incoming requests/notifications that arrive
   * while waiting are queued for `nextMessage()` and emitted to listeners as
   * they arrive.
   * Rejects with `ConnectionClosedError` at once when the connection has
   * closed or its output has ended, so nothing was sent (`outcome`
   * `'not-sent'`), and later when the connection closes before the answer
   * comes (`'no-response'`); with `ConnectionTimeoutError` when no answer comes
   * within the timeout; with `RpcError` when the other end answers with an
   * error; with a `TypeError`, before anything is sent, when `params`
   * can't be written as JSON; and with what the output threw, when it throws
   * as the request is written.
   *
   * @param timeout Override the default request timeout (ms). 0 = no timeout,
   *   as is a negative value or NaN.
   *   A timeout past 2147483647 ms (almost 25 days), setTimeout's limit, runs
   *   as that limit.
   */
  async sendRequest(method: string, params?: unknown, timeout?: number): Promise<unknown> {
    if (!this.canSend()) throw new ConnectionClosedError(NOT_SENT, 'not-sent');

    const id = this.nextId++;
    let line: string;
    try {
      line = JSON.stringify(makeRequest(id, method, params));
    } catch (err) {
      const why = types.isNativeError(err) ? err.message : "something that isn't an Error was thrown";
      throw new TypeError(`Nothing was sent: ${method}'s params can't be written as JSON (${why}).`, { cause: err });
    }

    const timeoutMs = Math.min(timeout ?? this.requestTimeout, MAX_TIMER_MS);

    return new Promise<unknown>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;

      const cleanup = () => {
        if (timer) clearTimeout(timer);
        this.pending.delete(String(id));
      };

      if (timeoutMs > 0) {
        timer = setTimeout(() => {
          const pending = this.pending.get(String(id));
          if (pending) {
            this.pending.delete(String(id));
            reject(new ConnectionTimeoutError(method, timeoutMs));
          }
        }, timeoutMs);
      }

      this.pending.set(String(id), {
        resolve: (value) => { cleanup(); resolve(value); },
        reject: (err) => { cleanup(); reject(err); },
        timer,
      });

      try {
        this.writeLine(line);
      } catch (err) {
        cleanup();
        reject(err);
      }
    });
  }

  /**
   * Send a JSON-RPC notification (fire-and-forget, no response expected).
   * This and the two response methods do nothing when the connection has
   * closed or its output has ended.
   */
  sendNotification(method: string, params?: unknown): void {
    if (!this.canSend()) return;
    const notification = makeNotification(method, params);
    this.writeLine(JSON.stringify(notification));
  }

  /** Send a JSON-RPC success response (answering an incoming request). */
  sendResponse(id: JsonRpcId, result: unknown): void {
    if (!this.canSend()) return;
    const response = makeResponse(id, result);
    this.writeLine(JSON.stringify(response));
  }

  /**
   * Send a JSON-RPC error response.
   *
   * `data` is the optional JSON-RPC `error.data` member — SPEC §6.6 documents
   * concrete shapes for it, e.g. `data: { capability }` on capability-denied
   * errors and `data: { featureSet }` on feature-set errors. Omitted entirely
   * from the wire when not provided.
   */
  sendError(id: JsonRpcId, code: number, message: string, data?: unknown): void {
    if (!this.canSend()) return;
    const response = makeErrorResponse(id, data === undefined ? { code, message } : { code, message, data });
    this.writeLine(JSON.stringify(response));
  }

  /**
   * Pull-based API: get the next incoming request or notification.
   * Responses to our pending requests are routed internally (never returned here).
   * Returns a message already taken in and queued for it, even once the
   * connection has closed; otherwise resolves when the next one arrives, or
   * rejects with `ConnectionClosedError` once the connection has closed.
   * Input read but not yet taken when the connection closed is dropped.
   */
  async nextMessage(): Promise<IncomingMessage> {
    const queued = this.incomingQueue.shift();
    if (queued) return queued;

    if (this.closed) throw new ConnectionClosedError(NO_MORE_MESSAGES);

    return new Promise<IncomingMessage>((resolve, reject) => {
      this.incomingWaiters.push({ resolve, reject });
    });
  }

  /**
   * Close the connection, if it isn't closed already, and release its output,
   * once: destroy the output stream (for `fromTcp`, the socket), and close the
   * WebSocket for `fromWebSocket`. The input given to `fromStreams` is left
   * to its caller. The release happens even when a `'close'` listener throws
   * or calls `close()` itself, and a connection that closed on its own (its
   * input ended or failed) is released by calling this after its `'close'`.
   */
  close(): void {
    try {
      this.handleClose();
    } finally {
      if (!this.released) {
        this.released = true;
        if ('destroy' in this.writer && typeof this.writer.destroy === 'function') {
          this.writer.destroy();
        }
        this.releaseTransport?.();
      }
    }
  }

  private canSend(): boolean {
    return !this.closed && !this.writer.writableEnded && !this.writer.destroyed;
  }

  private writeLine(json: string): void {
    this.writer.write(escapeLineSeparators(json) + '\n');
  }

  private handleParsedMessage(parsed: unknown): void {
    if (typeof parsed !== 'object' || parsed === null) return;
    const msg = parsed as Record<string, unknown>;
    if (msg.jsonrpc !== '2.0') return;

    const hasId = msg.id != null;
    const hasMethod = typeof msg.method === 'string';
    const hasResult = 'result' in msg;
    const hasError = 'error' in msg;

    if (hasId && (hasResult || hasError)) {
      this.routeResponse(msg as unknown as JsonRpcResponse);
    } else if (hasId && hasMethod) {
      this.routeIncoming({
        type: 'request',
        request: msg as unknown as JsonRpcRequest,
      });
    } else if (hasMethod && !hasId) {
      this.routeIncoming({
        type: 'notification',
        notification: msg as unknown as JsonRpcNotification,
      });
    }
  }

  private routeResponse(resp: JsonRpcResponse): void {
    // This connection sends number ids, so only a number, or a string a peer
    // may have echoed, can answer a request. Anything else answers none, and
    // String() could throw on it.
    const id: unknown = resp.id;
    if (typeof id !== 'number' && typeof id !== 'string') return;
    const key = String(id);
    const pending = this.pending.get(key);
    if (!pending) return;

    this.pending.delete(key);
    // `error: null` is no error: some peers send it beside a result.
    if (resp.error != null) {
      const error: unknown = resp.error;
      const fields = typeof error === 'object' ? (error as { code?: unknown; data?: unknown }) : {};
      // JSON-RPC requires an integer: JSON.parse reads 1e999 as Infinity, which
      // JSON writes back as null, and 1.5 is no code either.
      const code = Number.isInteger(fields.code) ? (fields.code as number) : -32603;
      pending.reject(new RpcError(code, errorResponseText(error), fields.data));
    } else {
      pending.resolve(resp.result);
    }
  }

  private routeIncoming(msg: IncomingMessage): void {
    // Feed pull-based consumers first, so a listener that throws, or closes
    // the connection, can't take the message from them. A waiter's
    // continuation runs on a later microtask, after the listeners'
    // synchronous part; an async listener's later steps can come after it.
    const waiter = this.incomingWaiters.shift();
    if (waiter) {
      waiter.resolve(msg);
    } else {
      this.incomingQueue.push(msg);
    }

    if (msg.type === 'request') {
      this.emit('request', msg.request);
    } else {
      this.emit('notification', msg.notification);
    }
  }

  private handleClose(error?: Error): void {
    if (this.closed) return;
    this.latch();
    this.announce(error);
  }

  private latch(): void {
    this.closed = true;
    this.stopReading();

    const unanswered = new ConnectionClosedError(UNANSWERED, 'no-response');
    const noMore = new ConnectionClosedError(NO_MORE_MESSAGES);
    for (const [, pending] of this.pending) {
      if (pending.timer) clearTimeout(pending.timer);
      pending.reject(unanswered);
    }
    this.pending.clear();

    for (const waiter of this.incomingWaiters) {
      waiter.reject(noMore);
    }
    this.incomingWaiters = [];
  }

  private announce(error?: Error): void {
    try {
      if (error) this.emit('error', error);
    } finally {
      this.emit('close');
    }
  }
}
