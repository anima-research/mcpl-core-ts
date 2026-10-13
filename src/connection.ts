/**
 * Bidirectional async JSON-RPC 2.0 connection for MCPL.
 * Port of mcpl-core/src/connection.rs
 *
 * Transport-agnostic: works over TCP, stdio, or any Node.js Readable/Writable pair.
 * Over a stream, messages are newline-delimited JSON: one JSON object per
 * line, ended by `\n` (a `\r` before it is whitespace), and the last line
 * also by the end of the input. Nothing else ends a line, so a U+2028 or
 * U+2029 inside a JSON string, which JSON allows raw, stays in its message.
 * Over a WebSocket, each text message is one JSON object (MCPL §4.2.2), not
 * split into lines at all. Outgoing JSON escapes U+2028 and U+2029
 * (`\u2028`, `\u2029`; the value is the same), so a peer that reads lines
 * with Node's readline, as earlier versions of this package did, still
 * reads one message: readline splits at them on Node 24.
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
 * which closes the connection anyway. Like any EventEmitter, a connection
 * with no `'error'` listener throws them; it still closes when the error is
 * a stream's. If reporting a malformed line throws, the messages behind it
 * are taken on a later turn while the input is open; at the end of the
 * input, the reading ends there, and the connection closes without taking
 * the rest. Once the connection has closed, nothing more is read from the
 * input, and a stream error that arrives later isn't reported, even before
 * `'close'` is. A message already read when a listener closes the
 * connection finishes its dispatch: the rest of its listeners hear it, and
 * a later `nextMessage()` returns it (one already waiting is rejected by
 * the close). Messages read behind it and not yet taken are dropped.
 *
 * `close()` also releases the connection's output, once: it destroys the
 * output stream (for `fromTcp`, the socket), and closes the WebSocket for
 * `fromWebSocket`, even when a `'close'` listener throws or calls `close()`
 * itself. The input given to `fromStreams` stays its caller's: the
 * connection only stops reading it. A connection that closed on its own
 * releases nothing, so call `close()` after it too, as you would after
 * closing it yourself.
 */

import { EventEmitter } from 'node:events';
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
import { ConnectionClosedError, RpcError } from './errors.js';

// ── Public Types ──

export type IncomingMessage =
  | { type: 'request'; request: JsonRpcRequest }
  | { type: 'notification'; notification: JsonRpcNotification };

export interface McplConnectionEvents {
  request: [request: JsonRpcRequest];
  notification: [notification: JsonRpcNotification];
  close: [];
  error: [error: Error];
}

// Typed EventEmitter — provides compile-time event name/argument checking
type TypedEmitter = {
  on<K extends keyof McplConnectionEvents>(event: K, listener: (...args: McplConnectionEvents[K]) => void): TypedEmitter;
  emit<K extends keyof McplConnectionEvents>(event: K, ...args: McplConnectionEvents[K]): boolean;
  once<K extends keyof McplConnectionEvents>(event: K, listener: (...args: McplConnectionEvents[K]) => void): TypedEmitter;
  removeListener<K extends keyof McplConnectionEvents>(event: K, listener: (...args: McplConnectionEvents[K]) => void): TypedEmitter;
} & EventEmitterType;

// ── Pending Request Tracking ──

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timer?: ReturnType<typeof setTimeout>;
}

/**
 * How messages are cut from the input: one per line of a stream, or one per
 * chunk of an object-mode input that carries whole messages (a WebSocket's).
 */
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

// ── Connection ──

export class McplConnection extends (EventEmitter as new () => TypedEmitter) {
  private writer: Writable;
  /** Stops taking anything in from the input (latch(): from handleClose, or at construction). */
  private stopReading: () => void;
  private nextId = 1;
  private pending = new Map<string, PendingRequest>();
  private incomingQueue: IncomingMessage[] = [];
  private incomingWaiters: Array<{
    resolve: (msg: IncomingMessage) => void;
    reject: (err: Error) => void;
  }> = [];
  private closed = false;
  /** Whether close() has released the output and the transport. */
  private released = false;
  /** Releases anything beyond the output stream, such as a WebSocket. */
  private releaseTransport?: () => void;

  private constructor(
    readable: Readable,
    writable: Writable,
    { framing = 'lines', releaseTransport }: { framing?: Framing; releaseTransport?: () => void } = {},
  ) {
    super();
    this.writer = writable;
    this.releaseTransport = releaseTransport;

    // One message: a closed connection parses nothing more, not even the
    // rest of a chunk it had already read when it closed.
    const take = (text: string): void => {
      if (this.closed) return;
      const trimmed = text.trim();
      if (!trimmed) return;
      try {
        const msg: Record<string, unknown> = JSON.parse(trimmed);
        this.handleParsedMessage(msg);
      } catch (e) {
        this.emit('error', new Error(`Malformed JSON-RPC message: ${(e as Error).message}`));
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
    // Lets go of input read but not yet taken, once the connection closes:
    // an input that outlives the connection (stdin, say) keeps it reachable
    // through the error and close listeners that stay attached.
    let discardQueued: () => void;
    if (framing === 'messages') {
      const messages: string[] = [];
      discardQueued = () => { messages.length = 0; };
      onData = (chunk) => {
        messages.push(String(chunk));
        takeQueued(() => messages.shift());
      };
      // What arrived before the socket closed is taken before the close.
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
      // decoded as UTF-8 across chunk boundaries. Each chunk is searched
      // once: its whole lines join the queue, and the pieces of a line still
      // arriving are joined once, when its '\n' comes, so a long line costs
      // time in proportion to its length however many chunks bring it.
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
    // reports nothing more: these listeners stay attached, so a late error
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

  // ── Factories ──

  /** Create from a TCP socket. */
  static fromTcp(socket: net.Socket): McplConnection {
    socket.setEncoding('utf-8');
    return new McplConnection(socket, socket);
  }

  /** Create from arbitrary readable/writable streams (e.g., stdin/stdout, child process). */
  static fromStreams(readable: Readable, writable: Writable): McplConnection {
    return new McplConnection(readable, writable);
  }

  /** Accept a single TCP connection from a server and return an McplConnection. */
  static acceptTcp(server: net.Server): Promise<McplConnection> {
    return new Promise((resolve, reject) => {
      server.once('connection', (socket: net.Socket) => {
        resolve(McplConnection.fromTcp(socket));
      });
      server.once('error', reject);
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
      readable.push(null); // EOF
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

    // close() closes the WebSocket too.
    connection = new McplConnection(readable, writable, { framing: 'messages', releaseTransport: () => ws.close() });
    return connection;
  }

  // ── Public API ──

  /**
   * Whether the connection has closed: nothing more is read or sent. An
   * output ended on purpose doesn't close it, since the input may still bring
   * answers, but the send methods refuse from its `end()` on.
   */
  get isClosed(): boolean {
    return this.closed;
  }

  /** Default timeout for sendRequest (ms). 0 = no timeout. */
  requestTimeout = 30_000;

  /**
   * Send a JSON-RPC request and wait for the response.
   * Responses are matched by ID; incoming requests/notifications that arrive
   * while waiting are queued for `nextMessage()` / event listeners.
   * Rejects with `ConnectionClosedError` at once when the connection has
   * closed or its output has ended.
   *
   * @param timeout Override the default request timeout (ms). 0 = no timeout.
   */
  async sendRequest(method: string, params?: unknown, timeout?: number): Promise<unknown> {
    if (!this.canSend()) throw new ConnectionClosedError();

    const id = this.nextId++;
    const request = makeRequest(id, method, params);

    const timeoutMs = timeout ?? this.requestTimeout;

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
            reject(new RpcError(-32000, `Request timed out after ${timeoutMs}ms: ${method}`));
          }
        }, timeoutMs);
      }

      this.pending.set(String(id), {
        resolve: (value) => { cleanup(); resolve(value); },
        reject: (err) => { cleanup(); reject(err); },
        timer,
      });

      this.writeLine(JSON.stringify(request));
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
    // Drain queued messages first
    const queued = this.incomingQueue.shift();
    if (queued) return queued;

    if (this.closed) throw new ConnectionClosedError();

    // Wait for the next one
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

  // ── Internal ──

  /** Whether a send can go out: the connection is open and its output hasn't ended. */
  private canSend(): boolean {
    return !this.closed && !this.writer.writableEnded && !this.writer.destroyed;
  }

  private writeLine(json: string): void {
    this.writer.write(escapeLineSeparators(json) + '\n');
  }

  private handleParsedMessage(msg: Record<string, unknown>): void {
    if (msg.jsonrpc !== '2.0') return; // Not a valid JSON-RPC 2.0 message

    const hasId = msg.id != null;
    const hasMethod = typeof msg.method === 'string';
    const hasResult = 'result' in msg;
    const hasError = 'error' in msg;

    if (hasId && (hasResult || hasError)) {
      // Response to one of our pending requests
      this.routeResponse(msg as unknown as JsonRpcResponse);
    } else if (hasId && hasMethod) {
      // Incoming request
      this.routeIncoming({
        type: 'request',
        request: msg as unknown as JsonRpcRequest,
      });
    } else if (hasMethod && !hasId) {
      // Notification
      this.routeIncoming({
        type: 'notification',
        notification: msg as unknown as JsonRpcNotification,
      });
    }
  }

  private routeResponse(resp: JsonRpcResponse): void {
    const key = String(resp.id);
    const pending = this.pending.get(key);
    if (!pending) return;

    this.pending.delete(key);
    if (resp.error) {
      pending.reject(new RpcError(resp.error.code, resp.error.message, resp.error.data));
    } else {
      pending.resolve(resp.result);
    }
  }

  private routeIncoming(msg: IncomingMessage): void {
    // Emit events for EventEmitter consumers
    if (msg.type === 'request') {
      this.emit('request', msg.request);
    } else {
      this.emit('notification', msg.notification);
    }

    // Feed pull-based consumers
    const waiter = this.incomingWaiters.shift();
    if (waiter) {
      waiter.resolve(msg);
    } else {
      this.incomingQueue.push(msg);
    }
  }

  /**
   * Closes the connection, once, whatever closed it, and reports it. The
   * closing takes effect before `error`, the stream error that closed it, is
   * reported, so an `'error'` or `'close'` listener finds it closed: nothing
   * it sends is written (`sendRequest()` rejects), and a stream failure it
   * sets off is not reported again.
   */
  private handleClose(error?: Error): void {
    if (this.closed) return;
    this.latch();
    this.announce(error);
  }

  /** The closing itself: nothing more is read or sent, and whatever waits on the connection rejects. */
  private latch(): void {
    this.closed = true;
    // Stop reading: nothing more is read.
    this.stopReading();

    // Reject all pending requests and clear their timers
    const closedErr = new ConnectionClosedError();
    for (const [, pending] of this.pending) {
      if (pending.timer) clearTimeout(pending.timer);
      pending.reject(closedErr);
    }
    this.pending.clear();

    // Reject all pull-based waiters
    for (const waiter of this.incomingWaiters) {
      waiter.reject(closedErr);
    }
    this.incomingWaiters = [];
  }

  /**
   * Reports a close that has taken effect: `error`, the stream error that
   * closed the connection, if any, and then `'close'`, which is emitted even
   * when reporting the error throws (no `'error'` listener).
   */
  private announce(error?: Error): void {
    try {
      if (error) this.emit('error', error);
    } finally {
      this.emit('close');
    }
  }
}
