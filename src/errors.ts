/**
 * Error types for MCPL connections.
 */

export class ConnectionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConnectionError';
  }
}

/**
 * The connection has closed, or can no longer send. Its message says what that
 * meant for the call: nothing was sent, a request it had handed to its output
 * got no answer, or no more messages will come.
 */
export class ConnectionClosedError extends ConnectionError {
  /**
   * For a request, what became of it: `'not-sent'` when the connection refused
   * it before writing anything, so the call didn't run, and `'no-response'`
   * when it was handed to the connection's output and the connection closed
   * before its answer, so whether it ran is unknown. Undefined for
   * `nextMessage()`, which made no call.
   */
  readonly outcome?: 'not-sent' | 'no-response';

  constructor(message = 'Connection closed', outcome?: 'not-sent' | 'no-response') {
    super(message);
    this.name = 'ConnectionClosedError';
    this.outcome = outcome;
  }
}

/**
 * A JSON-RPC error: the other end answered a request with one, or a handler
 * throws one to answer with it. Its subclass `ConnectionTimeoutError` is thrown
 * when no answer comes in time. The message has no code in front of it, so it
 * doesn't grow as it's passed on. From an error response it is the other
 * end's own message when that is text that isn't empty, and otherwise a
 * sentence that says what came instead. The code and any data are on the
 * error.
 */
export class RpcError extends ConnectionError {
  /**
   * The error's code. From an error response it is the integer the other end
   * sent, or -32603 (internal error) when a malformed one sent none, or a
   * value that isn't an integer, so it's a valid code to answer with. A
   * handler's own `RpcError` carries the code it was built with.
   */
  readonly code: number;
  /** The JSON-RPC `error.data` member, when the responder sent one (SPEC §6.6). */
  readonly data?: unknown;

  constructor(code: number, message: string, data?: unknown) {
    super(message);
    this.name = 'RpcError';
    this.code = code;
    this.data = data;
  }
}

/**
 * A request got no reply within its timeout, so whether it ran is unknown. It
 * is an `RpcError` with code -32000, so a caller that catches `RpcError` still
 * catches it, and `instanceof ConnectionTimeoutError` tells it from an error
 * the other end returned.
 */
export class ConnectionTimeoutError extends RpcError {
  /**
   * What became of the request, as `ConnectionClosedError`'s `outcome` says
   * it: it was handed to the connection's output and got no answer, so
   * whether it ran is unknown.
   */
  readonly outcome = 'no-response' as const;

  /** `method` and `timeoutMs` go into the message; the caller has both already. */
  constructor(method: string, timeoutMs: number) {
    super(
      -32000,
      `${method} timed out: no reply came within ${timeoutMs} ms, so whether it ran is unknown. ` +
        'Check its effect before retrying, since a retry may repeat it.',
    );
    this.name = 'ConnectionTimeoutError';
  }
}
