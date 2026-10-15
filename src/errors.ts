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
 * meant for the call: nothing was sent, a request sent was left unanswered, or
 * no more messages will come.
 */
export class ConnectionClosedError extends ConnectionError {
  constructor(message = 'Connection closed') {
    super(message);
    this.name = 'ConnectionClosedError';
  }
}

/**
 * A JSON-RPC error: the other end answered a request with one, or a handler
 * throws one to answer with it. The message is the error's own, with no code
 * in front of it, so it reads the same however many times it is passed on;
 * the code and any data are on the error.
 */
export class RpcError extends ConnectionError {
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
 * is an `RpcError` with code -32000, as a timeout was before this class was
 * thrown, so a caller that catches `RpcError` still catches it, and
 * `instanceof ConnectionTimeoutError` tells it from an error the other end
 * returned.
 */
export class ConnectionTimeoutError extends RpcError {
  /** The request's method. */
  readonly method: string;
  /** The timeout it was given, in milliseconds. */
  readonly timeoutMs: number;

  constructor(method: string, timeoutMs: number) {
    super(
      -32000,
      `${method} timed out: no reply came within ${timeoutMs} ms, so whether it ran is unknown. ` +
        'Check its effect before retrying, since a retry may repeat it.',
    );
    this.name = 'ConnectionTimeoutError';
    this.method = method;
    this.timeoutMs = timeoutMs;
  }
}
