/**
 * JSON-RPC 2.0 message types for MCPL transport.
 * Port of mcpl-core/src/types.rs
 */

// ── JSON-RPC 2.0 Core ──

export type JsonRpcId = number | string;

export interface JsonRpcRequest {
  jsonrpc: '2.0';
  id: JsonRpcId;
  method: string;
  params?: unknown;
}

export interface JsonRpcResponse {
  jsonrpc: '2.0';
  id: JsonRpcId;
  result?: unknown;
  error?: JsonRpcError;
}

export interface JsonRpcNotification {
  jsonrpc: '2.0';
  method: string;
  params?: unknown;
}

export interface JsonRpcError {
  code: number;
  message: string;
  data?: unknown;
}

export type JsonRpcMessage = JsonRpcRequest | JsonRpcResponse | JsonRpcNotification;

// ── Constructors ──

export function makeRequest(id: JsonRpcId, method: string, params?: unknown): JsonRpcRequest {
  return { jsonrpc: '2.0', id, method, ...(params !== undefined && { params }) };
}

export function makeResponse(id: JsonRpcId, result: unknown): JsonRpcResponse {
  return { jsonrpc: '2.0', id, result };
}

export function makeErrorResponse(id: JsonRpcId, error: JsonRpcError): JsonRpcResponse {
  return { jsonrpc: '2.0', id, error };
}

export function makeNotification(method: string, params?: unknown): JsonRpcNotification {
  return { jsonrpc: '2.0', method, ...(params !== undefined && { params }) };
}

// ── Content Blocks (Appendix B.1 of MCPL spec) ──

export type ContentBlock =
  | TextContent
  | ImageContent
  | AudioContent
  | ResourceContent;

export interface TextContent {
  type: 'text';
  text: string;
}

/** RFC-005 §3.1: requested context disposition. Testimony, never authority —
 *  `never` is a veto the host must honor (payload AND uri withheld from
 *  context); `ref` prefers a stub; nothing a server writes can compel
 *  inlining. Only legal on uri-form blocks (RFC-005 §8). */
export type ReferenceDisposition = 'never' | 'ref';

/** RFC-005 §3 reference metadata, shared by uri-form blocks. Every field is a
 *  server claim, not a fact (verification is the host's job, RFC-005 §7). */
export interface ReferenceFields {
  /** Claimed media type. */
  mimeType?: string;
  /** Claimed payload size in bytes: non-negative JSON-safe integer. */
  sizeBytes?: number;
  /** `sha256:` + base64url over the exact payload octets (identity coding). */
  digest?: string;
  /** Advisory availability horizon, ISO-8601. Unparseable ⇒ treated expired. */
  expiresAt?: string;
  /** Display label. Never a path component (RFC-005 §7.3). */
  name?: string;
  disposition?: ReferenceDisposition;
}

export type ImageContent = {
  type: 'image';
  data: string;
  mimeType?: string;
  uri?: never;
  disposition?: never;
} | ({
  type: 'image';
  uri: string;
  data?: never;
} & ReferenceFields);

export type AudioContent = {
  type: 'audio';
  data: string;
  mimeType?: string;
  uri?: never;
  disposition?: never;
} | ({
  type: 'audio';
  uri: string;
  data?: never;
} & ReferenceFields);

export interface ResourceContent extends ReferenceFields {
  type: 'resource';
  uri: string;
}

export function textContent(text: string): TextContent {
  return { type: 'text', text };
}

// ── MCPL Error Codes ──

export const ERR_FEATURE_SET_NOT_ENABLED = -32001;
/** Method requires a capability not in the effective grant; `data: { capability }` (SPEC §14.6). */
export const ERR_CAPABILITY_DENIED = -32002;
export const ERR_UNKNOWN_FEATURE_SET = -32003;
export const ERR_CHECKPOINT_NOT_FOUND = -32005;
export const ERR_CHANNEL_NOT_PERMITTED = -32017;
export const ERR_UNKNOWN_CHANNEL = -32023;
export const ERR_CHANNEL_OPEN_FAILED = -32024;
