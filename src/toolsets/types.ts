import type { ContentBlock } from '../types.js';
import type { AccessRefusedKind, ToolHintChanges } from '../methods.js';
import type { JsonSchema } from './schema.js';

/** An MCP tool definition. */
export interface ToolDef {
  name: string;
  description: string;
  inputSchema: JsonSchema;
  annotations?: Record<string, unknown>;
  /** `mcpl/class` (RFC-008); `mcpl/preempt: true` runs without waiting for a call in flight. */
  _meta?: Record<string, unknown>;
}

export interface CallOutcome {
  content: ContentBlock[];
  isError?: boolean;
}

/** RFC-014 §5.1. `default: "open"` cannot be combined with `instances`. */
export interface ToolsetDecl {
  title: string;
  summary: string;
  default: 'open' | 'closed';
  instances?: boolean;
  options?: JsonSchema;
}

export type HintChanges = ToolHintChanges;

/** An event an instance raises. During one of its own calls it is folded into that call's result. */
export interface InstanceEvent {
  /**
   * `activity`: something happened. `state`: a latest value: a held one is replaced by the next
   * of its instance and `key`, and a host that coalesces (RFC-006) replaces it while unread.
   */
  kind: 'activity' | 'state';
  text: string;
  /** Extra content: images, a board. */
  content?: ContentBlock[];
  /** Producer tags, e.g. RFC-014's `surface:turn`, `surface:alert`, `surface:done`. */
  tags?: string[];
  /** state only: coalescing sub-key. */
  key?: string;
  /** Append the instance's content() after the text. */
  includeContent?: boolean;
}

/** A credential the host delegated (RFC-015). Never put it in content, events or logs. */
export interface AccessCredential {
  scheme: 'bearer';
  credential: string;
  /** Unix ms. */
  expiresAt: number;
  principal?: string;
}

export interface AccessOptions {
  /** Where it will be presented, for the host's audit (RFC-015 §8.4). */
  endpoint?: string;
  /** Why, for the host's audit. */
  purpose?: string;
  /** Skip caches: the endpoint refused the last credential. */
  fresh?: boolean;
}

/** Access was not delegated. The message is safe to show a model. */
export class AccessError extends Error {
  readonly audience: string;
  /**
   * The RFC-015 refusal kind: the host's own when it refused with `-32040` and named one;
   * `unavailable` when the request was abandoned without an answer (RFC-015 §3.4);
   * `host_policy` when it answered with a scheme other than bearer (RFC-015 §3.2). Absent when
   * it was never asked, refused without a kind, or answered with an expired credential or
   * something that isn't one.
   */
  readonly kind?: AccessRefusedKind;
  constructor(audience: string, reason: string, kind?: AccessRefusedKind) {
    super(`no access to "${audience}": ${reason}`);
    this.name = 'AccessError';
    this.audience = audience;
    if (kind) this.kind = kind;
  }
}

export interface InstanceContext {
  readonly toolset: string;
  /** Present for toolsets with `instances: true`. */
  readonly instance?: string;
  /** Who owns the instance — the principal of the connection(s) it belongs to. */
  readonly principal: string;
  log(...args: unknown[]): void;
  emit(event: InstanceEvent): void;
  /** Visibility or definitions changed outside a call: the server reports the difference. */
  changed(): void;
  /** Ask the host to open or close a toolset (RFC-014 §6.2); the host decides. */
  suggest(s: { toolset: string; instance?: string; action: 'open' | 'close'; reason: string }): void;
  /** End this instance. Inside one of its calls, that call's result reports it. */
  close(reason: string): void;
  /**
   * A credential for `audience`, acting as this principal, from its host (RFC-015). Cached until
   * a minute before expiry; ask again at every dial. Throws AccessError when not delegated.
   */
  access(audience: string, opts?: AccessOptions): Promise<AccessCredential>;
}

/**
 * One open toolset instance. call() and close() are serialized, except a call of a tool whose
 * `_meta` sets `mcpl/preempt: true`, which runs at once while later work waits for it; the rest
 * may be called anytime.
 */
export interface InstanceHandler {
  title: string;
  /** Names of this toolset's tools that are available now. */
  visible(): string[];
  /** A definition that differs from the declared one right now (new enum options). */
  definition?(tool: string): ToolDef | undefined;
  /** What the agent sees on opening, and with an event that asks for it. */
  content?(): ContentBlock[] | Promise<ContentBlock[]>;
  call(tool: string, args: Record<string, unknown>): Promise<CallOutcome>;
  /** Release resources. Returned text is shown in the close result. */
  close(reason: string): Promise<void | string> | void | string;
}

export interface ToolsetSpec extends ToolsetDecl {
  /** Every tool this toolset can offer (the declared definitions). */
  tools: ToolDef[];
  open(ctx: InstanceContext, options: Record<string, unknown>): Promise<InstanceHandler> | InstanceHandler;
}

export interface StaticTool {
  tool: ToolDef;
  call(args: Record<string, unknown>, who: { principal: string }): Promise<CallOutcome> | CallOutcome;
}

export interface ToolsetServerSpec {
  name: string;
  version: string;
  /** Server-level hint: everything here is engagement (RFC-014 §5.1). */
  surface?: { title: string; summary: string };
  instructions?: string;
  toolsets: Record<string, ToolsetSpec>;
  staticTools?: StaticTool[];
  /**
   * How long a principal's instances stay open after its last connection closes, in ms; then
   * they are closed and the principal forgotten. Default 24 hours; 0 keeps them forever.
   */
  idleCloseMs?: number;
  /** Replace the built-in argument validator (e.g. with ajv). Return null or an explanation. */
  validate?: (schema: JsonSchema, value: unknown) => string | null;
  log?: (...args: unknown[]) => void;
}
