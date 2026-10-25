/**
 * Server side of MCPL RFC-014 (Tool Presence, Toolsets and Surfaces): a server keeps its full
 * tool list and says, per tool, whether it is static or dynamic, whether it is visible right now,
 * and which toolset it belongs to. Toolsets open and close per principal, optionally as named
 * instances. Hosts decide how a model sees any of it.
 *
 * Also the server side of MCPL RFC-015 (Delegated Access): `InstanceContext.access(audience)`.
 */
import type { ContentBlock } from '../types.js';
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

/** RFC-014 §6.1. */
export interface HintChanges {
  rev: number;
  tools?: Array<{ name: string; instance?: string; presence?: 'static' | 'dynamic'; visible?: boolean; definition?: ToolDef }>;
  toolsets?: Array<{ toolset: string; instance?: string; state: 'open' | 'closed'; title?: string }>;
  suggest?: Array<{ toolset: string; instance?: string; action: 'open' | 'close'; reason: string }>;
}

/** An event an instance raises. During one of its own calls it is folded into that call's result. */
export interface InstanceEvent {
  /** `activity`: something happened. `state`: a latest value, replaced while unread (RFC-006). */
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
  constructor(audience: string, reason: string) {
    super(`no access to "${audience}": ${reason}`);
    this.name = 'AccessError';
    this.audience = audience;
  }
}

/** What an instance's code is handed. */
export interface InstanceContext {
  readonly toolset: string;
  /** Present for toolsets with `instances: true`. */
  readonly instance?: string;
  /** Who owns the instance — the principal of the connection(s) it belongs to. */
  readonly principal: string;
  log(...args: unknown[]): void;
  /** Raise an event. */
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

/** One open toolset instance. call() and close() are serialized; the rest may be called anytime. */
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
  /** Replace the built-in argument validator (e.g. with ajv). Return null or an explanation. */
  validate?: (schema: JsonSchema, value: unknown) => string | null;
  log?: (...args: unknown[]) => void;
}
