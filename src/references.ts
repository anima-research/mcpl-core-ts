/**
 * RFC-005 bulk content references — the pure host-side half.
 *
 * This module implements the parser/classifier and stub builder a conformant
 * host needs: the one invalid-field rule (§8), fail-closed expiry (§7.4),
 * display-label sanitization (§5, §7.3), and stub construction whose output
 * size is independent of every server-supplied field length (§5). Everything
 * here is a pure function over a received content block; fetching (§6, §7),
 * the host-private reference record, and storage naming are host code — this
 * module only makes their inputs trustworthy and their stubs uniform.
 *
 * Terminology matches the RFC: every parsed field is *testimony* (a server
 * claim), never a fact. `disposition` is testimony with one binding value:
 * `"never"` obliges the host to withhold both payload and URI from model
 * context — buildStubText enforces the URI half here so no caller can
 * accidentally weaken it. A disposition rejected as a field is absent, as
 * §8 says of a rejected field; a host that withholds more for one can read
 * `rejectedFields`.
 */
import type { ContentBlock, ReferenceDisposition } from './types.js';

/** Emitter conformance maxima (RFC-005 §8), in code points, as JSON Schema
 *  counts. A value past its maximum is rejected as a field, and a uri past
 *  its maximum rejects the block. The outer fence, not the guarantee — the
 *  guarantee is host-side truncation in buildStubText. */
export const REFERENCE_LIMITS = {
  uri: 4096,
  mimeType: 255,
  name: 255,
  expiresAt: 64,
} as const;

/** `sha256:` + base64url, RFC-003 encoding (RFC-005 §3). */
export const DIGEST_PATTERN = /^sha256:[A-Za-z0-9_-]{43}$/;

const MAX_SAFE = 9007199254740991; // 2^53 - 1

/** Parsed server testimony about a referenced payload. Claims, not facts. */
export interface ReferenceTestimony {
  uri: string;
  mimeType?: string;
  sizeBytes?: number;
  digest?: string;
  /** Read through isReferenceExpired, which counts a rejected value, absent
   *  here, as expired. */
  expiresAt?: string;
  name?: string;
  disposition?: ReferenceDisposition;
  /** Fields dropped under the one invalid-field rule (§8): wrong type,
   *  format or length. Their absence is the treatment, with one exception:
   *  a rejected `expiresAt` reads as expired, not as no horizon
   *  (isReferenceExpired). A rejected `disposition` is absent, as §8 says;
   *  a host that treats one as `"never"` reads it here. */
  rejectedFields: string[];
}

export type BlockClassification =
  /** A text block — context-sized by construction, no disposition (§8). */
  | { kind: 'text' }
  /** An inline data block. `contradiction` is vector 2: it claimed bulk
   *  disposition (or carried both data and uri) — emitter nonconformant, and
   *  the host fails closed by withholding the inline data from context. */
  | { kind: 'inline'; contradiction: boolean }
  /** A uri-form block: a reference, with parsed testimony. */
  | { kind: 'reference'; testimony: ReferenceTestimony }
  /** Not a usable block: missing/overlong `uri` on a reference shape
   *  (§8: the one required property — the block is rejected whole),
   *  or not a recognizable content block at all. */
  | { kind: 'invalid'; reason: string };

function isRecord(x: unknown): x is Record<string, unknown> {
  return typeof x === 'object' && x !== null && !Array.isArray(x);
}

/** Whether `s` is longer than a JSON Schema maxLength, which counts code
 *  points, not UTF-16 units. Stops one code point past the bound, however
 *  long `s` is. */
function exceedsMaxLength(s: string, max: number): boolean {
  if (s.length <= max) return false;
  let n = 0;
  for (const _ of s) if (++n > max) return true;
  return false;
}

function takeString(
  raw: unknown, field: keyof typeof REFERENCE_LIMITS & string,
  out: { rejected: string[] },
): string | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw !== 'string' || exceedsMaxLength(raw, REFERENCE_LIMITS[field])) {
    out.rejected.push(field);
    return undefined;
  }
  return raw;
}

/**
 * Classify a received content block per RFC-005 §8.
 *
 * Accepts `unknown` deliberately: this runs on wire data, and the block may
 * predate or postdate every type this package knows. Unknown `type` values
 * are `invalid` (the caller decides between dropping and JSON-displaying
 * them — but never `undefined`-propagation).
 */
export function classifyBlock(block: unknown): BlockClassification {
  if (!isRecord(block) || typeof block.type !== 'string') {
    return { kind: 'invalid', reason: 'not a content block' };
  }
  switch (block.type) {
    case 'text':
      return typeof block.text === 'string'
        ? { kind: 'text' }
        : { kind: 'invalid', reason: 'text block without string text' };
    case 'image':
    case 'audio': {
      const hasData = typeof block.data === 'string';
      const hasUri = typeof block.uri === 'string';
      if (hasData) {
        // Inline form. Claiming a disposition here — or smuggling a uri
        // beside the data — is the vector-2 contradiction: fail closed.
        const contradiction = block.disposition !== undefined || hasUri;
        return { kind: 'inline', contradiction };
      }
      if (hasUri) return parseReferenceFields(block);
      return { kind: 'invalid', reason: `${block.type} block with neither data nor uri` };
    }
    case 'resource':
      return parseReferenceFields(block);
    default:
      return { kind: 'invalid', reason: `unknown block type: ${block.type}` };
  }
}

function parseReferenceFields(block: Record<string, unknown>): BlockClassification {
  // §8: uri is the one required property; a block whose uri violates the
  // schema is rejected whole.
  if (typeof block.uri !== 'string' || block.uri.length === 0) {
    return { kind: 'invalid', reason: 'reference without string uri' };
  }
  if (exceedsMaxLength(block.uri, REFERENCE_LIMITS.uri)) {
    return { kind: 'invalid', reason: 'uri exceeds schema maximum' };
  }
  const out = { rejected: [] as string[] };
  const t: ReferenceTestimony = {
    uri: block.uri,
    rejectedFields: out.rejected,
  };
  t.mimeType = takeString(block.mimeType, 'mimeType', out);
  t.name = takeString(block.name, 'name', out);
  t.expiresAt = takeString(block.expiresAt, 'expiresAt', out);

  if (block.sizeBytes !== undefined) {
    const n = block.sizeBytes;
    if (typeof n === 'number' && Number.isInteger(n) && n >= 0 && n <= MAX_SAFE) {
      t.sizeBytes = n;
    } else out.rejected.push('sizeBytes');
  }
  if (block.digest !== undefined) {
    if (typeof block.digest === 'string' && DIGEST_PATTERN.test(block.digest)) {
      t.digest = block.digest;
    } else out.rejected.push('digest');
  }
  if (block.disposition !== undefined) {
    if (block.disposition === 'never' || block.disposition === 'ref') {
      t.disposition = block.disposition;
    } else out.rejected.push('disposition');
  }
  return { kind: 'reference', testimony: t };
}

/**
 * Fail-closed expiry (§7.4): absent means no horizon claimed (not expired);
 * unparseable means expired, never immortal, and so does a value rejected as
 * a field, since field invalidity never widens exposure (§8).
 */
export function isReferenceExpired(
  t: Pick<ReferenceTestimony, 'expiresAt' | 'rejectedFields'>, nowMs = Date.now(),
): boolean {
  if (t.rejectedFields.includes('expiresAt')) return true;
  if (t.expiresAt === undefined) return false;
  const exp = Date.parse(t.expiresAt);
  if (Number.isNaN(exp)) return true;
  return exp <= nowMs;
}

/** Strip control characters, Unicode bidi controls and unpaired surrogates
 *  from a display label, and bound it (§5, §7.3). Never produces a path —
 *  labels are labels; storage names are host-generated elsewhere. */
export function sanitizeLabel(s: string, maxChars: number): string {
  // Cc: the C0 controls, DEL and the C1 controls. Bidi_Control: every bidi
  // mark, embedding, override and isolate, U+061C ARABIC LETTER MARK included.
  // Cs, under the u flag: a surrogate with no partner, which isn't text.
  const cleaned = s.replace(/[\p{Cc}\p{Bidi_Control}\p{Cs}]/gu, '');
  if (!Number.isFinite(maxChars) || maxChars < 1) return '';
  if (cleaned.length <= maxChars) return cleaned;
  // The bound counts UTF-16 units, as RFC-005's display profile does, so a cut can
  // fall inside a surrogate pair. One that would end on the pair's first half drops it.
  const cut = cleaned.slice(0, maxChars - 1);
  return (/[\uD800-\uDBFF]$/.test(cut) ? cut.slice(0, -1) : cut) + '…';
}

export function formatSizeBytes(n: number): string {
  if (n < 1024) return `${n}B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)}KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)}MB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(1)}GB`;
}

export interface StubOptions {
  /** Host-generated opaque reference id (§5): stable per session, never
   *  reused, host-local naming — expressly not a server-resolvable handle. */
  refId: string;
  /** One line of provenance, e.g. `from tool vst_render`. Host-authored, but
   *  bounded anyway in case server strings were interpolated into it. */
  provenance?: string;
  /** Include the raw URI. Honored only when the disposition isn't 'never' —
   *  under `never` the URI is withheld unconditionally (§5), and this option
   *  cannot override that. Default false (the safe default everywhere, §5). */
  includeUri?: boolean;
  /** Per-field display bound (§5), in UTF-16 units, as RFC-005's display
   *  profile counts them. Default 120. An included uri is bounded at no less
   *  than 256. Below 1 a label shows nothing, and NaN or Infinity shows no
   *  label and no uri. */
  maxFieldChars?: number;
}

/**
 * Build the model-visible stub line for a reference (§4.1 of rev 1, §5 of the
 * current draft). Output length is bounded by host-chosen constants and is
 * independent of every server-supplied field length (vectors 8, 17).
 */
export function buildStubText(t: ReferenceTestimony, opts: StubOptions): string {
  const max = opts.maxFieldChars ?? 120;
  const parts: string[] = [];
  if (t.name) parts.push(sanitizeLabel(t.name, max));
  const meta: string[] = [];
  if (t.mimeType) meta.push(sanitizeLabel(t.mimeType, max));
  if (t.sizeBytes !== undefined) meta.push(`~${formatSizeBytes(t.sizeBytes)} claimed`);
  parts.push(meta.filter(Boolean).join(', '));
  if (opts.provenance) parts.push(sanitizeLabel(opts.provenance, max));
  if (opts.includeUri && t.disposition !== 'never') {
    parts.push(sanitizeLabel(t.uri, Math.max(max, 256)));
  }
  return `[${sanitizeLabel(opts.refId, 64)}] ${parts.filter(Boolean).join(' — ') || 'referenced content'}`;
}

/**
 * Convenience: full host treatment for one block of unknown provenance.
 * Returns what may enter model context in place of the block:
 *  - text/plain-inline blocks: `null` (caller keeps the block as-is);
 *  - references: the stub line;
 *  - contradiction inline blocks (vector 2): a stub-like marker, data withheld;
 *  - invalid blocks: a bounded marker, never `undefined`.
 */
export function stubForContext(
  block: ContentBlock | unknown,
  opts: StubOptions,
): string | null {
  const c = classifyBlock(block);
  switch (c.kind) {
    case 'text': return null;
    case 'inline':
      return c.contradiction
        ? `[${sanitizeLabel(opts.refId, 64)}] inline content withheld (nonconformant bulk disposition on inline data)`
        : null;
    case 'reference': return buildStubText(c.testimony, opts);
    case 'invalid': return `[unrecognized content block: ${sanitizeLabel(c.reason, 120)}]`;
  }
}
