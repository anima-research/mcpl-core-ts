/**
 * A small JSON Schema validator for tool arguments, so the toolset server can refuse bad
 * arguments without a dependency. It covers the keywords tool schemas use in practice — type,
 * enum, const, properties, required, additionalProperties, items, min/max (items, length,
 * value), pattern, oneOf/anyOf/allOf — fills `default`s into objects, and ignores the rest.
 * A server that wants full JSON Schema passes its own validator (ToolsetServerSpec.validate).
 */
import { createHash } from 'node:crypto';

export type JsonSchema = Record<string, unknown>;

/** Returns null when `value` satisfies `schema`, else a short explanation. Fills defaults in place. */
export function validateJson(schema: JsonSchema, value: unknown): string | null {
  const errors: string[] = [];
  check(schema, value, '', errors);
  return errors.length ? errors.slice(0, 6).join('; ') : null;
}

function typeOf(v: unknown): string {
  if (v === null) return 'null';
  if (Array.isArray(v)) return 'array';
  if (typeof v === 'number') return Number.isInteger(v) ? 'integer' : 'number';
  return typeof v;
}

function typeMatches(want: string, v: unknown): boolean {
  const t = typeOf(v);
  return want === t || (want === 'number' && t === 'integer');
}

function equal(a: unknown, b: unknown): boolean {
  return canonicalJson(a) === canonicalJson(b);
}

function check(schema: JsonSchema, v: unknown, at: string, errors: string[]): void {
  if (!schema || typeof schema !== 'object') return;
  const where = at || '(args)';

  if (schema.type !== undefined) {
    const types = Array.isArray(schema.type) ? (schema.type as string[]) : [schema.type as string];
    if (!types.some((t) => typeMatches(t, v))) { errors.push(`${where}: must be ${types.join(' or ')}`); return; }
  }
  if (Array.isArray(schema.enum) && !schema.enum.some((e) => equal(e, v))) {
    errors.push(`${where}: must be one of ${(schema.enum as unknown[]).map((e) => JSON.stringify(e)).join(', ')}`);
    return;
  }
  if (schema.const !== undefined && !equal(schema.const, v)) { errors.push(`${where}: must be ${JSON.stringify(schema.const)}`); return; }

  if (typeof v === 'number') {
    if (typeof schema.minimum === 'number' && v < schema.minimum) errors.push(`${where}: must be ≥ ${schema.minimum}`);
    if (typeof schema.maximum === 'number' && v > schema.maximum) errors.push(`${where}: must be ≤ ${schema.maximum}`);
    if (typeof schema.exclusiveMinimum === 'number' && v <= schema.exclusiveMinimum) errors.push(`${where}: must be > ${schema.exclusiveMinimum}`);
    if (typeof schema.exclusiveMaximum === 'number' && v >= schema.exclusiveMaximum) errors.push(`${where}: must be < ${schema.exclusiveMaximum}`);
  }
  if (typeof v === 'string') {
    const len = [...v].length;
    if (typeof schema.minLength === 'number' && len < schema.minLength) errors.push(`${where}: must be at least ${schema.minLength} characters`);
    if (typeof schema.maxLength === 'number' && len > schema.maxLength) errors.push(`${where}: must be at most ${schema.maxLength} characters`);
    if (typeof schema.pattern === 'string') {
      let re: RegExp | null = null;
      try { re = new RegExp(schema.pattern, 'u'); } catch { re = null; }
      if (re && !re.test(v)) errors.push(`${where}: must match ${schema.pattern}`);
    }
  }
  if (Array.isArray(v)) {
    if (typeof schema.minItems === 'number' && v.length < schema.minItems) errors.push(`${where}: must have at least ${schema.minItems} items`);
    if (typeof schema.maxItems === 'number' && v.length > schema.maxItems) errors.push(`${where}: must have at most ${schema.maxItems} items`);
    if (schema.items && typeof schema.items === 'object' && !Array.isArray(schema.items)) {
      v.forEach((item, i) => check(schema.items as JsonSchema, item, `${at}[${i}]`, errors));
    }
  }
  if (v && typeof v === 'object' && !Array.isArray(v)) {
    const o = v as Record<string, unknown>;
    const props = (schema.properties ?? {}) as Record<string, JsonSchema>;
    for (const [k, ps] of Object.entries(props)) {
      if (o[k] === undefined && ps && typeof ps === 'object' && ps.default !== undefined) o[k] = structuredClone(ps.default);
    }
    for (const r of (schema.required as string[] | undefined) ?? []) {
      if (o[r] === undefined) errors.push(`${where}: missing required property "${r}"`);
    }
    for (const [k, val] of Object.entries(o)) {
      const path = at ? `${at}.${k}` : k;
      if (props[k]) check(props[k]!, val, path, errors);
      else if (schema.additionalProperties === false) errors.push(`${where}: unexpected property "${k}"`);
      else if (schema.additionalProperties && typeof schema.additionalProperties === 'object') check(schema.additionalProperties as JsonSchema, val, path, errors);
    }
  }
  if (Array.isArray(schema.allOf)) for (const s of schema.allOf as JsonSchema[]) check(s, v, at, errors);
  const alts = (schema.oneOf ?? schema.anyOf) as JsonSchema[] | undefined;
  if (Array.isArray(alts)) {
    const passing = alts.filter((s) => { const e: string[] = []; check(s, structuredClone(v), at, e); return e.length === 0; }).length;
    if (schema.oneOf ? passing !== 1 : passing === 0) errors.push(`${where}: must match ${schema.oneOf ? 'exactly one' : 'one'} of the allowed shapes`);
  }
}

/** Deterministic JSON: object keys sorted recursively, undefined members dropped. */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value) ?? 'null';
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  const o = value as Record<string, unknown>;
  return `{${Object.keys(o).sort().filter((k) => o[k] !== undefined).map((k) => `${JSON.stringify(k)}:${canonicalJson(o[k])}`).join(',')}}`;
}

/** `sha256:<base64url>` over the canonical JSON. */
export function digest(value: unknown): string {
  return 'sha256:' + createHash('sha256').update(canonicalJson(value)).digest('base64url');
}
