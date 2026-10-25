import { createHash } from 'node:crypto';
import { canonicalizeJson } from '../manifest.js';

export type JsonSchema = Record<string, unknown>;

/**
 * Returns null when `value` satisfies `schema`'s type, enum, const, properties, required,
 * additionalProperties, items, minimum/maximum, exclusiveMinimum/exclusiveMaximum,
 * minItems/maxItems, minLength/maxLength, pattern, allOf, oneOf and anyOf, else a short
 * explanation; every other keyword is ignored, and so are boolean subschemas and an array
 * `items`. Fills `properties` defaults into `value` in place, except inside oneOf and anyOf
 * branches.
 */
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
  try { return canonicalizeJson(a) === canonicalizeJson(b); } catch { return Object.is(a, b); }
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
    const own = (k: string) => Object.prototype.hasOwnProperty.call(props, k);
    for (const [k, ps] of Object.entries(props)) {
      if (o[k] === undefined && ps && typeof ps === 'object' && ps.default !== undefined) o[k] = structuredClone(ps.default);
    }
    for (const r of (schema.required as string[] | undefined) ?? []) {
      if (o[r] === undefined) errors.push(`${where}: missing required property "${r}"`);
    }
    for (const [k, val] of Object.entries(o)) {
      const path = at ? `${at}.${k}` : k;
      if (own(k)) check(props[k]!, val, path, errors);
      else if (schema.additionalProperties === false) errors.push(`${where}: unexpected property "${k}"`);
      else if (schema.additionalProperties && typeof schema.additionalProperties === 'object') check(schema.additionalProperties as JsonSchema, val, path, errors);
    }
  }
  if (Array.isArray(schema.allOf)) for (const s of schema.allOf as JsonSchema[]) check(s, v, at, errors);
  const passing = (alts: JsonSchema[]) => alts.filter((s) => { const e: string[] = []; check(s, structuredClone(v), at, e); return e.length === 0; }).length;
  if (Array.isArray(schema.oneOf) && passing(schema.oneOf as JsonSchema[]) !== 1) errors.push(`${where}: must match exactly one of the allowed shapes`);
  if (Array.isArray(schema.anyOf) && passing(schema.anyOf as JsonSchema[]) === 0) errors.push(`${where}: must match one of the allowed shapes`);
}

export function digest(value: unknown): string {
  return 'sha256:' + createHash('sha256').update(canonicalizeJson(JSON.parse(JSON.stringify(value)))).digest('base64url');
}
