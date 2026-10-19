// RFC-005 §11 conformance vectors — the parser/stub subset, executable.
// Vector numbers refer to RFC-005 revision 3 (mcpl repo, f684cae). Fetcher
// vectors (10-13, 19) and record vectors (20) belong to a host-treatment
// implementation; everything a pure parser/stub layer can decide is here.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  classifyBlock, buildStubText, stubForContext, isReferenceExpired,
  sanitizeLabel, REFERENCE_LIMITS, DIGEST_PATTERN,
} from '../src/references.js';

const REF = {
  type: 'resource',
  uri: 'https://example.com/files?path=%2Fchord.wav',
  mimeType: 'audio/wav',
  sizeBytes: 4233704,
  digest: 'sha256:' + 'A'.repeat(43),
  name: 'chord.wav',
  disposition: 'never',
};

function testimony(block: unknown) {
  const c = classifyBlock(block);
  assert.equal(c.kind, 'reference');
  return (c as Extract<ReturnType<typeof classifyBlock>, { kind: 'reference' }>).testimony;
}

// ── vector 1 + 9: `never` withholds payload AND uri, unconditionally ──

test('v1/v9: never-disposition stub carries no uri, even when asked', () => {
  const stub = buildStubText(testimony(REF), {
    refId: 'ref_1', provenance: 'from tool vst_render', includeUri: true,
  });
  assert.ok(!stub.includes('example.com'), 'uri leaked into never stub');
  assert.ok(stub.includes('chord.wav') && stub.includes('audio/wav') && stub.includes('ref_1'));
});

test('v9: signed/query-capability uri stays out of the stub by default under ref too', () => {
  const signed = { ...REF, disposition: 'ref', uri: 'https://cdn.example/x?sig=SECRETCAP' };
  const stub = buildStubText(testimony(signed), { refId: 'ref_2' });
  assert.ok(!stub.includes('SECRETCAP'));
  // and only explicit host opt-in includes it for ref/absent disposition
  const opted = buildStubText(testimony(signed), { refId: 'ref_2', includeUri: true });
  assert.ok(opted.includes('SECRETCAP'), 'host policy opt-in must work for ref');
});

// ── vector 2: inline data + bulk disposition = contradiction, fail closed ──

test('v2: inline data claiming disposition is a contradiction; data withheld', () => {
  const c = classifyBlock({ type: 'audio', data: 'AAAA', mimeType: 'audio/wav', disposition: 'never' });
  assert.deepEqual(c, { kind: 'inline', contradiction: true });
  const stub = stubForContext({ type: 'audio', data: 'AAAA', disposition: 'never' }, { refId: 'ref_3' });
  assert.ok(stub !== null && !stub.includes('AAAA'));
});

test('plain inline data block is untouched (no contradiction)', () => {
  assert.deepEqual(classifyBlock({ type: 'image', data: 'AAAA', mimeType: 'image/png' }),
    { kind: 'inline', contradiction: false });
  assert.equal(stubForContext({ type: 'image', data: 'AAAA', mimeType: 'image/png' }, { refId: 'x' }), null);
});

// ── vector 7: degradation — unknown types never propagate undefined ──

test('v7: unknown block type classifies invalid, stubs to bounded marker', () => {
  const c = classifyBlock({ type: 'holo', frames: 9000 });
  assert.equal(c.kind, 'invalid');
  const stub = stubForContext({ type: 'holo' }, { refId: 'x' });
  assert.equal(typeof stub, 'string');
});

// ── vector 15: invalid sizeBytes rejected as a field, block + disposition kept ──

test('v15: sizeBytes -1 / 3.5 / 2^53 rejected as fields; never survives', () => {
  for (const bad of [-1, 3.5, 2 ** 53, '1000', NaN]) {
    const t = testimony({ ...REF, sizeBytes: bad });
    assert.equal(t.sizeBytes, undefined, `sizeBytes=${String(bad)} accepted`);
    assert.ok(t.rejectedFields.includes('sizeBytes'));
    assert.equal(t.disposition, 'never', 'subtractive ceiling lost');
  }
});

test('invalid digest and disposition rejected as fields', () => {
  const t = testimony({ ...REF, digest: 'md5:abc', disposition: 'inline-ok' });
  assert.equal(t.digest, undefined);
  assert.equal(t.disposition, undefined);
  assert.deepEqual(t.rejectedFields.sort(), ['digest', 'disposition']);
});

test('uri is the one required property: missing/overlong rejects the block whole', () => {
  assert.equal(classifyBlock({ type: 'resource' }).kind, 'invalid');
  assert.equal(classifyBlock({ type: 'resource', uri: 'x'.repeat(REFERENCE_LIMITS.uri + 1) }).kind, 'invalid');
});

// ── vector 16: unparseable expiresAt fails closed ──

test('v16: unparseable expiresAt is expired, absent is not', () => {
  assert.equal(isReferenceExpired({ expiresAt: 'not-a-date', rejectedFields: [] }), true);
  assert.equal(isReferenceExpired({ rejectedFields: [] }), false);
  assert.equal(isReferenceExpired({ expiresAt: '2999-01-01T00:00:00Z', rejectedFields: [] }, Date.parse('2026-01-01')), false);
  assert.equal(isReferenceExpired({ expiresAt: '2020-01-01T00:00:00Z', rejectedFields: [] }, Date.parse('2026-01-01')), true);
});

test('v16: an expiresAt rejected as a field is expired too, never immortal', () => {
  for (const expiresAt of [1_900_000_000, null, [], {}, true]) {
    const t = testimony({ ...REF, expiresAt });
    assert.deepEqual(t.rejectedFields, ['expiresAt']);
    assert.equal(isReferenceExpired(t), true, `expiresAt=${JSON.stringify(expiresAt)} read as no horizon`);
  }
  // Another field's rejection leaves a valid expiry as it was.
  const live = testimony({ ...REF, sizeBytes: -1, expiresAt: '2999-01-01T00:00:00Z' });
  assert.deepEqual(live.rejectedFields, ['sizeBytes']);
  assert.equal(isReferenceExpired(live, Date.parse('2026-01-01')), false);
});

// ── vector 17: oversized metadata cannot inflate the stub ──

test('v17: 1MB name — stub bounded, wake-cost claim holds', () => {
  const t = testimony({ ...REF, name: 'x'.repeat(1_000_000), mimeType: 'y'.repeat(1_000_000) });
  assert.ok(t.truncatedFields.includes('name') && t.truncatedFields.includes('mimeType'));
  const stub = buildStubText(t, { refId: 'ref_17' });
  assert.ok(stub.length < 600, `stub is ${stub.length} chars`);
});

// ── vector 18: hostile display names ──

test('v18: control chars and bidi overrides stripped from labels', () => {
  const t = testimony({ ...REF, name: 'inno\u202Ecent\u0007\u2066.exe' });
  const stub = buildStubText(t, { refId: 'ref_18' });
  assert.ok(!/[\u202E\u0007\u2066]/.test(stub));
  assert.equal(sanitizeLabel('a\u0000b\u202Ac', 10), 'abc');
});

// ── format anchors ──

test('digest pattern matches RFC-003 encoding exactly', () => {
  assert.ok(DIGEST_PATTERN.test('sha256:' + 'a1-_'.repeat(10) + 'aaa'));
  assert.ok(!DIGEST_PATTERN.test('sha256:' + 'A'.repeat(42)));
  assert.ok(!DIGEST_PATTERN.test('sha256:' + 'A'.repeat(43) + '='));
  assert.ok(!DIGEST_PATTERN.test('sha512:' + 'A'.repeat(43)));
});

test('uri-form media blocks parse as references with the same fields', () => {
  const t = testimony({ type: 'audio', uri: 'https://x/y.wav', sizeBytes: 10, disposition: 'ref' });
  assert.equal(t.sizeBytes, 10);
  assert.equal(t.disposition, 'ref');
});
