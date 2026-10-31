import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

import {
  ManifestDigestError,
  ManifestTracker,
  canonicalizeJson,
  changedDomains,
  isManifestRevision,
  manifestCanonicalString,
  manifestDigest,
  sortSetArray,
  withRevision,
} from '../src/manifest.js';
import type { McplManifest } from '../src/capabilities.js';
import { method } from '../src/methods.js';

// `test/vectors/manifest-digest-vectors.json` is a verbatim copy of
// anima-research/mcpl `conformance/manifest-digest-vectors.json` at commit
// 2cdc7fb, the interop artifact shared with Anarchid/mcpl-core. Do not edit it
// here: per anima-research/mcpl `conformance/CONSUMING.md` §8, a vector
// believed wrong is an issue against anima-research/mcpl, never a local fix.
// Set MCPL_DIGEST_VECTORS to the path of a newer copy of that file to run it.

interface DigestVector {
  name: string;
  description?: string;
  input: unknown;
  canonicalJson?: string;
  sha256Hex?: string;
  digest?: string;
  expectError?: string;
  sameDigestAs?: string;
  differentDigestFrom?: string;
}

interface SortVector {
  name: string;
  input: string[];
  sorted: string[];
}

interface VectorFile {
  vectors: DigestVector[];
  sortVectors?: SortVector[];
}

const here = dirname(fileURLToPath(import.meta.url));
const vectorPath = process.env.MCPL_DIGEST_VECTORS
  ? resolve(process.env.MCPL_DIGEST_VECTORS)
  : resolve(here, 'vectors/manifest-digest-vectors.json');

const vectorFile = JSON.parse(readFileSync(vectorPath, 'utf8')) as VectorFile;
const vectors = vectorFile.vectors ?? [];
const sortVectors = vectorFile.sortVectors ?? [];
const byName = new Map(vectors.map((v) => [v.name, v]));

test('frozen vector file is present and carries both positive and negative vectors', () => {
  assert.ok(vectors.length > 0, `no digest vectors loaded from ${vectorPath}`);
  assert.ok(vectors.some((v) => v.digest), 'expected positive vectors');
  assert.ok(vectors.some((v) => v.expectError), 'expected negative vectors');
  assert.ok(sortVectors.length > 0, 'expected set-comparator vectors');
});

for (const vector of vectors) {
  test(`vector: ${vector.name}`, () => {
    if (vector.expectError) {
      let thrown: unknown;
      try {
        manifestDigest(vector.input as Record<string, unknown>);
      } catch (e) {
        thrown = e;
      }
      assert.ok(thrown instanceof ManifestDigestError, 'expected a ManifestDigestError, got none');
      assert.equal((thrown as ManifestDigestError).code, vector.expectError);
      return;
    }

    assert.equal(manifestCanonicalString(vector.input as Record<string, unknown>), vector.canonicalJson);
    assert.equal(
      createHash('sha256').update(Buffer.from(vector.canonicalJson!, 'utf8')).digest('hex'),
      vector.sha256Hex,
    );
    assert.equal(manifestDigest(vector.input as Record<string, unknown>), vector.digest);
    assert.ok(isManifestRevision(vector.digest), 'revision must match App. B.3 pattern');

    if (vector.sameDigestAs) {
      const other = byName.get(vector.sameDigestAs);
      assert.ok(other, `unknown sameDigestAs target: ${vector.sameDigestAs}`);
      assert.equal(
        manifestDigest(vector.input as Record<string, unknown>),
        manifestDigest(other!.input as Record<string, unknown>),
      );
    }
    if (vector.differentDigestFrom) {
      const other = byName.get(vector.differentDigestFrom);
      assert.ok(other, `unknown differentDigestFrom target: ${vector.differentDigestFrom}`);
      assert.notEqual(
        manifestDigest(vector.input as Record<string, unknown>),
        manifestDigest(other!.input as Record<string, unknown>),
      );
    }
  });
}

for (const vector of sortVectors) {
  test(`set comparator: ${vector.name}`, () => {
    assert.deepEqual(sortSetArray(vector.input), vector.sorted);
  });
}

test('canonicalizeJson sorts object members and preserves array order', () => {
  assert.equal(canonicalizeJson({ b: 1, a: 2 }), '{"a":2,"b":1}');
  assert.equal(canonicalizeJson([3, 1, 2]), '[3,1,2]');
  assert.equal(canonicalizeJson({ a: undefined, b: null }), '{"b":null}');
});

test('sortSetArray dedupes and sorts by UTF-8 byte order', () => {
  assert.deepEqual(sortSetArray(['b', 'a', 'b']), ['a', 'b']);
  // U+FFFD (3 UTF-8 bytes, EF BF BD) vs U+10000 (4 bytes, F0 90 80 80):
  // UTF-8 byte order puts U+FFFD first; UTF-16 code-unit order does not.
  assert.deepEqual(sortSetArray(['\u{10000}', '�']), ['�', '\u{10000}']);
});

test('a host mirror advertising featureSets: true still digests', () => {
  // §5.2: the host mirrors the server shape and may set `featureSets: true`.
  // That is not an object of declarations, and must not be walked as one.
  const mirror = { version: '0.5', channels: { publish: true }, featureSets: true } as unknown as McplManifest;
  assert.ok(isManifestRevision(manifestDigest(mirror)));
});

test('descriptions are free text, not identifiers', () => {
  const manifest = {
    version: '0.5',
    featureSets: { 'demo.x': { description: 'Café — «naïve» 🙂', uses: ['pushEvents'] } },
  } as unknown as McplManifest;
  assert.ok(isManifestRevision(manifestDigest(manifest)));
});

test('a charset violation is a different failure from §6.4 invalid_uses', () => {
  const unrecognisedButLegalChars = {
    version: '0.5',
    featureSets: { 'demo.x': { description: 'Not in the §6.2 enum', uses: ['channels.observe'] } },
  } as unknown as McplManifest;
  assert.ok(isManifestRevision(manifestDigest(unrecognisedButLegalChars)));

  const badChars = {
    version: '0.5',
    featureSets: { 'demo.x': { description: 'Trailing space', uses: ['channels.publish '] } },
  } as unknown as McplManifest;
  assert.throws(() => manifestDigest(badChars), ManifestDigestError);
});

test('§17.2 totality: a set-declared array with any non-string member is hashed VERBATIM, never refused', () => {
  const manifest = {
    version: '0.5',
    featureSets: { 'demo.messaging': { description: 'd', uses: ['tools', 1, 'pushEvents', 'tools'] } },
  } as unknown as McplManifest;
  assert.equal(
    manifestCanonicalString(manifest),
    '{"featureSets":{"demo.messaging":{"description":"d","uses":["tools",1,"pushEvents","tools"]}},"version":"0.5"}',
  );
  assert.equal(manifestDigest(manifest), 'sha256:WhsR8JeGw0kWcnAxvkVtb_nXqJwO2LoUHqDupqEKFxU');

  const single = {
    version: '0.5',
    featureSets: { f: { description: 'd', uses: [1] } },
  } as unknown as McplManifest;
  assert.equal(manifestDigest(single), 'sha256:BELFX8LnKutmE6-hXj51-QtC9by4BgmoRgF2eRNm7qE');

  const mixedBad = {
    version: '0.5',
    featureSets: { f: { description: 'd', uses: ['bad path ', 1] } },
  } as unknown as McplManifest;
  assert.ok(isManifestRevision(manifestDigest(mixedBad)));

  const allStringBad = {
    version: '0.5',
    featureSets: { f: { description: 'd', uses: ['bad path '] } },
  } as unknown as McplManifest;
  assert.throws(() => manifestDigest(allStringBad), ManifestDigestError);
});

test('a non-object manifest is manifest_not_object, not a digest', () => {
  let thrown: unknown;
  try {
    manifestDigest([] as unknown as McplManifest);
  } catch (e) {
    thrown = e;
  }
  assert.ok(thrown instanceof ManifestDigestError);
  assert.equal((thrown as ManifestDigestError).code, 'manifest_not_object');
});

test('digest excludes revision so it never covers itself', () => {
  const base = { version: '0.5', pushEvents: true } as unknown as McplManifest;
  const stamped = withRevision(base);
  assert.equal(stamped.revision, manifestDigest(base));
  assert.equal(manifestDigest(stamped), manifestDigest(base));
});

test('digest is stable across member insertion order', () => {
  const a = { version: '0.5', pushEvents: true, modelInfo: true } as unknown as McplManifest;
  const b = { modelInfo: true, version: '0.5', pushEvents: true } as unknown as McplManifest;
  assert.equal(manifestDigest(a), manifestDigest(b));
});

const baseManifest = (): McplManifest =>
  ({
    version: '0.5',
    pushEvents: true,
    channels: { register: true, publish: true },
    featureSets: {
      chat: { description: 'Chat', uses: ['channels.publish'], tagOntology: { coreTags: ['chat:dm'] } },
    },
  }) as unknown as McplManifest;

test('changedDomains: capabilities is every member but version/revision/featureSets', () => {
  const next = baseManifest() as Record<string, unknown>;
  next.modelInfo = true;
  assert.deepEqual(changedDomains(baseManifest(), next), ['capabilities']);
});

test('changedDomains: featureSets excludes tagOntology within it', () => {
  const next = baseManifest() as Record<string, unknown>;
  (next.featureSets as Record<string, Record<string, unknown>>).chat.description = 'Chatting';
  assert.deepEqual(changedDomains(baseManifest(), next), ['featureSets']);
});

test('changedDomains: a tagOntology edit is its own domain', () => {
  const next = baseManifest() as Record<string, unknown>;
  (next.featureSets as Record<string, Record<string, unknown>>).chat.tagOntology = {
    coreTags: ['chat:dm', 'chat:mention'],
  };
  assert.deepEqual(changedDomains(baseManifest(), next), ['tagOntology']);
});

test('changedDomains: an absent member differs from an empty one — appearing IS a change (§17.3)', () => {
  const bare = { version: '0.5' } as unknown as McplManifest;
  const empty = { version: '0.5', featureSets: {} } as unknown as McplManifest;
  assert.deepEqual(changedDomains(bare, empty).sort(), ['featureSets', 'tagOntology']);
  assert.deepEqual(changedDomains(empty, bare).sort(), ['featureSets', 'tagOntology']);
  assert.notEqual(manifestDigest(bare), manifestDigest(empty));
  assert.deepEqual(changedDomains(empty, { version: '0.5', featureSets: {} } as unknown as McplManifest), []);
});

test('changedDomains: absent vs EXPLICIT NULL featureSets is a change — null is not the absence sentinel (§17.3)', () => {
  const bare = { version: '0.5' } as unknown as McplManifest;
  const explicitNull = { version: '0.5', featureSets: null } as unknown as McplManifest;
  assert.deepEqual(changedDomains(bare, explicitNull).sort(), ['featureSets', 'tagOntology']);
  assert.deepEqual(changedDomains(explicitNull, bare).sort(), ['featureSets', 'tagOntology']);
  assert.notEqual(manifestDigest(bare), manifestDigest(explicitNull));
  assert.deepEqual(changedDomains(explicitNull, { version: '0.5', featureSets: null } as unknown as McplManifest), []);
  const nullToEmpty = changedDomains(explicitNull, { version: '0.5', featureSets: {} } as unknown as McplManifest);
  assert.ok(nullToEmpty.includes('featureSets') && !nullToEmpty.includes('capabilities'), 'featureSets, and not capabilities');
});

test('changedDomains: version alone is not a domain', () => {
  const next = baseManifest() as Record<string, unknown>;
  next.version = '0.6';
  assert.deepEqual(changedDomains(baseManifest(), next), []);
});

test('changedDomains: reordering a set-semantic uses array is not a change', () => {
  const next = baseManifest() as Record<string, unknown>;
  (next.featureSets as Record<string, Record<string, unknown>>).chat.uses = ['channels.publish', 'channels.publish'];
  assert.deepEqual(changedDomains(baseManifest(), next), []);
});

class RecordingNotifier {
  sent: Array<{ method: string; params: unknown }> = [];
  sendNotification(m: string, params?: unknown): void {
    this.sent.push({ method: m, params });
  }
}

test('tracker: revision is the content digest of the initial manifest', () => {
  const tracker = new ManifestTracker(baseManifest());
  assert.equal(tracker.revision, manifestDigest(baseManifest()));
  assert.equal(tracker.snapshot().revision, tracker.revision);
});

test('tracker: mcpl/manifest answers with the complete current manifest, never a delta', () => {
  const tracker = new ManifestTracker(baseManifest());
  const result = tracker.handleManifestRequest() as Record<string, unknown>;
  assert.equal(result.version, '0.5');
  assert.deepEqual(result.channels, { register: true, publish: true });
  assert.ok(result.featureSets);
  assert.equal(result.revision, tracker.revision);
  assert.equal(result.capabilities, undefined);
});

test('tracker: attach seeds from the handshake, so no redundant announcement follows initialize', () => {
  const tracker = new ManifestTracker(baseManifest());
  const conn = new RecordingNotifier();
  tracker.attach(conn, { announcedRevision: tracker.revision });
  assert.deepEqual(conn.sent, []);

  const change = tracker.setManifest(baseManifest());
  assert.equal(change.changed, false);
  assert.deepEqual(conn.sent, []);
});

test('tracker: setManifest announces revision plus changed domains, with no payload', () => {
  const tracker = new ManifestTracker(baseManifest());
  const conn = new RecordingNotifier();
  tracker.attach(conn);

  const next = baseManifest() as Record<string, unknown>;
  next.modelInfo = true;
  const change = tracker.setManifest(next as McplManifest);

  assert.equal(change.changed, true);
  assert.deepEqual(change.domains, ['capabilities']);
  assert.equal(change.announcedTo, 1);
  assert.equal(conn.sent.length, 1);
  assert.equal(conn.sent[0]!.method, method.MCPL_MANIFEST_CHANGED);
  assert.deepEqual(conn.sent[0]!.params, { revision: tracker.revision, domains: ['capabilities'] });
  assert.deepEqual(Object.keys(conn.sent[0]!.params as object).sort(), ['domains', 'revision']);
});

test('tracker: a transaction coalesces many edits into one announcement', () => {
  const tracker = new ManifestTracker(baseManifest());
  const conn = new RecordingNotifier();
  tracker.attach(conn);

  const change = tracker.transaction((draft) => {
    const d = draft as unknown as Record<string, unknown>;
    d.modelInfo = true;
    d.inferenceLifecycle = true;
    (d.channels as Record<string, unknown>).incoming = true;
    (d.featureSets as Record<string, Record<string, unknown>>).chat.description = 'Chatting';
    (d.featureSets as Record<string, Record<string, unknown>>).chat.tagOntology = { coreTags: ['chat:mention'] };
    (d.featureSets as Record<string, Record<string, unknown>>).extra = {
      description: 'Extra',
      uses: ['pushEvents'],
    };
  });

  assert.equal(change.changed, true);
  assert.deepEqual(change.domains, ['capabilities', 'featureSets', 'tagOntology']);
  assert.equal(conn.sent.length, 1);
  assert.equal(tracker.revision, change.revision);
});

test('tracker: a throwing transaction leaves the installed manifest untouched', () => {
  const tracker = new ManifestTracker(baseManifest());
  const conn = new RecordingNotifier();
  tracker.attach(conn);
  const before = tracker.revision;
  const installed = tracker.snapshot();
  let threw = false;

  try {
    tracker.transaction((draft) => {
      (draft as unknown as Record<string, unknown>).modelInfo = true;
      threw = true;
      throw new Error('boom');
    });
  } catch { }

  assert.equal(threw, true);
  assert.deepEqual(tracker.snapshot(), installed);

  assert.equal(tracker.revision, before);
  assert.deepEqual(conn.sent, []);
});

test('tracker: a version-only change moves the revision but names no domain, so nothing is announced', () => {
  const tracker = new ManifestTracker(baseManifest());
  const conn = new RecordingNotifier();
  tracker.attach(conn);

  const next = baseManifest() as Record<string, unknown>;
  next.version = '0.6';
  const change = tracker.setManifest(next as McplManifest);

  assert.equal(change.changed, true);
  assert.deepEqual(change.domains, []);
  assert.deepEqual(conn.sent, []);
});

test('tracker: per-connection domains reflect what that connection was last told', () => {
  const tracker = new ManifestTracker(baseManifest());
  const early = new RecordingNotifier();
  tracker.attach(early);

  const step1 = baseManifest() as Record<string, unknown>;
  step1.modelInfo = true;
  tracker.setManifest(step1 as McplManifest);
  assert.deepEqual((early.sent[0]!.params as { domains: string[] }).domains, ['capabilities']);

  const late = new RecordingNotifier();
  tracker.attach(late);

  const step2 = { ...step1 } as Record<string, unknown>;
  step2.featureSets = {
    chat: { description: 'Chatting', uses: ['channels.publish'], tagOntology: { coreTags: ['chat:dm'] } },
  };
  tracker.setManifest(step2 as McplManifest);

  assert.equal(early.sent.length, 2);
  assert.deepEqual((early.sent[1]!.params as { domains: string[] }).domains, ['featureSets']);
  assert.equal(late.sent.length, 1);
  assert.deepEqual((late.sent[0]!.params as { domains: string[] }).domains, ['featureSets']);
});

test('tracker: an unknown seeded revision names all three domains rather than under-informing', () => {
  const tracker = new ManifestTracker(baseManifest());
  const conn = new RecordingNotifier();
  tracker.attach(conn, { announcedRevision: 'sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' });

  const next = baseManifest() as Record<string, unknown>;
  next.modelInfo = true;
  tracker.setManifest(next as McplManifest);

  assert.deepEqual((conn.sent[0]!.params as { domains: string[] }).domains, [
    'capabilities',
    'featureSets',
    'tagOntology',
  ]);
});

test('tracker: detach stops announcements', () => {
  const tracker = new ManifestTracker(baseManifest());
  const conn = new RecordingNotifier();
  const detach = tracker.attach(conn);
  detach();

  const next = baseManifest() as Record<string, unknown>;
  next.modelInfo = true;
  const change = tracker.setManifest(next as McplManifest);
  assert.equal(change.announcedTo, 0);
  assert.deepEqual(conn.sent, []);
});

test('tracker: snapshot is a copy — mutating it cannot move the revision', () => {
  const tracker = new ManifestTracker(baseManifest());
  const before = tracker.revision;
  const snap = tracker.snapshot() as Record<string, unknown>;
  snap.modelInfo = true;
  assert.equal(tracker.revision, before);
  assert.equal((tracker.snapshot() as Record<string, unknown>).modelInfo, undefined);
});

test('suggested treatment matchers meet the identifier charset, under the deprecated name too', () => {
  const withRules = (field: 'suggestedTreatment' | 'defaultTreatment', tag: string) => ({
    version: '0.5',
    featureSets: { chat: { description: 'Chat', uses: ['pushEvents'], tagOntology: { [field]: [{ tagsAny: [tag], behavior: 'immediate' }] } } },
  } as unknown as McplManifest);
  assert.ok(isManifestRevision(manifestDigest(withRules('suggestedTreatment', 'chat:addressed'))));
  assert.ok(isManifestRevision(manifestDigest(withRules('defaultTreatment', 'chat:addressed'))));
  assert.throws(() => manifestDigest(withRules('suggestedTreatment', 'chat:addressed ')), ManifestDigestError);
  assert.throws(() => manifestDigest(withRules('defaultTreatment', 'chat:addressed ')), ManifestDigestError);
});
