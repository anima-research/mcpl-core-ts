import { test } from 'node:test';
import assert from 'node:assert/strict';
import { expandTags, CHAT_TAG_IMPLIES, CHAT_TAGS } from '../src/tags.js';

test('expandTags: chat:mention implies chat:addressed', () => {
  assert.deepEqual(expandTags(['chat:mention']).sort(), ['chat:addressed', 'chat:mention']);
});

test('expandTags: additive, idempotent, passes through unknowns', () => {
  assert.deepEqual(expandTags(['robotics:collision']), ['robotics:collision']);
  const once = expandTags(['chat:dm']);
  assert.deepEqual(expandTags(once).sort(), once.sort());
});

test('expandTags: merges a custom (server ontology) implies map', () => {
  const merged = { ...CHAT_TAG_IMPLIES, 'discord:everyone': ['chat:broadcast'] };
  assert.ok(expandTags(['discord:everyone'], merged).includes('chat:broadcast'));
});

test('expandTags: cycles are safe', () => {
  const cyclic = { 'a:1': ['a:2'], 'a:2': ['a:1'] };
  assert.deepEqual(expandTags(['a:1'], cyclic).sort(), ['a:1', 'a:2']);
});

test('CHAT_TAGS constants are chat:-namespaced', () => {
  for (const v of Object.values(CHAT_TAGS)) assert.ok(v.startsWith('chat:'), v);
});
