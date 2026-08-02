import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  CAPABILITY_PATHS,
  advertisedCapabilities,
  advertisedCapabilitiesFromInitialize,
  capabilityGranted,
  capabilityPatternMatches,
  conflictingCapabilityEntries,
  deriveFeatureSets,
  hasInferenceRequest,
  hasInferenceStreaming,
  isCapabilityPath,
  validateUses,
} from '../src/capabilities.js';
import type { McplCapabilities } from '../src/capabilities.js';
import type { FeatureSetDeclaration } from '../src/methods.js';

const sorted = (s: Set<string>): string[] => [...s].sort();

// ── The closed vocabulary (SPEC §6.2 / App. B.2) ──

test('CAPABILITY_PATHS is exactly the §6.2 vocabulary', () => {
  assert.deepEqual([...CAPABILITY_PATHS].sort(), [
    'channels.acknowledge',
    'channels.incoming',
    'channels.lifecycle',
    'channels.publish',
    'channels.register',
    'channels.streaming',
    'channels.typing',
    'contextHooks.beforeInference.inject.afterUser',
    'contextHooks.beforeInference.inject.beforeUser',
    'contextHooks.beforeInference.inject.system',
    'contextHooks.beforeInference.observe',
    'inferenceLifecycle',
    'inferenceRequest',
    'inferenceRequest.streaming',
    'modelInfo',
    'pushEvents',
    'tools',
  ]);
});

test('isCapabilityPath rejects near-misses and removed values', () => {
  assert.ok(isCapabilityPath('channels.streaming'));
  // Interior advertisement nodes are not themselves capability paths.
  assert.equal(isCapabilityPath('channels'), false);
  assert.equal(isCapabilityPath('contextHooks.beforeInference'), false);
  assert.equal(isCapabilityPath('contextHooks.beforeInference.inject'), false);
  // Struck in 0.5.0.
  assert.equal(isCapabilityPath('channels.observe'), false);
  assert.equal(isCapabilityPath('contextHooks.afterInference'), false);
});

// ── Recursive advertisement (SPEC §5.1) ──

test('channels is an object, so channels.streaming is declarable', () => {
  const caps: McplCapabilities = { version: '0.5', channels: { publish: true, streaming: true } };
  assert.deepEqual(sorted(advertisedCapabilities(caps)), ['channels.publish', 'channels.streaming']);
});

test('boolean true is shorthand for every leaf beneath the node', () => {
  assert.deepEqual(sorted(advertisedCapabilities({ version: '0.5', channels: true })), [
    'channels.acknowledge',
    'channels.incoming',
    'channels.lifecycle',
    'channels.publish',
    'channels.register',
    'channels.streaming',
    'channels.typing',
  ]);

  assert.deepEqual(sorted(advertisedCapabilities({ version: '0.5', contextHooks: { beforeInference: true } })), [
    'contextHooks.beforeInference.inject.afterUser',
    'contextHooks.beforeInference.inject.beforeUser',
    'contextHooks.beforeInference.inject.system',
    'contextHooks.beforeInference.observe',
  ]);
});

test('the walk descends three levels: inject-only, observe denied', () => {
  const caps: McplCapabilities = {
    version: '0.5',
    contextHooks: {
      beforeInference: { observe: false, inject: { system: false, beforeUser: true, afterUser: true } },
    },
  };
  assert.deepEqual(sorted(advertisedCapabilities(caps)), [
    'contextHooks.beforeInference.inject.afterUser',
    'contextHooks.beforeInference.inject.beforeUser',
  ]);
});

test('false and absence both advertise nothing — absence is denial, never default-allow', () => {
  assert.deepEqual(sorted(advertisedCapabilities({ version: '0.5' })), []);
  assert.deepEqual(sorted(advertisedCapabilities({ version: '0.5', pushEvents: false, channels: false })), []);
  assert.deepEqual(sorted(advertisedCapabilities(undefined)), []);
});

test('unrecognised members cannot mint a capability', () => {
  const caps = {
    version: '0.5',
    channels: { publish: true, observe: true, telepathy: true },
    streamObserver: true,
    scopedAccess: true,
    rollback: true,
  } as unknown as McplCapabilities;
  assert.deepEqual(sorted(advertisedCapabilities(caps)), ['channels.publish']);
});

test('an object at a path-bearing node advertises that node too', () => {
  const caps: McplCapabilities = { version: '0.5', inferenceRequest: { streaming: true } };
  assert.deepEqual(sorted(advertisedCapabilities(caps)), ['inferenceRequest', 'inferenceRequest.streaming']);
  assert.ok(hasInferenceRequest(caps));
  assert.ok(hasInferenceStreaming(caps));

  const noStream: McplCapabilities = { version: '0.5', inferenceRequest: { streaming: false } };
  assert.ok(hasInferenceRequest(noStream));
  assert.equal(hasInferenceStreaming(noStream), false);
});

test('featureSets is not a capability path and never becomes one', () => {
  const caps: McplCapabilities = {
    version: '0.5',
    featureSets: { chat: { description: 'Chat', uses: ['pushEvents'] } },
  };
  assert.deepEqual(sorted(advertisedCapabilities(caps)), []);
});

test('tools is honoured from the MCP capability as well as the mcpl block', () => {
  assert.ok(advertisedCapabilitiesFromInitialize({ tools: {} }).has('tools'));
  assert.ok(advertisedCapabilitiesFromInitialize({ experimental: { mcpl: { version: '0.5', tools: true } } }).has('tools'));
  assert.equal(advertisedCapabilitiesFromInitialize({}).has('tools'), false);
});

// ── Grant matching (SPEC §5.4) ──

test('capabilityPatternMatches handles wildcards over full paths', () => {
  assert.ok(capabilityPatternMatches('channels.publish', 'channels.publish'));
  assert.ok(capabilityPatternMatches('channels.*', 'channels.publish'));
  assert.ok(capabilityPatternMatches('*', 'pushEvents'));
  assert.ok(capabilityPatternMatches('contextHooks.*', 'contextHooks.beforeInference.inject.system'));
  assert.equal(capabilityPatternMatches('channels.*', 'pushEvents'), false);
  assert.equal(capabilityPatternMatches('channels.publish', 'channels.publishing'), false);
});

test('capabilityGranted fails closed on an empty or missing grant', () => {
  assert.equal(capabilityGranted(undefined, 'channels.publish'), false);
  assert.equal(capabilityGranted(null, 'channels.publish'), false);
  assert.equal(capabilityGranted([], 'channels.publish'), false);
  assert.ok(capabilityGranted(['channels.publish'], 'channels.publish'));
});

test('a path in both effective and denied makes the policy message malformed', () => {
  assert.deepEqual(conflictingCapabilityEntries(['a', 'b'], ['b', 'c']), ['b']);
  assert.deepEqual(conflictingCapabilityEntries(['a'], ['c']), []);
});

// ── uses validation and feature-set derivation (SPEC §6.2, §6.4) ──

test('uses that is absent, empty, or unrecognised is invalid_uses', () => {
  assert.deepEqual(validateUses(undefined), { valid: false, reason: 'invalid_uses', unrecognized: [] });
  assert.deepEqual(validateUses([]), { valid: false, reason: 'invalid_uses', unrecognized: [] });
  assert.deepEqual(validateUses(['channels.observe']), {
    valid: false,
    reason: 'invalid_uses',
    unrecognized: ['channels.observe'],
  });
  assert.deepEqual(validateUses(['contextHooks.beforeInference']), {
    valid: false,
    reason: 'invalid_uses',
    unrecognized: ['contextHooks.beforeInference'],
  });
  assert.deepEqual(validateUses(['pushEvents', 'tools']), { valid: true, unrecognized: [] });
});

test('deriveFeatureSets disables on denial and on invalid_uses, and never default-allows', () => {
  const featureSets: Record<string, FeatureSetDeclaration> = {
    'chat.publish': { description: 'Publish', uses: ['channels.publish'] },
    'chat.stream': { description: 'Stream', uses: ['channels.publish', 'channels.streaming'] },
    'chat.broken': { description: 'Broken', uses: ['channels.observe'] as unknown as FeatureSetDeclaration['uses'] },
  };

  const derived = deriveFeatureSets(featureSets, ['channels.publish']);
  assert.deepEqual(derived.enabled, ['chat.publish']);
  assert.deepEqual(derived.disabled['chat.stream'], {
    reason: 'capability_denied',
    missingCapabilities: ['channels.streaming'],
  });
  assert.equal(derived.disabled['chat.broken']!.reason, 'invalid_uses');

  // An empty grant enables nothing.
  assert.deepEqual(deriveFeatureSets(featureSets, []).enabled, []);
  assert.deepEqual(deriveFeatureSets(featureSets, undefined).enabled, []);
});
