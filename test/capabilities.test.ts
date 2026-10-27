import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  CAPABILITY_PATHS,
  PATHS_NOT_ADVERTISED_IN_MCPL,
  advertisedCapabilities,
  advertisedCapabilitiesFromInitialize,
  capabilityGranted,
  capabilityPatternMatches,
  conflictingCapabilityEntries,
  deriveFeatureSets,
  emptyGrantState,
  featureSetSelected,
  grantFromUpdate,
  hasInferenceRequest,
  hasInferenceStreaming,
  isCapabilityPath,
  validateUses,
} from '../src/capabilities.js';
import type { McplCapabilities } from '../src/capabilities.js';
import type { FeatureSetDeclaration, FeatureSetsUpdateParams } from '../src/methods.js';

const sorted = (s: Set<string>): string[] => [...s].sort();

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
  assert.equal(isCapabilityPath('channels'), false);
  assert.equal(isCapabilityPath('contextHooks.beforeInference'), false);
  assert.equal(isCapabilityPath('contextHooks.beforeInference.inject'), false);
  assert.equal(isCapabilityPath('channels.observe'), false);
  assert.equal(isCapabilityPath('contextHooks.afterInference'), false);
});

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

test('tools comes ONLY from the outer MCP capability, never from the mcpl block (§5.1)', () => {
  assert.deepEqual(sorted(advertisedCapabilities({ version: '0.5', tools: true })), []);
  assert.deepEqual(
    sorted(advertisedCapabilitiesFromInitialize({ experimental: { mcpl: { version: '0.5', tools: true } } })),
    [],
  );
  assert.ok(advertisedCapabilitiesFromInitialize({ tools: {} }).has('tools'));
  assert.deepEqual(
    sorted(advertisedCapabilitiesFromInitialize({ tools: {}, experimental: { mcpl: { version: '0.5' } } })),
    ['tools'],
  );
  assert.equal(advertisedCapabilitiesFromInitialize({}).has('tools'), false);
  assert.ok(isCapabilityPath('tools'));
  assert.deepEqual(PATHS_NOT_ADVERTISED_IN_MCPL, ['tools']);
});

test('capabilityPatternMatches: * is exactly one segment, counts must be equal', () => {
  assert.ok(capabilityPatternMatches('channels.publish', 'channels.publish'));
  assert.ok(capabilityPatternMatches('channels.*', 'channels.publish'));
  assert.ok(capabilityPatternMatches('*', 'pushEvents'));
  assert.equal(capabilityPatternMatches('channels.*', 'pushEvents'), false);
  assert.equal(capabilityPatternMatches('channels.publish', 'channels.publishing'), false);

  assert.equal(capabilityPatternMatches('channels.*', 'channels.publish.anything'), false);
  assert.equal(capabilityPatternMatches('*', 'inferenceRequest.streaming'), false);
  assert.equal(capabilityPatternMatches('channels.*', 'channels'), false);
  assert.ok(capabilityPatternMatches('inferenceRequest.*', 'inferenceRequest.streaming'));
  assert.ok(capabilityPatternMatches('contextHooks.*.observe', 'contextHooks.beforeInference.observe'));
});

test('contextHooks.* grants NONE of the depth-4 injection leaves', () => {
  const injectionLeaves = [
    'contextHooks.beforeInference.inject.system',
    'contextHooks.beforeInference.inject.beforeUser',
    'contextHooks.beforeInference.inject.afterUser',
  ] as const;
  for (const leaf of injectionLeaves) {
    assert.equal(capabilityPatternMatches('contextHooks.*', leaf), false, leaf);
    assert.equal(capabilityGranted(['contextHooks.*'], leaf), false, leaf);
  }
  assert.equal(capabilityGranted(['contextHooks.*'], 'contextHooks.beforeInference.observe'), false);
  assert.ok(capabilityGranted(['contextHooks.beforeInference.inject.*'], 'contextHooks.beforeInference.inject.system'));
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

test('grant fields carry CapabilityPattern: a §5.4 wildcard like channels.* typechecks without casts', () => {
  const params: FeatureSetsUpdateParams = {
    effectiveCapabilities: ['channels.*', 'pushEvents'],
    deniedCapabilities: ['inferenceRequest'],
  };
  const result = grantFromUpdate(emptyGrantState(), params, 'request');
  assert.equal(result.malformed, false);
  assert.ok(capabilityGranted(result.state.effectiveCapabilities, 'channels.publish'));
  assert.ok(capabilityGranted(result.state.effectiveCapabilities, 'pushEvents'));
  assert.equal(capabilityGranted(result.state.effectiveCapabilities, 'inferenceRequest'), false);
});

test('grantFromUpdate: Request with effectiveCapabilities replaces the grant and establishes ready', () => {
  const result = grantFromUpdate(
    emptyGrantState(),
    { effectiveCapabilities: ['channels.publish', 'pushEvents'] },
    'request',
  );
  assert.equal(result.malformed, false);
  assert.deepEqual(result.state.effectiveCapabilities, ['channels.publish', 'pushEvents']);
  assert.equal(result.state.ready, true);
  assert.equal(result.state.enabledFeatureSets, null);
});

test('grantFromUpdate: Request with ABSENT effectiveCapabilities is a grant of NOTHING, never "no change"', () => {
  const prior = grantFromUpdate(
    emptyGrantState(),
    { effectiveCapabilities: ['channels.publish', 'channels.streaming'] },
    'request',
  ).state;
  assert.deepEqual(prior.effectiveCapabilities, ['channels.publish', 'channels.streaming']);

  const result = grantFromUpdate(prior, { enabled: ['chat'] }, 'request');
  assert.equal(result.malformed, false);
  assert.deepEqual(result.state.effectiveCapabilities, []);
  assert.equal(capabilityGranted(result.state.effectiveCapabilities, 'channels.publish'), false);
  assert.equal(result.state.ready, true);
});

test('grantFromUpdate: Request enabled is an allowlist when present, no constraint when absent', () => {
  const absent = grantFromUpdate(emptyGrantState(), { effectiveCapabilities: ['pushEvents'] }, 'request');
  assert.equal(absent.state.enabledFeatureSets, null);
  assert.ok(featureSetSelected(absent.state, 'anything.declared'));

  const present = grantFromUpdate(
    emptyGrantState(),
    { effectiveCapabilities: ['pushEvents'], enabled: ['chat'], disabled: ['ops'] },
    'request',
  );
  assert.deepEqual(present.state.enabledFeatureSets, ['chat']);
  assert.deepEqual(present.state.disabledFeatureSets, ['ops']);
  assert.ok(featureSetSelected(present.state, 'chat'));
  assert.equal(featureSetSelected(present.state, 'other'), false);
  assert.equal(featureSetSelected(present.state, 'ops'), false);
});

test('grantFromUpdate: disabled subtracts even from a set named in enabled', () => {
  const result = grantFromUpdate(
    emptyGrantState(),
    { effectiveCapabilities: [], enabled: ['chat'], disabled: ['chat'] },
    'request',
  );
  assert.equal(featureSetSelected(result.state, 'chat'), false);
});

test('grantFromUpdate: a malformed Request fails closed — nothing granted, not ready', () => {
  const prior = grantFromUpdate(
    emptyGrantState(),
    { effectiveCapabilities: ['channels.publish'] },
    'request',
  ).state;

  const result = grantFromUpdate(
    prior,
    {
      effectiveCapabilities: ['channels.publish', 'pushEvents'],
      deniedCapabilities: ['pushEvents'],
    },
    'request',
  );
  assert.equal(result.malformed, true);
  assert.deepEqual(result.conflicts, ['pushEvents']);
  assert.deepEqual(result.state.effectiveCapabilities, []);
  assert.equal(result.state.ready, false);
});

test('grantFromUpdate: Notification NEVER alters the grant except disabled reductions', () => {
  const prior = grantFromUpdate(
    emptyGrantState(),
    { effectiveCapabilities: ['channels.publish'], disabled: ['ops'] },
    'request',
  ).state;
  assert.equal(prior.ready, true);

  const result = grantFromUpdate(
    prior,
    {
      effectiveCapabilities: ['channels.publish', 'channels.streaming', 'pushEvents'],
      enabled: ['chat', 'ops'],
      disabled: ['metrics'],
    },
    'notification',
  );
  assert.equal(result.malformed, false);
  assert.deepEqual(result.discarded, ['effectiveCapabilities', 'enabled']);
  assert.deepEqual(result.state.effectiveCapabilities, ['channels.publish']);
  assert.equal(capabilityGranted(result.state.effectiveCapabilities, 'pushEvents'), false);
  assert.deepEqual(result.state.disabledFeatureSets, ['ops', 'metrics']);
});

test('grantFromUpdate: Notification with absent effectiveCapabilities is also no change to the grant', () => {
  const prior = grantFromUpdate(
    emptyGrantState(),
    { effectiveCapabilities: ['channels.publish'] },
    'request',
  ).state;
  const result = grantFromUpdate(prior, {}, 'notification');
  assert.deepEqual(result.state.effectiveCapabilities, ['channels.publish']);
  assert.deepEqual(result.discarded, []);
  assert.equal(result.state.ready, true);
});

test('grantFromUpdate: a Notification never establishes ready', () => {
  const result = grantFromUpdate(
    emptyGrantState(),
    { effectiveCapabilities: ['channels.publish'], disabled: [] },
    'notification',
  );
  assert.equal(result.state.ready, false);
  assert.deepEqual(result.state.effectiveCapabilities, []);
});

test('grantFromUpdate: null previous starts from the empty grant', () => {
  const result = grantFromUpdate(null, { disabled: ['x'] }, 'notification');
  assert.deepEqual(result.state.effectiveCapabilities, []);
  assert.deepEqual(result.state.disabledFeatureSets, ['x']);
  assert.equal(result.state.ready, false);
});

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

  assert.deepEqual(deriveFeatureSets(featureSets, []).enabled, []);
  assert.deepEqual(deriveFeatureSets(featureSets, undefined).enabled, []);
});
