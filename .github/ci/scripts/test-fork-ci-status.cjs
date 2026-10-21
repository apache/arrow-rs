// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');

function workflow(name) {
  return JSON.parse(execFileSync('python3', [
    '-c', 'import json, sys, yaml; print(json.dumps(yaml.safe_load(sys.stdin)))',
  ], {
    input: readFileSync(path.join(__dirname, '../../workflows', name)),
    encoding: 'utf8',
  }));
}
const bridgeWorkflow = workflow('fork_ci_status.yml');
const ciWorkflow = workflow('ci.yml');
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const bridge = new AsyncFunction('github', 'context', 'core', 'Date',
  bridgeWorkflow.jobs.bridge.steps[0].with.script);
const now = Date.UTC(2026, 9, 20, 12);
class Clock extends Date { static now() { return now; } }
const sha = 'a'.repeat(40);
const pr = {
  number: 10, state: 'open', updated_at: new Date(now).toISOString(),
  head: {
    sha, ref: 'feature',
    repo: { owner: { login: 'contributor' }, name: 'arrow-rs', full_name: 'contributor/arrow-rs' },
  },
  base: { ref: 'main' },
};
const run = (id = 1, extra = {}) => ({
  id, event: 'push', head_sha: sha, head_branch: 'feature', run_attempt: 1,
  status: 'completed', conclusion: 'success',
  html_url: 'https://github.com/contributor/arrow-rs/actions/runs/' + id,
  ...extra,
});

async function execute({
  event = 'schedule', checks = [], runs = [run()], workflowState = 'active',
  recent = [pr], slice = [pr], candidates = [pr], repo = 'arrow-rs',
  owner = 'apache', runError, count = 1, notification = {},
} = {}) {
  const writes = [], calls = [];
  let sweepPage;
  const github = { rest: { checks: {}, actions: {}, pulls: {} } };
  github.rest.checks.listForRef = async () => {};
  github.rest.pulls.list = async (params) => {
    calls.push(['pulls', params]);
    if (params.per_page === 1) return {
      data: count ? [pr] : [],
      headers: count ? { link: '<https://api.github.com/pulls?per_page=1&page=' + count + '>; rel="last"' } : {},
    };
    assert.equal(params.per_page, 25);
    sweepPage = params.page;
    return { data: slice };
  };
  github.rest.actions.listWorkflowRuns = async (params) => {
    calls.push(['runs', params]);
    assert.equal(params.owner, 'contributor');
    assert.equal(params.workflow_id, 'ci.yml');
    assert.equal(params.head_sha, sha);
    assert.equal(params.branch, 'feature');
    if (runError) throw Object.assign(new Error('API failure'), { status: runError });
    return { data: { workflow_runs: runs } };
  };
  github.rest.actions.getWorkflow = async () => {
    if (workflowState === null) throw Object.assign(new Error('Not Found'), { status: 404 });
    return { data: { state: workflowState } };
  };
  github.rest.checks.create = async (payload) => writes.push({ method: 'create', ...payload });
  github.rest.checks.update = async (payload) => writes.push({ method: 'update', ...payload });
  github.paginate = async (fn, params) => {
    if (fn === github.rest.checks.listForRef) {
      calls.push(['checks', params]);
      return checks;
    }
    assert.equal(fn, github.rest.pulls.list);
    assert.equal(params.head, 'contributor:feature');
    return candidates;
  };
  github.paginate.iterator = async function* () {
    for (let i = 0; i < recent.length; i += 100) yield { data: recent.slice(i, i + 100) };
  };
  await bridge(github, {
    repo: { owner, repo }, eventName: event,
    payload: { workflow_run: {
      event: 'pull_request', head_sha: sha, head_branch: 'feature',
      head_repository: pr.head.repo, pull_requests: [], ...notification,
    } },
  }, { info: () => {} }, Clock);
  return { writes, calls, sweepPage };
}
async function existing(runValue = run()) {
  const { writes } = await execute({ runs: [runValue] });
  return {
    id: 100, app: { slug: 'github-actions' }, external_id: 'arrow-rs-fork-ci:10',
    ...writes[0], conclusion: writes[0].conclusion || null,
  };
}

test('CI completion discovers a fork PR even when the event PR list is empty', async () => {
  const { writes } = await execute({ event: 'workflow_run' });
  assert.equal(writes.length, 1);
  assert.equal(writes[0].head_sha, sha);
  assert.equal(writes[0].name, 'CI (fork)');
  assert.equal(writes[0].conclusion, 'success');
  assert.equal(writes[0].details_url, run().html_url);
});
test('notifications ignore stale branch heads, release PRs, closed PRs and deleted forks', async () => {
  for (const candidate of [
    { ...pr, head: { ...pr.head, sha: 'b'.repeat(40) } },
    { ...pr, base: { ref: 'maintenance' } },
    { ...pr, state: 'closed' },
    { ...pr, head: { ...pr.head, repo: null } },
  ]) {
    assert.deepEqual((await execute({ event: 'workflow_run', candidates: [candidate] })).writes, []);
  }
});
test('upstream pushes and merge-queue completions cannot create fork checks', async () => {
  for (const event of ['push', 'merge_group']) {
    assert.deepEqual((await execute({ event: 'workflow_run', notification: { event } })).writes, []);
  }
});
test('the bridge does nothing when copied into a fork', async () => {
  const result = await execute({ owner: 'contributor' });
  assert.deepEqual(result.calls, []);
  assert.deepEqual(result.writes, []);
});
test('only runs for the exact branch head on push/manual events are mirrored', async () => {
  const result = await execute({ runs: [
    run(8, { head_sha: 'b'.repeat(40) }),
    run(7, { head_branch: 'other' }),
    run(6, { event: 'pull_request' }),
    run(5, { event: 'workflow_dispatch' }),
    run(1),
  ] });
  assert.equal(result.writes[0].details_url, run(5).html_url);
});
for (const [state, expected] of [
  ['active', 'neutral'], ['disabled_fork', 'action_required'], [null, 'action_required'],
]) {
  test('missing run reports workflow state ' + state, async () => {
    const { writes } = await execute({ runs: [], workflowState: state });
    assert.equal(writes[0].conclusion, expected);
    assert.match(writes[0].output.summary, /Enable workflows/);
    assert.equal(JSON.parse(writes[0].output.text).run_id, null);
  });
}
test('a missing workflow endpoint is handled as missing CI', async () => {
  const { writes } = await execute({ runError: 404, workflowState: null });
  assert.equal(writes[0].conclusion, 'action_required');
});
test('permission, rate limit and server errors are not reported as missing CI', async () => {
  for (const runError of [403, 429, 500]) {
    await assert.rejects(execute({ runError }), /API failure/);
  }
});
for (const conclusion of ['failure', 'cancelled', 'timed_out', 'startup_failure']) {
  test('mirrors ' + conclusion + ' without reporting success', async () => {
    const { writes } = await execute({ runs: [run(1, { conclusion })] });
    assert.equal(writes[0].conclusion, conclusion === 'startup_failure' ? 'failure' : conclusion);
  });
}
test('a rerun of the same workflow run updates the existing check', async () => {
  const check = await existing(run(1, { conclusion: 'failure' }));
  const { writes } = await execute({ checks: [check], runs: [run(1, { run_attempt: 2 })] });
  assert.equal(writes[0].method, 'update');
  assert.equal(writes[0].check_run_id, 100);
  assert.equal(writes[0].conclusion, 'success');
  assert.equal(JSON.parse(writes[0].output.text).run_attempt, 2);
});
test('a manual run can replace a failed push run', async () => {
  const check = await existing(run(1, { conclusion: 'failure' }));
  const { writes } = await execute({
    checks: [check], runs: [run(2, { event: 'workflow_dispatch' }), run(1)],
  });
  assert.equal(writes[0].details_url, run(2).html_url);
});
test('a deleted run clears its stale success result', async () => {
  const { writes } = await execute({ checks: [await existing()], runs: [] });
  assert.equal(writes[0].method, 'update');
  assert.equal(writes[0].conclusion, 'neutral');
});
test('unchanged results do not consume check write requests', async () => {
  assert.deepEqual((await execute({ checks: [await existing()] })).writes, []);
});
test('a same-name check from another app or PR is not overwritten', async () => {
  const check = await existing();
  for (const foreign of [
    { ...check, app: { slug: 'other-app' } },
    { ...check, external_id: 'arrow-rs-fork-ci:20' },
  ]) {
    assert.equal((await execute({ checks: [foreign] })).writes[0].method, 'create');
  }
});
test('recent completed checks wait for the rotating sweep', async () => {
  const { writes, calls } = await execute({ checks: [await existing()], slice: [] });
  assert.deepEqual(writes, []);
  assert.equal(calls.filter(([kind]) => kind === 'runs').length, 0);
});
test('late runs are recovered outside the sweep slice', async () => {
  const missing = await execute({ runs: [] });
  const check = { ...await existing(), ...missing.writes[0] };
  const { writes } = await execute({ checks: [check], slice: [] });
  assert.equal(writes[0].conclusion, 'success');
});
for (const status of ['queued', 'in_progress']) {
  test('polls recent ' + status + ' runs outside the sweep slice', async () => {
    const check = await existing(run(1, { status, conclusion: null }));
    const { writes } = await execute({ checks: [check], slice: [] });
    assert.equal(writes[0].conclusion, 'success');
  });
}
test('the sweep reaches old PRs beyond 500 open PRs', async () => {
  const old = { ...pr, updated_at: new Date(now - 24 * 3600 * 1000).toISOString() };
  const { writes, sweepPage } = await execute({ count: 600, recent: [old], slice: [old] });
  assert.equal(sweepPage, Math.floor(now / (15 * 60 * 1000)) % 24 + 1);
  assert.equal(writes.length, 1);
});
test('an active PR appearing in both lists is polled once', async () => {
  const { calls } = await execute();
  assert.equal(calls.filter(([kind]) => kind === 'runs').length, 1);
});
test('no open PRs means no fork API calls', async () => {
  const result = await execute({ count: 0 });
  assert.equal(result.calls.length, 1);
  assert.deepEqual(result.writes, []);
});

function group(github) {
  return ciWorkflow.concurrency.group.replace(/\$\{\{\s*(.*?)\s*\}\}/g,
    (_, expression) => new Function('github', 'return (' + expression + ');')(github));
}
const forkPush = {
  // GitHub expressions resolve absent properties to an empty string.
  repository: 'contributor/arrow-rs', event_name: 'push', event: { pull_request: { number: '' } },
  ref: 'refs/heads/feature', sha: 'first', workflow: 'CI',
};
test('successive fork pushes share a cancellation group, separate branches do not', () => {
  assert.equal(ciWorkflow.concurrency['cancel-in-progress'], true);
  assert.equal(group(forkPush), group({ ...forkPush, sha: 'second' }));
  assert.notEqual(group(forkPush), group({ ...forkPush, ref: 'refs/heads/other' }));
});
test('upstream main and merge queue runs remain separate per commit', () => {
  for (const event_name of ['push', 'merge_group']) {
    const upstream = { ...forkPush, repository: 'apache/arrow-rs', event_name };
    assert.notEqual(group(upstream), group({ ...upstream, sha: 'second' }));
  }
});
test('same-named branches from different fork PRs cannot cancel each other', () => {
  const event = { pull_request: { number: 1 } };
  const context = { ...forkPush, repository: 'apache/arrow-rs', event_name: 'pull_request', event };
  assert.notEqual(group(context), group({ ...context, event: { pull_request: { number: 2 } } }));
});
