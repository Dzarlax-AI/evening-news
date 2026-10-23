"""API response parsing remains useful when proxies return HTML or no body."""

import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="Node is required for JavaScript behavior tests")


def test_api_request_and_job_poller_handle_non_json_responses():
    source = (ROOT / "web/static/js/main.js").read_text()
    script = r"""
const vm = require('node:vm');
const assert = require('node:assert/strict');
const input = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
const window = {addEventListener() {}};
const document = {addEventListener() {}, querySelector() {return null;}};
const context = vm.createContext({window, document, AbortController,
  console: {log() {}, error() {}}, setTimeout: fn => {fn(); return 1;},
  fetch: async url => url === '/initial'
    ? {status: 502, ok: false, text: async () => '<html>proxy failure</html>'}
    : {status: 200, ok: true, text: async () => '{}'}});
vm.runInContext(input.source, context);
async function main() {
  await assert.rejects(window.NewsAggregator.apiRequest('/initial'),
    /non-JSON response \(HTTP 502\)/);
  assert.equal(JSON.stringify(await window.readApiResponse(
    {status: 204, ok: true, text: async () => ''})), '{}');
  await assert.rejects(window.resolveBackgroundResponse(
    {status: 202}, {job_id: 'fixture', status_url: '/jobs/fixture'}),
    /Invalid job status response \(HTTP 200\)/);
}
main().catch(error => {console.error(error); process.exitCode = 1;});
"""
    result = subprocess.run(
        [NODE, "-e", script], input=json.dumps({"source": source}),
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr
