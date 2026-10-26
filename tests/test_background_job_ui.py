"""Exercise the shipped job poller and article modal in a small JavaScript VM."""

import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="Node is required for JavaScript behavior tests")


def run_ui(scenario):
    source = (ROOT / "web/static/js/main.js").read_text()
    modal = (ROOT / "go-web/templates/public/fragments/article_modal.html").read_text()
    modal = modal.split("window.refreshArticle = async function()", 1)[1].split("</script>", 1)[0]
    script = r"""
const vm = require('node:vm');
const assert = require('node:assert/strict');
const input = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
const history = [], alerts = [], updates = [], calls = [];
let reloads = 0, label = 'Refresh';
const button = {disabled: false, get innerHTML() {return label;},
  set innerHTML(value) {label = value; history.push(value);}};
const notice = {style: {}};
const window = {addEventListener() {}, location: {pathname: '/admin/dashboard'},
  openArticleModal: async () => {reloads++;}};
const document = {addEventListener() {},
  getElementById: id => id === 'modalRefreshBtn' ? button : notice,
  querySelector: () => null};
const responses = [...input.scenario.responses];
const context = vm.createContext({window, document, AbortController, console: {log(){}, error(){}},
  currentArticleId: 7, updateAdminAccessIndicator() {}, alert: value => alerts.push(value),
  setTimeout: fn => {fn(); return 1;},
  fetch: async url => {
    calls.push(url);
    const response = responses.shift();
    assert.ok(response, 'unexpected extra request');
    return {status: response.http || 200, ok: (response.http || 200) < 400,
      json: async () => response.body,
      text: async () => Object.hasOwn(response, 'raw') ? response.raw : JSON.stringify(response.body)};
  }});
vm.runInContext(input.source, context);
async function main() {
  if (input.scenario.modal) {
    vm.runInContext('window.refreshArticle = async function()' + input.modal, context);
    await window.refreshArticle();
  } else if (input.scenario.api) {
    try {
      const result = await window.NewsAggregator.apiRequest('/fixture');
      if (input.scenario.error) assert.fail('Expected an HTTP response error');
      if (input.scenario.empty) assert.equal(Object.keys(result).length, 0);
    } catch (error) {
      if (!input.scenario.error) throw error;
      assert.match(error.message, new RegExp(input.scenario.error));
    }
  } else {
    let resolved;
    try {
      resolved = await window.resolveBackgroundResponse({status:202},
        {job_id:'fixture-job',status_url:'/api/v1/jobs/fixture-job'}, value => updates.push(value.status));
      assert.equal(resolved.success, input.scenario.success);
      assert.equal(resolved.status, input.scenario.status);
    } catch (error) {
      if (!input.scenario.error) throw error;
      assert.match(error.message, typeof input.scenario.error === 'string'
        ? new RegExp(input.scenario.error) : /session expired/);
    }
    if (input.scenario.message) assert.match(resolved.message, new RegExp(input.scenario.message));
    input.scenario.resolvedMessage = resolved && resolved.message;
  }
  await Promise.resolve();
  assert.equal(responses.length, 0);
  if (input.scenario.reloads !== undefined) assert.equal(reloads, input.scenario.reloads);
  if (input.scenario.label) assert.ok(history.some(value => value.includes(input.scenario.label)));
  if (input.scenario.noErrors) assert.ok(!alerts.some(value => value.includes('Не удалось')));
  if (input.scenario.updates) assert.deepEqual(updates, input.scenario.updates);
  process.stdout.write(JSON.stringify({calls:calls.length,reloads,message:input.scenario.resolvedMessage}));
}
main().catch(error => {console.error(error); process.exitCode=1;});
"""
    result = subprocess.run([NODE, "-e", script], input=json.dumps({
        "source": source, "modal": modal, "scenario": scenario,
    }), capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


@pytest.mark.parametrize("result_status,success", [
    ("success", True), ("warning", False), ("deferred", False),
])
def test_poller_uses_domain_outcome(result_status, success):
    run_ui({"responses": [{"body": {"status": "succeeded", "result": {"status": result_status}}}],
            "success": success, "status": "succeeded"})


def test_interrupted_job_keeps_polling_until_recovery():
    run_ui({"responses": [{"body": {"status": "interrupted"}},
                          {"body": {"status": "succeeded", "result": {"status": "success"}}}],
            "success": True, "status": "succeeded", "updates": ["queued", "interrupted", "succeeded"]})


def test_poller_stops_on_authentication_failure():
    result = run_ui({"responses": [{"http": 401, "body": {}}], "error": True})
    assert result["calls"] == 1


def test_unknown_delivery_is_not_success():
    run_ui({"responses": [{"body": {"status": "delivery_unknown"}}],
            "success": False, "status": "delivery_unknown"})


@pytest.mark.parametrize("result,success,message", [
    ({"status": "retry_wait", "success": False, "settled": False, "retryable": True,
      "article_outcomes": [{"article_id": 7, "state": "retry_wait"}]},
     False, "another attempt"),
    ({"status": "exhausted", "success": False, "settled": False,
      "article_outcomes": [{"article_id": 7, "state": "exhausted"}]},
     False, "article-level outcomes"),
    ({"status": "rejected", "success": False, "settled": True,
      "article_outcomes": [{"article_id": 7, "state": "rejected"}]},
     False, "article-level outcomes"),
    ({"status": "settled", "success": True, "settled": True,
      "article_outcomes": [{"article_id": 7, "state": "completed"}]},
     True, None),
    ({"status": "settled", "success": True, "settled": True,
      "article_outcomes": [{"article_id": 7, "state": "rejected"}]},
     False, "article-level outcomes"),
    ({"status": "incomplete", "success": False, "settled": False, "retryable": True},
     False, "another attempt"),
])
def test_poller_preserves_article_processing_states(result, success, message):
    run_ui({"responses": [{"body": {"status": "succeeded", "result": result}}],
            "success": success, "status": "succeeded", "message": message})


def test_proxy_html_error_preserves_http_status():
    run_ui({"api": True, "responses": [{"http": 502, "raw": "<html>Proxy unavailable</html>"}],
            "error": "HTTP 502"})


def test_empty_no_content_response_does_not_raise_json_error():
    run_ui({"api": True, "responses": [{"http": 204, "raw": ""}], "empty": True})


@pytest.mark.parametrize("response", [
    {"http": 200, "raw": "<html>Login required</html>"},
    {"http": 200, "body": {}},
    {"http": 204, "raw": ""},
])
def test_invalid_job_status_response_stops_polling(response):
    result = run_ui({"responses": [response], "error": "response|status"})
    assert result["calls"] == 1


@pytest.mark.parametrize("status,success,state,reason,label", [
    ("completed", True, "completed", None, "Обновлено"),
    ("rejected", False, "rejected", "advertisement", "Реклама"),
    ("exhausted", False, "exhausted", "APIError", "Лимит попыток"),
])
def test_embedded_article_outcomes_reload_modal(status, success, state, reason, label):
    run_ui({"modal": True, "responses": [
        {"body": {"admin": True}},
        {"body": {"success": success, "status": status, "reason": reason,
                  "stats": {"article_outcomes": [{"article_id": 7, "state": state, "reason": reason}]}}},
    ], "reloads": 1, "label": label, "noErrors": True})
