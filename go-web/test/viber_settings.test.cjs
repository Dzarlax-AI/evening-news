const assert = require('node:assert/strict');
const {execFileSync} = require('node:child_process');
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');
const {after, before, test} = require('node:test');
const {chromium} = require('playwright');

const repo = path.resolve(__dirname, '../..');
const artifacts = path.resolve(repo, '../../outputs');
const secret = 'synthetic-viber-channel-token-never-display';
let browser;
let server;
let origin;
let page;
let state;
let requestCounts;
let forcedConflicts;
let nextToggleStatus;
let nextTestStatus;
let nextTestState;
let testRequestIds;

const initialConfig = () => ({
  revision: 1, delivery_enabled: false, channel_id: '', channel_name: '', sender_id: '',
  credential_revision: 0, verified_credential_revision: 0, webhook_ready: false,
  verified_at: null, enabled_from: null, last_delivery: null
});
const json = (res, status, value) => {
  res.writeHead(status, {'Content-Type': 'application/json; charset=utf-8'});
  res.end(JSON.stringify(value));
};
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

function renderSettings(locale) {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'viber-settings-render-'));
  const source = `package main
import (
  "html/template"
  "os"
)
type pageData struct { Managed bool; Locale string; ActiveMenu string; AdminUser string }
func main() {
  t := template.New("admin").Funcs(template.FuncMap{"not": func(v bool) bool { return !v }})
  template.Must(t.ParseFiles(${JSON.stringify(path.join(repo, 'go-web/templates/admin/admin_base.html'))}, ${JSON.stringify(path.join(repo, 'go-web/templates/admin/settings.html'))}))
  if err := t.ExecuteTemplate(os.Stdout, "admin-base", pageData{Managed:true, Locale:${JSON.stringify(locale)}, ActiveMenu:"settings", AdminUser:"synthetic-admin"}); err != nil { panic(err) }
}`;
  const file = path.join(temp, 'render.go');
  fs.writeFileSync(file, source);
  try { return execFileSync('go', ['run', file], {cwd: repo, encoding: 'utf8'}); }
  finally { fs.rmSync(temp, {recursive: true, force: true}); }
}

function startServer() {
  state = {config: initialConfig(), credential: {provider: 'viber', revision: 0, configured: false}, superadmins: [
    {id: 'sender-a', name: 'Synthetic Sender A'}, {id: 'sender-b', name: 'Synthetic Sender B'}
  ], callbackArrived: false};
  requestCounts = {configGet: 0, credentialGet: 0, credentialPut: 0, verify: 0, connect: 0, configPut: 0, preview: 0, test: 0};
  forcedConflicts = new Set();
  nextToggleStatus = null;
  nextTestStatus = null;
  nextTestState = 'sent';
  testRequestIds = [];
  server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname.startsWith('/api/')) {
      let body = '';
      for await (const chunk of req) body += chunk;
      const input = body ? JSON.parse(body) : {};
      if (req.method === 'GET' && url.pathname === '/api/v1/configuration/viber') {
        requestCounts.configGet++;
        if (state.callbackArrived) state.config.webhook_ready = true;
        return json(res, 200, state.config);
      }
      if (req.method === 'GET' && url.pathname === '/api/v1/credentials/viber') {
        requestCounts.credentialGet++;
        return json(res, 200, state.credential);
      }
      if (req.method === 'PUT' && url.pathname === '/api/v1/credentials/viber') {
        requestCounts.credentialPut++;
        await delay(70);
        if (forcedConflicts.delete('credential')) return json(res, 409, {detail: 'stale revision'});
        if (input.expected_revision !== state.credential.revision) return json(res, 409, {detail: 'stale revision'});
        assert.equal(input.value, secret, 'credential body should carry only the newly entered value');
        state.credential = {provider: 'viber', revision: state.credential.revision + 1, configured: true};
        return json(res, 200, state.credential);
      }
      if (req.method === 'POST' && url.pathname === '/api/v1/viber/verify') {
        requestCounts.verify++;
        await delay(70);
        if (forcedConflicts.delete('verify') || input.expected_revision !== state.config.revision) return json(res, 409, {detail: 'stale revision'});
        state.config = {...state.config, revision: state.config.revision + 1, channel_id: 'channel-123', channel_name: 'Synthetic News',
          sender_id: input.sender_id || state.config.sender_id, credential_revision: state.credential.revision,
          verified_credential_revision: state.credential.revision, verified_at: '2026-10-31T11:00:00Z'};
        return json(res, 200, {configuration: state.config, superadmins: state.superadmins});
      }
      if (req.method === 'POST' && url.pathname === '/api/v1/viber/connect') {
        requestCounts.connect++;
        await delay(70);
        if (forcedConflicts.delete('connect') || input.expected_revision !== state.config.revision) return json(res, 409, {detail: 'stale revision'});
        state.config = {...state.config, revision: state.config.revision + 1, webhook_ready: false};
        state.callbackArrived = true;
        return json(res, 200, state.config);
      }
      if (req.method === 'POST' && url.pathname === '/api/v1/viber/test') {
        requestCounts.test++;
        testRequestIds.push(input.request_id);
        assert.match(input.request_id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
        assert.equal(input.expected_revision, state.config.revision);
        await delay(110);
        if (nextTestStatus === 409) { nextTestStatus = null; return json(res, 409, {detail: 'stale revision'}); }
        if (nextTestStatus === 503) {
          nextTestStatus = null;
          state.config = {...state.config, last_test: {request_id: input.request_id, state: 'unknown', created_at: '2026-10-31T11:10:00Z'}};
          return json(res, 503, {state: 'unknown', message_token: 'synthetic-secret-message-token'});
        }
        const resultState = nextTestState;
        state.config = {...state.config, last_test: {request_id: input.request_id, state: resultState, created_at: '2026-10-31T11:10:00Z'}};
        return json(res, 200, {request_id: input.request_id, state: resultState, message_token: 'synthetic-secret-message-token', status_code: 0, api_status: 'ok'});
      }
      if (req.method === 'PUT' && url.pathname === '/api/v1/configuration/viber') {
        requestCounts.configPut++;
        await delay(110);
        if (forcedConflicts.delete('config') || input.expected_revision !== state.config.revision) return json(res, 409, {detail: 'stale revision'});
        if (nextToggleStatus) { const status = nextToggleStatus; nextToggleStatus = null; return json(res, status, {detail: 'synthetic failure'}); }
        state.config = {...state.config, revision: state.config.revision + 1,
          delivery_enabled: input.delivery_enabled, sender_id: input.sender_id,
          enabled_from: input.delivery_enabled ? '2026-10-31T11:05:00Z' : null};
        return json(res, 200, state.config);
      }
      if (req.method === 'GET' && url.pathname === '/api/v1/viber/preview') {
        requestCounts.preview++;
        if (url.searchParams.get('edition_id') === 'missing-edition') return json(res, 404, {detail: 'not found'});
        return json(res, 200, {edition_id: url.searchParams.get('edition_id'), version: 'synthetic-v1', parts: [
          {text: '*Synthetic digest*\nhttps://news.example/a', utf16_units: 42, json_bytes: 148},
          {text: 'Unicode 😀 and literal <script>alert(1)</script>', utf16_units: 52, json_bytes: 171}
        ]});
      }
      // Existing settings-page scripts issue unrelated reads; they stay isolated to synthetic data.
      if (req.method === 'GET') {
        if (url.pathname.includes('telegram')) return json(res, 200, {revision: 1, chat_id: '', service_chat_id: '', serbian_chat_id: '', serbian_delivery_enabled: false});
        if (url.pathname.includes('credentials/telegram')) return json(res, 200, {configured: false});
        if (url.pathname.includes('schedule')) return json(res, 200, []);
        if (url.pathname.includes('/ai/')) return json(res, 200, {});
        if (url.pathname.includes('operational')) return json(res, 200, {});
      }
      return json(res, 404, {detail: 'unmocked synthetic route'});
    }
    if (url.pathname === '/admin/settings') {
      const locale = url.searchParams.get('locale') === 'sr-Latn' ? 'sr-Latn' : 'ru';
      res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'});
      return res.end(renderSettings(locale));
    }
    if (url.pathname.startsWith('/static/css/')) {
      const file = path.join(repo, 'web/static/css', path.basename(url.pathname));
      if (!file.startsWith(path.join(repo, 'web/static/css') + path.sep) || !fs.existsSync(file)) { res.writeHead(404); return res.end(); }
      res.writeHead(200, {'Content-Type': 'text/css; charset=utf-8'});
      return res.end(fs.readFileSync(file));
    }
    if (url.pathname === '/static/js/viber-settings.js') {
      res.writeHead(200, {'Content-Type': 'text/javascript; charset=utf-8'});
      return res.end(fs.readFileSync(path.join(repo, 'web/static/js/viber-settings.js')));
    }
    if (url.pathname === '/static/js/locale.js') {
      res.writeHead(200, {'Content-Type': 'text/javascript; charset=utf-8'});
      return res.end(fs.readFileSync(path.join(repo, 'web/static/js/locale.js')));
    }
    if (url.pathname === '/static/js/admin-csrf.js' || url.pathname === '/static/js/main.js') {
      res.writeHead(200, {'Content-Type': 'text/javascript; charset=utf-8'});
      return res.end("window.managedSettings=false;window.readApiResponse=async response=>response.json();");
    }
    res.writeHead(404); res.end();
  });
  return new Promise(resolve => server.listen(0, '127.0.0.1', () => {
    origin = `http://127.0.0.1:${server.address().port}`;
    resolve();
  }));
}

before(async () => {
  assert.ok(fs.existsSync(path.join(repo, 'web/static/js/viber-settings.js')), 'build the TypeScript bundle before running this test');
  await startServer();
  browser = await chromium.launch({headless: true, args: ['--no-sandbox']});
  page = await browser.newPage({viewport: {width: 1440, height: 1000}, colorScheme: 'light'});
  await page.route('**/*', route => {
    const requestURL = route.request().url();
    return requestURL.startsWith(origin) ? route.continue() : route.abort();
  });
  await page.goto(`${origin}/admin/settings?locale=ru`);
  await page.waitForSelector('#viber-refresh', {state: 'attached'});
  await page.locator('.settings-tab').filter({hasText: 'Viber'}).click();
});

after(async () => {
  if (browser) await browser.close();
  if (server) await new Promise(resolve => server.close(resolve));
});

test('credential is saved by revision, cleared from the form, and never displayed', async () => {
  forcedConflicts.add('credential');
  await page.locator('#viber-token').fill(secret);
  await page.locator('#viber-save-token').click();
  await page.getByText('Настройки изменились в другом окне.', {exact: false}).waitFor();
  assert.equal(requestCounts.credentialPut, 1, 'a 409 is not retried automatically');
  assert.equal(await page.locator('#viber-token').inputValue(), '', 'CAS refresh clears the password field');
  assert.equal(await page.locator('#viber-settings').innerText().then(text => text.includes(secret)), false);

  await page.locator('#viber-token').fill(secret);
  await page.locator('#viber-save-token').click();
  await page.getByText('Токен сохранён. Проверьте канал перед подключением.', {exact: true}).waitFor();
  assert.equal(requestCounts.credentialPut, 2);
  assert.equal(await page.locator('#viber-token').inputValue(), '');
  assert.equal(await page.locator('#viber-settings').innerText().then(text => text.includes(secret)), false);
  assert.equal(await page.locator('#viber-token').getAttribute('type'), 'password');
  assert.equal(await page.locator('#viber-enable').isDisabled(), true);
  assert.equal(await page.locator('#viber-test').isDisabled(), true, 'manual testing stays gated until the current token is verified and the webhook is ready');
});

test('verification and connect conflicts refresh state without duplicate POSTs; channel selection and webhook gate delivery', async () => {
  forcedConflicts.add('verify');
  await page.locator('#viber-verify').click();
  await page.getByText('Настройки изменились в другом окне.', {exact: false}).waitFor();
  assert.equal(requestCounts.verify, 1);

  await page.locator('#viber-verify').click();
  await page.locator('#viber-sender').waitFor();
  await page.locator('#viber-sender').selectOption('sender-b');
  await page.locator('#viber-verify').click();
  await page.getByText('Канал проверен.', {exact: true}).waitFor();
  assert.equal(state.config.sender_id, 'sender-b');
  assert.equal(await page.locator('#viber-enable').isDisabled(), true, 'webhook is still pending');
  assert.equal(await page.locator('#viber-test').isDisabled(), true, 'manual testing is also gated on webhook confirmation');

  forcedConflicts.add('connect');
  await page.locator('#viber-connect').click();
  await page.getByText('Настройки изменились в другом окне.', {exact: false}).waitFor();
  assert.equal(requestCounts.connect, 1, 'a connect CAS conflict is not retried');
  await page.locator('#viber-connect').click();
  await page.getByText('Webhook зарегистрирован.', {exact: false}).waitFor();
  assert.equal(state.config.webhook_ready, false, 'registration response is not callback confirmation');
  assert.equal(await page.locator('#viber-enable').isDisabled(), true);

  const callbackReload = page.waitForResponse(response => response.url().includes('/api/v1/configuration/viber') && response.request().method() === 'GET');
  await page.locator('#viber-refresh').click();
  await callbackReload;
  await page.waitForFunction(() => document.querySelector('#viber-enable')?.disabled === false);
  assert.equal(state.config.webhook_ready, true, 'synthetic callback becomes visible after reload');
  assert.equal(await page.locator('#viber-enable').isDisabled(), false);
});

test('manual test works with automatic delivery off and posts once with a request ID', async () => {
  assert.equal(state.config.delivery_enabled, false, 'automatic delivery remains off');
  const button = page.locator('#viber-test');
  assert.equal(await button.isDisabled(), false, 'a verified channel and callback are enough to enable manual testing');
  const before = requestCounts.test;
  await button.click();
  await page.waitForFunction(() => document.querySelector('#viber-test')?.disabled === true);
  await page.locator('#viber-test').dispatchEvent('click');
  await page.getByText('Viber подтвердил приём тестового сообщения.', {exact: true}).waitFor();
  assert.equal(requestCounts.test, before + 1, 'a second click while the first request is pending does not POST again');
  assert.equal(state.config.delivery_enabled, false, 'manual testing does not enable automation');
  assert.equal(testRequestIds[before].length, 36);
  assert.equal(await page.locator('#viber-settings').innerText().then(text => text.includes('synthetic-secret-message-token')), false);
});

test('test POST CAS and 503 refresh never retry; a possible duplicate requires explicit confirmation', async () => {
  const beforeConflict = requestCounts.test;
  nextTestStatus = 409;
  await page.locator('#viber-test').click();
  await page.getByText('Настройки изменились. Состояние обновлено; тест автоматически не повторялся.', {exact: true}).waitFor();
  assert.equal(requestCounts.test, beforeConflict + 1);

  const before503 = requestCounts.test;
  nextTestStatus = 503;
  await page.locator('#viber-test').click();
  await page.getByText('Результат тестового сообщения не подтверждён. Настройки обновлены; новая попытка может создать дубликат.', {exact: true}).waitFor();
  assert.equal(requestCounts.test, before503 + 1, 'a 503 is not automatically retried');
  assert.equal(state.config.last_test.state, 'unknown');
  assert.equal(await page.locator('#viber-settings').innerText().then(text => text.includes('synthetic-secret-message-token')), false);

  page.once('dialog', dialog => dialog.accept());
  const beforeConfirmedAttempt = requestCounts.test;
  await page.locator('#viber-test').click();
  await page.getByText('Viber подтвердил приём тестового сообщения.', {exact: true}).waitFor();
  assert.equal(requestCounts.test, beforeConfirmedAttempt + 1, 'the user-confirmed retry is a separate request');
  assert.notEqual(testRequestIds[before503], testRequestIds[beforeConfirmedAttempt], 'each deliberate attempt gets a fresh request ID');
});

test('delivery toggle rejects duplicate clicks and keeps enable/disable revisioned', async () => {
  const before = requestCounts.configPut;
  forcedConflicts.add('config');
  const button = page.locator('#viber-enable');
  await button.click();
  await page.getByText('Настройки изменились в другом окне.', {exact: false}).waitFor();
  assert.equal(requestCounts.configPut, before + 1, 'configuration CAS conflict is not retried automatically');
  assert.equal(state.config.delivery_enabled, false);

  await page.locator('#viber-enable').click();
  await page.waitForFunction(() => document.querySelector('#viber-enable')?.disabled === true);
  await page.waitForFunction(() => document.querySelector('#viber-enable')?.textContent.includes('Отключить'));
  assert.equal(requestCounts.configPut, before + 2);
  assert.equal(state.config.delivery_enabled, true);
  assert.equal(await page.locator('#viber-settings').innerText().then(text => text.includes('2026')), true);

  await page.locator('#viber-enable').click();
  await page.waitForFunction(() => document.querySelector('#viber-enable')?.textContent.includes('Включить'));
  assert.equal(requestCounts.configPut, before + 3);
  assert.equal(state.config.delivery_enabled, false);
});

test('failed enable shows a typed 422 or ambiguous 503 after reload without retrying or changing state', async () => {
  const button = page.locator('#viber-enable');
  assert.equal(state.config.delivery_enabled, false);

  const getsBefore422 = requestCounts.configGet;
  nextToggleStatus = 422;
  const putsBefore422 = requestCounts.configPut;
  await button.click();
  await page.getByText('Изменение отклонено (HTTP 422). Настройки обновлены; проверьте состояние перед повторной попыткой.', {exact: true}).waitFor();
  assert.equal(requestCounts.configPut, putsBefore422 + 1, '422 is not retried');
  assert.ok(requestCounts.configGet > getsBefore422, 'the current configuration is reloaded before presenting the rejection');
  assert.equal(state.config.delivery_enabled, false, 'rejected enable does not change the setting');
  assert.equal(await page.locator('#viber-enable').isDisabled(), false, 'the refreshed state allows a deliberate new attempt');

  const getsBefore503 = requestCounts.configGet;
  const putsBefore503 = requestCounts.configPut;
  nextToggleStatus = 503;
  await page.locator('#viber-enable').click();
  await page.getByText('Результат изменения не подтверждён (HTTP 503). Настройки обновлены; проверьте состояние перед повторной попыткой.', {exact: true}).waitFor();
  assert.equal(requestCounts.configPut, putsBefore503 + 1, '503 is not retried');
  assert.ok(requestCounts.configGet > getsBefore503, 'ambiguous result is followed by a state reload');
  assert.equal(state.config.delivery_enabled, false, 'the mock 503 leaves delivery disabled');
});

test('preview renders exact returned parts as text, shows both budgets, and never posts', async () => {
  const postCount = requestCounts.verify + requestCounts.connect + requestCounts.configPut;
  await page.locator('#viber-edition').fill('synthetic-edition-id');
  await page.locator('#viber-preview').click();
  await page.getByText('Unicode 😀 and literal <script>alert(1)</script>', {exact: true}).waitFor();
  assert.equal(await page.locator('#viber-preview-result pre').count(), 2);
  assert.match(await page.locator('#viber-preview-result').innerText(), /42\/6500/);
  assert.match(await page.locator('#viber-preview-result').innerText(), /148\/28000/);
  assert.equal(await page.locator('#viber-preview-result script').count(), 0, 'preview text is not interpreted as HTML');
  assert.equal(requestCounts.preview, 1);
  assert.equal(requestCounts.verify + requestCounts.connect + requestCounts.configPut, postCount, 'preview performs no delivery action');

  await page.locator('#viber-edition').fill('missing-edition');
  await page.locator('#viber-preview').click();
  await page.locator('#viber-preview-result').getByText('Не удалось загрузить предпросмотр.', {exact: true}).waitFor();
  assert.equal(requestCounts.preview, 2);
});

test('Serbian labels localize the Viber controls without leaking Russian strings', async () => {
  const srPage = await browser.newPage({viewport: {width: 1024, height: 900}, locale: 'sr-Latn-RS'});
  await srPage.route('**/*', route => route.request().url().startsWith(origin) ? route.continue() : route.abort());
  await srPage.goto(`${origin}/admin/settings?locale=sr-Latn`);
  await srPage.waitForSelector('#viber-refresh', {state: 'attached'});
  const labels = await srPage.locator('#viber-settings').innerText();
  assert.match(labels, /Status veze/);
  assert.match(labels, /Token za kanal/);
  assert.doesNotMatch(labels, /Токен|Настройки|Канал проверен/);
  assert.equal(await srPage.locator('#viber-enable').isDisabled(), false, 'the verified shared synthetic config is ready in Serbian too');
  await srPage.close();
});

test('keyboard focus and six responsive light/dark views render without horizontal overflow', async () => {
  fs.mkdirSync(artifacts, {recursive: true});
  await page.reload();
  await page.waitForSelector('#viber-refresh', {state: 'attached'});
  await page.waitForFunction(() => document.querySelector('#settings-tab-viber')?.getAttribute('style')?.includes('block'));
  const views = [
    [1440, 'desktop'], [768, 'tablet'], [390, 'mobile']
  ];
  for (const [width, label] of views) {
    for (const scheme of ['light', 'dark']) {
      await page.setViewportSize({width, height: 1000});
      await page.emulateMedia({colorScheme: scheme});
      await page.screenshot({path: path.join(artifacts, `viber-settings-${label}-${scheme}.png`), fullPage: true});
      const dimensions = await page.evaluate(() => ({scroll: document.documentElement.scrollWidth, client: document.documentElement.clientWidth}));
      assert.ok(dimensions.scroll <= dimensions.client + 1, `${label} ${scheme} overflows horizontally: ${JSON.stringify(dimensions)}`);
    }
  }
  await page.setViewportSize({width: 390, height: 844});
  await page.locator('.settings-tab').first().focus();
  await page.keyboard.press('Tab');
  const focus = await page.evaluate(() => ({tag: document.activeElement?.tagName, label: document.activeElement?.textContent?.trim()}));
  assert.equal(focus.tag, 'BUTTON');
  assert.ok(focus.label, 'tab navigation should land on a labeled button');
  fs.writeFileSync(path.join(artifacts, 'viber-settings-qa.json'), JSON.stringify({
    result: 'passed', browser: 'Chromium headless', api: 'localhost synthetic mock only',
    checks: ['credential mask and clear', 'CAS refresh without retry', 'sender selection', 'webhook gate', 'manual test while auto delivery is off', 'request ID and duplicate guard', 'test 409 and 503 behavior', 'confirmed retry after unknown', '422 rejection after reload', '503 ambiguity after reload', 'preview parts and budgets', 'RU/SR labels', 'responsive light/dark', 'keyboard focus'],
    screenshots: views.flatMap(([, label]) => ['light', 'dark'].map(scheme => `viber-settings-${label}-${scheme}.png`)),
    requests: requestCounts
  }, null, 2) + '\n');
});
