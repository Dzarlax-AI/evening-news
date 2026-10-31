interface ViberConfiguration {
  revision: number;
  delivery_enabled: boolean;
  channel_id: string;
  channel_name: string;
  sender_id: string;
  credential_revision: number;
  verified_credential_revision: number;
  webhook_ready: boolean;
  verified_at: string | null;
  enabled_from: string | null;
  last_delivery?: { state: string; parts_sent: number; parts_total: number };
  last_test?: { request_id: string; state: 'sent' | 'rejected' | 'unknown' | 'sending'; created_at: string };
}

interface ViberCredentialMetadata {
  provider: string;
  revision: number;
  configured: boolean;
}

interface ViberSender {
  id: string;
  name: string;
}

interface ViberPreviewPart {
  text: string;
  utf16_units: number;
  json_bytes: number;
}

interface ViberTestResult {
  request_id: string;
  state: 'sent' | 'rejected' | 'unknown' | 'sending';
  message_token?: string;
  status_code?: number;
  api_status?: string;
}

(() => {
  const sr = document.documentElement.lang === 'sr-Latn';
  const host = document.getElementById('viber-settings');
  if (!host) return;
  const root: HTMLElement = host;
  const tr = (key: string): string => window.viberSettingsText(key);
  const state: { config: ViberConfiguration | null; credential: ViberCredentialMetadata | null; senders: ViberSender[] } = {
    config: null, credential: null, senders: []
  };
  let testInFlight = false;
  let testPending = false;
  let latestTestState: ViberTestResult['state'] | null = null;
  const escapeHTML = (value: string): string => value.replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char] || char));
  const setStatus = (message: string, error = false): void => {
    const status = root.querySelector<HTMLElement>('[data-viber-status]');
    if (status) { status.textContent = message; status.classList.toggle('err', error); status.classList.toggle('ok', !error); }
  };
  const dateLabel = (value: string | null): string => value ? new Date(value).toLocaleString(sr ? 'sr-Latn-RS' : 'ru-RU') : '—';
  const configReady = (): boolean => Boolean(state.config && state.credential?.configured &&
    state.config.verified_credential_revision === state.credential.revision && state.config.channel_id &&
    state.config.sender_id && state.config.webhook_ready);

  const render = (): void => {
    const config = state.config;
    const credential = state.credential;
    const credentialRevisionMatch = Boolean(config && credential && config.verified_credential_revision === credential.revision);
    const statusLabel = !config ? tr('errorStatus') : config.delivery_enabled ? tr('enabledStatus') : tr('disabledStatus');
    const lastTest = config?.last_test;
    const shownTestState = latestTestState || lastTest?.state;
    const testStateLabel = shownTestState ? tr(`testState_${shownTestState}`) : tr('noTest');
    const senderSelection = state.senders.length > 1
      ? `<label class="settings-form-group"><span>${tr('sender')}</span><select id="viber-sender" class="settings-input"><option value="">${tr('selectSender')}</option>${state.senders.map(item => `<option value="${escapeHTML(item.id)}" ${config?.sender_id === item.id ? 'selected' : ''}>${escapeHTML(item.name)} · ${escapeHTML(item.id)}</option>`).join('')}</select><small>${tr('senderRequired')}</small></label>`
      : `<div class="tg-config-row"><span class="tg-config-label">${tr('sender')}</span><span class="tg-config-value">${escapeHTML(config?.sender_id || '—')}</span></div>`;
    root.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap;">
        <h2 style="margin:0;">${tr('heading')}</h2>
        <span class="tg-status-badge ${config?.delivery_enabled ? 'tg-status-ok' : 'tg-status-unknown'}">${statusLabel}</span>
      </div>
      <div class="settings-card"><div class="settings-card-header">${tr('overview')}</div><div class="settings-card-body">
        <div class="tg-config-grid">
          <div class="tg-config-row"><span class="tg-config-label">${tr('credential')}</span><span class="tg-config-value">${credential?.configured ? tr('configured') : tr('missing')}</span></div>
          <div class="tg-config-row"><span class="tg-config-label">${tr('channel')}</span><span class="tg-config-value">${escapeHTML(config?.channel_name || '—')} <span class="empty">${escapeHTML(config?.channel_id || '')}</span></span></div>
          <div class="tg-config-row"><span class="tg-config-label">Webhook</span><span class="tg-config-value">${config?.webhook_ready ? tr('ready') : tr('webhookPending')}</span></div>
          <div class="tg-config-row"><span class="tg-config-label">${tr('verifiedAt')}</span><span class="tg-config-value">${dateLabel(config?.verified_at || null)}</span></div>
          <div class="tg-config-row"><span class="tg-config-label">${tr('enabledFrom')}</span><span class="tg-config-value">${dateLabel(config?.enabled_from || null)}</span></div>
          <div class="tg-config-row"><span class="tg-config-label">${tr('lastDelivery')}</span><span class="tg-config-value">${config?.last_delivery ? `${escapeHTML(config.last_delivery.state)} · ${config.last_delivery.parts_sent}/${config.last_delivery.parts_total} ${tr('part')}` : tr('noDelivery')}</span></div>
        </div>
        ${senderSelection}
        <p class="settings-hint">${tr('enableHint')}</p>
        <p class="settings-hint">${tr('sourceHint')} ${tr('noSend')}</p>
        <div class="settings-action-item" style="margin:12px 0;">
          <strong>${tr('testTitle')}</strong>
          <p>${tr('testHelp')}</p>
          <button id="viber-test" class="btn btn--secondary" type="button" ${!configReady() || testInFlight || testPending || config?.last_test?.state === 'sending' ? 'disabled' : ''}>${tr('testButton')}</button>
          <p id="viber-last-test" role="status">${escapeHTML(testStateLabel)}${lastTest?.created_at ? ` · ${escapeHTML(dateLabel(lastTest.created_at))}` : ''}</p>
        </div>
        <div class="settings-actions-row">
          <button id="viber-refresh" class="btn btn--secondary" type="button">${tr('refresh')}</button>
          <button id="viber-verify" class="btn btn--secondary" type="button" ${credential?.configured ? '' : 'disabled'}>${tr('verify')}</button>
          <button id="viber-connect" class="btn btn--secondary" type="button" ${credentialRevisionMatch && config?.channel_id && config?.sender_id ? '' : 'disabled'}>${tr('connect')}</button>
          <button id="viber-enable" class="btn btn--primary" type="button" ${config?.delivery_enabled || configReady() ? '' : 'disabled'}>${config?.delivery_enabled ? tr('disable') : tr('enable')}</button>
        </div>
        <p data-viber-status role="status" aria-live="polite">${config ? '' : tr('loading')}</p>
      </div></div>
      <div class="settings-card"><div class="settings-card-header">${tr('credential')}</div><div class="settings-card-body">
        <label class="settings-form-group" for="viber-token"><span>${tr('tokenLabel')}</span><input id="viber-token" class="settings-input" type="password" autocomplete="new-password" spellcheck="false" placeholder="${tr('tokenPlaceholder')}" aria-describedby="viber-token-help"></label>
        <small id="viber-token-help">${tr('tokenHelp')}</small>
        <div style="display:flex;gap:10px;margin-top:12px;flex-wrap:wrap;"><button id="viber-save-token" class="btn btn--primary" type="button">${tr('saveToken')}</button></div>
      </div></div>
      <div class="settings-card"><div class="settings-card-header">${tr('previewTitle')}</div><div class="settings-card-body">
        <p class="settings-hint">${tr('noPreview')}</p>
        <label class="settings-form-group" for="viber-edition"><span>${tr('editionLabel')}</span><input id="viber-edition" class="settings-input" type="text" inputmode="text" autocomplete="off" placeholder="${tr('editionPlaceholder')}"></label>
        <button id="viber-preview" class="btn btn--secondary" type="button">${tr('preview')}</button>
        <div id="viber-preview-result" aria-live="polite"></div>
      </div></div>`;

    root.querySelector<HTMLButtonElement>('#viber-refresh')?.addEventListener('click', () => { void load(); });
    root.querySelector<HTMLButtonElement>('#viber-save-token')?.addEventListener('click', () => { void saveCredential(); });
    root.querySelector<HTMLButtonElement>('#viber-verify')?.addEventListener('click', () => { void verify(); });
    root.querySelector<HTMLButtonElement>('#viber-connect')?.addEventListener('click', () => { void connect(); });
    root.querySelector<HTMLButtonElement>('#viber-enable')?.addEventListener('click', () => { void toggleDelivery(); });
    root.querySelector<HTMLButtonElement>('#viber-test')?.addEventListener('click', () => { void sendTest(); });
    root.querySelector<HTMLButtonElement>('#viber-preview')?.addEventListener('click', () => { void preview(); });
  };

  const requestJSON = async <T>(url: string, init?: RequestInit): Promise<T> => {
    const response = await fetch(url, init);
    if (!response.ok) {
      if (response.status === 409) throw new Error('conflict');
      throw new Error(`http-${response.status}`);
    }
    return response.json() as Promise<T>;
  };
  const post = (body: unknown): RequestInit => ({ method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const put = (body: unknown): RequestInit => ({ method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const genericError = (error: unknown, fallback: string): string => error instanceof Error && error.message === 'conflict' ? tr('conflict') : fallback;

  async function load(): Promise<boolean> {
    setStatus(tr('loading'));
    try {
      const [config, credential] = await Promise.all([
        requestJSON<ViberConfiguration>('/api/v1/configuration/viber'),
        requestJSON<ViberCredentialMetadata>('/api/v1/credentials/viber')
      ]);
      state.config = config;
      state.credential = credential;
      state.senders = [];
      if (config.last_test) latestTestState = config.last_test.state;
      if (config.last_test) testPending = config.last_test.state === 'sending';
      else if (!testInFlight) testPending = false;
      render();
      return true;
    } catch (_) {
      setStatus(tr('loadingError'), true);
      ['#viber-save-token', '#viber-verify', '#viber-connect', '#viber-enable'].forEach(selector => {
        const button = root.querySelector<HTMLButtonElement>(selector);
        if (button) button.disabled = true;
      });
      const status = root.querySelector<HTMLElement>('[data-viber-status]');
      if (status && !root.querySelector('#viber-refresh')) {
        status.innerHTML = `${tr('loadingError')} <button class="btn btn--secondary" type="button" id="viber-retry">${tr('retryLoad')}</button>`;
        root.querySelector<HTMLButtonElement>('#viber-retry')?.addEventListener('click', () => { void load(); });
      }
      return false;
    }
  }

  async function saveCredential(): Promise<void> {
    if (!state.credential) return;
    const input = root.querySelector<HTMLInputElement>('#viber-token');
    const value = input?.value.trim() || '';
    if (!value) { input?.focus(); setStatus(tr('missing'), true); return; }
    const button = root.querySelector<HTMLButtonElement>('#viber-save-token');
    if (button) button.disabled = true;
    try {
      state.credential = await requestJSON<ViberCredentialMetadata>('/api/v1/credentials/viber', put({ expected_revision: state.credential.revision, value }));
      if (input) input.value = '';
      setStatus(tr('tokenSaved'));
      render();
      setStatus(tr('tokenSaved'));
    } catch (error) {
      setStatus(genericError(error, tr('tokenSaveUnknown')), true);
      // A credential write may have committed before a transport failure; require a fresh read.
      if (error instanceof Error && error.message !== 'conflict') {
        try { state.credential = await requestJSON<ViberCredentialMetadata>('/api/v1/credentials/viber'); render(); setStatus(tr('tokenSaveUnknown'), true); } catch (_) {}
      } else if (error instanceof Error && error.message === 'conflict') {
        try { state.credential = await requestJSON<ViberCredentialMetadata>('/api/v1/credentials/viber'); render(); setStatus(tr('conflict'), true); } catch (_) {}
      }
    } finally { if (button) button.disabled = false; }
  }

  async function verify(): Promise<void> {
    if (!state.config) return;
    const selected = root.querySelector<HTMLSelectElement>('#viber-sender')?.value || state.config.sender_id || '';
    const button = root.querySelector<HTMLButtonElement>('#viber-verify');
    if (button) button.disabled = true;
    try {
      const result = await requestJSON<{ configuration: ViberConfiguration; superadmins: ViberSender[] }>('/api/v1/viber/verify', post({ expected_revision: state.config.revision, ...(selected ? { sender_id: selected } : {}) }));
      state.config = result.configuration;
      state.senders = result.superadmins || [];
      render();
      const many = state.senders.length > 1;
      setStatus(many && !state.config.sender_id ? tr('senderRequired') : tr('verified'), many && !state.config.sender_id);
    } catch (error) {
      if (error instanceof Error && error.message === 'conflict') { await load(); setStatus(tr('conflict'), true); }
      else setStatus(tr('verifyError'), true);
    }
    finally { const next = root.querySelector<HTMLButtonElement>('#viber-verify'); if (next) next.disabled = !state.credential?.configured; }
  }

  async function connect(): Promise<void> {
    if (!state.config || !state.credential?.configured) return;
    const sender = root.querySelector<HTMLSelectElement>('#viber-sender')?.value || state.config.sender_id;
    if (state.senders.length > 1 && !sender) { setStatus(tr('senderRequired'), true); return; }
    const button = root.querySelector<HTMLButtonElement>('#viber-connect');
    if (button) button.disabled = true;
    try {
      state.config = await requestJSON<ViberConfiguration>('/api/v1/viber/connect', post({ expected_revision: state.config.revision }));
      render();
      setStatus(state.config.webhook_ready ? tr('ready') : tr('connected'));
    } catch (error) {
      if (error instanceof Error && error.message === 'conflict') { await load(); setStatus(tr('conflict'), true); }
      else setStatus(tr('connectError'), true);
    }
    finally { const next = root.querySelector<HTMLButtonElement>('#viber-connect'); if (next) next.disabled = !state.config || !state.config.channel_id || !state.config.sender_id; }
  }

  async function toggleDelivery(): Promise<void> {
    if (!state.config) return;
    const current = state.config;
    const enabled = !current.delivery_enabled;
    if (enabled && !configReady()) { setStatus(tr('notVerified'), true); return; }
    const button = root.querySelector<HTMLButtonElement>('#viber-enable');
    if (button) button.disabled = true;
    let stateRefreshed = true;
    try {
      state.config = await requestJSON<ViberConfiguration>('/api/v1/configuration/viber', put({ expected_revision: current.revision, delivery_enabled: enabled, sender_id: current.sender_id }));
      render();
      setStatus(enabled ? tr('enabled') : tr('disabled'));
    } catch (error) {
      stateRefreshed = await load();
      if (error instanceof Error && error.message === 'conflict') {
        setStatus(tr(stateRefreshed ? 'conflict' : 'conflictNoRefresh'), true);
      } else {
        const status = error instanceof Error ? /^http-(\d{3})$/.exec(error.message)?.[1] : undefined;
        const key = status === '422'
          ? stateRefreshed ? 'toggleRejected' : 'toggleRejectedNoRefresh'
          : stateRefreshed ? 'toggleUnknown' : 'toggleUnknownNoRefresh';
        setStatus(tr(key).replace('{status}', status || 'unknown'), true);
      }
    } finally {
      const next = root.querySelector<HTMLButtonElement>('#viber-enable');
      if (next) next.disabled = !stateRefreshed || (!state.config?.delivery_enabled && !configReady());
    }
  }

  async function sendTest(): Promise<void> {
    const current = state.config;
    if (!current || !configReady() || testInFlight || testPending) return;
    if ((current.last_test?.state === 'unknown' || latestTestState === 'unknown') && !window.confirm(tr('testConfirmDuplicate'))) return;

    let requestId: string;
    try { requestId = crypto.randomUUID(); }
    catch (_) { setStatus(tr('testUnavailable'), true); return; }

    testInFlight = true;
    latestTestState = 'sending';
    render();
    setStatus(tr('testSending'));
    let notice = tr('testUnknown');
    let isError = false;
    let stateRefreshed = true;
    try {
      const result = await requestJSON<ViberTestResult>('/api/v1/viber/test', post({ request_id: requestId, expected_revision: current.revision }));
      if (result.request_id !== requestId) {
        latestTestState = 'unknown';
        notice = tr('testUnknown');
        isError = true;
        stateRefreshed = await load();
        latestTestState = 'unknown';
        testPending = false;
      } else {
        const resultState = ['sent', 'rejected', 'unknown', 'sending'].includes(result.state) ? result.state : 'unknown';
        latestTestState = resultState;
        testPending = resultState === 'sending';
        if (resultState !== 'sending') {
          stateRefreshed = await load();
          // Preserve this request's receipt if the configuration read is briefly stale.
          latestTestState = resultState;
          testPending = false;
        }
        notice = resultState === 'sent' ? tr('testSent')
          : resultState === 'rejected' ? tr('testRejected')
          : resultState === 'sending' ? tr('testSendingState')
          : stateRefreshed ? tr('testUnknown') : tr('testUnknownNoRefresh');
        isError = resultState === 'rejected' || resultState === 'unknown';
      }
    } catch (error) {
      const status = error instanceof Error ? /^http-(\d{3})$/.exec(error.message)?.[1] : undefined;
      if (error instanceof Error && error.message === 'conflict') {
        stateRefreshed = await load();
        notice = tr(stateRefreshed ? 'testConflict' : 'testConflictNoRefresh');
      } else {
        const failureState: ViberTestResult['state'] = status?.startsWith('4') ? 'rejected' : 'unknown';
        latestTestState = failureState;
        testPending = false;
        stateRefreshed = await load();
        latestTestState = failureState;
        notice = failureState === 'rejected' ? tr('testRejected')
          : stateRefreshed ? tr('testUnknown') : tr('testUnknownNoRefresh');
      }
      isError = true;
    } finally {
      testInFlight = false;
      if (stateRefreshed) render();
      const testButton = root.querySelector<HTMLButtonElement>('#viber-test');
      if (testButton && !stateRefreshed) testButton.disabled = true;
      setStatus(notice, isError);
    }
  }

  async function preview(): Promise<void> {
    const id = root.querySelector<HTMLInputElement>('#viber-edition')?.value.trim() || '';
    if (!id) { setStatus(tr('previewIdRequired'), true); return; }
    const button = root.querySelector<HTMLButtonElement>('#viber-preview');
    const target = root.querySelector<HTMLElement>('#viber-preview-result');
    if (button) button.disabled = true;
    if (target) target.textContent = tr('loading');
    try {
      const result = await requestJSON<{ edition_id: string; version: string; parts: ViberPreviewPart[] }>(`/api/v1/viber/preview?edition_id=${encodeURIComponent(id)}`);
      if (!target) return;
      target.replaceChildren();
      result.parts.forEach((part, index) => {
        const card = document.createElement('section');
        card.className = 'settings-action-item';
        const title = document.createElement('h3');
        title.textContent = `${tr('previewPart')} ${index + 1}/${result.parts.length}`;
        const metrics = document.createElement('p');
        metrics.textContent = `${tr('budget')}: ${part.utf16_units}/6500 ${tr('partUnit')} · ${part.json_bytes}/28000 ${tr('byteUnit')}`;
        const text = document.createElement('pre');
        text.style.whiteSpace = 'pre-wrap';
        text.style.overflowWrap = 'anywhere';
        text.textContent = part.text;
        card.append(title, metrics, text);
        target.append(card);
      });
      setStatus(`${tr('ready')} · ${result.parts.length} ${tr('part')}`);
    } catch (error) {
      if (target) target.textContent = tr('previewError');
      setStatus(genericError(error, tr('previewError')), true);
    } finally { if (button) button.disabled = false; }
  }

  void load();
})();
