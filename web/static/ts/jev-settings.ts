interface JevCredentialMetadata { provider: string; revision: number; configured: boolean }

(() => {
  const root = document.getElementById('jev-settings');
  if (!root) return;
  const el = <T extends HTMLElement>(id: string): T => document.getElementById(id) as T;
  const input = el<HTMLInputElement>('jev-key');
  const save = el<HTMLButtonElement>('jev-save');
  const remove = el<HTMLButtonElement>('jev-delete');
  const refresh = el<HTMLButtonElement>('jev-refresh');
  const badge = el<HTMLElement>('jev-badge');
  const status = el<HTMLElement>('jev-status');
  const sr = document.documentElement.lang === 'sr-Latn';
  const text = (ru: string, serbian: string): string => sr ? serbian : ru;
  let metadata: JevCredentialMetadata | null = null;
  let busy = false;
  const render = (): void => {
    badge.textContent = metadata ? (metadata.configured ? text('Ключ настроен', 'Ključ je podešen') : text('Ключ не настроен', 'Ključ nije podešen')) : text('Статус неизвестен', 'Status nije poznat');
    badge.classList.toggle('tg-status-ok', Boolean(metadata?.configured));
    badge.classList.toggle('tg-status-unknown', !metadata?.configured);
    save.disabled = busy || !metadata; remove.disabled = busy || !metadata?.configured; refresh.disabled = busy;
  };
  async function load(): Promise<boolean> {
    busy = true; render();
    try {
      const response = await fetch('/api/v1/credentials/jev', {credentials: 'same-origin'});
      if (!response.ok) throw new Error('load');
      const data = await response.json() as JevCredentialMetadata;
      if (data.provider !== 'jev' || !Number.isSafeInteger(data.revision) || data.revision < 0 || typeof data.configured !== 'boolean') throw new Error('metadata');
      metadata = data; status.textContent = ''; return true;
    } catch (_) { metadata = null; status.textContent = text('Не удалось загрузить статус ключа.', 'Status ključa nije moguće učitati.'); return false; }
    finally { busy = false; render(); }
  }
  async function update(method: 'PUT' | 'DELETE'): Promise<void> {
    if (!metadata || busy) return;
    const value = input.value.trim();
    if (method === 'PUT' && !value) { input.focus(); status.textContent = text('Введите ключ.', 'Unesite ključ.'); return; }
    const revision = metadata.revision;
    busy = true; render(); status.textContent = text('Сохранение...', 'Čuvanje...');
    try {
      const body = method === 'PUT' ? {expected_revision: revision, value} : {expected_revision: revision};
      const response = await fetch('/api/v1/credentials/jev', {method, credentials:'same-origin', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
      input.value = '';
      if (response.status === 409) { await load(); status.textContent = text('Ключ изменён в другом окне. Проверьте статус и повторите действие.', 'Ključ je promenjen u drugom prozoru. Proverite status i ponovite radnju.'); return; }
      if (!response.ok) throw new Error('write');
      const data = await response.json() as JevCredentialMetadata;
      if (data.provider !== 'jev' || !Number.isSafeInteger(data.revision) || data.revision < 0 || typeof data.configured !== 'boolean') throw new Error('metadata');
      metadata = data; status.textContent = method === 'PUT' ? text('Ключ сохранён.', 'Ključ je sačuvan.') : text('Ключ удалён.', 'Ključ je obrisan.');
    } catch (_) {
      input.value = ''; metadata = null; await load();
      status.textContent = text('Результат операции не подтверждён. Проверьте статус перед повтором.', 'Ishod radnje nije potvrđen. Proverite status pre ponavljanja.');
    } finally { busy = false; render(); }
  }
  el<HTMLFormElement>('jev-form').addEventListener('submit', event => { event.preventDefault(); void update('PUT'); });
  remove.addEventListener('click', () => { void update('DELETE'); });
  refresh.addEventListener('click', () => { void load(); });
  void load();
})();
