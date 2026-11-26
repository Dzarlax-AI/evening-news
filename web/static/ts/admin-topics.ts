(() => {
  interface TopicArticle {
    title?: string;
    summary?: string;
    fact?: string;
    reason?: string;
    novelty?: string;
    url?: string;
  }
  interface Topic { id: string; title: string; context: string; state: string; opened_at?: string; last_material_at?: string; quieted_at?: string }
  interface TopicEvent extends TopicArticle { kind: string; event_at?: string; available_at?: string }
  interface TopicData {
    topics: Topic[];
    events: TopicEvent[];
    decisions: TopicArticle[];
    checkpoint?: { covered_through?: string; scan_article_id: number; replay_in_progress_at?: string };
    index?: { scanned_articles: number; completed_at?: string };
  }

  const locale = document.documentElement.lang === 'sr-Latn' ? 'sr-Latn' : 'ru';
  const sr = locale === 'sr-Latn';
  const eventLabels: Record<string, string> = sr
    ? { opened: 'Otvaranje', updated: 'Ažuriranje', quieted: 'Zatišje', returned: 'Povratak' }
    : { opened: 'Открытие', updated: 'Обновление', quieted: 'Затишье', returned: 'Возвращение' };
  const states: Record<string, string> = sr
    ? { active: 'Aktivna', dormant: 'U zatišju', candidate: 'Kandidat', review: 'Za proveru' }
    : { active: 'Активный', dormant: 'Затихший', candidate: 'Кандидат', review: 'На проверке' };
  const reasons: Record<string, [string, string]> = {
    unresolved_referent: ['Неясно, к чему относится сообщение', 'Nije jasno na šta se vest odnosi'],
    match_uncertain: ['Связь на проверке', 'Veza je na proveri'],
    novelty_uncertain: ['Новизна на проверке', 'Novina je na proveri'],
    material_update: ['Значимое обновление', 'Značajno ažuriranje'],
    repeat: ['Повтор', 'Ponavljanje'],
    context: ['Контекст', 'Kontekst'],
    one_material: ['Подтверждённое событие', 'Potvrđen događaj'],
    multiple_materials: ['Несколько фактов', 'Više činjenica'],
    quiet_window_elapsed: ['Семь суток без значимого обновления', 'Sedam dana bez značajnog ažuriranja'],
    new_material_update: ['Новый подтверждённый факт', 'Nova potvrđena činjenica']
  };

  function byId<T extends HTMLElement = HTMLElement>(id: string): T {
    const element = document.getElementById(id);
    if (!element) throw new Error(`Missing topics element: ${id}`);
    return element as T;
  }

  const list = byId('topics-list');
  const detail = byId('topics-detail');
  const status = byId('topics-status');
  const filter = byId<HTMLSelectElement>('topics-state');
  const query = byId<HTMLInputElement>('topics-query');
  const prev = byId<HTMLButtonElement>('topics-prev');
  const next = byId<HTMLButtonElement>('topics-next');
  const pageSize = 25;
  const loading = sr ? 'Učitavanje…' : 'Загрузка…';
  let offset = 0;
  let listRequest: AbortController | undefined;
  let detailRequest: AbortController | undefined;

  function node<K extends keyof HTMLElementTagNameMap>(tag: K, text = '', className = ''): HTMLElementTagNameMap[K] {
    const element = document.createElement(tag);
    element.textContent = text;
    element.className = className;
    return element;
  }

  const date = (value?: string) => value ? new Date(value).toLocaleString(locale) : '—';

  function article(target: HTMLElement, item: TopicArticle) {
    const box = node('div', '', 'topics-entry');
    if (item.title) box.append(node('strong', item.title));
    if (item.summary) box.append(node('p', item.summary));
    if (item.fact) box.append(node('p', `${sr ? 'Činjenica' : 'Факт'}: ${item.fact}`));
    const reason = reasons[item.reason || item.novelty || ''];
    if (reason) box.append(node('p', reason[sr ? 1 : 0], 'topics-meta'));
    try {
      const url = new URL(item.url || '');
      if (!url.username && !url.password && (url.protocol === 'https:' || url.protocol === 'http:')) {
        const link = node('a', sr ? 'Original' : 'Оригинал');
        link.href = url.href;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        box.append(link);
      }
    } catch (_) { /* Invalid source links are omitted. */ }
    target.append(box);
  }

  function coverage(data: TopicData): string {
    const { checkpoint, index } = data;
    if (checkpoint) {
      const lag = checkpoint.covered_through
        ? `${Math.max(0, Math.round((Date.now() - new Date(checkpoint.covered_through).getTime()) / 60000))} ${sr ? 'min' : 'мин'}`
        : (sr ? 'nepoznato' : 'неизвестна');
      const replay = checkpoint.replay_in_progress_at ? (sr ? ' · ponovna obrada' : ' · повторная обработка') : '';
      return sr
        ? `Pokriveno do ${date(checkpoint.covered_through)} · poslednji prilog ${checkpoint.scan_article_id} · kašnjenje ${lag}${replay}`
        : `Покрытие до ${date(checkpoint.covered_through)} · последний материал ${checkpoint.scan_article_id} · задержка ${lag}${replay}`;
    }
    if (index) {
      return sr
        ? `Članaka u indeksu — ${index.scanned_articles} · ${index.completed_at ? 'učitavanje završeno' : 'učitavanje je u toku'}`
        : `Статей в индексе — ${index.scanned_articles} · ${index.completed_at ? 'загрузка завершена' : 'загрузка идёт'}`;
    }
    return sr ? 'Pilot još nije pokrenut.' : 'Пилот ещё не запущен.';
  }

  async function get(params: URLSearchParams, signal: AbortSignal): Promise<TopicData> {
    const response = await fetch('/api/v1/topics?' + params, { credentials: 'same-origin', signal });
    if (!response.ok) throw new Error(`Topics request failed (HTTP ${response.status})`);
    return response.json();
  }

  async function load() {
    listRequest?.abort();
    detailRequest?.abort();
    const request = new AbortController();
    listRequest = request;
    const state = filter.value;
    status.textContent = loading;
    list.replaceChildren();
    list.setAttribute('aria-busy', 'true');
    detail.replaceChildren(node('p', sr ? 'Izaberite temu.' : 'Выберите сюжет.', 'topics-empty'));
    detail.removeAttribute('aria-busy');
    prev.disabled = true;
    next.disabled = true;
    try {
      const params = new URLSearchParams({ state, q: query.value, limit: String(pageSize), offset: String(offset) });
      const data = await get(params, request.signal);
      if (request.signal.aborted) return;
      status.textContent = coverage(data);
      if (state === 'review') {
        for (const item of data.decisions) article(list, item);
      } else {
        for (const item of data.topics) {
          const button = node('button', '', 'topic-row');
          button.type = 'button';
          button.setAttribute('aria-pressed', 'false');
          button.append(node('strong', item.title || (sr ? 'Bez naslova' : 'Без названия'), 'topic-row-title'));
          button.append(node('span', states[item.state] || item.state, 'topic-state'));
          if (item.context) button.append(node('span', item.context, 'topic-row-context'));
          button.addEventListener('click', () => show(item.id, button));
          list.append(button);
        }
      }
      const count = state === 'review' ? data.decisions.length : data.topics.length;
      if (count === 0) list.append(node('p', sr ? 'Nema rezultata.' : 'Ничего не найдено.', 'topics-empty'));
      next.disabled = count < pageSize;
    } catch (_) {
      if (request.signal.aborted) return;
      status.textContent = sr ? 'Učitavanje tema nije uspelo. Pokušajte ponovo.' : 'Не удалось загрузить сюжеты. Попробуйте ещё раз.';
    } finally {
      if (!request.signal.aborted) {
        prev.disabled = offset === 0;
        list.removeAttribute('aria-busy');
      }
    }
  }

  async function show(id: string, button: HTMLButtonElement) {
    detailRequest?.abort();
    const request = new AbortController();
    detailRequest = request;
    list.querySelector('.topic-row[aria-pressed="true"]')?.setAttribute('aria-pressed', 'false');
    button.setAttribute('aria-pressed', 'true');
    detail.setAttribute('aria-busy', 'true');
    detail.replaceChildren(node('p', loading, 'topics-empty'));
    try {
      const data = await get(new URLSearchParams({ id, limit: '100' }), request.signal);
      if (request.signal.aborted) return;
      const topic = data.topics[0];
      if (!topic) {
        detail.replaceChildren(node('p', sr ? 'Tema nije pronađena.' : 'Сюжет не найден.', 'topics-empty'));
        return;
      }
      const heading = node('h2', topic.title);
      const dates = sr
        ? `otvorena ${date(topic.opened_at)} · poslednji događaj ${date(topic.last_material_at)} · zatišje ${date(topic.quieted_at)}`
        : `открыт ${date(topic.opened_at)} · последнее событие ${date(topic.last_material_at)} · затих ${date(topic.quieted_at)}`;
      detail.replaceChildren(heading, node('p', topic.context),
        node('p', `${states[topic.state] || topic.state} · ${dates}`, 'topics-meta'),
        node('h3', sr ? 'Hronologija' : 'Хронология'));
      if (!data.events.length) detail.append(node('p', sr ? 'Još nema događaja.' : 'Событий пока нет.', 'topics-empty'));
      for (const event of data.events) {
        const entry = node('div');
        entry.append(node('h4', `${date(event.event_at || event.available_at)} · ${eventLabels[event.kind] || event.kind}`, 'topics-event-date'));
        article(entry, event);
        detail.append(entry);
      }
      const more = node('details');
      more.append(node('summary', sr ? 'Prilozi i odluke' : 'Материалы и решения'));
      for (const item of data.decisions) article(more, item);
      detail.append(more);
      if (matchMedia('(max-width: 700px)').matches && document.activeElement === button) {
        heading.tabIndex = -1;
        heading.focus({ preventScroll: true });
        detail.scrollIntoView({ block: 'start' });
      }
    } catch (_) {
      if (!request.signal.aborted) {
        detail.replaceChildren(node('p', sr ? 'Učitavanje hronologije nije uspelo.' : 'Не удалось загрузить историю.', 'topics-empty'));
      }
    } finally {
      if (!request.signal.aborted) detail.removeAttribute('aria-busy');
    }
  }

  byId('topics-filter').addEventListener('submit', event => { event.preventDefault(); offset = 0; void load(); });
  prev.addEventListener('click', () => { offset = Math.max(0, offset - pageSize); void load(); });
  next.addEventListener('click', () => { offset += pageSize; void load(); });
  void load();
})();
