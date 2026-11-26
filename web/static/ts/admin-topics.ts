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
  function byId<T extends HTMLElement = HTMLElement>(id: string): T {
    const element = document.getElementById(id);
    if (!element) throw new Error(`Missing topics element: ${id}`);
    return element as T;
  }
  const list = byId('topics-list'), detail = byId('topics-detail'), status = byId('topics-status');
  const filter = byId<HTMLSelectElement>('topics-state'), query = byId<HTMLInputElement>('topics-query');
  let offset = 0;
  function node<K extends keyof HTMLElementTagNameMap>(tag: K, text?: string): HTMLElementTagNameMap[K] {
    const element = document.createElement(tag);
    element.textContent = text || '';
    return element;
  }
  const date = (value?: string) => value ? new Date(value).toLocaleString() : '—';
  const eventLabels: Record<string,string> = {opened:'Открытие', updated:'Обновление', quieted:'Затишье', returned:'Возвращение'};
  const states: Record<string,string> = {active:'Активный', dormant:'Затихший', candidate:'Кандидат', review:'На проверке'};
  function article(target: HTMLElement, item: TopicArticle) {
    const box = node('div', ''); box.className = 'topics-entry';
    if (item.title) box.append(node('strong', item.title));
    if (item.summary) box.append(node('p', item.summary));
    if (item.fact) box.append(node('p', `Факт: ${item.fact}`));
    const reasons: Record<string,string> = {unresolved_referent:'Неясно, к чему относится сообщение', match_uncertain:'Связь на проверке', novelty_uncertain:'Новизна на проверке', material_update:'Значимое обновление', repeat:'Повтор', context:'Контекст', one_material:'Подтверждённое событие', multiple_materials:'Несколько фактов', quiet_window_elapsed:'Семь суток без значимого обновления', new_material_update:'Новый подтверждённый факт'};
    const key = item.reason || item.novelty;
    if (key && reasons[key]) box.append(node('p', reasons[key]));
    try { const url = new URL(item.url || ''); if (!url.username && !url.password && (url.protocol === 'https:' || url.protocol === 'http:')) { const a = node('a', 'Оригинал'); a.href = url.href; a.target = '_blank'; a.rel = 'noopener noreferrer'; box.append(a); } } catch (_) { /* Invalid source links are omitted. */ }
    target.append(box);
  }
  async function get(params: URLSearchParams): Promise<TopicData> { const response = await fetch('/api/v1/topics?' + params, {credentials:'same-origin'}); if (!response.ok) throw new Error('Не удалось загрузить сюжеты'); return response.json(); }
  async function load() {
    status.textContent = 'Загрузка…'; list.replaceChildren(); detail.replaceChildren(node('p','Выберите сюжет.'));
    try {
      const params = new URLSearchParams({state:filter.value,q:query.value,limit:'25',offset:String(offset)}), data = await get(params);
      const cp = data.checkpoint, index = data.index; status.textContent = cp ? `Покрытие до: ${date(cp.covered_through)} · последний материал: ${cp.scan_article_id} · задержка: ${cp.covered_through ? Math.max(0, Math.round((Date.now()-new Date(cp.covered_through).getTime())/60000)) + ' мин' : 'неизвестна'}${cp.replay_in_progress_at ? ' · повторная обработка' : ''}` : index ? `Статей в индексе — ${index.scanned_articles} · ${index.completed_at ? 'загрузка завершена' : 'загрузка идёт'}` : 'Пилот ещё не запущен.';
      if (filter.value === 'review') {
        for (const item of data.decisions) article(list,item);
        if (!data.decisions.length) list.append(node('p','Ничего не найдено.'));
      } else {
        for (const item of data.topics) {
          const button = node('button', `${item.title || 'Без названия'} · ${states[item.state] || item.state}\n${item.context || ''}`);
          button.type = 'button';
          button.addEventListener('click', () => show(item.id));
          list.append(button);
        }
        if (!data.topics.length) list.append(node('p','Ничего не найдено.'));
      }
      const count = filter.value === 'review' ? data.decisions.length : data.topics.length;
      byId<HTMLButtonElement>('topics-prev').disabled = offset === 0;
      byId<HTMLButtonElement>('topics-next').disabled = count < 25;
    } catch (_) { status.textContent = 'Не удалось загрузить сюжеты. Попробуйте ещё раз.'; }
  }
  async function show(id: string) {
    detail.replaceChildren(node('p','Загрузка…'));
    try {
      const data = await get(new URLSearchParams({id,limit:'100'})); const topic = data.topics[0]; detail.replaceChildren();
      if (!topic) { detail.append(node('p','Сюжет не найден.')); return; }
      detail.append(node('h2',topic.title),node('p',topic.context),node('p',`${states[topic.state] || topic.state} · открыт: ${date(topic.opened_at)} · последнее событие: ${date(topic.last_material_at)} · затих: ${date(topic.quieted_at)}`),node('h3','Хронология'));
      if (!data.events.length) detail.append(node('p','Событий пока нет.'));
      for (const event of data.events) { const wrap = node('div',''); wrap.append(node('h4',`${date(event.event_at || event.available_at)} · ${eventLabels[event.kind] || event.kind}`)); article(wrap,event); detail.append(wrap); }
      const more = node('details',''); more.append(node('summary','Материалы и решения')); for (const item of data.decisions) article(more,item); detail.append(more);
    } catch (_) { detail.replaceChildren(node('p','Не удалось загрузить историю.')); }
  }
  byId('topics-filter').addEventListener('submit', e => { e.preventDefault(); offset=0; load(); });
  byId('topics-prev').addEventListener('click', () => { offset=Math.max(0,offset-25); load(); });
  byId('topics-next').addEventListener('click', () => { offset+=25; load(); });
  load();
})();
