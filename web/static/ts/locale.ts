interface Window {
  serbianCountForm: (value: number, one: string, few: string, many: string) => string;
  presentError: (error: unknown) => string;
  presentMessage: (message: string | undefined, code: unknown, success?: boolean) => unknown;
  localizedURL: (raw: string) => string;
  viberSettingsText: (key: string) => string;
}

(() => {
  const locale = document.documentElement.lang === 'sr-Latn' ? 'sr-Latn' : 'ru';
  const viberLabels: Record<string, Record<string, string>> = {
    ru: {
      heading:'Viber · Srpski', loading:'Загрузка настроек…', overview:'Состояние подключения', credential:'Токен канала', channel:'Канал', sender:'Отправитель',
      tokenLabel:'Токен канала', tokenPlaceholder:'Введите новый токен', tokenHelp:'Токен хранится в зашифрованном виде. Его значение больше не показывается.',
      configured:'Токен сохранён', missing:'Токен не задан', saveToken:'Сохранить токен', verify:'Проверить канал', connect:'Подключить webhook',
      enable:'Включить доставку в Viber', disable:'Отключить доставку в Viber', refresh:'Обновить', previewTitle:'Предпросмотр сохранённого выпуска',
      editionLabel:'ID сербского выпуска', editionPlaceholder:'Введите ID выпуска', preview:'Показать предпросмотр',
      noPreview:'Введите ID сохранённого сербского выпуска, чтобы увидеть предпросмотр.',
      enableHint:'Доставка новых выпусков начинается только после включения. Глобальная остановка доставки действует и для Viber.',
      sourceHint:'Названия источников убраны из короткой публикации; ссылки остаются отдельными.', nothingSent:'Предпросмотр не отправляет сообщения.',
      loadingError:'Не удалось загрузить настройки.', saveError:'Не удалось сохранить настройки.',
      testTitle:'Пробное сообщение', testButton:'Отправить тест в Viber',
      testHelp:'Отправляет одно пробное сообщение в этот канал. Не публикует дайджест и не меняет автоматическую доставку.',
      testConfirmDuplicate:'Результат предыдущего теста неизвестен. Новая попытка может создать дубликат. Всё равно отправить новое тестовое сообщение?',
      testSending:'Отправляем одно тестовое сообщение…', testSent:'Viber подтвердил приём тестового сообщения.',
      testRejected:'Viber отклонил тестовое сообщение. Автоматического повтора не будет.',
      testUnknown:'Результат тестового сообщения не подтверждён. Настройки обновлены; новая попытка может создать дубликат.',
      testUnknownNoRefresh:'Результат тестового сообщения не подтверждён, а состояние не обновлено. Не отправляйте тест повторно, пока не проверите статус.',
      testSendingState:'Тестовое сообщение ещё обрабатывается. Обновите состояние перед новой отправкой.',
      testConflict:'Настройки изменились. Состояние обновлено; тест автоматически не повторялся.',
      testConflictNoRefresh:'Настройки изменились, но состояние не удалось обновить. Тест не повторялся; обновите страницу перед новой попыткой.',
      testError:'Не удалось подтвердить результат тестового сообщения. Проверьте состояние перед новой попыткой.',
      testUnavailable:'Не удалось создать идентификатор тестового сообщения. Сообщение не отправлено.',
      noTest:'Тестовых сообщений пока нет.', testState_sent:'Отправлено и подтверждено',
      testState_rejected:'Отклонено', testState_unknown:'Результат не подтверждён', testState_sending:'Отправка выполняется',
      conflict:'Настройки изменились в другом окне. Обновите их перед повторной попыткой.', tokenSaved:'Токен сохранён. Проверьте канал перед подключением.',
      tokenSaveUnknown:'Результат сохранения токена не подтверждён. Обновите состояние перед повторной попыткой.', verified:'Канал проверен.',
      toggleRejected:'Изменение отклонено (HTTP 422). Настройки обновлены; проверьте состояние перед повторной попыткой.',
      toggleRejectedNoRefresh:'Изменение отклонено (HTTP 422), но не удалось обновить настройки. Обновите состояние перед новым действием.',
      toggleUnknown:'Результат изменения не подтверждён (HTTP {status}). Настройки обновлены; проверьте состояние перед повторной попыткой.',
      toggleUnknownNoRefresh:'Результат изменения не подтверждён (HTTP {status}). Не удалось обновить настройки; проверьте состояние вручную перед повторной попыткой.',
      conflictNoRefresh:'Настройки изменились в другом окне, но их не удалось обновить. Обновите состояние перед повторной попыткой.',
      verifyError:'Проверка канала не подтверждена.', connected:'Webhook зарегистрирован. Ожидается подтверждение входящего callback.',
      connectError:'Подключение webhook не подтверждено.', enabled:'Доставка в Viber включена для новых выпусков.', disabled:'Доставка в Viber отключена.',
      previewError:'Не удалось загрузить предпросмотр.', selectSender:'Выберите superadmin-отправителя',
      senderRequired:'Выберите отправителя и повторно проверьте канал.', notVerified:'Канал нужно проверить с текущим токеном.',
      webhookPending:'Ожидается подтверждение callback.', ready:'Готово', enabledStatus:'Включена', disabledStatus:'Выключена',
      verifiedAt:'Проверен', enabledFrom:'Включена с', lastDelivery:'Последняя доставка', noDelivery:'Доставок пока нет',
      part:'частей', partUnit:'единиц UTF-16', byteUnit:'байт JSON', previewPart:'Часть', budget:'Лимит',
      previewIdRequired:'Введите ID выпуска.', noSend:'Предпросмотр не отправляет сообщения.', retryLoad:'Повторить загрузку', errorStatus:'Состояние недоступно.'
    },
    'sr-Latn': {
      heading:'Viber · Srpski', loading:'Učitavanje podešavanja…', overview:'Status veze', credential:'Token kanala', channel:'Kanal', sender:'Pošiljalac',
      tokenLabel:'Token za kanal', tokenPlaceholder:'Unesite novi token', tokenHelp:'Token se čuva šifrovano. Vrednost se nikada ponovo ne prikazuje.',
      configured:'Token je sačuvan', missing:'Token nije podešen', saveToken:'Sačuvaj token', verify:'Proveri kanal', connect:'Poveži webhook',
      enable:'Uključi Viber dostavu', disable:'Isključi Viber dostavu', refresh:'Osveži', previewTitle:'Pregled sačuvanog izdanja',
      editionLabel:'ID srpskog izdanja', editionPlaceholder:'Unesite ID izdanja', preview:'Prikaži pregled',
      noPreview:'Unesite ID sačuvanog srpskog izdanja da biste videli pregled.',
      enableHint:'Dostava za nova izdanja počinje tek nakon uključivanja. Globalno zaustavljanje dostave važi i za Viber.',
      sourceHint:'Nazivi izvora su izostavljeni iz kratke objave; linkovi ostaju odvojeni.', nothingSent:'Pregled ne šalje poruke.',
      loadingError:'Podešavanja nisu učitana.', saveError:'Podešavanja nisu sačuvana.',
      testTitle:'Probna poruka', testButton:'Pošalji probnu poruku',
      testHelp:'Šalje jednu probnu poruku u ovaj kanal. Ne objavljuje dnevni pregled i ne menja automatsku dostavu.',
      testConfirmDuplicate:'Ishod prethodnog testa nije poznat. Novi pokušaj može poslati duplikat. Ipak poslati novu probnu poruku?',
      testSending:'Šalje se jedna probna poruka…', testSent:'Viber je potvrdio prijem probne poruke.',
      testRejected:'Viber je odbio probnu poruku. Automatsko ponavljanje neće biti izvršeno.',
      testUnknown:'Ishod probne poruke nije potvrđen. Podešavanja su osvežena; novi pokušaj može poslati duplikat.',
      testUnknownNoRefresh:'Ishod probne poruke nije potvrđen, a stanje nije osveženo. Ne šaljite test ponovo dok ne proverite status.',
      testSendingState:'Probna poruka se još obrađuje. Osvežite stanje pre novog slanja.',
      testConflict:'Podešavanja su promenjena. Stanje je osveženo; test nije automatski ponovljen.',
      testConflictNoRefresh:'Podešavanja su promenjena, ali stanje nije osveženo. Test nije ponovljen; osvežite stranicu pre novog pokušaja.',
      testError:'Ishod probne poruke nije potvrđen. Proverite stanje pre novog pokušaja.',
      testUnavailable:'Nije moguće napraviti ID probne poruke. Poruka nije poslata.',
      noTest:'Još nema probnih poruka.', testState_sent:'Poslato i potvrđeno',
      testState_rejected:'Odbijeno', testState_unknown:'Ishod nije potvrđen', testState_sending:'Slanje je u toku',
      conflict:'Podešavanja su promenjena drugde. Osvežite ih pre novog pokušaja.', tokenSaved:'Token je sačuvan. Proverite kanal pre povezivanja.',
      tokenSaveUnknown:'Ishod čuvanja tokena nije potvrđen. Osvežite status pre novog pokušaja.', verified:'Kanal je proveren.',
      toggleRejected:'Izmena je odbijena (HTTP 422). Podešavanja su osvežena; proverite stanje pre novog pokušaja.',
      toggleRejectedNoRefresh:'Izmena je odbijena (HTTP 422), ali podešavanja nisu osvežena. Osvežite stanje pre nove radnje.',
      toggleUnknown:'Ishod izmene nije potvrđen (HTTP {status}). Podešavanja su osvežena; proverite stanje pre novog pokušaja.',
      toggleUnknownNoRefresh:'Ishod izmene nije potvrđen (HTTP {status}). Podešavanja nisu osvežena; ručno proverite stanje pre novog pokušaja.',
      conflictNoRefresh:'Podešavanja su promenjena drugde, ali nisu ponovo učitana. Osvežite stanje pre novog pokušaja.',
      verifyError:'Provera kanala nije potvrđena.', connected:'Webhook je registrovan. Sačekajte potvrdu dolaznog callback-a.',
      connectError:'Webhook nije potvrđeno povezan.', enabled:'Viber dostava je uključena za nova izdanja.', disabled:'Viber dostava je isključena.',
      previewError:'Pregled nije učitan.', selectSender:'Izaberite superadmin pošiljaoca',
      senderRequired:'Izaberite pošiljaoca pa ponovo proverite kanal.', notVerified:'Kanal treba proveriti trenutnim tokenom.',
      webhookPending:'Čeka se potvrda callback-a.', ready:'Spremno', enabledStatus:'Uključeno', disabledStatus:'Isključeno',
      verifiedAt:'Proveren', enabledFrom:'Uključeno od', lastDelivery:'Poslednja dostava', noDelivery:'Još nema dostava',
      part:'delova', partUnit:'UTF-16 jedinica', byteUnit:'JSON bajtova', previewPart:'Deo', budget:'Budžet',
      previewIdRequired:'Unesite ID izdanja.', noSend:'Pregled ne šalje poruke.', retryLoad:'Pokušaj ponovo', errorStatus:'Status nije dostupan.'
    }
  };
  window.viberSettingsText = (key: string): string => viberLabels[locale][key] || key;
  const nativeFetch = window.fetch.bind(window);
  window.serbianCountForm = (number: number, one: string, few: string, many: string): string => {
    const n = Math.abs(Math.trunc(Number(number) || 0));
    const lastTwo = n % 100, last = n % 10;
    return lastTwo >= 11 && lastTwo <= 14 ? many : last === 1 ? one : last >= 2 && last <= 4 ? few : many;
  };
  const srErrors: Record<number, string> = {400:'Proverite unete podatke.',401:'Prijavite se ponovo.',403:'Nemate dozvolu za ovu radnju.',404:'Traženi podatak nije pronađen.',409:'Podaci su promenjeni. Osvežite ih i pokušajte ponovo.',422:'Proverite unete podatke.',429:'Previše zahteva. Pokušajte ponovo kasnije.',503:'Usluga trenutno nije dostupna.'};
  // Backend detail text is matched only against known, source-owned errors.
  // Unknown detail text is never displayed as Serbian presentation copy.
  const knownErrors: Record<string, string> = {
    'Article processing is disabled':'Obrada članaka je isključena.',
    'Telegram delivery is disabled':'Slanje na Telegram je isključeno.',
    'Configure AI before starting processing':'Podesite AI pre pokretanja obrade.',
    'Configure Telegram before starting delivery':'Podesite Telegram pre slanja.',
    'Integration configuration unavailable':'Podešavanja integracije trenutno nisu dostupna.',
    'Mapping was saved but private category cache invalidation failed; do not retry automatically':'Mapiranje je sačuvano, ali osvežavanje keša kategorija nije uspelo. Nemojte automatski ponavljati radnju.',
    'unsupported source type':'Ovaj tip izvora nije podržan.',
    'Article not found':'Članak nije pronađen.',
    'Digest not found':'Izdanje nije pronađeno.',
    'Public data unavailable':'Javni podaci trenutno nisu dostupni.',
    'Search query must be at least 2 characters':'Upit za pretragu mora da sadrži najmanje 2 znaka.',
  };
  const safeCode = (value: unknown): string => typeof value === 'string' && /^[A-Za-z0-9_.-]{1,64}$/.test(value) ? value : '';
  const statusText = (status: number, code: unknown, detail: string): string => (knownErrors[detail] || srErrors[status] || `Zahtev nije uspeo (HTTP ${status}).`) + (safeCode(code) ? ` [${code}]` : '');
  window.presentError = (error: unknown): string => {
    const info = error as {message?: string; code?: unknown} | null | undefined;
    if (locale !== 'sr-Latn') return info && info.message ? info.message : String(error || '');
    const text = info && info.message || '';
    const allowed = [...Object.values(srErrors), ...Object.values(knownErrors)].some(message => text === message || text.startsWith(message + ' [')) || /^Zahtev nije uspeo \(HTTP [0-9]{3}\)\.( \[[A-Za-z0-9_.-]{1,64}\])?$/.test(text);
    return allowed ? text : 'Ishod nije potvrđen. Proverite stanje pre ponovnog pokušaja.' + (safeCode(info?.code) ? ` [${safeCode(info?.code)}]` : '');
  };
  window.presentMessage = (message: string | undefined, code: unknown, success = false): unknown => locale === 'sr-Latn'
    ? (success ? 'Uspešno završeno.' : 'Ishod nije potvrđen. Proverite stanje pre ponovnog pokušaja.' + (safeCode(code) ? ` [${code}]` : ''))
    : (message || code || '');
  window.fetch = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const raw = input instanceof Request ? input.url : String(input);
    const url = new URL(raw, window.location.href);
    if (url.origin === location.origin && url.pathname.startsWith('/api/public/')) {
      url.searchParams.set('locale', locale);
      input = input instanceof Request ? new Request(url, input) : url.toString();
    }
    return nativeFetch(input, init).then(response => {
      if (locale !== 'sr-Latn' || url.origin !== location.origin || response.ok) return response;
      const sanitized = async () => {
        let data: Record<string, unknown> = {};
        try { const parsed = await response.clone().json(); if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) data = parsed as Record<string, unknown>; } catch (_) {}
        const code = safeCode(data.code) || safeCode(data.error_code);
        const message = statusText(response.status, code, typeof data.detail === 'string' ? data.detail : '');
        const copy: Record<string, unknown> & {detail: string; message: string} = {...data, detail: message, message};
        if (typeof copy.error === 'string' && !safeCode(copy.error)) copy.error = message;
        return copy;
      };
      return new Proxy(response, {get(target, property) {
        if (property === 'json') return sanitized;
        if (property === 'text') return async () => {
          const data = await sanitized();
          return (target.headers.get('content-type') || '').includes('json') ? JSON.stringify(data) : data.detail;
        };
        const value = Reflect.get(target, property, target);
        return typeof value === 'function' ? value.bind(target) : value;
      }});
    });
  };
  window.localizedURL = (raw: string): string => { const url = new URL(raw, location.href); if (url.origin === location.origin) url.searchParams.set('locale', locale); return url.toString(); };
  let dirty = false;
  const markEdited = (event: Event): void => { const el = event.target; if (el instanceof Element && el.matches('input:not([type=hidden]):not([type=button]):not([type=submit]), textarea, select') && !el.closest('[data-language-switch]')) dirty = true; };
  document.addEventListener('input', markEdited, true);
  document.addEventListener('change', markEdited, true);
  document.addEventListener('click', event => {
    const link = event.target instanceof Element ? event.target.closest<HTMLAnchorElement>('a[href]') : null;
    if (!link) return;
    const url = new URL(link.href, location.href);
    if (url.origin !== location.origin || !/^\/(?:$|list$|search$|article\/|digest\/|admin(?:\/|$)|setup$|login$)/.test(url.pathname)) return;
    if (!url.searchParams.has('locale')) { url.searchParams.set('locale', locale); link.href = url.toString(); }
  }, true);
  document.addEventListener('DOMContentLoaded', () => {
    const host = document.querySelector<HTMLElement>('[data-language-switch]');
    if (!host) return;
    host.classList.add('lang-switcher');
    host.setAttribute('role', 'group');
    host.setAttribute('aria-label', locale === 'ru' ? 'Язык интерфейса' : 'Jezik interfejsa');
    for (const [value, label, accessibleLabel] of [['ru', 'RU', 'Русский'], ['sr-Latn', 'SR', 'Srpski (latinica)']]) {
      const button = document.createElement('button');
      button.type = 'button';
      button.value = value;
      button.className = 'lang-btn' + (value === locale ? ' active' : '');
      button.textContent = label;
      button.setAttribute('lang', value);
      button.setAttribute('aria-label', accessibleLabel);
      button.setAttribute('aria-pressed', String(value === locale));
      button.addEventListener('click', () => {
        if (value === locale) return;
        if (dirty && !confirm(locale === 'ru' ? 'Есть несохранённые изменения. Сменить язык?' : 'Imate nesačuvane izmene. Promeniti jezik?')) return;
        let url = new URL(location.href);
        if (host.dataset.editionSwitch === 'true') {
          const target = value === 'ru' ? host.dataset.localeRu : host.dataset.localeSr;
          if (!target) { alert(locale === 'ru' ? 'Выпуск на выбранном языке недоступен.' : 'Izdanje na izabranom jeziku nije dostupno.'); return; }
          url = new URL(target, location.href);
        } else { url.searchParams.set('locale', value); }
        document.cookie = 'news_locale=' + encodeURIComponent(value) + '; Path=/; SameSite=Lax; Max-Age=31536000' + (location.protocol === 'https:' ? '; Secure' : '');
        location.assign(url.toString());
      });
      host.append(button);
    }
  });
})();
