interface Window {
  serbianCountForm: (value: number, one: string, few: string, many: string) => string;
  presentError: (error: unknown) => string;
  presentMessage: (message: string | undefined, code: unknown, success?: boolean) => unknown;
  localizedURL: (raw: string) => string;
}

(() => {
  const locale = document.documentElement.lang === 'sr-Latn' ? 'sr-Latn' : 'ru';
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
    const select = document.createElement('select');
    select.setAttribute('aria-label', locale === 'ru' ? 'Язык интерфейса' : 'Jezik interfejsa');
    select.style.cssText = 'font:inherit;color:inherit;background:var(--surface,transparent);border:1px solid currentColor;border-radius:4px;padding:.2rem';
    for (const [value, label] of [['ru','Русский'], ['sr-Latn','Srpski']]) { const option = document.createElement('option'); option.value=value; option.textContent=label; select.append(option); }
    select.value=locale;
    select.addEventListener('change', () => {
      if (dirty && !confirm(locale === 'ru' ? 'Есть несохранённые изменения. Сменить язык?' : 'Imate nesačuvane izmene. Promeniti jezik?')) { select.value=locale; return; }
      let url = new URL(location.href);
      if (host.dataset.editionSwitch === 'true') {
        const target = select.value === 'ru' ? host.dataset.localeRu : host.dataset.localeSr;
        if (!target) { alert(locale === 'ru' ? 'Выпуск на выбранном языке недоступен.' : 'Izdanje na izabranom jeziku nije dostupno.'); select.value=locale; return; }
        url = new URL(target, location.href);
      } else { url.searchParams.set('locale', select.value); }
      document.cookie = 'news_locale=' + encodeURIComponent(select.value) + '; Path=/; SameSite=Lax; Max-Age=31536000' + (location.protocol === 'https:' ? '; Secure' : '');
      location.assign(url.toString());
    });
    host.append(select);
  });
})();
