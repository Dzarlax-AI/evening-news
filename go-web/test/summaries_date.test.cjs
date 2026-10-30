const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

process.env.TZ = 'Pacific/Kiritimati';

const template = fs.readFileSync(
    path.join(__dirname, '../templates/admin/summaries.html'),
    'utf8',
);
const inlineScript = [...template.matchAll(/<script>([\s\S]*?)<\/script>/g)].at(-1)?.[1];
assert.ok(inlineScript, 'summaries page should include its admin script');

function pageFixture(locale) {
    const utcInstant = '2026-10-27T23:30:00Z';
    class FixedDate extends Date {
        constructor(...args) {
            super(...(args.length ? args : [utcInstant]));
        }
    }

    let ready = false;
    const statusURLs = [];
    const generationURLs = [];
    const nodes = {
        'date-filter': {value: ''},
        'generate-summaries-button': {disabled: false},
        'editorial-generation-state': {dataset: {}, textContent: ''},
        'editorial-status-message': {textContent: ''},
        'editorial-preview-link': {
            hidden: false,
            removeAttribute(name) {
                if (name === 'href') delete this.href;
            },
        },
    };
    const section = {dataset: {}};
    const context = {
        Date: FixedDate,
        console,
        document: {
            documentElement: {lang: locale},
            addEventListener() {},
            getElementById(id) { return nodes[id] || {value: '', style: {}}; },
            querySelector(selector) {
                return selector === '.sum-editorial-status' ? section : null;
            },
        },
        fetch: async url => {
            statusURLs.push(url);
            const date = new URL(url, 'http://fixture').searchParams.get('date');
            return {
                ok: true,
                status: 200,
                json: async () => ({
                    date,
                    locale,
                    ready,
                    edition_id: ready ? '8c93bb34-383d-449c-9e85-28aa9121a4fe' : undefined,
                    preview_url: ready ? `/digest/${date}?locale=${locale}&edition=8c93bb34-383d-449c-9e85-28aa9121a4fe` : undefined,
                    main_count: ready ? 1 : 0,
                    brief_count: ready ? 2 : 0,
                    article_count: ready ? 3 : 0,
                }),
            };
        },
        URL,
        Request,
        location: {href: 'http://fixture/admin/summaries', origin: 'http://fixture', protocol: 'http:'},
        window: {addEventListener() {}},
    };
    context.window.location = context.location;
    context.window.fetch = context.fetch;

    vm.createContext(context);
    const localeScript = fs.readFileSync(path.join(__dirname, '../../web/static/js/locale.js'), 'utf8');
    const mainScript = fs.readFileSync(path.join(__dirname, '../../web/static/js/main.js'), 'utf8');
    vm.runInContext(localeScript, context);
    context.fetch = context.window.fetch;
    vm.runInContext(mainScript, context);
    context.window.NewsAggregator.apiRequest = async (url, options) => {
        generationURLs.push(url);
        options.onJobUpdate?.({status: 'succeeded'});
        ready = true;
        return {status: 'succeeded', success: true};
    };
    context.window.NewsAggregator.showNotification = () => {};
    vm.runInContext(inlineScript, context);
    context.loadSummaries = async () => {};
    return {context, nodes, section, statusURLs, generationURLs};
}

for (const locale of ['ru', 'sr-Latn']) {
    test(`default status and successful generation use the backend UTC edition date (${locale})`, async () => {
        const {context, nodes, section, statusURLs, generationURLs} = pageFixture(locale);
        assert.equal(new Date('2026-10-27T23:30:00Z').getDate(), 28, 'fixture should be on the next local day');

        await vm.runInContext('loadEditorialStatus()', context);
        assert.match(statusURLs[0], new RegExp(`date=2026-10-27&locale=${locale}`));
        assert.equal(section.dataset.state, 'missing');
        assert.equal(nodes['editorial-preview-link'].hidden, true);

        await vm.runInContext('generateDailySummaries()', context);
        assert.deepEqual(generationURLs, ['/api/v1/summaries/generate']);
        assert.equal(nodes['date-filter'].value, '2026-10-27');
        assert.match(statusURLs[1], new RegExp(`date=2026-10-27&locale=${locale}`));
        assert.equal(section.dataset.state, 'ready');
        assert.equal(nodes['editorial-preview-link'].href, `/digest/2026-10-27?locale=${locale}&edition=8c93bb34-383d-449c-9e85-28aa9121a4fe`);
    });
}
