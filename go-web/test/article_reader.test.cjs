const assert = require('node:assert/strict');
const {spawn} = require('node:child_process');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const {after, before, test} = require('node:test');
const {chromium} = require('playwright');

const repo = path.resolve(__dirname, '../..');
const helperPath = path.join(__dirname, `article_reader_fixture_${process.pid}.go`);
const helperSource = String.raw`package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/httpui"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/repositories"
)

type fixtureReader struct { base string; articleFiveCalls int }

func syntheticWAV() []byte {
	const samples = 800
	data := make([]byte, 44+samples)
	copy(data[0:4], "RIFF")
	binary.LittleEndian.PutUint32(data[4:8], uint32(len(data)-8))
	copy(data[8:12], "WAVE")
	copy(data[12:16], "fmt ")
	binary.LittleEndian.PutUint32(data[16:20], 16)
	binary.LittleEndian.PutUint16(data[20:22], 1)
	binary.LittleEndian.PutUint16(data[22:24], 1)
	binary.LittleEndian.PutUint32(data[24:28], 8000)
	binary.LittleEndian.PutUint32(data[28:32], 8000)
	binary.LittleEndian.PutUint16(data[32:34], 1)
	binary.LittleEndian.PutUint16(data[34:36], 8)
	copy(data[36:40], "data")
	binary.LittleEndian.PutUint32(data[40:44], samples)
	for i := range data[44:] { data[44+i] = 128 }
	return data
}

func (f *fixtureReader) media() []any {
	return []any{
		nil,
		"scalar media entry",
		42,
		map[string]any{},
		map[string]any{"type":"image", "url":f.base+"/media/first.png", "filename":"First image"},
		map[string]any{"type":"image", "url":f.base+"/media/broken.png", "filename":"Broken image"},
		map[string]any{"type":"image", "url":f.base+"/media/second.png", "thumbnail_url":f.base+"/media/thumb.png", "filename":"Second image"},
		map[string]any{"type":"image", "url":"javascript:alert(1)", "filename":"Unsafe image"},
		map[string]any{"type":"image", "url":"data:image/png;base64,AA==", "filename":"Data image"},
		map[string]any{"type":"image", "url":"https://user:pass@fixture.invalid/image.png", "filename":"Credential image"},
		map[string]any{"type":"video", "url":f.base+"/media/clip.mp4", "filename":"Direct clip"},
		map[string]any{"type":"video", "url":"https://video.example.invalid/watch?id=synthetic", "filename":"Provider clip"},
		map[string]any{"type":"audio", "url":f.base+"/media/sample.wav", "filename":"Synthetic audio"},
		map[string]any{"type":"document", "url":"https://files.example.invalid/report.pdf", "filename":"Synthetic document"},
		map[string]any{"type":"gif", "url":"https://media.example.invalid/loop.gif", "filename":"Synthetic GIF"},
		map[string]any{"type":"sticker", "url":"https://media.example.invalid/sticker.webp", "filename":"Synthetic sticker"},
		map[string]any{"type":"animated_sticker", "url":"https://media.example.invalid/sticker.tgs", "filename":"Synthetic animated sticker"},
		map[string]any{"type":"poll", "url":"https://media.example.invalid/poll", "filename":"Unsupported poll"},
		map[string]any{"type":"video", "url":"//video.example.invalid/protocol-relative.mp4", "filename":"Unsafe protocol relative"},
	}
}

func (f *fixtureReader) article(id int64) map[string]any {
	title := fmt.Sprintf("Synthetic article %d", id)
	summary := fmt.Sprintf("Synthetic summary %d", id)
	content := fmt.Sprintf("Synthetic body line %d.\nSecond synthetic paragraph %d.", id, id)
	item := map[string]any{"id":id, "title":title, "summary":summary, "content":content, "source_name":"Synthetic source", "url":f.base+"/original/"+fmt.Sprint(id), "published_at":"2026-10-31T10:00:00Z", "image_url":"", "primary_image":"", "media_files":[]any{}, "category":"Synthetic category", "categories":[]any{map[string]any{"name":"synthetic","display_name":"Synthetic category","color":"#345678"}}, "ai_categories":[]any{}, "is_advertisement":false, "ad_reasoning":"", "ad_markers":[]any{}}
	switch id {
	case 1:
		item["media_files"] = f.media()
	case 2:
		item["image_url"] = f.base+"/media/fallback.png"
	case 3:
		item["image_url"] = "javascript:alert(1)"
		item["url"] = "javascript:alert(1)"
	case 4:
		item["image_url"] = f.base+"/media/fallback.png"
		item["media_files"] = []any{map[string]any{"type":"image","url":f.base+"/media/broken.png","filename":"Broken only image"}}
	case 5:
		item["title"] = "Original title must not appear in missing translation"
	case 6:
		item["summary"] = ""
		item["content"] = ""
	}
	return item
}

func (f *fixtureReader) Feed(_ context.Context, q repositories.FeedQuery) (repositories.FeedResponse, error) { return repositories.FeedResponse{Articles:[]map[string]any{f.article(1)},Total:1,Limit:q.Limit,Offset:q.Offset},nil }
func (f *fixtureReader) Article(_ context.Context, id int64, _ bool) (map[string]any, error) {
	if id == 5 { f.articleFiveCalls++; if f.articleFiveCalls == 1 { return nil,nil } }
	if id < 1 || id > 6 { return nil,nil }
	return f.article(id),nil
}
func (*fixtureReader) CategoriesConfig(context.Context) (map[string]any,error) { return map[string]any{"categories":map[string]any{}},nil }
func (*fixtureReader) Categories(context.Context,repositories.StatsQuery) (map[string]any,error) { return map[string]any{"categories":map[string]int{"all":1},"total":1},nil }
func (*fixtureReader) Sources(context.Context,repositories.StatsQuery) (map[string]any,error) { return map[string]any{"sources":map[string]int{},"source_names":map[string]string{},"total":0},nil }
func (f *fixtureReader) Search(_ context.Context,q repositories.SearchQuery) (map[string]any,error) { return map[string]any{"articles":[]any{f.article(1)},"query":q.Query,"results_count":1},nil }
func (*fixtureReader) Digest(context.Context,time.Time) (repositories.DigestPage,error) { return repositories.DigestPage{},nil }
func (*fixtureReader) APIFeed(context.Context,repositories.APIFeedQuery) (repositories.APIFeedResponse,error) { return repositories.APIFeedResponse{},nil }
func (*fixtureReader) RSS(context.Context,int) ([]repositories.RSSArticle,error) { return []repositories.RSSArticle{},nil }
func (*fixtureReader) SupportsPublicLocale(locale string) bool { return locale == "sr-Latn" }

func main() {
	listener,err:=net.Listen("tcp","127.0.0.1:0"); if err!=nil { log.Fatal(err) }
	base:="http://"+listener.Addr().String()
	reader:=&fixtureReader{base:base}
	h:=httpui.New(httpui.Config{Reader:reader,TemplatesDir:"templates",StaticDir:"../web/static",SiteBaseURL:base})
	mux:=http.NewServeMux(); h.Register(mux)
	mux.HandleFunc("GET /media/",func(w http.ResponseWriter,r *http.Request) {
    if r.URL.Path=="/media/first.png" || r.URL.Path=="/media/second.png" || r.URL.Path=="/media/thumb.png" || r.URL.Path=="/media/fallback.png" { w.Header().Set("Content-Type","image/png"); _,_=w.Write([]byte{137,80,78,71,13,10,26,10,0,0,0,13,73,72,68,82,0,0,0,1,0,0,0,1,8,6,0,0,0,31,21,196,137,0,0,0,13,73,68,65,84,8,215,99,248,207,192,240,31,0,5,0,1,255,137,153,61,29,0,0,0,0,73,69,78,68,174,66,96,130}); return }
		if r.URL.Path=="/media/sample.wav" { w.Header().Set("Content-Type","audio/wav"); _,_=w.Write(syntheticWAV()); return }
		http.NotFound(w,r)
	})
	mux.HandleFunc("GET /original/",func(w http.ResponseWriter,r *http.Request){ w.Header().Set("Content-Type","text/plain"); _,_=w.Write([]byte("synthetic original")) })
	fmt.Println("ARTICLE_READER_FIXTURE_READY "+base)
	if err=http.Serve(listener,mux); err!=nil { log.Fatal(err) }
}
`;

let serverProcess;
let origin;
let browser;
let page;
let serverOutput = '';

function waitForServer() {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`Go fixture server did not start: ${serverOutput}`)), 60000);
    serverProcess.stdout.on('data', chunk => {
      serverOutput += chunk.toString();
      const match = serverOutput.match(/ARTICLE_READER_FIXTURE_READY (http:\/\/\S+)/);
      if (match) { clearTimeout(timeout); resolve(match[1]); }
    });
    serverProcess.stderr.on('data', chunk => { serverOutput += chunk.toString(); });
    serverProcess.once('exit', code => { clearTimeout(timeout); reject(new Error(`Go fixture server exited ${code}: ${serverOutput}`)); });
  });
}

before(async () => {
  assert.ok(fs.existsSync(path.join(repo, 'web/static/js/article-reader.js')), 'build the TypeScript browser assets before running this test');
  fs.writeFileSync(helperPath, helperSource);
  serverProcess = spawn('go', ['run', helperPath], {cwd: path.join(repo, 'go-web'), stdio: ['ignore', 'pipe', 'pipe'], detached: true});
  origin = await waitForServer();
  browser = await chromium.launch({headless: true, args: ['--no-sandbox']});
  page = await browser.newPage({viewport: {width: 1365, height: 900}, colorScheme: 'light'});
  await page.route('**/*', route => {
    const url = new URL(route.request().url());
    return url.origin === origin ? route.continue() : route.abort();
  });
  await page.route(`${origin}/media/broken.png`, route => route.fulfill({status: 404, contentType: 'text/plain', body: 'synthetic missing image'}));
});

after(async () => {
  if (browser) await browser.close();
  if (serverProcess) {
    try { process.kill(-serverProcess.pid, 'SIGTERM'); } catch {}
    if (serverProcess.exitCode === null) await new Promise(resolve => serverProcess.once('exit', resolve));
  }
  if (fs.existsSync(helperPath)) fs.rmSync(helperPath, {force: true});
});

async function openArticle(id) {
  await page.evaluate(articleId => window.openArticleModal(articleId), id);
  await page.locator('#modalTitle').filter({hasText: `Synthetic article ${id}`}).waitFor();
}

test('feed and search modals render the same ordered article content and local media', async () => {
  await page.goto(`${origin}/?locale=ru`);
  await page.locator('#news-list .news-list-item').first().waitFor();
  await openArticle(1);
  await page.locator('#galleryImages img').first().waitFor();
  await page.waitForFunction(() => [...document.querySelectorAll('#galleryImages img')].some(img => img.complete && img.naturalWidth > 0));
  const modal = await page.evaluate(() => ({
    title: document.querySelector('#modalTitle').textContent,
    summary: document.querySelector('#modalSummary').innerText,
    content: document.querySelector('#modalContent').textContent,
    images: [...document.querySelectorAll('#galleryImages img')].map(img => ({src: new URL(img.src).pathname, alt: img.alt, loaded: img.complete && img.naturalWidth > 0})),
    videos: [...document.querySelectorAll('#articleMediaLinks video')].map(v => ({src: new URL(v.src).pathname, controls: v.controls, autoplay: v.autoplay})),
    audios: [...document.querySelectorAll('#articleMediaLinks audio')].map(a => ({src: new URL(a.src).pathname, controls: a.controls, autoplay: a.autoplay})),
    links: [...document.querySelectorAll('#articleMediaLinks > a')].map(a => ({text: a.textContent, href: a.href, rel: a.rel})),
    category: document.querySelector('#modalCategory').textContent,
    categoryColor: getComputedStyle(document.querySelector('#modalCategory')).backgroundColor,
    sourceLink: document.querySelector('#readerSourceLink a')?.href || '',
  }));
  assert.equal(modal.title, 'Synthetic article 1');
  assert.equal(modal.category, 'Synthetic category');
  assert.equal(modal.sourceLink, `${origin}/original/1`);
  assert.equal(modal.summary, 'Synthetic summary 1');
  assert.match(modal.content, /Synthetic body line 1/);
  assert.deepEqual(modal.images.map(i => i.src), ['/media/first.png', '/media/second.png'], 'broken image is removed while surviving image order is preserved');
  assert.deepEqual(await page.locator('#galleryThumbnails img').evaluateAll(imgs => imgs.map(img => new URL(img.src).pathname)), ['/media/first.png', '/media/thumb.png'], 'thumbnail_url is preserved for the thumbnail without replacing the full-resolution image');
  assert.ok(modal.images.every(i => i.loaded), 'gallery images must be actual successfully loaded local resources');
  assert.deepEqual(modal.videos, [{src: '/media/clip.mp4', controls: true, autoplay: false}]);
  assert.deepEqual(modal.audios, [{src: '/media/sample.wav', controls: true, autoplay: false}]);
  await page.locator('#articleMediaLinks audio').evaluate(a => a.load());
  await page.waitForFunction(() => document.querySelector('#articleMediaLinks audio').readyState >= 1);
  await page.locator('#articleMediaLinks video').evaluate(v => v.load());
  await page.locator('#articleMediaLinks a[href$="/media/clip.mp4"]').waitFor();
  assert.equal(await page.locator('#articleMediaLinks video').count(), 0, 'failed native video becomes its safe fallback link');
  assert.ok(await page.locator('#articleMediaLinks audio').evaluate(a => a.readyState >= 1), 'local WAV should load metadata');
  assert.deepEqual(modal.links.map(x => x.text), ['Provider clip', 'Synthetic document', 'Synthetic GIF', 'Synthetic sticker', 'Synthetic animated sticker']);
  assert.ok(modal.links.every(x => x.rel.includes('noopener')));
  assert.equal(modal.links.some(x => /Unsupported poll|Unsafe|Credential/.test(x.text)), false);
  assert.equal(await page.locator('#articleMediaLinks').innerText().then(t => /javascript:|protocol-relative/.test(t)), false);

  await page.goto(`${origin}/search?locale=ru&q=synthetic`);
  await openArticle(1);
  await page.waitForFunction(() => ![...document.querySelectorAll('#galleryImages img')].some(img => img.src.includes('/media/broken.png')));
  const searchModal = await page.evaluate(() => ({
    title: document.querySelector('#modalTitle').textContent,
    summary: document.querySelector('#modalSummary').innerText,
    content: document.querySelector('#modalContent').textContent,
    images: [...document.querySelectorAll('#galleryImages img')].map(img => new URL(img.src).pathname),
    media: [...document.querySelectorAll('#articleMediaLinks > *')].map(el => {
      const url = new URL(el.src || el.href);
      return `${el.tagName.toLowerCase()}:${url.origin === location.origin ? `${url.pathname}${url.search}` : url.href}`;
    }),
  }));
  assert.deepEqual(searchModal, {
    title: modal.title,
    summary: modal.summary,
    content: modal.content,
    images: ['/media/first.png', '/media/second.png'],
    media: ['video:/media/clip.mp4', 'a:https://video.example.invalid/watch?id=synthetic', 'audio:/media/sample.wav', 'a:https://files.example.invalid/report.pdf', 'a:https://media.example.invalid/loop.gif', 'a:https://media.example.invalid/sticker.webp', 'a:https://media.example.invalid/sticker.tgs'],
  });
  await page.goto(`${origin}/article/1?locale=ru`);
  assert.equal(await page.locator('#modalCategory').evaluate(el => getComputedStyle(el).backgroundColor), modal.categoryColor, 'standalone and modal category styling match');
});

test('standalone SSR uses the same fragment, safe links, media ordering and no-JS content', async () => {
  const noJS = await browser.newPage({javaScriptEnabled: false});
  for (const locale of ['ru', 'sr-Latn']) {
    const response = await noJS.goto(`${origin}/article/1?locale=${locale}`);
    assert.equal(response.status(), 200);
    assert.equal(await noJS.locator('[data-article-reader]').count(), 1);
    assert.equal(await noJS.locator('#modalTitle').innerText(), 'Synthetic article 1');
    assert.equal(await noJS.locator('#modalSummary').innerText(), 'Synthetic summary 1');
    assert.match(await noJS.locator('#modalContent').textContent(), /Synthetic body line 1/);
    assert.match((await noJS.locator('#modalCategory').innerText()).toLowerCase(), /synthetic category/);
    assert.deepEqual(await noJS.locator('#galleryImages img').evaluateAll(imgs => imgs.map(img => new URL(img.src).pathname)), ['/media/first.png', '/media/broken.png', '/media/second.png']);
    assert.deepEqual(await noJS.locator('#galleryThumbnails img').evaluateAll(imgs => imgs.map(img => new URL(img.src).pathname)), ['/media/first.png', '/media/broken.png', '/media/thumb.png']);
    assert.deepEqual(await noJS.locator('#articleMediaLinks > *').evaluateAll(nodes => nodes.map(el => { const url = new URL(el.src || el.href); return `${el.tagName.toLowerCase()}:${url.protocol === 'https:' ? url.href : `${url.pathname}${url.search}`}`; })), ['video:/media/clip.mp4', 'a:https://video.example.invalid/watch?id=synthetic', 'audio:/media/sample.wav', 'a:https://files.example.invalid/report.pdf', 'a:https://media.example.invalid/loop.gif', 'a:https://media.example.invalid/sticker.webp', 'a:https://media.example.invalid/sticker.tgs']);
    assert.equal(await noJS.locator('a[href^="javascript:"]').count(), 0);
    if (locale === 'sr-Latn') {
      assert.equal(await noJS.locator('html').getAttribute('lang'), 'sr-Latn');
      assert.equal(await noJS.locator('#modalContent').getAttribute('lang'), 'und');
      assert.equal(await noJS.locator('#modalContentSpoiler h2').count(), 1);
    }
  }
  const missing = await noJS.goto(`${origin}/article/5?locale=sr-Latn`);
  assert.equal(missing.status(), 404);
  assert.match(await noJS.locator('body').innerText(), /Prevod nije dostupan/);
  assert.doesNotMatch(await noJS.locator('body').innerText(), /Original title must not appear/);
  const noJSResponse = await noJS.goto(`${origin}/article/1?locale=ru`);
  assert.equal(noJSResponse.status(), 200);
  assert.match(await noJS.locator('#modalContent').textContent(), /Synthetic body line 1/);
  assert.equal(await noJS.locator('#modalContentSpoiler').evaluate(el => el.open), false);
  await noJS.locator('#modalContentSpoiler > summary').focus();
  await noJS.keyboard.press('Enter');
  assert.equal(await noJS.locator('#modalContentSpoiler').evaluate(el => el.open), true, 'SSR original text is keyboard-accessible without JavaScript');
  assert.equal(await noJS.locator('#modalContentSpoiler').evaluate(el => getComputedStyle(el).display), 'block');
  await noJS.close();
});

test('empty, fallback and unsafe media states are cleared across reopen; Escape and keyboard controls work', async () => {
  await page.goto(`${origin}/?locale=ru`);
  await page.locator('#news-list .news-list-item').first().waitFor();
  const trigger = page.locator('.btn-read-more').first();
  await page.evaluate(() => { document.body.style.overflow = 'scroll'; });
  await trigger.focus();
  await page.keyboard.press('Enter');
  await page.locator('#modalTitle').filter({hasText: 'Synthetic article 1'}).waitFor();
  assert.equal(await page.evaluate(() => document.activeElement.className), 'modal-close', 'opening moves focus to the close control');
  await page.keyboard.press('Tab');
  assert.ok(await page.locator('#articleModal').evaluate(el => el.contains(document.activeElement)), 'Tab stays within the dialog');
  await page.keyboard.press('Escape');
  await page.locator('#articleModal').waitFor({state: 'hidden'});
  assert.equal(await trigger.evaluate(el => document.activeElement === el), true, 'closing restores focus to the opener');
  assert.equal(await page.evaluate(() => document.body.style.overflow), 'scroll', 'closing restores previous body overflow');
  await openArticle(2);
  assert.deepEqual(await page.locator('#galleryImages img').evaluateAll(imgs => imgs.map(img => new URL(img.src).pathname)), ['/media/fallback.png']);
  await page.locator('#galleryImages img').first().waitFor();
  await openArticle(1);
  await page.waitForFunction(() => ![...document.querySelectorAll('#galleryImages img')].some(img => img.src.includes('/media/broken.png')));
  const secondImageThumbnail = page.locator('#galleryThumbnails button[aria-label="Second image"]');
  await secondImageThumbnail.waitFor();
  await secondImageThumbnail.focus();
  await page.keyboard.press('Enter');
  assert.ok(await secondImageThumbnail.evaluate(el => el.classList.contains('active')), 'thumbnail selection is keyboard operable');
  assert.match(await page.locator('#galleryImages img.active').getAttribute('src'), /\/media\/second\.png$/);
  await page.keyboard.press('ArrowLeft');
  assert.ok(await page.locator('#galleryThumbnails button[aria-label="First image"]').evaluate(el => el.classList.contains('active')), 'gallery arrows are keyboard-operable');
  assert.match(await page.locator('#galleryImages img.active').getAttribute('src'), /\/media\/first\.png$/);
  await page.keyboard.press('Escape');
  await page.locator('#articleModal').waitFor({state: 'hidden'});
  await openArticle(3);
  assert.equal(await page.locator('#modalGallery').evaluate(el => getComputedStyle(el).display), 'none');
  assert.equal(await page.locator('#readerSourceLink a').count(), 0, 'unsafe javascript image/source URLs are not exposed as links');
  await page.locator('.modal-close').focus();
  await page.keyboard.press('Enter');
  await page.locator('#articleModal').waitFor({state: 'hidden'});
  await openArticle(1);
  assert.equal(await page.locator('#modalTitle').innerText(), 'Synthetic article 1', 'modal can be reopened after keyboard close');
  await openArticle(4);
  await page.waitForFunction(() => [...document.querySelectorAll('#galleryImages img')].some(img => img.complete && img.naturalWidth > 0));
  assert.deepEqual(await page.locator('#galleryImages img').evaluateAll(imgs => imgs.map(img => new URL(img.src).pathname)), ['/media/fallback.png'], 'when every listed image fails, a valid image_url fallback is shown');
  await openArticle(6);
  assert.equal(await page.locator('#modalContentSpoiler').evaluate(el => getComputedStyle(el).display), 'none', 'empty content keeps its spoiler hidden');
});

test('late response from article A cannot overwrite article B; mobile light and dark layouts stay within the viewport', async () => {
  await page.goto(`${origin}/?locale=ru`);
  await page.locator('#news-list .news-list-item').first().waitFor();
  let releaseA;
  let notifyA;
  const routeAReady = new Promise(resolve => { notifyA = resolve; });
  const delayedA = new Promise(resolve => { releaseA = resolve; });
  const matchA = url => url.origin === origin && url.pathname === '/api/public/article/1';
  await page.route(matchA, async route => {
    notifyA();
    await delayedA;
    await route.continue();
  });
  const requestA = page.evaluate(() => window.openArticleModal(1));
  await routeAReady;
  await page.waitForFunction(() => document.querySelector('#articleModal').style.display === 'flex');

  let releaseB;
  let notifyB;
  const routeBReady = new Promise(resolve => { notifyB = resolve; });
  const delayedB = new Promise(resolve => { releaseB = resolve; });
  const matchB = url => url.origin === origin && url.pathname === '/api/public/article/2';
  await page.route(matchB, async route => {
    notifyB();
    await delayedB;
    await route.continue();
  });
  const requestB = page.evaluate(() => window.openArticleModal(2));
  await routeBReady;
  assert.equal(await page.locator('#modalTitle').innerText(), 'Загрузка...');
  assert.equal(await page.locator('#modalSource').innerText(), 'Загрузка...');
  assert.equal(await page.locator('#modalCategory').innerText(), '', 'old category is cleared while the next article loads');
  assert.equal(await page.locator('#readerSourceLink a').count(), 0, 'old source link is cleared while the next article loads');
  assert.equal(await page.locator('#articleMediaLinks > *').count(), 0, 'old media is cleared while the next article loads');
  releaseB();
  await page.locator('#modalTitle').filter({hasText: 'Synthetic article 2'}).waitFor();
  releaseA();
  await Promise.all([requestA, requestB]);
  await page.unroute(matchA);
  await page.unroute(matchB);
  await page.waitForTimeout(100);
  assert.equal(await page.locator('#modalTitle').innerText(), 'Synthetic article 2');

  let releaseOld;
  let notifyOld;
  const oldReady = new Promise(resolve => { notifyOld = resolve; });
  const delayedOld = new Promise(resolve => { releaseOld = resolve; });
  let firstArticleOne = true;
  const sameArticle = url => url.origin === origin && url.pathname === '/api/public/article/1';
  await page.route(sameArticle, async route => {
    if (!firstArticleOne) return route.continue();
    firstArticleOne = false;
    notifyOld();
    await delayedOld;
    await route.fulfill({status: 200, contentType: 'application/json', body: JSON.stringify({id:1,title:'Stale closed response',summary:'Stale summary',content:'Stale body',media_files:[],categories:[],ai_categories:[]})});
  });
  const closedRequest = page.evaluate(() => window.openArticleModal(1));
  await oldReady;
  await page.keyboard.press('Escape');
  await page.locator('#articleModal').waitFor({state: 'hidden'});
  await openArticle(1);
  releaseOld();
  await closedRequest;
  assert.equal(await page.locator('#modalTitle').innerText(), 'Synthetic article 1', 'a response from a closed dialog cannot overwrite a same-ID reopen');
  await page.unroute(sameArticle);

  const matchFailure = url => url.origin === origin && url.pathname === '/api/public/article/3';
  await page.route(matchFailure, route => route.fulfill({status: 503, contentType: 'application/json', body: JSON.stringify({detail: 'synthetic unavailable'})}));
  await page.evaluate(() => window.openArticleModal(3));
  await page.locator('#modalSummary').getByText(/Ошибка загрузки статьи/).waitFor();
  assert.equal(await page.locator('#modalCategory').innerText(), '', 'failed load does not retain the previous category');
  assert.equal(await page.locator('#readerSourceLink a').count(), 0, 'failed load does not retain the previous source link');
  assert.equal(await page.locator('#articleMediaLinks > *').count(), 0, 'failed load does not retain the previous media');
  await page.unroute(matchFailure);

  for (const mode of ['light', 'dark']) {
    await page.setViewportSize({width: 375, height: 812});
    await page.emulateMedia({colorScheme: mode});
    await page.evaluate(value => {
      if (value === 'dark') document.documentElement.setAttribute('dark-mode', '');
      else document.documentElement.removeAttribute('dark-mode');
    }, mode);
    await page.evaluate(() => window.closeArticleModal());
    await openArticle(1);
    const geometry = await page.evaluate(() => ({
      documentWidth: document.documentElement.scrollWidth,
      viewportWidth: innerWidth,
      modalWidth: document.querySelector('#articleModal .modal-content').getBoundingClientRect().width,
      background: getComputedStyle(document.querySelector('#articleModal .modal-content')).backgroundColor,
      rootDark: document.documentElement.hasAttribute('dark-mode'),
    }));
    assert.ok(geometry.documentWidth <= geometry.viewportWidth, `${mode} mobile layout should not overflow horizontally`);
    assert.ok(geometry.modalWidth <= geometry.viewportWidth, `${mode} modal should fit the viewport width`);
    assert.notEqual(geometry.background, 'rgba(0, 0, 0, 0)');
    assert.equal(geometry.rootDark, mode === 'dark');
  }
});

test('admin refresh checks, jobs and duplicate clicks stay bound to the article that started them', async () => {
  let adminChecks = 0;
  let holdPreflight;
  let notifyPreflight;
  let preflightPending = Promise.resolve();
  let preflightStarted = Promise.resolve();
  const adminMatch = url => url.origin === origin && url.pathname === '/api/public/admin-check';
  const adminRoute = async route => {
    adminChecks++;
    if (adminChecks === 2) {
      preflightStarted = new Promise(resolve => { notifyPreflight = resolve; });
      preflightPending = new Promise(resolve => { holdPreflight = resolve; });
      notifyPreflight();
      await preflightPending;
    }
    await route.fulfill({status: 200, contentType: 'application/json', body: JSON.stringify({admin: true})});
  };
  await page.route(adminMatch, adminRoute);
  await page.goto(`${origin}/?locale=ru`);
  await page.locator('#news-list .news-list-item').first().waitFor();
  await page.waitForFunction(() => window.adminAccessVerified === true);
  await openArticle(1);

  const reprocesses = [];
  const reprocessMatch = url => url.origin === origin && url.pathname.endsWith('/reprocess');
  let notifyJob;
  let releaseJob;
  const jobStarted = new Promise(resolve => { notifyJob = resolve; });
  const jobPending = new Promise(resolve => { releaseJob = resolve; });
  await page.route(reprocessMatch, async route => {
    reprocesses.push({url: new URL(route.request().url()).pathname, body: route.request().postDataJSON()});
    if (reprocesses.length === 1) {
      await route.fulfill({status: 202, contentType: 'application/json', body: JSON.stringify({job_id: 'synthetic-job', status_url: '/synthetic-job-status'})});
    } else {
      await route.fulfill({status: 200, contentType: 'application/json', body: JSON.stringify({success: true, status: 'succeeded'})});
    }
  });
  await page.route(url => url.origin === origin && url.pathname === '/synthetic-job-status', async route => {
    notifyJob();
    await jobPending;
    await route.fulfill({status: 200, contentType: 'application/json', body: JSON.stringify({status: 'succeeded', result: {status: 'completed', success: true}})});
  });

  const staleRefresh = page.evaluate(() => window.refreshArticle());
  await preflightStarted;
  await openArticle(2);
  holdPreflight();
  await staleRefresh;
  assert.equal(reprocesses.length, 0, 'a delayed admin check for article A cannot submit after article B opens');
  assert.equal(await page.locator('#modalTitle').innerText(), 'Synthetic article 2');

  await openArticle(1);
  const acceptedRefresh = page.evaluate(() => window.refreshArticle());
  await jobStarted;
  assert.equal(reprocesses.length, 1);
  assert.deepEqual(reprocesses[0], {url: '/api/public/article/1/reprocess', body: {force: true, refetch_content: true}});
  await openArticle(2);
  releaseJob();
  await acceptedRefresh;
  await page.waitForTimeout(2200);
  assert.equal(await page.locator('#modalTitle').innerText(), 'Synthetic article 2', 'late job completion cannot relabel or reopen article B');
  assert.equal(await page.locator('#modalRefreshBtn').innerText(), '🔄 Обновить статью');

  await openArticle(1);
  const duplicateRefresh = page.evaluate(() => window.refreshArticle());
  await page.waitForFunction(() => document.querySelector('#modalRefreshBtn').disabled);
  const duplicateCall = page.evaluate(() => window.refreshArticle());
  await Promise.all([duplicateRefresh, duplicateCall]);
  assert.equal(reprocesses.length, 2, 'a second refresh click while pending does not submit a duplicate');
  assert.equal(reprocesses[1].url, '/api/public/article/1/reprocess', 'the authorized refresh targets only article A');
  await page.evaluate(() => window.closeArticleModal());
  await page.unroute(adminMatch);
  await page.unroute(reprocessMatch);
});
