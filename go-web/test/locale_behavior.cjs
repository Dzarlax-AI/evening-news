const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

(async () => {
  const listeners = {};
  let selection, confirmed = 0, navigated = '';
  const host = {dataset:{}, append(el){selection=el;}};
  class Element { constructor(editable=false){this.editable=editable;} matches(){return this.editable;} closest(){return null;} }
  const document = {
    documentElement:{lang:'sr-Latn'}, cookie:'',
    addEventListener(name, fn){(listeners[name] ||= []).push(fn);},
    querySelector(){return host;},
    createElement(tag){return {tag, style:{}, children:[], events:{}, setAttribute(){}, append(child){this.children.push(child);}, addEventListener(name,fn){this.events[name]=fn;}};},
  };
  const location = {href:'https://example.test/admin/settings?tab=telegram', origin:'https://example.test', protocol:'https:', assign(url){navigated=url;}};
  let payload = {detail:'Mapping was saved but private category cache invalidation failed; do not retry automatically',code:'cache_invalidation_failed',user_value:'Исходный пользовательский текст'};
  const window = {location, fetch:async()=>new Response(JSON.stringify(payload),{status:503,headers:{'content-type':'application/json'}})};
  const context = vm.createContext({window,document,location,Element,Request,Response,URL,console,confirm:()=>{confirmed++;return false;},alert:()=>{}});
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../../web/static/js/locale.js'),'utf8'),context);
  for (const [n,expected] of [[0,'rezultata'],[1,'rezultat'],[2,'rezultata'],[4,'rezultata'],[5,'rezultata'],[11,'rezultata'],[14,'rezultata'],[21,'rezultat'],[22,'rezultata'],[111,'rezultata']]) assert.equal(window.serbianCountForm(n,'rezultat','rezultata','rezultata'),expected);
  for (const [n,expected] of [[1,'članak'],[2,'članka'],[4,'članka'],[5,'članaka'],[11,'članaka'],[21,'članak'],[22,'članka']]) assert.equal(window.serbianCountForm(n,'članak','članka','članaka'),expected);
  const response = await window.fetch('/api/v1/configuration/operational');
  const data = await response.json();
  assert.match(data.detail,/Nemojte automatski ponavljati/);
  assert.equal(data.code,'cache_invalidation_failed');
  assert.equal(data.user_value,'Исходный пользовательский текст');
  payload = {detail:'Unknown private raw detail',error_code:'fixture_code'};
  const other = await (await window.fetch('/api/v1/configuration/operational')).json();
  assert.doesNotMatch(other.detail,/Unknown private raw detail/);
  assert.match(other.detail,/fixture_code/);
  assert.doesNotMatch(window.presentError(new Error('raw network error')),/raw network error/);
  for(const fn of listeners.DOMContentLoaded) fn();
  for(const fn of listeners.input) fn({target:new Element(true)});
  selection.value='ru';selection.events.change();
  assert.equal(confirmed,1);assert.equal(navigated,'');assert.equal(document.cookie,'');
  let pairedSelection, pairedNavigation = '', missingAlerts = 0;
  const pairedListeners = {};
  const pairedHost = {dataset:{editionSwitch:'true',localeRu:'/digest/2026-10-26?edition=ru-id&locale=ru',localeSr:'/digest/2026-10-26?edition=sr-id&locale=sr-Latn'},append(el){pairedSelection=el;}};
  const pairedDocument = {...document, cookie:'', documentElement:{lang:'ru'}, querySelector(){return pairedHost;},addEventListener(name,fn){(pairedListeners[name] ||= []).push(fn);}};
  const pairedLocation = {href:'https://example.test/digest/2026-10-26?edition=ru-id',origin:'https://example.test',protocol:'https:',assign(url){pairedNavigation=url;}};
  const pairedWindow = {location:pairedLocation,fetch:window.fetch};
  const pairedContext = vm.createContext({window:pairedWindow,document:pairedDocument,location:pairedLocation,Element,Request,Response,URL,console,confirm:()=>true,alert:()=>{missingAlerts++;}});
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../../web/static/js/locale.js'),'utf8'),pairedContext);
  for (const fn of pairedListeners.DOMContentLoaded) fn();
  pairedSelection.value='sr-Latn';pairedSelection.events.change();
  assert.equal(pairedNavigation,'https://example.test/digest/2026-10-26?edition=sr-id&locale=sr-Latn');
  assert.match(pairedDocument.cookie,/news_locale=sr-Latn/);
  pairedNavigation='';pairedDocument.cookie='';pairedHost.dataset.localeSr='';pairedSelection.value='sr-Latn';pairedSelection.events.change();
  assert.equal(missingAlerts,1);assert.equal(pairedNavigation,'');assert.equal(pairedDocument.cookie,'');
})().catch(error=>{console.error(error);process.exit(1);});
