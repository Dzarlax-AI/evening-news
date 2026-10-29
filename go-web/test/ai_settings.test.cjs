const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

test('AI form preserves both profiles, sends revision and handles conflicts', async () => {
  const template = fs.readFileSync('templates/admin/settings.html', 'utf8');
  const script = template.match(/<script>([\s\S]*?)<\/script>/)[1];
  const els = new Map();
  const get = id => { if (!els.has(id)) els.set(id, {value:'', textContent:'', disabled:false, hidden:false}); return els.get(id); };
  const settings = {revision:7, active_provider:'gemini', source:'env', configured:{gemini:true,openai:true}, profiles:{gemini:{models:{article:'ga',digest:'gd',text:'gt'}},openai:{models:{article:'oa',digest:'od',text:'ot'}}}};
  let sent; let conflict = false;
  const ctx = {document:{getElementById:get, addEventListener(){}}, window:{readApiResponse: async r => r.data}, fetch:async (url,opts) => { assert.equal(url,'/api/v1/ai/settings'); if (!opts) return {ok:true,data:settings}; sent=JSON.parse(opts.body); return conflict ? {ok:false,status:409} : {ok:true,data:{...settings,revision:8,source:'override',active_provider:sent.active_provider,profiles:sent.profiles}}; }, console};
  vm.createContext(ctx); vm.runInContext(script,ctx);
  await ctx.aiLoad();
  assert.equal(get('ai-gemini-article').value,'ga');
  get('ai-gemini-article').value='edited'; get('ai-provider').value='openai';
  await ctx.aiSave(null,false);
  assert.equal(sent.expected_revision,7); assert.equal(sent.profiles.gemini.models.article,'edited'); assert.equal(sent.profiles.openai.models.article,'oa');
  assert.deepEqual(Object.keys(sent).sort(),['active_provider','expected_revision','profiles','reset_to_env']);
  conflict=true; await ctx.aiSave(null,true); assert.equal(sent.expected_revision,8); assert.equal(sent.reset_to_env,true); assert.match(get('ai-status').textContent,/другом окне/);
  settings.configured.openai=false; await ctx.aiLoad(); get('ai-provider').value='openai'; sent=null; await ctx.aiSave(null,false); assert.equal(sent,null); assert.match(get('ai-status').textContent,/ключ/);
});
