const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
test('AI cost distinguishes unknown, partial and known totals', () => {
 const html=fs.readFileSync('templates/admin/stats.html','utf8');
 const fn=html.match(/function formatAICost\(total\) \{[\s\S]*?\n\}/)[0];
 const c={}; vm.createContext(c); vm.runInContext(fn,c);
 assert.equal(c.formatAICost({cost:null,unknown_cost_count:2}),'Неизвестно');
 assert.match(c.formatAICost({cost:0.25,unknown_cost_count:1}),/частично/);
 assert.equal(c.formatAICost({cost:0,unknown_cost_count:0}),'$0.000000');
 assert.equal(c.formatAICost({cost:0.000002,unknown_cost_count:0}),'$0.000002');
 assert.equal(c.formatAICost({cost:0.00000002,unknown_cost_count:0}),'$2.00e-8');
});
