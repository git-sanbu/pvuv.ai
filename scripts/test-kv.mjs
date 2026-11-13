import { build } from 'esbuild';
import { Miniflare } from 'miniflare';
import { readFile, readdir } from 'node:fs/promises';
import assert from 'node:assert/strict';
import { test } from 'node:test';

const bundled = await build({
  stdin: { contents: `export {getSiteConfig,isBlocklisted,default as ingest} from './workers/ingest/src/index';
    export {runDailyBatch} from './workers/cron/src/batch';
    export {default as consumer} from './workers/consumer/src/index';
    export {EVENT_COLUMNS,monthSuffix} from './shared/events';`, resolveDir: process.cwd(), loader: 'ts' },
  bundle: true, write: false, format: 'esm', platform: 'node',
  plugins: [{ name: 'test-private-functions', setup(build) {
    build.onLoad({ filter: /workers\/ingest\/src\/index\.ts$/ }, async args => ({
      contents: await readFile(args.path, 'utf8') + '\nexport { getSiteConfig, isBlocklisted };', loader: 'ts',
    }));
  } }],
});
const {getSiteConfig,isBlocklisted,ingest,runDailyBatch,consumer,EVENT_COLUMNS,monthSuffix} =
  await import('data:text/javascript;base64,' + Buffer.from(bundled.outputFiles[0].text).toString('base64'));
const site = id => ({site_id:id,allowed_domains:['example.com'],adguard_mode:'off',adclient:null,status:'active',shadow_until:null});

test('site cache shares concurrent lookups and refreshes configuration after 30 minutes', async t => {
  let now = Date.now(); t.mock.method(Date, 'now', () => now);
  let reads=0; let config=site('cacheA1');
  const env={SITE_CONFIG:{async get(){reads++;return {...config};}}};
  await Promise.all(Array.from({length:20},()=>getSiteConfig(env,'cacheA1')));
  assert.equal(reads,1);
  config={...config,status:'inactive'};
  now+=29*60_000;
  assert.equal((await getSiteConfig(env,'cacheA1')).status,'active');
  assert.equal(reads,1);
  now+=60_001;
  assert.equal((await getSiteConfig(env,'cacheA1')).status,'inactive');
  assert.equal(reads,2);
});

test('missing sites expire after 60 seconds and lookup failures can be retried immediately', async t => {
  let now=Date.now();t.mock.method(Date,'now',()=>now);
  let reads=0,dbReads=0;
  const env={SITE_CONFIG:{async get(){reads++;return null;},async put(){}},DB:{prepare(){return {bind(){return this;},async first(){dbReads++;return null;}}}}};
  assert.equal(await getSiteConfig(env,'missA1'),null);
  await getSiteConfig(env,'missA1');assert.equal(reads,1);assert.equal(dbReads,1);
  now+=60_001;await getSiteConfig(env,'missA1');assert.equal(reads,2);
  let fail=true;
  const recovering={SITE_CONFIG:{async get(){if(fail)throw Error('KV unavailable');return site('retryA1');}}};
  await assert.rejects(getSiteConfig(recovering,'retryA1'),/KV unavailable/);
  fail=false;assert.equal((await getSiteConfig(recovering,'retryA1')).status,'active');
});

test('valid site KV refills last 24 hours; invalid site IDs never query storage', async()=>{
  let reads=0,ttl;
  const config=site('fillA1');
  const env={SITE_CONFIG:{async get(){reads++;return null;},async put(key,value,options){ttl=options.expirationTtl;}},DB:{prepare(){return {bind(){return this;},async first(){return {...config,allowed_domains:JSON.stringify(config.allowed_domains)};}}}}};
  assert.equal(await getSiteConfig(env,'invalid!'),null);assert.equal(reads,0);
  assert.deepEqual(await getSiteConfig(env,'fillA1'),config);assert.equal(ttl,86400);
});

test('blacklist switch skips all reads by default and supports explicit re-enabling',async()=>{
  const keys=[];const env={BLOCKLIST:{async get(key){keys.push(key);return key==='bl:fp:known'?'{}':null;}}};
  assert.equal(await isBlocklisted(env,'subnet','known'),false);assert.equal(keys.length,0);
  env.BLOCKLIST_ENABLED='false';assert.equal(await isBlocklisted(env,'subnet','known'),false);assert.equal(keys.length,0);
  env.BLOCKLIST_ENABLED='true';assert.equal(await isBlocklisted(env,'subnet','known'),true);
  assert.deepEqual(keys,['bl:ip24:subnet','bl:fp:known']);
  assert.equal(await isBlocklisted(env,'subnet','unknown'),false);
});

test('/in and /v keep working with blacklist KV disabled and origin checks intact',async()=>{
  const messages=[];const config=site('routeA1');
  const env={HMAC_KEY:'local-test-only',BLOCKLIST_ENABLED:'false',BLOCKLIST:{async get(){throw Error('blacklist must not be read');}},SITE_CONFIG:{async get(key){return key.startsWith('site:')?config:[];}},INGEST_QUEUE:{async sendBatch(batch){messages.push(...batch);}}};
  const event={s:config.site_id,e:'pageview',u:'https://example.com/test',vid:'testvisitor',sid:'testsession',ts:Date.now()};
  const req=(path,body,origin='https://example.com')=>new Request('https://ingest.example.com'+path,{method:'POST',headers:{origin,'content-type':'text/plain','cf-connecting-ip':'192.0.2.1'},body:JSON.stringify(body)});
  const ctx={waitUntil(){}};
  assert.equal((await ingest.fetch(req('/in',event),env,ctx)).status,204);assert.equal(messages.length,1);
  const verdict=await ingest.fetch(req('/v',{s:config.site_id}),env,ctx);assert.equal(verdict.status,200);assert((await verdict.json()).state);
  await ingest.fetch(req('/in',event,'https://unapproved.example'),env,ctx);assert.equal(messages.length,1);
});

test('daily analysis retains evidence while KV publication can be disabled and re-enabled',async()=>{
  const mf=new Miniflare({modules:true,script:'export default {fetch(){return new Response("ok")}}',d1Databases:{DB:'kv-switch-test'}});
  try {
    const db=await mf.getD1Database('DB');
    for(const f of (await readdir('migrations')).filter(f=>f.endsWith('.sql')).sort()) {
      const sql=await readFile('migrations/'+f,'utf8');
      for(const stmt of sql.replace(/--[^\n]*/g,'').split(';').map(s=>s.trim()).filter(Boolean))await db.prepare(stmt).run();
    }
    const ts=Date.now()-600_000;
    await db.prepare("INSERT INTO sites(site_id,name,owner_id,allowed_domains,created_at) VALUES('dailyA1','local','local','[]',?)").bind(ts).run();
    const rows=Array.from({length:20},(_,i)=>({...Object.fromEntries(EVENT_COLUMNS.map(k=>[k,null])),eid:'daily-'+i,site_id:'dailyA1',visitor_id:'visitor-'+i,session_id:'session-'+i,event:'pageview',url:'https://example.com/',hostname:'example.com',path:'/',fp_hash:'shared-fingerprint',ts:ts+i,created_at:ts,bot_score:0,bot_flags:0,verdict:'clean',had_interaction:0,duration_ms:0}));
    await consumer.queue({messages:rows.map(body=>({body,ack(){},retry(){throw Error('consumer failed');}}))},{DB:db});
    const writes=[];const env={DB:db,BLOCKLIST_ENABLED:'false',BLOCKLIST:{async put(...args){writes.push(args);}}};
    await runDailyBatch(env);assert.equal(writes.length,0);
    const evidence=await db.prepare("SELECT COUNT(*) AS n FROM cluster_flags WHERE type='fp' AND action='block'").first();assert.equal(evidence.n,1);
    // Rewind only the idempotent cluster watermark in this isolated test DB.
    await db.prepare("DELETE FROM instance_settings WHERE key='batch_wm_clusters_ts'").run();
    env.BLOCKLIST_ENABLED='true';await runDailyBatch(env);
    assert(writes.some(([key])=>key==='bl:fp:shared-fingerprint'));
    const count=await db.prepare('SELECT COUNT(*) AS n FROM events_'+monthSuffix(ts)).first();assert.equal(count.n,20);
  }finally{await mf.dispose();}
});
