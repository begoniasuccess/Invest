const assert=require('node:assert/strict');
const fs=require('node:fs'),vm=require('node:vm');
const {ReplayEngine,ma,macd}=require('../dashboard/static/replay_engine.js');
const script=fs.readFileSync(require.resolve('../dashboard/static/replay.js'),'utf8');
const storage=new Map();
const start=Date.parse('2026-11-10T08:45:00+08:00');
function makeBrowser(){
  const elements=new Map(), listeners={}, opts=[];
  function el(){const classes=new Set();return {value:'',checked:true,children:[],style:{},classList:{toggle(k,v){if(v===undefined)v=!classes.has(k);v?classes.add(k):classes.delete(k);},contains:k=>classes.has(k)},addEventListener(k,fn){this[k]=fn;},setAttribute(){},replaceChildren(){this.children=[];},append(...xs){this.children.push(...xs);}};}
  function $(id){if(!elements.has(id))elements.set(id,el());return elements.get(id);}
  Object.entries({market:'tx_day',symbol:'TAIEX',period:'5',date:'2026-11-10',speed:'1',lineKind:'h',lineColor:'#ef4444',lineStyle:'2',lineWidth:'2'}).forEach(([k,v])=>$(k).value=v);
  $('replayLevelLabels').parentElement=el();
  const series={setData(){},update(){},createPriceLine(){return {};},removePriceLine(){},attachPrimitive(){},detachPrimitive(){}};
  const ts={getVisibleLogicalRange(){return null;},setVisibleLogicalRange(){},scrollToPosition(){},fitContent(){}};
  const chart={addSeries(kind,options){opts.push({kind,options});return {...series};},panes(){return [];},subscribeCrosshairMove(){},subscribeClick(){},timeScale(){return ts;},resize(){},remove(){}};
  const LC={createChart:()=>chart,CandlestickSeries:'candle',LineSeries:'line',HistogramSeries:'hist',CrosshairMode:{Normal:0}};
  const window={LightweightCharts:LC,ReplayCore:{ReplayEngine,ma,macd},PriceLevelLabels:class{schedule(){} destroy(){} setItems(items){this.items=items;}},addEventListener(k,fn){listeners[k]=fn;},confirm:()=>true};
  const context={window,document:{getElementById:$,createElement:el,addEventListener(){}},localStorage:{getItem:k=>storage.get(k)||null,setItem:(k,v)=>storage.set(k,v)},performance:{now:()=>3000},ResizeObserver:class{observe(){} disconnect(){}},requestAnimationFrame(){},URLSearchParams,Intl,Date,fetch:async url=>({ok:true,json:async()=>url.includes('/init?')?{event_start:'2026-11-10T08:45:00',start_ms:start,end_ms:start+60000,history:[],volume_available:true,resolution:'tick'}:{events:[[start,start/1000,100,100,100,100,1],[start+1000,start/1000,101,101,101,101,1]]}})};
  vm.runInNewContext(script,context);
  // Use the same interval for every simulated reopen.
  $('end').value='2026-11-10T08:46:00';
  return {$,listeners,opts,load:()=> $('replayForm').onsubmit({preventDefault(){}})};
}
(async()=>{
  let b=makeBrowser();await b.load();assert.equal(b.$('sourceBadge').textContent,'TX 202611 日盤 · 逐筆成交');
  b.$('step').onclick();
  b.$('linePrice').value='101';b.$('lineName').value='支撐';b.$('lineForm').onsubmit({preventDefault(){}});
  assert.equal(b.$('lineList').children.length,1);
  b.$('lineKind').value='v';b.$('lineTime').value='2026-11-10T08:45:00';b.$('lineName').value='開盤';b.$('lineForm').onsubmit({preventDefault(){}});
  assert.equal(b.$('lineList').children.length,2);
  b.$('speed').value='5';b.$('speed').onchange();b.listeners.pagehide();
  b=makeBrowser();assert.equal(b.$('speed').value,'5');await b.load();assert.equal(b.$('lineList').children.length,2);assert.match(b.$('clock').textContent,/08:45:01/);
  assert.ok(b.opts.filter(x=>x.kind==='line'&&x.options.title==='').length>=4);
  assert.ok(b.opts.filter(x=>x.kind==='line'&&x.options.title==='').every(x=>!x.options.lastValueVisible&&!x.options.priceLineVisible));
  b.$('hideLines').onclick();b=makeBrowser();await b.load();assert.ok(b.$('lineList').children.every(x=>!x.children[0].checked));
  b.$('reset').onclick();assert.equal(b.$('lineList').children.length,0);b.$('step').onclick();assert.equal(b.$('lineList').children.length,2);
  b.$('lineList').children[0].children[2].onclick();b.$('lineName').value='新支撐';b.$('lineForm').onsubmit({preventDefault(){}});
  b=makeBrowser();await b.load();assert.match(b.$('lineList').children[0].children[1].textContent,/新支撐/);
  b.$('lineList').children[0].children[3].onclick();b=makeBrowser();await b.load();assert.equal(b.$('lineList').children.length,1);
  b.$('period').value='15';await b.load();assert.equal(b.$('lineList').children.length,0);b.$('period').value='5';await b.load();assert.equal(b.$('lineList').children.length,1);
  b.$('clearLines').onclick();b=makeBrowser();await b.load();assert.equal(b.$('lineList').children.length,0);
  console.log('Replay storage / reload / scope / MA tests passed');
})().catch(e=>{console.error(e);process.exitCode=1;});
