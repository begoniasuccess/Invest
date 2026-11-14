(()=>{
'use strict';
const $=id=>document.getElementById(id), C=window.PaperCommon, B=window.ReplayTradeBridge;
let accountList=[],run=null,snapshot=null,context='',generation=0,busy=false,syncJob=null,lastTick=null,lastIndex=null,lastEnd=false,blocked=false,accountReady=false,retryAt=0,failures=0,permanent=false,uncertainOrder=null;
const effectName=x=>({AUTO:'自動',OPEN:'新倉',CLOSE:'平倉'}[x]||'新倉');
const key='investDashboard.paperTrade.account';
const state=()=>B.getState();
const scope=s=>s?JSON.stringify([$('ptAccount').value,s.market,s.contract,s.start,s.end]):'';
const say=(s,bad=false)=>{$('ptStatus').textContent=s;$('ptStatus').classList.toggle('text-danger',bad);};
const empty=(body,count,text='尚無資料')=>{body.replaceChildren();const r=C.el('tr');const td=C.cell(r,text,'text-secondary');td.colSpan=count;body.append(r);};
function clear(){B.setTradeMarkers([]);snapshot=null;lastTick=null;lastIndex=null;lastEnd=false;blocked=false;retryAt=0;failures=0;permanent=false;uncertainOrder=null;empty($('ptOpen'),10);empty($('ptOrders'),12);empty($('ptClosed'),12);$('ptSummary').replaceChildren();}
function controls(){const link=$('ptRunNote');if(link){link.href=run&&$('ptAccount').value?'/paper-journal?'+new URLSearchParams({account_id:$('ptAccount').value,run_id:run.id}):'/paper-journal';link.setAttribute('aria-disabled',String(!run));link.classList.toggle('disabled',!run);}const s=state(),ready=Boolean(s?.tickId&&run&&context===scope(s)&&!busy&&!blocked&&!syncJob&&!!snapshot);$('ptBuy').disabled=!ready;$('ptSell').disabled=!ready;$('ptCloseAll').disabled=!ready||!snapshot?.trades.some(t=>!t.exit_time);$('ptNewRun').disabled=!s||!$('ptAccount').value||busy;$('ptRun').disabled=busy||!s||!$('ptAccount').value;$('ptAccount').disabled=busy;$('ptResync').disabled=busy||!!syncJob||!run||!s?.tickId;}
function journalLink(t){const a=C.el('a','相關日記','btn btn-sm btn-outline-secondary');a.href='/paper-journal?'+new URLSearchParams({account_id:$('ptAccount').value,run_id:t.run_id,trade_id:t.id,link_kind:'trade'});a.target='_blank';a.rel='noopener';return a;}
function render(data,index){snapshot=data;B.setTradeMarkers($('ptMarkers').checked?data.trades:[]);lastTick=String(data.run.cursor_id);lastIndex=index;blocked=false;const mark=data.mark,open=data.trades.filter(t=>!t.exit_time),closed=data.trades.filter(t=>t.exit_time);$('ptSummary').replaceChildren();
  const realized=closed.reduce((sum,t)=>sum+Number(t.actual.net),0),unreal=open.reduce((sum,t)=>sum+Number(t.actual?.net||0),0);for(const [label,value,cls] of [['最新已知成交',C.num(mark),''],['已平倉淨損益（選定商品）',C.num(realized),C.pnlClass(realized)],['未平倉淨損益估計',C.num(unreal),C.pnlClass(unreal)],['委託中／未平倉',`${data.orders.filter(o=>o.status==='PENDING').length} / ${open.length}`,'']]){const box=C.el('div',undefined,'paper-stat');box.append(C.el('small',label),C.el('strong',value,cls));$('ptSummary').append(box);}
  const ob=$('ptOpen');ob.replaceChildren();for(const t of open){const r=C.el('tr');[t.id,C.productNames[t.product],C.direction(t.direction),t.quantity,C.hist(t.entry_time),C.num(t.entry_price),`${C.num(t.stop_loss)} / ${C.num(t.take_profit)}`].forEach(x=>C.cell(r,x));C.cell(r,C.num(t.actual?.net),C.pnlClass(t.actual?.net));C.cell(r,'').append(journalLink(t));C.cell(r,'').append(C.btn('平倉',()=>act('close',{item_id:t.id},`確定平倉交易 #${t.id}（全部 ${t.quantity} 口）？`),'btn btn-sm btn-outline-danger'));ob.append(r);}if(!open.length)empty(ob,10);
  const orders=$('ptOrders');orders.replaceChildren();for(const o of data.orders){const r=C.el('tr');[o.id,C.productNames[o.product],C.direction(o.direction),o.quantity,effectName(o.position_effect),C.orderType(o.order_type)+(o.trigger_direction==='ABOVE'?' ≥':o.trigger_direction==='BELOW'?' ≤':''),o.time_in_force|| (o.order_type==='LIMIT'?'ROD':'IOC'),C.num(o.target),C.hist(o.submitted_clock_at),C.wall(o.submitted_client_at),C.status(o.status)+(o.cancel_reason?`（${o.cancel_reason}）`: '')].forEach(x=>C.cell(r,x));const td=C.cell(r,'');if(o.status==='PENDING')td.append(C.btn('取消',()=>act('cancel',{item_id:o.id},`取消委託 #${o.id}？`)));orders.append(r);}if(!data.orders.length)empty(orders,12);
  const cb=$('ptClosed');cb.replaceChildren();for(const t of closed){const r=C.el('tr');[t.id,C.direction(t.direction),t.quantity,C.num(t.entry_price),C.num(t.exit_price),C.num(t.tick_points)].forEach(x=>C.cell(r,x));for(const p of ['TX','MTX','TMF'])C.cell(r,C.num(t.comparison[p].net),C.pnlClass(t.comparison[p].net));C.cell(r,C.hist(t.exit_time));C.cell(r,C.reason(t.exit_reason));C.cell(r,'').append(journalLink(t));cb.append(r);}if(!closed.length)empty(cb,12);controls();
}
async function loadAccounts(){try{const old=$('ptAccount').value||C.read(key);accountList=await C.api('accounts');C.options($('ptAccount'),accountList.map(a=>[a.id,a.name]),'選擇帳號');if(accountList.some(a=>String(a.id)===old))$('ptAccount').value=old;accountReady=true;await loadRuns();if(!accountList.length)say('尚無模擬帳號，請點「帳號／績效」新增後更新帳號。');}catch(e){say(e.message,true);controls();}}
async function loadRuns(){const s=state();context=scope(s);const g=++generation;run=null;clear();C.options($('ptRun'),[],'選擇場次');controls();if(!s||!$('ptAccount').value){say(s?'請選擇交易帳號。':'請先載入台指期 TX 逐筆回放。');return;}const account_id=$('ptAccount').value;try{const runs=await C.api('runs?'+new URLSearchParams({account_id,market:s.market,contract:s.contract,start:s.start,end:s.end}));if(g!==generation)return;C.options($('ptRun'),runs.map(r=>[r.id,`#${r.id} · ${C.wall(r.created_at)}`]),'選擇場次');const stored=C.read('investDashboard.paperTrade.run.'+context);if(runs.some(r=>String(r.id)===stored)){$('ptRun').value=stored;run=runs.find(r=>String(r.id)===stored);await sync();}else say('選擇既有場次，或按「＋ 新場次」開始獨立練習。');controls();}catch(e){if(g===generation)say(e.message,true);}}
$('ptAccount').onchange=()=>{C.save(key,$('ptAccount').value);loadRuns();};$('ptRefresh').onclick=loadAccounts;
$('ptRun').onchange=()=>{generation++;clear();run=$('ptRun').value?{id:Number($('ptRun').value)}:null;C.save('investDashboard.paperTrade.run.'+context,$('ptRun').value);if(run)sync();else say('請選擇場次。');controls();};
$('ptNewRun').onclick=async()=>{const s=state();if(!s||!$('ptAccount').value||busy)return;B.pause();if(!window.confirm('建立新的獨立交易場次？既有帳號、交易與績效不會被覆蓋。'))return;busy=true;controls();const ctx=context;try{const r=await C.api('runs','POST',{account_id:Number($('ptAccount').value),market:s.market,contract:s.contract,start:s.start,end:s.end});C.save('investDashboard.paperTrade.run.'+ctx,r.id);await loadRuns();}catch(e){say(e.message,true);}finally{busy=false;controls();sync();}};
function onClock(){if(!accountReady)return;const s=state(),ctx=scope(s);if(ctx!==context){loadRuns();return;}if(s&&run&&snapshot&&(!s.tickId||s.index<lastIndex||Number(s.tickTime)<Date.parse(String(snapshot.run.cursor_time).replace(' ','T')+'+08:00'))){const pending=uncertainOrder;clear();uncertainOrder=pending;blocked=true;say('回放已倒回已交易時間之前；請回到原進度或建立新場次。',true);}controls();}
window.ReplayPaper={onClock};
function sync(){
 const s=state();
 if(!s?.tickId||!run||context!==scope(s)||busy||syncJob||Date.now()<retryAt||(lastTick===String(s.tickId)&&(s.now<s.endMs||lastEnd)))return syncJob;
 const g=generation,id=run.id;
 syncJob=(async()=>{
   try{
     const data=await C.api(`runs/${id}/action`,'POST',{action:'sync',tick_id:s.tickId,clock_ms:s.now,client_at:new Date().toISOString()});
     if(g!==generation)return;
     if(!state()||state().index<s.index){clear();blocked=true;lastTick=null;say('回放已倒回已交易時間之前；請回到原進度或建立新場次。',true);return;}
     render(data,s.index);retryAt=0;failures=0;permanent=false;lastEnd=s.now>=s.endMs;
     let message=`場次 #${id}｜${s.market==='tx_day'?'日盤':'夜盤'} ${s.contract}｜最新成交 ${C.num(data.mark)}｜模擬帳務已同步`;
     if(uncertainOrder){
       const found=data.orders.find(o=>String(o.request_id)===uncertainOrder);
       message+=found?`；前次委託 #${found.id} 已確認（${C.status(found.status)}）。`:'；前次逾時委託未在目前帳務中找到，請確認委託列表後再決定是否下單。';
       uncertainOrder=null;
     }
     say(message);
   }catch(e){
     if(g===generation){
       permanent=/倒回|原始行情已變更|成交資料不存在/.test(e.message);
       if(permanent){const pending=uncertainOrder;clear();uncertainOrder=pending;permanent=true;lastEnd=s.now>=s.endMs;}
       else B.pause();
       blocked=true;lastTick=permanent?String(s.tickId):null;
       retryAt=permanent?0:Date.now()+Math.min(30000,1000*2**Math.min(++failures,5));
       say(e.message+(permanent?'':'；已暫停，可按重新同步交易。'),true);
     }
   }finally{syncJob=null;controls();}
 })();
 controls();return syncJob;
}
$('ptResync').onclick=()=>{if(busy||syncJob)return;lastTick=null;retryAt=0;failures=0;sync();};
setInterval(()=>{onClock();sync();},700);
async function act(action,extra={},confirmation){
 const s=state();if(!s?.tickId||!run||busy||blocked||!snapshot||context!==scope(s))return;
 B.pause();if(confirmation&&!window.confirm(confirmation))return;
 busy=true;controls();const g=generation,id=run.id;
 try{
   if(syncJob)await syncJob;
   if(g!==generation||blocked)return;
   const now=state();if(!now||context!==scope(now)||now.index!==s.index||String(now.tickId)!==String(s.tickId)){say('回放位置已變更，請重新確認委託。',true);return;}
   const data=await C.api(`runs/${id}/action`,'POST',{action,tick_id:s.tickId,clock_ms:s.now,client_at:new Date().toISOString(),...extra});
   if(g===generation){render(data,s.index);say(`場次 #${id}｜操作完成｜最新已知成交 ${C.num(data.mark)}`);}
 }catch(e){
   if(g===generation){blocked=true;lastTick=null;retryAt=0;
     if(action==='order'&&(!e.status||e.status>=500||e.uncertain))uncertainOrder=extra.request_id;
     say(e.message+'；請先確認交易同步結果，再重新下單。',true);}
 }finally{busy=false;controls();if(blocked)sync();}
}
$('ptMarkers').checked=C.read('investDashboard.paperTrade.markers')!=='false';$('ptMarkers').onchange=()=>{C.save('investDashboard.paperTrade.markers',$('ptMarkers').checked);B.setTradeMarkers($('ptMarkers').checked?snapshot?.trades||[]:[]);};
$('ptEffect').value=C.read('investDashboard.paperTrade.effect')||'AUTO';
function effectChanged(){const close=$('ptEffect').value==='CLOSE';for(const id of ['ptStop','ptTake'])$(id).disabled=close;C.save('investDashboard.paperTrade.effect',$('ptEffect').value);}
$('ptEffect').onchange=effectChanged;effectChanged();
function typeChanged(){const kind=$('ptType').value,tif=$('ptTif');$('ptTargetField').hidden=kind==='MARKET';$('ptTriggerField').hidden=kind!=='STOP';tif.options[0].disabled=kind!=='LIMIT';const saved=C.read('investDashboard.paperTrade.tif.'+kind);tif.value=['ROD','IOC','FOK'].includes(saved)&&!(saved==='ROD'&&kind!=='LIMIT')?saved:(kind==='LIMIT'?'ROD':'IOC');}
$('ptType').onchange=typeChanged;
$('ptTrigger').value=C.read('investDashboard.paperTrade.triggerDirection')==='BELOW'?'BELOW':'ABOVE';
$('ptTrigger').onchange=()=>C.save('investDashboard.paperTrade.triggerDirection',$('ptTrigger').value);
$('ptTif').onchange=()=>C.save('investDashboard.paperTrade.tif.'+$('ptType').value,$('ptTif').value);
typeChanged();
function order(direction){const quantity=Number($('ptQty').value),kind=$('ptType').value,effect=$('ptEffect').value;const val=id=>$(id).value.trim()===''?null:Number($(id).value);const target=kind==='MARKET'?null:val('ptTarget'),stop=effect==='CLOSE'?null:val('ptStop'),take=effect==='CLOSE'?null:val('ptTake');if(!Number.isInteger(quantity)||quantity<1||quantity>1000||[target,stop,take].some(v=>v!==null&&(!Number.isFinite(v)||v<=0||!Number.isInteger(v)))||(kind!=='MARKET'&&target===null)){say('請輸入有效整數口數及點位。',true);return;}const s=state();if(!s)return;if(s.now>=s.endMs){say('回放時段已結束，不可新增委託。',true);return;}const request_id=crypto.randomUUID?crypto.randomUUID():'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g,c=>{const r=crypto.getRandomValues(new Uint8Array(1))[0]%16;return (c==='x'?r:(r&3)|8).toString(16);});act('order',{request_id,product:$('ptProduct').value,direction,quantity,order_type:kind,trigger_direction:kind==='STOP'?$('ptTrigger').value:null,position_effect:effect,time_in_force:$('ptTif').value,target,stop_loss:stop,take_profit:take},`帳號：${$('ptAccount').selectedOptions[0]?.textContent}\n場次 #${run?.id}｜${C.productNames[$('ptProduct').value]} ${direction==='LONG'?'買進':'賣出'} ${quantity} 口｜${effectName(effect)}\n${C.orderType(kind)} ${kind==='STOP'?($('ptTrigger').value==='ABOVE'?'≥':'≤'):''} / ${$('ptTif').value} ${target??'最新成交 '+C.num(s.price)}\n確認模擬下單？`);}
$('ptBuy').onclick=()=>order('LONG');$('ptSell').onclick=()=>order('SHORT');$('ptCloseAll').onclick=()=>act('close_all',{},'確定將本場次全部未平倉部位平倉？（未成交委託不會取消）');
clear();controls();loadAccounts();
})();
