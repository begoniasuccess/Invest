(function(root){
'use strict';
const productNames={TX:'大台 TX',MTX:'小台 MTX',TMF:'微台 TMF'};
const format=new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Taipei',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'});
const num=x=>x===null||x===undefined?'—':Number(x).toLocaleString('zh-TW',{maximumFractionDigits:2});
const hist=x=>x?String(x).replace('T',' '):'—';
const wall=x=>x?format.format(new Date(x)):'—';
const direction=x=>x==='LONG'?'多單':'空單';
const orderType=x=>({MARKET:'市價',LIMIT:'限價',STOP:'觸價'}[x]||x);
const status=x=>({PENDING:'委託中',FILLED:'已成交',CANCELLED:'已取消'}[x]||x);
const reason=x=>({MANUAL:'手動平倉',STOP_LOSS:'停損',TAKE_PROFIT:'停利'}[x]||'未平倉');
async function api(path,method='GET',body){const r=await fetch('/api/paper-trade/'+path,{method,headers:body?{'Content-Type':'application/json'}:undefined,body:body?JSON.stringify(body):undefined});let data;try{data=await r.json();}catch{throw Error('模擬交易 API 回應失敗，請檢查 Dashboard 伺服器。');}if(!r.ok)throw Error(typeof data.detail==='string'?data.detail:Array.isArray(data.detail)?data.detail.map(x=>x.msg).join('；'):'交易操作失敗');return data;}
function el(tag,text,cls){const e=document.createElement(tag);if(text!==undefined)e.textContent=text;if(cls)e.className=cls;return e;}
function cell(row,value,cls){const td=el('td',value,cls);row.append(td);return td;}
function btn(text,fn,cls='btn btn-sm btn-outline-secondary'){const b=el('button',text,cls);b.type='button';b.onclick=fn;return b;}
function pnlClass(n){return Number(n)>0?'paper-gain':Number(n)<0?'paper-loss':'';}
function options(select,items,placeholder){select.replaceChildren();if(placeholder){const o=el('option',placeholder);o.value='';select.append(o);}for(const [value,text] of items){const o=el('option',text);o.value=String(value);select.append(o);}}
function read(key){try{return localStorage.getItem(key)||'';}catch{return '';}}
function save(key,value){try{localStorage.setItem(key,String(value));}catch{}}
root.PaperCommon={productNames,num,hist,wall,direction,orderType,status,reason,api,el,cell,btn,pnlClass,options,read,save};
})(window);
