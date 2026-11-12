const assert=require('node:assert/strict');
const {ReplayEngine,ma,macd}=require('../dashboard/static/replay_engine.js');
const h=[{time:0,open:90,high:90,low:90,close:90,volume:4}];
const e=[[1000,1,100,100,100,100,1],[1500,1,105,105,105,105,2],[1500,1,95,95,95,95,3],[2000,2,101,101,101,101,4],[3000,3,99,99,99,99,1]];
const r=new ReplayEngine(h,e,1250,3000);
assert.equal(r.bars.at(-1).close,100); // initial partial bar only prior ticks
assert.equal(r.index,1);
r.step();assert.equal(r.bars.at(-1).close,105);assert.equal(r.index,2);
r.step();assert.deepEqual(r.bars.at(-1),{time:1,open:100,high:105,low:95,close:95,volume:6});
r.advance(3000);assert.equal(r.bars.length,4);assert.equal(r.bars[1].close,95);
r.seek(1250);assert.equal(r.bars.length,2);assert.equal(r.bars.at(-1).high,100);assert.equal(r.index,1);
r.advance(2000);assert.equal(r.bars.length,3);assert.equal(r.bars.at(-1).close,101);assert.equal(r.bars[1].high,105);
assert.equal(h[0].close,90);assert.equal(ma(r.bars,2).at(-1).value,98);
const bars=Array.from({length:40},(_,i)=>({time:i,close:100+i}));
const before=macd(bars.slice(0,35));const after=macd(bars);assert.deepEqual(before,after.slice(0,before.length));assert.equal(before[0].time,25);assert.equal(before[8].time,33);
const minutes=new ReplayEngine([],[[60000,0,100,110,90,105,10],[120000,0,105,115,103,112,20]],0,120000);
minutes.advance(59999);assert.equal(minutes.bars.length,0);minutes.advance(60000);assert.equal(minutes.bars[0].high,110);minutes.advance(120000);assert.equal(minutes.bars[0].high,115);assert.equal(minutes.bars[0].volume,30);
console.log('Replay engine tests passed');

// Resume an exact cursor without revealing other trades at the same timestamp.
const restored=new ReplayEngine(h,e,1250,3000);
restored.step();const cursor={now:restored.now,index:restored.index};
const reopened=new ReplayEngine(h,e,1250,3000);
reopened.restore(cursor.now,cursor.index);
assert.equal(reopened.index,2);assert.equal(reopened.bars.at(-1).close,105);
reopened.step();assert.equal(reopened.bars.at(-1).close,95);
reopened.restore(2000,999);assert.equal(reopened.index,4);
reopened.seek(1250);assert.equal(reopened.index,1);
console.log('Replay resume cursor tests passed');
