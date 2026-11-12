/* Pure replay state; also testable under Node. Events: [availability ms, bar unix sec, O,H,L,C,V]. */
(function(root){
  class ReplayEngine {
    constructor(history, events, start, end) {
      this.history=history; this.events=events; this.start=start; this.end=end; this.reset();
    }
    reset(){this.bars=this.history.map(b=>({...b}));this.index=0;this.now=this.start;this.advance(this.start);}
    consume(e){
      const [,time,open,high,low,close,volume]=e; let b=this.bars.at(-1);
      if(b && time<b.time) return null; // prior completed minute at initial boundary
      if(!b || time!==b.time){b={time,open,high,low,close,volume};this.bars.push(b);}
      else {b.high=Math.max(b.high,high);b.low=Math.min(b.low,low);b.close=close;b.volume+=volume;}
      return b;
    }
    advance(target){
      target=Math.max(this.start,Math.min(this.end,target));
      if(target<this.now){this.reset();}
      const changed=new Map();
      while(this.index<this.events.length && this.events[this.index][0]<=target){
        const b=this.consume(this.events[this.index++]);if(b)changed.set(b.time,b);
      }
      this.now=target;return [...changed.values()];
    }
    step(){
      const e=this.events[this.index];if(!e)return [];
      const b=this.consume(e);this.index++;this.now=Math.max(this.now,e[0]);return b?[b]:[];
    }
    seek(target){this.reset();return this.advance(target);}
    // Preserve an exact single-tick cursor even when several events share a timestamp.
    restore(target,index){
      this.reset();
      const valid=Number.isInteger(index)&&index>=this.index&&index<=this.events.length&&
        (!index||this.events[index-1][0]<=target)&&
        (index===this.events.length||this.events[index][0]>=target);
      if(!valid)return this.advance(target);
      while(this.index<index)this.consume(this.events[this.index++]);
      this.now=target;return [];
    }
  }
  function ma(bars,period){let sum=0;return bars.flatMap((b,i)=>{sum+=b.close;if(i>=period)sum-=bars[i-period].close;return i>=period-1?[{time:b.time,value:sum/period}]:[];});}
  // SMA-seeded EMA, matching the existing K-line analysis page.
  function macd(bars){let f=0,s=0,fe=null,se=null,signal=null,ss=0,n=0;const out=[];
    bars.forEach((b,i)=>{const c=b.close;if(i<12)f+=c;if(i<26)s+=c;
      if(i===11)fe=f/12;else if(i>11)fe+=(c-fe)*2/13;
      if(i===25)se=s/26;else if(i>25)se+=(c-se)*2/27;
      if(se===null)return;const dif=fe-se;n++;if(n<=9)ss+=dif;if(n===9)signal=ss/9;else if(n>9)signal+=(dif-signal)*2/10;
      out.push({time:b.time,value:dif,signal,osc:signal===null?null:dif-signal});
    });return out;
  }
  root.ReplayCore={ReplayEngine,ma,macd};if(typeof module!=='undefined')module.exports=root.ReplayCore;
})(typeof globalThis!=='undefined'?globalThis:this);
