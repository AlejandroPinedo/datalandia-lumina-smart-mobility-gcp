/* Agregaciones de registros exportados. No contiene datos de demostración. */
(function(root){
'use strict';
const metrics=['total_trips','total_km','total_revenue_usd','duration_sum','duration_count','passengers_sum','passengers_count','missing_distance_rows','missing_amount_rows','negative_distance_rows'];
function unpack(data){
 const x=data.exploration;if(!x||x.status!=='ok')return null;
 return x.rows.map(values=>Object.fromEntries(x.columns.map((key,i)=>[key,x.dictionaries[key]?x.dictionaries[key][values[i]]:values[i]])));
}
function filter(rows,f){return rows.filter(r=>(!f.modes.length||f.modes.includes(r.transport_mode))&&(!f.counties.length||f.counties.includes(r.county_name))&&(!f.from||r.travel_date!==null&&r.travel_date>=f.from)&&(!f.to||r.travel_date!==null&&r.travel_date<=f.to)&&(f.hour===''||r.hod!==null&&Number(r.hod)===Number(f.hour)));}
function group(rows,key){
 const groups=new Map();
 for(const r of rows){const k=r[key];if(!groups.has(k))groups.set(k,{[key]:k});const target=groups.get(k);
 for(const m of metrics){if(r[m]!=null)target[m]=(target[m]??0)+Number(r[m]);}}
 return Array.from(groups.values()).map(r=>({...r,avg_duration_min:r.duration_count>0?r.duration_sum/r.duration_count:null,avg_passengers:r.passengers_count>0?r.passengers_sum/r.passengers_count:null,revenue_per_km_usd:r.total_km>0&&r.total_revenue_usd!=null?r.total_revenue_usd/r.total_km:null}));
}
function sum(rows,key){const values=rows.map(r=>r[key]).filter(v=>v!=null);return values.length?values.reduce((a,b)=>a+Number(b),0):null;}
function summary(rows){return {total_trips:sum(rows,'total_trips')??0,total_km:sum(rows,'total_km'),total_revenue_usd:sum(rows.filter(r=>r.transport_mode!=='PERSONAL_CAR_BENCHMARK'),'total_revenue_usd')};}
function csv(rows,columns){const cell=v=>'"'+String(v??'').replace(/"/g,'""')+'"';return [columns,...rows.map(r=>columns.map(k=>r[k]))].map(r=>r.map(cell).join(',')).join('\r\n');}
const api={unpack,filter,group,sum,summary,csv};root.LuminaData=api;if(typeof module!=='undefined')module.exports=api;
})(typeof globalThis!=='undefined'?globalThis:this);
