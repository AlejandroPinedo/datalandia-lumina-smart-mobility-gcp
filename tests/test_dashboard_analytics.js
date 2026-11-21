// Comprueba el cálculo del explorador contra la exportación real del proyecto.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const D=require('../P3/web/analytics.js');
const report=JSON.parse(fs.readFileSync(path.join(__dirname,'../P3/evidencias/datos_existentes_latest.json'),'utf8'));
const rows=D.unpack(report);
const close=(a,b)=>assert.ok(Math.abs(a-b)<=Math.max(.01,Math.abs(b)*1e-10),`${a} != ${b}`);
test('El cubo completo conserva totales y promedios del SELECT por modo',()=>{
 assert.ok(rows?.length>0,'Se requiere el desglose real');
 const grouped=D.group(rows,'transport_mode'),summary=D.summary(grouped);
 assert.equal(summary.total_trips,report.summary.total_trips);
 close(summary.total_km,report.summary.total_km);close(summary.total_revenue_usd,report.summary.total_revenue_usd);
 for(const actual of report.by_mode){const r=grouped.find(r=>r.transport_mode===actual.transport_mode);assert.equal(r.total_trips,actual.total_trips);for(const k of['total_km','total_revenue_usd','avg_duration_min','avg_passengers','revenue_per_km_usd'])close(r[k],actual[k]);}
});
test('Fecha, condado, hora y modo se intersectan sin perder ni duplicar registros',()=>{
 const observed=rows.find(r=>r.travel_date&&r.county_name&&r.hod!=null),f={modes:[observed.transport_mode],counties:[observed.county_name],from:observed.travel_date,to:observed.travel_date,hour:String(observed.hod)};
 const selection=D.filter(rows,f);assert.ok(selection.length);
 for(const r of selection){assert.equal(r.travel_date,observed.travel_date);assert.equal(r.county_name,observed.county_name);assert.equal(r.hod,observed.hod);assert.equal(r.transport_mode,observed.transport_mode);}
 assert.equal(D.summary(D.group(selection,'transport_mode')).total_trips,selection.reduce((n,r)=>n+r.total_trips,0));
 const hours=report.exploration.dictionaries.hod;
 const partition=hours.reduce((n,h)=>n+D.summary(D.group(D.filter(rows,{modes:[],counties:[],from:'',to:'',hour:String(h)}),'transport_mode')).total_trips,0);
 assert.equal(partition,report.summary.total_trips);
});
test('La media usa SUM/COUNT y las mediciones ausentes no se vuelven cero',()=>{
 const subset=rows.slice(0,15),grouped=D.group(subset,'transport_mode');
 for(const r of grouped){const original=subset.filter(s=>s.transport_mode===r.transport_mode);close(r.avg_duration_min,D.sum(original,'duration_sum')/D.sum(original,'duration_count'));}
 const missing=rows.slice(0,1).map(r=>({...r,duration_sum:null,duration_count:0,total_km:null,total_revenue_usd:null}));
 const r=D.group(missing,'transport_mode')[0];assert.equal(r.avg_duration_min,null);assert.equal(r.revenue_per_km_usd,null);assert.equal(D.summary([r]).total_km,null);
 assert.equal(D.summary([]).total_revenue_usd,null);
});
test('Modo de referencia excluido de tarifas y CSV conserva cifras de selección',()=>{
 const personal=report.by_mode.filter(r=>r.transport_mode==='PERSONAL_CAR_BENCHMARK');assert.ok(personal.length);assert.equal(D.summary(personal).total_revenue_usd,null);
 const csv=D.csv(report.by_mode,['transport_mode','total_trips']);assert.equal(csv.split('\r\n').length,report.by_mode.length+1);assert.ok(csv.includes(String(report.by_mode[0].total_trips)));
 assert.equal(D.csv([{value:'a,"b"\nc'}],['value']),'"value"\r\n"a,""b""\nc"');
});
test('La ausencia de cubo no inventa filas',()=>{assert.equal(D.unpack({...report,exploration:null}),null);});
