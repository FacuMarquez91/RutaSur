import {TOPICS,db,connect,ensureIndexes,kafka,publish,event,consume,once,opposite} from '../common/core.js';
const role=process.argv[2]||process.env.ROLE;
const baseRole=role==='proyector-replay'?'proyector':role;
await connect();await ensureIndexes();
const D=db();
const statusOrder=['CREADO','INGRESADO_HUB','CLASIFICADO','EN_REPARTO','ENTREGADO','ENTREGA_FALLIDA'];
async function project({e,topic}){await once(role,e,async(session)=>{
 const opts={session,upsert:true};const env=D.collection('envios');
 if(topic===TOPICS.estados){const old=await env.findOne({shipmentId:e.shipmentId},{session});if(old&&e.secuencia<=old.ultimaSecuencia)return;
 const attempts=e.tipo==='EN_REPARTO'?(old?.intentos||0)+1:(old?.intentos||0);
 await env.updateOne({shipmentId:e.shipmentId},{$set:{shipmentId:e.shipmentId,estado:e.tipo,hubId:e.hubId,zonaId:e.zonaId,repartidorId:e.repartidorId,ultimaSecuencia:e.secuencia,actualizadoEn:e.ocurridoEn,intentos:attempts,...(e.motivo?{ultimoMotivo:e.motivo}:{})},$push:{historial:{eventId:e.eventId,tipo:e.tipo,ocurridoEn:e.ocurridoEn,secuencia:e.secuencia,motivo:e.motivo||null}}},opts);
 }else if(topic===TOPICS.promesas){await env.updateOne({shipmentId:e.shipmentId},{$set:{promesa:{venceEn:e.venceEn},hubId:e.hubId,zonaId:e.zonaId,shipmentId:e.shipmentId}},opts)}
 else if(topic===TOPICS.reprogramaciones){await env.updateOne({shipmentId:e.shipmentId},{$set:{decision:e.decision,proximoIntento:e.proximoIntento||null,franjaProxima:e.franjaProxima||null},$push:{acciones:{tipo:e.tipo,decision:e.decision,ocurridoEn:e.ocurridoEn}}},opts)}
 else if(topic===TOPICS.alertas){await D.collection('alertas').updateOne({alertaId:e.alertaId},{$set:{alertaId:e.alertaId,shipmentId:e.shipmentId,estado:e.estado,regla:e.regla,prioridad:e.prioridad,causa:e.causa,ocurridoEn:e.ocurridoEn}},opts)}
 else if(topic===TOPICS.notificaciones){await D.collection('notificaciones').updateOne({eventId:e.eventId},{$setOnInsert:e},opts)}
 });}
async function notify({e,topic,producer}){if((topic===TOPICS.estados&&e.tipo!=='ENTREGA_FALLIDA')||(topic===TOPICS.alertas&&e.estado!=='ABIERTA'))return;
 let output=null;await once(role,e,async(session)=>{output=event('AVISO',e.shipmentId,{secuencia:e.secuencia,hubId:e.hubId,zonaId:e.zonaId,repartidorId:e.repartidorId,origenId:e.eventId,mensaje:topic===TOPICS.estados?`Entrega fallida (${e.motivo}). Puede solicitar una reprogramación.`:`Alerta de entrega: ${e.regla} (${e.prioridad})`},e.ocurridoEn);
 // Outbox persistente: enviar luego del commit y reintentar con el mismo ID.
 await D.collection('outbox').updateOne({consumidor:role,origenId:e.eventId},{$setOnInsert:{consumidor:role,origenId:e.eventId,topic:TOPICS.notificaciones,evento:output,enviado:false}},{upsert:true,session});});await flushOutbox(producer);}
async function reschedule({e,topic,producer}){if(e.tipo!=='ENTREGA_FALLIDA')return;await once(role,e,async(session)=>{const current=await D.collection('envios').findOne({shipmentId:e.shipmentId},{session});const attempts=e.intento||current?.intentos||1;let decision,proximoIntento=null,franjaProxima=null;
 if(attempts>=3||['RECHAZADO','ZONA_INACCESIBLE'].includes(e.motivo))decision='DEVOLUCION';
 else if(e.motivo==='DIRECCION_INCORRECTA')decision='RETENIDO';
 else{decision='REPROGRAMADO';const x=new Date(e.ocurridoEn);do{x.setUTCDate(x.getUTCDate()+1)}while([0,6].includes(x.getUTCDay()));proximoIntento=x.toISOString();franjaProxima=opposite(e.franja||'MANANA');}
 const out=event('REPROGRAMACION',e.shipmentId,{secuencia:e.secuencia,hubId:e.hubId,zonaId:e.zonaId,repartidorId:e.repartidorId,origenId:e.eventId,decision,proximoIntento,franjaProxima},e.ocurridoEn);await D.collection('outbox').updateOne({consumidor:role,origenId:e.eventId},{$setOnInsert:{consumidor:role,origenId:e.eventId,topic:TOPICS.reprogramaciones,evento:out,enviado:false}},{upsert:true,session});});await flushOutbox(producer);}
async function flushOutbox(producer){for(const item of await D.collection('outbox').find({consumidor:role,enviado:false}).toArray()){await publish(producer,item.topic,item.evento);await D.collection('outbox').updateOne({_id:item._id},{$set:{enviado:true}});}}
const dateKey=e=>({fecha:e.ocurridoEn.slice(0,10),zonaId:e.zonaId,hubId:e.hubId,franja:e.franja||'MANANA'});
async function analytics({e,topic}){if(topic!==TOPICS.estados)return;await once(role,e,async(session)=>{const key=dateKey(e);const col=D.collection('metricas_diarias');let inc={};if(e.tipo==='EN_REPARTO'){inc.intentos=1;if((e.intento||1)===1)inc.primerosIntentos=1;}
 if(e.tipo==='ENTREGADO'){inc.entregados=1;if((e.intento||1)===1)inc.primerIntento=1;if(e.venceEn&&Date.parse(e.ocurridoEn)<=Date.parse(e.venceEn))inc.dentroSla=1;}
 if(e.tipo==='ENTREGA_FALLIDA'){inc.fallos=1;inc[`motivos.${e.motivo}`]=1;}
 if(Object.keys(inc).length)await col.updateOne(key,{$inc:inc,$setOnInsert:key},{upsert:true,session});});}
// Respaldo del monitor en MongoDB; el reloj simulado avanza de manera determinista desde el último evento.
const monitor=D.collection('monitor_estado');let monitorProducer;
const rules=(s,now)=>{if(!s.venceEn||s.estado==='ENTREGADO'||s.estado==='DEVOLUCION')return [];let remaining=Date.parse(s.venceEn)-now;const r=[];
 if(remaining<0)r.push(['VENCIDO','CRITICA']);
 else{if(remaining<10*3600000&&!['INGRESADO_HUB','CLASIFICADO','EN_REPARTO','ENTREGADO','ENTREGA_FALLIDA'].includes(s.estado))r.push(['SIN_INGRESO','MEDIA']);
 if(remaining<4*3600000&&!['EN_REPARTO','ENTREGADO'].includes(s.estado))r.push(['SIN_SALIDA','ALTA']);
 if(s.estado==='ENTREGA_FALLIDA'&&new Date(s.venceEn).toISOString().slice(0,10)===new Date(now).toISOString().slice(0,10))r.push(['FALLO_DIA_LIMITE','ALTA']);}return r;};
async function evaluate(s,now){if(!s.venceEn)return;const active=rules(s,now);for(const [rule,priority]of active){if((s.emitidas||[]).includes(rule))continue;
 const alertaId=`${s.shipmentId}:${rule}`;const result=await monitor.updateOne({shipmentId:s.shipmentId,emitidas:{$ne:rule}},{$addToSet:{emitidas:rule}});if(result.modifiedCount)await publish(monitorProducer,TOPICS.alertas,event('ALERTA_SLA',s.shipmentId,{secuencia:s.secuencia||0,hubId:s.hubId,zonaId:s.zonaId,repartidorId:s.repartidorId,alertaId,regla:rule,estado:'ABIERTA',prioridad:priority,causa:rule},new Date(now).toISOString()));}
 if(s.estado==='ENTREGADO'){for(const rule of s.emitidas||[]){if((s.cerradas||[]).includes(rule))continue;const result=await monitor.updateOne({shipmentId:s.shipmentId,cerradas:{$ne:rule}},{$addToSet:{cerradas:rule}});if(result.modifiedCount)await publish(monitorProducer,TOPICS.alertas,event('CIERRE_SLA',s.shipmentId,{secuencia:s.secuencia||0,hubId:s.hubId,zonaId:s.zonaId,repartidorId:s.repartidorId,alertaId:`${s.shipmentId}:${rule}`,regla:rule,estado:'CERRADA',prioridad:({SIN_INGRESO:'MEDIA',SIN_SALIDA:'ALTA',FALLO_DIA_LIMITE:'ALTA',VENCIDO:'CRITICA'})[rule],causa:'ENTREGADO'},new Date(now).toISOString()));}}
}
async function monitorHandle({e,topic}){await once(role,e,async(session)=>{let update={hubId:e.hubId,zonaId:e.zonaId,repartidorId:e.repartidorId,tiempoReferencia:e.ocurridoEn,tiempoRealReferencia:Date.now()};if(topic===TOPICS.promesas)update.venceEn=e.venceEn;
 else if(topic===TOPICS.estados){const cur=await monitor.findOne({shipmentId:e.shipmentId},{session});if(cur&&e.secuencia<=cur.secuencia)return;update.estado=e.tipo;update.secuencia=e.secuencia;}
 await monitor.updateOne({shipmentId:e.shipmentId},{$set:update,$setOnInsert:{emitidas:[],cerradas:[]}},{upsert:true,session});});const s=await monitor.findOne({shipmentId:e.shipmentId});if(s)await evaluate(s,Date.parse(e.ocurridoEn));}
const handlers={proyector:{topics:[TOPICS.estados,TOPICS.promesas,TOPICS.reprogramaciones,TOPICS.alertas,TOPICS.notificaciones],handle:project},notificaciones:{topics:[TOPICS.estados,TOPICS.alertas],handle:notify},reprogramacion:{topics:[TOPICS.estados],handle:reschedule},analitica:{topics:[TOPICS.estados],handle:analytics},'monitor-sla':{topics:[TOPICS.estados,TOPICS.promesas],handle:monitorHandle}};
if(!handlers[baseRole])throw Error('Rol desconocido: '+role);
const {producer}=await consume({role,...handlers[baseRole]});if(['notificaciones','reprogramacion'].includes(role)){await flushOutbox(producer);setInterval(()=>flushOutbox(producer).catch(console.error),4000).unref();}
if(role==='monitor-sla'){monitorProducer=producer;setInterval(async()=>{try{for(const s of await monitor.find({estado:{$ne:'ENTREGADO'}}).toArray()){if(!s.tiempoReferencia)continue;const simulated=Date.parse(s.tiempoReferencia)+(Date.now()-(s.tiempoRealReferencia||Date.now()))*Number(process.env.SIM_SPEED||120);await evaluate(s,simulated);}}catch(err){console.error(err)}},500).unref();}
