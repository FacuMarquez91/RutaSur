import { Kafka, logLevel } from 'kafkajs';
import { MongoClient } from 'mongodb';
import { randomUUID } from 'node:crypto';
export const TOPICS={estados:'envios.estados',promesas:'envios.promesas',reprogramaciones:'envios.reprogramaciones',alertas:'alertas.sla',notificaciones:'notificaciones.enviadas',dlq:'envios.estados.dlq'};
export const kafka=new Kafka({clientId:process.env.ROLE||'rutasur',brokers:(process.env.KAFKA_BROKERS||'localhost:29092').split(','),logLevel:logLevel.WARN,retry:{retries:20,initialRetryTime:500}});
export const mongo=new MongoClient(process.env.MONGO_URL||'mongodb://localhost:27017/?directConnection=true');
export const db=()=>mongo.db(process.env.DB_NAME||'rutasur');
export const uuid=()=>randomUUID();
export const sleep=ms=>new Promise(r=>setTimeout(r,ms));
export async function connect(){await mongo.connect();await db().command({ping:1});}
export async function ensureIndexes(){const d=db();await Promise.all([
 d.collection('envios').createIndex({shipmentId:1},{unique:true}),d.collection('envios').createIndex({estado:1,zonaId:1}),d.collection('envios').createIndex({'promesa.venceEn':1}),
 d.collection('alertas').createIndex({estado:1,prioridad:1}),d.collection('alertas').createIndex({shipmentId:1}),d.collection('alertas').createIndex({alertaId:1},{unique:true}),
 d.collection('notificaciones').createIndex({shipmentId:1}),d.collection('notificaciones').createIndex({eventId:1},{unique:true}),
 d.collection('metricas_diarias').createIndex({fecha:1,zonaId:1,hubId:1,franja:1},{unique:true}),
 d.collection('eventos_procesados').createIndex({consumidor:1,eventId:1},{unique:true}),d.collection('eventos_procesados').createIndex({createdAt:1},{expireAfterSeconds:604800}),
 d.collection('monitor_estado').createIndex({shipmentId:1},{unique:true}),d.collection('indicador_hub').createIndex({hubId:1},{unique:true})
 ]);}
export function event(tipo,shipmentId,extra={},time=new Date().toISOString()){return {eventId:uuid(),shipmentId,tipo,ocurridoEn:time,secuencia:extra.secuencia??0,hubId:extra.hubId||'HUB-1',zonaId:extra.zonaId||'ZONA-1',repartidorId:extra.repartidorId||'REP-001',version:1,...extra};}
export function valid(e,topic){if(!e||typeof e!=='object'||!e.eventId||!e.shipmentId||!e.tipo||!Number.isInteger(e.secuencia)||e.secuencia<0||!e.ocurridoEn||!Number.isFinite(Date.parse(e.ocurridoEn))||!e.hubId||!e.zonaId||!e.repartidorId||e.version!==1)return 'Campos comunes faltantes o invalidos';
 if(topic===TOPICS.estados&&!['CREADO','INGRESADO_HUB','CLASIFICADO','EN_REPARTO','ENTREGADO','ENTREGA_FALLIDA'].includes(e.tipo))return 'Estado desconocido';
 if(topic===TOPICS.estados&&e.tipo==='ENTREGA_FALLIDA'&&!['AUSENTE','DIRECCION_INCORRECTA','RECHAZADO','ZONA_INACCESIBLE'].includes(e.motivo))return 'Motivo invalido';
 if(topic===TOPICS.promesas&&(!e.venceEn||!Number.isFinite(Date.parse(e.venceEn))))return 'Vencimiento invalido';return null;}
export async function publish(producer,topic,e){await producer.send({topic,messages:[{key:e.shipmentId,value:JSON.stringify(e)}]});}
export function logEvent(role,topic,partition,message){console.log(JSON.stringify({servicio:role,topic,partition,offset:message.offset,eventId:(()=>{try{return JSON.parse(message.value).eventId}catch{return null}})()}));}
export async function consume({role,topics,handle}){const consumer=kafka.consumer({groupId:role});const producer=kafka.producer();await producer.connect();await consumer.connect();for(const topic of topics)await consumer.subscribe({topic,fromBeginning:true});console.log(role,'listo, escuchando',topics);
 await consumer.run({autoCommit:false,partitionsConsumedConcurrently:1,sessionTimeout:60000,heartbeatInterval:3000,eachMessage:async({topic,partition,message})=>{
 logEvent(role,topic,partition,message);let e;try{e=JSON.parse(message.value.toString())}catch{e=null}
 const error=valid(e,topic);if(error){if(topic===TOPICS.estados)await publish(producer,TOPICS.dlq,event('INVALIDO',e?.shipmentId||'DESCONOCIDO',{motivo:error,original:message.value.toString(),origen:topic}));console.error(role,'descartado',error);await consumer.commitOffsets([{topic,partition,offset:String(BigInt(message.offset)+1n)}]);return;}
 try{await handle({e,topic,producer});await consumer.commitOffsets([{topic,partition,offset:String(BigInt(message.offset)+1n)}]);}catch(err){console.error(role,'ERROR',err);throw err;}
 }});return {consumer,producer};}
// Marcador idempotente y cambios de lectura se confirman en una sola transacción.
// Los conflictos transitorios (112/251) se reintentan con una nueva sesión.
export async function once(role,e,fn){
 const marks=db().collection('eventos_procesados');
 for(let attempt=0;attempt<12;attempt++){
  const session=mongo.startSession();
  try{
   if(await marks.findOne({consumidor:role,eventId:e.eventId},{projection:{_id:1}}))return false;
   await session.withTransaction(async()=>{
    await marks.insertOne({consumidor:role,eventId:e.eventId,createdAt:new Date()},{session});
    await fn(session);
   },{maxCommitTimeMS:20000});
   return true;
  }catch(err){
   // A unique marker collision means another attempt already applied this event.
   if(err.code===11000){
    if(await marks.findOne({consumidor:role,eventId:e.eventId},{projection:{_id:1}}))return false;
   }
   const transient=err.hasErrorLabel?.('TransientTransactionError')||err.hasErrorLabel?.('UnknownTransactionCommitResult')||[112,251,244,50].includes(err.code);
   if(!transient||attempt===11)throw err;
   await sleep(Math.min(2500,100*(attempt+1)**2));
  }finally{await session.endSession();}
 }
}
export function plusDaysISO(iso,days){const d=new Date(iso);d.setUTCDate(d.getUTCDate()+days);return d.toISOString()}
export function opposite(f){return f==='MANANA'?'TARDE':'MANANA'}
