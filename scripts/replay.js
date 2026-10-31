import {kafka,db,connect,ensureIndexes,TOPICS} from '../common/core.js';
await connect();await ensureIndexes();
const d=db();const consumer=kafka.consumer({groupId:'replay-tiempo-hub-v1'});
await consumer.connect();await consumer.subscribe({topic:TOPICS.estados,fromBeginning:true});
console.log('Reprocesando eventos históricos para tiempo medio de hub');
await consumer.run({eachMessage:async({message})=>{
 let e;try{e=JSON.parse(message.value.toString())}catch{return;}
 if(!e.eventId||!e.shipmentId||!['INGRESADO_HUB','EN_REPARTO'].includes(e.tipo))return;
 const col=d.collection('replay_pasos');
 if(e.tipo==='INGRESADO_HUB')await col.updateOne({shipmentId:e.shipmentId},{$set:{hubId:e.hubId,ingresado:e.ocurridoEn}},{upsert:true});
 else await col.updateOne({shipmentId:e.shipmentId},{$set:{salida:e.ocurridoEn}},{upsert:true});
 const row=await col.findOne({shipmentId:e.shipmentId});
 if(row?.ingresado&&row?.salida&&!row.calculado){
  const dur=(Date.parse(row.salida)-Date.parse(row.ingresado))/60000;
  if(dur>=0){await d.collection('indicador_hub').updateOne({hubId:row.hubId},{$set:{hubId:row.hubId},$inc:{totalMinutos:dur,cantidad:1}},{upsert:true});await col.updateOne({_id:row._id},{$set:{calculado:true}});}
 }
}});
