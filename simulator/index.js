import express from 'express';import {kafka,event,publish,TOPICS,uuid,sleep} from '../common/core.js';
const app=express();app.use(express.json());const producer=kafka.producer();await producer.connect();
let seed=Number(process.env.SIM_SEED||2026)>>>0;const rnd=()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296};
const speed=Number(process.env.SIM_SPEED||120);const start=Date.now(),virtualStart=Date.now();const now=()=>new Date(virtualStart+(Date.now()-start)*speed).toISOString();const delay=ms=>sleep(Math.max(20,ms/speed));
const motives=['AUSENTE','DIRECCION_INCORRECTA','RECHAZADO','ZONA_INACCESIBLE'];function chooseMotivo(){let r=rnd();return r<.6?motives[0]:r<.8?motives[1]:r<.95?motives[2]:motives[3]};
let generated=0;
async function sendState(shipmentId,tipo,seq,ctx,extra={},time=now()){const e=event(tipo,shipmentId,{...ctx,...extra,secuencia:seq},time);await publish(producer,TOPICS.estados,e);if(rnd()<.02)await publish(producer,TOPICS.estados,e);if(rnd()<.02&&seq>1)await publish(producer,TOPICS.estados,event('CREADO',shipmentId,{...ctx,secuencia:1},time));if(rnd()<.01)await publish(producer,TOPICS.estados,{...e,eventId:uuid(),tipo:'ESTADO_INVALIDO'});return e;}
async function runScenario(scenario='feliz',fast=false){generated++;const shipmentId=`RS-${String(generated).padStart(7,'0')}-${uuid().slice(0,4)}`;const zoneIdx=Math.floor(rnd()*3);const ctx={hubId:`HUB-${zoneIdx+1}`,zonaId:`ZONA-${zoneIdx+1}`,repartidorId:`REP-${String(Math.floor(rnd()*120)+1).padStart(3,'0')}`,franja:rnd()<.5?'MANANA':'TARDE'};
 let date=now();const venceEn=new Date(Date.parse(date)+(scenario==='demorado'?5:24)*3600000).toISOString();await publish(producer,TOPICS.promesas,event('PROMESA',shipmentId,{...ctx,venceEn},date));
 const stages=['CREADO','INGRESADO_HUB','CLASIFICADO','EN_REPARTO'];let seq=0;
 const send=async(tipo,extra={},deltaMinutes=10)=>{date=new Date(Date.parse(date)+deltaMinutes*60000).toISOString();return sendState(shipmentId,tipo,++seq,ctx,{...extra,venceEn},date)};
 await send('CREADO',{},0);await delay(fast?10:150);
 if(scenario==='demorado'){await send('INGRESADO_HUB',{},20);return shipmentId;}
 for(const stage of stages.slice(1)){await send(stage,{},10);await delay(fast?10:170);}
 if(scenario==='tercer-fallo'){for(let i=1;i<=3;i++){await send('ENTREGA_FALLIDA',{motivo:'AUSENTE',intento:i},30);if(i<3){await send('EN_REPARTO',{intento:i+1},60);}}}
 else if(scenario==='ausente'){await send('ENTREGA_FALLIDA',{motivo:'AUSENTE',intento:1},40);await delay(120);await send('EN_REPARTO',{intento:2},1440);await send('ENTREGADO',{intento:2},30);}
 else if(scenario==='feliz'){await send('ENTREGADO',{intento:1},40);}
 else{const rates=[.05,.18,.30];if(rnd()<rates[zoneIdx])await send('ENTREGA_FALLIDA',{motivo:chooseMotivo(),intento:1},40);else await send('ENTREGADO',{intento:1},40);}
 return shipmentId;}
app.get('/health',(req,res)=>res.json({ok:true,generated,speed,clock:now()}));
app.post('/scenario/:name',async(req,res)=>{const name=req.params.name;if(!['feliz','ausente','demorado','tercer-fallo','rafaga'].includes(name))return res.status(400).json({error:'Escenario inválido'});try{if(name==='rafaga'){for(let i=0;i<500;i++)runScenario('aleatorio',true).catch(console.error);return res.json({ok:true,cantidad:500});}const shipmentId=await runScenario(name);res.json({ok:true,shipmentId});}catch(err){res.status(500).json({error:err.message})}});
app.listen(4000,'0.0.0.0',()=>console.log('Simulador puerto 4000'));
setInterval(()=>runScenario('aleatorio').catch(console.error),60000/Math.max(1,Number(process.env.SIM_RATE||20))).unref();
