// One native process serves synchronous registry calls and asynchronous task
// calls. A synchronous caller pumps frames so nested host callbacks can finish.
import {Worker,MessageChannel,receiveMessageOnPort} from "node:worker_threads";
import {appendFileSync} from "node:fs";
let worker,port,signal,next=1,closed=false,closing=false,transportFailure,exitResolve;
const pending=new Map(),handlers=new Map(),events=new Map();
let exited;
export function failure(value){const error=new Error(value.message);error.name=value.name??"Error";return error;}
function trace(direction,frame){if(process.env.PI_DURABLE_NATIVE_TRACE)appendFileSync(process.env.PI_DURABLE_NATIVE_TRACE,JSON.stringify({direction,type:frame.type,id:frame.id,method:frame.method,error:!!frame.error})+"\n");}
function send(frame){trace("send",frame);port.postMessage(frame);}
function fail(error){transportFailure=error;for(const [id,waiter] of pending){if(!waiter.sync){pending.delete(id);waiter.detach?.();waiter.reject(error);}}}
function dispatch(frame){
 trace("receive",frame);
 if(frame.type==="admitted"){const waiter=pending.get(frame.id);if(waiter)waiter.admitted=true;return;}
 if(frame.type==="callback"){
  const handler=handlers.get(frame.method);
  const reply=value=>send({type:"callback.result",id:frame.id,...(value===undefined?{missing:true}:{value})});
  const reject=error=>send({type:"callback.result",id:frame.id,error:{name:error.name,message:error.message}});
  try{
   if(!handler)throw new Error(`Unknown native callback ${frame.method}`);
   const value=handler(frame.args,pending.get(frame.context)?.context,frame);
   if(frame.sync){if(value&&typeof value.then==="function")throw new Error(`Synchronous callback returned a Promise: ${frame.method}`);reply(value);}
   else Promise.resolve(value).then(reply,reject);
  }catch(error){reject(error);}
  return;
 }
 if(frame.type==="transport.error"){fail(new Error(frame.message));return;}
 if(frame.type==="transport.exit"){
  closed=true;exitResolve(frame.code);
  if(frame.code!==0||pending.size)fail(new Error(`Native durable bridge exited ${frame.code}`));return;
 }
 if(events.has(frame.type)){events.get(frame.type)(frame);return;}
 const waiter=pending.get(frame.id);
 if(!waiter){fail(new Error(`Unknown native response ${frame.id}`));return;}
 if(waiter.sync){waiter.frame=frame;return;}
 pending.delete(frame.id);waiter.detach?.();frame.error?waiter.reject(failure(frame.error)):waiter.resolve(frame.missing?undefined:frame.value);
}
function start(){
 if(worker)return;
 const binary=process.env.PI_DURABLE_NATIVE_TASK_BRIDGE;if(!binary)throw new Error("Missing native durable bridge");
 const {port1,port2}=new MessageChannel();port=port1;signal=new Int32Array(new SharedArrayBuffer(4));exited=new Promise(resolve=>{exitResolve=resolve;});
 worker=new Worker(new URL("./durable-sync-worker.mjs",import.meta.url),{workerData:{port:port2,signal:signal.buffer,binary},transferList:[port2]});
 port.on("message",dispatch);worker.on("error",fail);
}
function drain(){for(let message;(message=receiveMessageOnPort(port));)dispatch(message.message);}
export const nativeTransport={
 get nextId(){return next;},get started(){return !!worker;},get pending(){return pending.size;},
 register(method,handler){if(handlers.has(method))throw new Error(`Duplicate native handler ${method}`);handlers.set(method,handler);},
 event(type,handler){if(events.has(type))throw new Error(`Duplicate native event ${type}`);events.set(type,handler);},
 notify(frame){if(worker&&!closing)send(frame);},
 request(method,args={},context){
  start();const id=next++;
  const response=new Promise((resolve,reject)=>{
   const signal=context?.abortSignal;const abort=()=>send({type:"abort",id,message:signal.reason?.message??String(signal.reason)});
   pending.set(id,{resolve,reject,context,detach:()=>signal?.removeEventListener("abort",abort)});send({id,method,...args,...(signal?.aborted?{abortInitially:signal.reason?.message??String(signal.reason)}:{})});
   if(!signal?.aborted)signal?.addEventListener("abort",abort,{once:true});
  });
  // Native close can reject several waits before the held commit's reply
  // crosses the pipe. Observe transport failures immediately; callers still
  // receive this same rejecting promise and the original assertions see it.
  response.catch(()=>{});return response;
 },
 requestAdmitted(method,args={},context){
  const id=next,response=this.request(method,{...args,admit:true},context),waiter=pending.get(id),deadline=Date.now()+30000;
  while(pending.has(id)&&!waiter.admitted){
   const observed=Atomics.load(signal,0);drain();
   if(transportFailure)throw transportFailure;if(closed)throw new Error("Native durable transport closed");
   if(Date.now()>deadline)throw new Error(`Native durable admission timed out: ${method}`);
   if(pending.has(id)&&!waiter.admitted)Atomics.wait(signal,0,observed,10);
  }
  return response;
 },
 sync(method,args={}){
  start();const id=next++,waiter={sync:true};pending.set(id,waiter);send({id,method,...args});const deadline=Date.now()+30000;
  try{
   while(!waiter.frame){
    const observed=Atomics.load(signal,0);drain();
    if(transportFailure)throw transportFailure;if(closed)throw new Error("Native durable transport closed");
    if(Date.now()>deadline)throw new Error(`Native durable request timed out: ${method}`);
    if(!waiter.frame)Atomics.wait(signal,0,observed,10);
   }
   if(waiter.frame.error)throw failure(waiter.frame.error);return waiter.frame.missing?undefined:waiter.frame.value;
  }finally{pending.delete(id);}
 },
 async close(){
  if(!worker)throw new Error("Stock tests did not reach Rust");
  if(pending.size)throw new Error(`Native requests still pending: ${pending.size}`);
  closing=true;send({type:"transport.close"});if(await exited!==0)throw new Error("Native durable bridge failed");
  port.close();await worker.terminate();
 },
};
