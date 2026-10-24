// Test transport only. The native process owns task state, invocation lifetime,
// reservations and the Session line. These callbacks run the original JS tasks.
import {writeFileSync} from "node:fs";
import {BACKGROUND_CONTEXT,withAbortSignal,withoutAbortSignal} from "@earendil-works/chord/context";
import {nativeTransport,failure as error} from "./durable-native-transport.mjs";
import {describeTask,objectFor} from "./durable-native-handles.mjs";
import {nativeRegistryId,snapshot} from "./durable-registry-adapter.mjs";
let storageNext=1,callbackNext=1;
const storages=new Map(),callbacks=new Map(),runtimes=new Map(),harnesses=new Map();
const stats={phases:0,commits:0,storageCalls:0};
const call=(method,args={},context)=>nativeTransport.request(method,args,context);
const send=frame=>nativeTransport.notify(frame);
nativeTransport.event("signal",frame=>runtime(frame.runtime).controller.abort(new Error(frame.message)));
nativeTransport.event("report",frame=>harnesses.get(frame.harness)?.onReport?.(error(frame.error),frame.info));
nativeTransport.register("now",id=>harnesses.get(id)?.options.now?.()??Date.now());
for(const method of ["phase","change","storage"])nativeTransport.register(method,(args,context,frame)=>invoke(method,args,frame.abortable?(context??BACKGROUND_CONTEXT):withoutAbortSignal(context??BACKGROUND_CONTEXT)));
function change(method,args,callback,context){const id=callbackNext++;callbacks.set(id,callback);const operation=call(method,{...args,callback:id},context).finally(()=>callbacks.delete(id));operation.catch(()=>{});return operation;}
function transaction(id){return{
 createTask:(task,input,options)=>call("createTask",{tx:id,definition:describeTask(task),input,options}),
 appendEntry:(conversation,entry)=>call("appendEntry",{tx:id,conversation,entry}),
 entry:entry=>call("entry",{tx:id,entry}),
 scanTasks:(query,limit,cursor)=>call("scanTasks",{tx:id,query,limit,cursor}),
};}
function runtime(id){
 let held=runtimes.get(id);if(held)return held;
 const controller=new AbortController();
 held={controller,signal:controller.signal,get registry(){return snapshot(nativeTransport.sync("runtimeRegistry",{runtime:id}));},commit:(callback,context)=>change("commit",{runtime:id},callback,context),
  memo:(name,...args)=>args.length===1?call("memo",{runtime:id,name},args[0]):call("memo",{runtime:id,name,candidate:args[0]},args[1]),
  getTask:(task,context)=>call("runtimeGetTask",{runtime:id,task},context),
  outcomes:(tasks,context)=>call("outcomes",{runtime:id,tasks},context),
 };
 runtimes.set(id,held);return held;
}
async function invoke(method,args,context){
 if(method==="phase"){
  stats.phases++;const[id,phase,task,runtimeId]=args;const current=runtime(runtimeId);current.taskId=task.id;current.conversationId=task.conversationId;
  const context=withAbortSignal(current.signal,BACKGROUND_CONTEXT);const definition=objectFor(id).definition;
  return (phase==="$abort"?definition.abort:definition.phases[phase])(task,current,context);
 }
 if(method==="change")return callbacks.get(args.callback)(transaction(args.tx),args.current??undefined);
 if(method==="storage"){
  stats.storageCalls++;const storage=storages.get(args.handle);let values=args.args;
  if(args.method==="commit"){
   const[writes,adoption]=values;return storage.commit(writes,seq=>send({type:"storage.adopt",id:adoption,seq}),context);
  }
  // JSON null is used only for omitted optional cursor/cutoff arguments here.
  if(args.method.startsWith("scan")||args.method==="findLatestHeadMarker")values=values.map(value=>value===null?undefined:value);
  return storage[args.method](...values,context);
 }
 throw new Error(`Unknown native callback ${method}`);
}
export const Harness={async open(storage,options,context){
 const storageId=storageNext++;storages.set(storageId,storage);
 const registry=nativeRegistryId(options.registry);if(registry===undefined)throw new Error("Task tests require the native registry adapter");
 const expected=nativeTransport.nextId;harnesses.set(expected,{options,onReport:options.onReport});
 const id=await call("open",{storage:storageId,registry,retention:Number(process.env.PI_DURABLE_DOCUMENT_RETENTION_MS??600000)},context);
 const harness={options,onReport:options.onReport,
  root:async context=>conversation(await call("root",{harness:id},context)),
  resume:()=>{nativeTransport.sync("resume",{harness:id});},
  createConversation:async(options,context)=>conversation(await call("createConversation",{harness:id,options},context)),
  waitForIdle:context=>call("waitForIdle",{harness:id,conversation:null},context),
  getTask:(task,context)=>call("getTask",{harness:id,task},context),
  waitForTask:(task,context)=>call("waitForTask",{harness:id,task},context),
  abortTask:(task,context)=>call("abortTask",{harness:id,task},context),
  commit:(callback,context)=>change("commit",{harness:id,conversation:null},callback,context),
  close:context=>call("close",{harness:id},context),
 };
 function conversation(conversationId){return{id:conversationId,waitForIdle:context=>call("waitForIdle",{harness:id,conversation:conversationId},context),commit:(callback,context)=>change("commit",{harness:id,conversation:conversationId},callback,context)};}
 harnesses.set(id,harness);return harness;
}};
const storageIds=new WeakMap();let shuttingDown=false;
export const transport={request:call,storage(storage){let id=storageIds.get(storage);if(id===undefined){id=storageNext++;storageIds.set(storage,id);storages.set(id,storage);}return id;},release(range){if(!shuttingDown&&nativeTransport.started)send({type:"context.release",range});}};
async function finish(contextOnly){
 shuttingDown=true;
 if(!nativeTransport.started)throw new Error("Stock task tests did not reach Rust");
 const native=await call("stats");
 if(contextOnly){if(native.contextReads===0||stats.storageCalls===0)throw new Error("Native context implementation was bypassed");}
 else if(native.phases!==stats.phases||native.phases===0||native.commits===0||stats.storageCalls===0)throw new Error("Native scheduler was bypassed");
 Object.assign(stats,native);await nativeTransport.close();
 if(process.env.PI_DURABLE_NATIVE_TASK_STATS)writeFileSync(process.env.PI_DURABLE_NATIVE_TASK_STATS,JSON.stringify(stats));
}
export async function shutdown(){await finish(false);}
export async function shutdownContextTransport(){await finish(true);}
