// Test transport only. The native process owns task state, invocation lifetime,
// reservations and the Session line. These callbacks run the original JS tasks.
import {spawn} from "node:child_process";
import {createInterface} from "node:readline";
import {writeFileSync} from "node:fs";
import {BACKGROUND_CONTEXT,withAbortSignal} from "@earendil-works/chord/context";
let child,exited,next=1,storageNext=1,definitionNext=1,callbackNext=1;
const pending=new Map(),storages=new Map(),definitions=new Map(),definitionIds=new WeakMap(),callbacks=new Map(),runtimes=new Map(),harnesses=new Map();
const stats={phases:0,commits:0,storageCalls:0};
function error(value){const failure=new Error(value.message);failure.name=value.name??"Error";return failure;}
function send(frame){child.stdin.write(JSON.stringify(frame)+"\n");}
function start(){
 if(child)return;
 if(!process.env.PI_DURABLE_NATIVE_TASK_BRIDGE)throw new Error("Missing native task bridge");
 child=spawn(process.env.PI_DURABLE_NATIVE_TASK_BRIDGE,[],{stdio:["pipe","pipe","inherit"]});
 exited=new Promise(resolve=>child.once("exit",code=>{for(const waiter of pending.values())waiter.reject(new Error(`Native task bridge exited ${code}`));pending.clear();resolve(code);}));
 createInterface({input:child.stdout}).on("line",line=>{
  const frame=JSON.parse(line);
  if(frame.type==="callback"){
   invoke(frame.method,frame.args).then(value=>send({type:"callback.result",id:frame.id,...(value===undefined?{missing:true}:{value})}),failure=>send({type:"callback.result",id:frame.id,error:{name:failure.name,message:failure.message}}));return;
  }
  if(frame.type==="signal"){runtime(frame.runtime).controller.abort(new Error(frame.message));return;}
  if(frame.type==="report"){const harness=harnesses.get(frame.harness);harness?.onReport?.(error(frame.error),frame.info);return;}
  const waiter=pending.get(frame.id);if(!waiter)throw new Error(`Unknown native task response ${frame.id}`);
  pending.delete(frame.id);waiter.detach?.();frame.error?waiter.reject(error(frame.error)):waiter.resolve(frame.missing?undefined:frame.value);
 });
}
function call(method,args={},context){
 start();
 const id=next++;
 return new Promise((resolve,reject)=>{
  const signal=context?.abortSignal;const abort=()=>send({type:"abort",id,message:signal.reason?.message??String(signal.reason)});
  pending.set(id,{resolve,reject,detach:()=>signal?.removeEventListener("abort",abort)});
  send({id,method,...args});
  if(signal?.aborted)abort();else signal?.addEventListener("abort",abort,{once:true});
 });
}
function describe(task){
 let id=definitionIds.get(task);if(id===undefined){id=definitionNext++;definitionIds.set(task,id);definitions.set(id,task.definition);}
 const {name,version,phases,migrate}=task.definition;return{id,name,version,phases:Object.keys(phases),migrate:migrate!==undefined};
}
function change(method,args,callback,context){const id=callbackNext++;callbacks.set(id,callback);return call(method,{...args,callback:id},context).finally(()=>callbacks.delete(id));}
function transaction(id){return{
 createTask:(task,input,options)=>call("createTask",{tx:id,definition:describe(task),input,options}),
};}
function runtime(id){
 let held=runtimes.get(id);if(held)return held;
 const controller=new AbortController();
 held={controller,signal:controller.signal,commit:(callback,context)=>change("commit",{runtime:id},callback,context),
  memo:(name,...args)=>args.length===1?call("memo",{runtime:id,name},args[0]):call("memo",{runtime:id,name,candidate:args[0]},args[1]),
  getTask:(task,context)=>call("runtimeGetTask",{runtime:id,task},context),
  outcomes:(tasks,context)=>call("outcomes",{runtime:id,tasks},context),
 };
 runtimes.set(id,held);return held;
}
async function invoke(method,args){
 if(method==="now"){const harness=harnesses.get(args);return harness?.options.now?.()??Date.now();}
 if(method==="initial"){const[id,input]=args;return definitions.get(id).initial(input);}
 if(method==="migrate"){const[id,input,checkpoint,version]=args;return definitions.get(id).migrate(input,checkpoint,version);}
 if(method==="phase"){
  stats.phases++;const[id,phase,task,runtimeId]=args;const current=runtime(runtimeId);current.taskId=task.id;current.conversationId=task.conversationId;
  const context=withAbortSignal(current.signal,BACKGROUND_CONTEXT);const definition=definitions.get(id);
  return (phase==="$abort"?definition.abort:definition.phases[phase])(task,current,context);
 }
 if(method==="change")return callbacks.get(args.callback)(transaction(args.tx),args.current??undefined);
 if(method==="storage"){
  stats.storageCalls++;const storage=storages.get(args.handle);let values=args.args;
  if(args.method==="commit"){
   const[writes,adoption]=values;return storage.commit(writes,seq=>send({type:"storage.adopt",id:adoption,seq}),BACKGROUND_CONTEXT);
  }
  // JSON null is used only for omitted optional cursor/cutoff arguments here.
  if(args.method.startsWith("scan")||args.method==="findLatestHeadMarker")values=values.map(value=>value===null?undefined:value);
  return storage[args.method](...values,BACKGROUND_CONTEXT);
 }
 throw new Error(`Unknown native callback ${method}`);
}
export const Harness={async open(storage,options,context){
 const storageId=storageNext++;storages.set(storageId,storage);
 const tasks=options.registry.snapshot().installed().flatMap(extension=>extension.tasks??[]);
 const expected=next;harnesses.set(expected,{options,onReport:options.onReport});
 const id=await call("open",{storage:storageId,tasks:tasks.map(describe),retention:Number(process.env.PI_DURABLE_DOCUMENT_RETENTION_MS??600000)},context);
 const harness={options,onReport:options.onReport,
  root:async context=>conversation(await call("root",{harness:id},context)),
  resume:()=>{void call("resume",{harness:id}).catch(failure=>{throw failure;});},
  getTask:(task,context)=>call("getTask",{harness:id,task},context),
  waitForTask:(task,context)=>call("waitForTask",{harness:id,task},context),
  abortTask:(task,context)=>call("abortTask",{harness:id,task},context),
  commit:(callback,context)=>change("commit",{harness:id,conversation:null},callback,context),
  close:context=>call("close",{harness:id},context),
 };
 function conversation(conversationId){return{id:conversationId,commit:(callback,context)=>change("commit",{harness:id,conversation:conversationId},callback,context)};}
 harnesses.set(id,harness);return harness;
}};
export async function shutdown(){
 if(!child)throw new Error("Stock task tests did not reach Rust");
 const native=await call("stats");
 if(native.phases!==stats.phases||native.phases===0||native.commits===0||stats.storageCalls===0)throw new Error("Native scheduler was bypassed");
 Object.assign(stats,native);if(pending.size)throw new Error(`Native requests still pending: ${pending.size}`);
 child.stdin.end();if(await exited!==0)throw new Error("Native task bridge failed");
 if(process.env.PI_DURABLE_NATIVE_TASK_STATS)writeFileSync(process.env.PI_DURABLE_NATIVE_TASK_STATS,JSON.stringify(stats));
}
