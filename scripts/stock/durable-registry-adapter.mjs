// Test-only handles preserve the identity of original JS registrations. All
// registry decisions and agent composition execute in the Rust process.
import {Worker,MessageChannel,receiveMessageOnPort} from "node:worker_threads";
import {writeFileSync} from "node:fs";
import {GenerationTask,ToolTask,CompactionTask} from "@earendil-works/pi-durable";
let worker,port,signal,next=1,handleNext=1,closed=false,transportFailure;
const objects=new Map(),ids=new WeakMap(),responses=new Map(),snapshots=new Map(),agents=new WeakMap();
const callbacks={listeners:0,wrappers:0,reports:0};
function handle(value){let id=ids.get(value);if(id===undefined){id=handleNext++;ids.set(value,id);objects.set(id,value);}return id;}
function failure(value){const error=new Error(value.message);error.name=value.name??"Error";return error;}
function start(){
 if(worker)return;
 const binary=process.env.PI_DURABLE_NATIVE_TASK_BRIDGE;if(!binary)throw new Error("Missing native registry bridge");
 const {port1,port2}=new MessageChannel();port=port1;signal=new Int32Array(new SharedArrayBuffer(4));
 worker=new Worker(new URL("./durable-sync-worker.mjs",import.meta.url),{workerData:{port:port2,signal:signal.buffer,binary},transferList:[port2]});
}
function describeTool(tool){return{id:handle(tool),name:tool.name,callers:tool.callers,deferred:tool.deferred===true};}
function describeSection(section){return{id:handle(section),key:section.key};}
function describeTask(task){const {name,version,phases,migrate}=task.definition;return{id:handle(task),name,version,phases:Object.keys(phases),migrate:migrate!==undefined};}
function describeExtension(extension){return{
 id:handle(extension),name:extension.name,tools:(extension.tools??[]).map(describeTool),sections:(extension.sections??[]).map(describeSection),tasks:(extension.tasks??[]).map(describeTask),
 hooks:(extension.hooks??[]).map(hook=>({task:hook.task,handlers:handle(hook.handlers)})),
 wraps:(extension.wraps??[]).map(wrap=>({...(Object.hasOwn(wrap,"tool")?{tool:wrap.tool}:{section:wrap.section}),callback:handle(wrap.wrap)})),
};}
function invoke(frame){
 const args=frame.args;
 if(frame.method==="registry.listener"){callbacks.listeners++;objects.get(args)();return null;}
 if(frame.method==="registry.wrap"){callbacks.wrappers++;const result=objects.get(args.callback)(objects.get(args.item));return args.kind==="tool"?describeTool(result):describeSection(result);}
 if(frame.method==="registry.report"){callbacks.reports++;objects.get(args.callback)(failure(args.error),args.extension);return null;}
 if(frame.method==="initial"){const [id,input]=args;return objects.get(id).definition.initial(input);}
 if(frame.method==="migrate"){const [id,input,checkpoint,version]=args;return objects.get(id).definition.migrate(input,checkpoint,version);}
 throw new Error(`Unknown synchronous callback ${frame.method}`);
}
function drain(){
 for(let message;(message=receiveMessageOnPort(port));){const frame=message.message;
  if(frame.type==="callback"){
   if(!frame.sync)throw new Error("Unexpected asynchronous registry callback");
   try{const value=invoke(frame);port.postMessage({type:"callback.result",id:frame.id,...(value===undefined?{missing:true}:{value})});}
   catch(error){port.postMessage({type:"callback.result",id:frame.id,error:{name:error.name,message:error.message}});}
  }else if(frame.type==="transport.error")transportFailure=new Error(frame.message);
  else if(frame.type==="transport.exit"){closed=true;if(frame.code!==0)transportFailure=new Error(`Native registry bridge exited ${frame.code}`);}
  else responses.set(frame.id,frame);
 }
}
function call(method,args={}){
 start();const id=next++;port.postMessage({id,method:`registry.${method}`,...args});
 const deadline=Date.now()+30000;
 while(!responses.has(id)){
  const observed=Atomics.load(signal,0);drain();
  if(transportFailure)throw transportFailure;
  if(closed)throw new Error("Native registry transport closed");
  if(Date.now()>deadline)throw new Error(`Native registry request timed out: ${method}`);
  if(!responses.has(id))Atomics.wait(signal,0,observed,10);
 }
 const frame=responses.get(id);responses.delete(id);if(frame.error)throw failure(frame.error);return frame.missing?undefined:frame.value;
}
export function createRegistry(){
 const registry=call("create",{builtins:[GenerationTask,ToolTask,CompactionTask].map(describeTask)});
 return{
  snapshot:()=>snapshot(call("snapshot",{registry})),
  install:extension=>{call("install",{registry,extension:describeExtension(extension)});},
  uninstall:extension=>{call("uninstall",{registry,name:extension.name});},
  subscribe:listener=>{const subscription=call("subscribe",{registry,callback:handle(listener)});return()=>{call("unsubscribe",{subscription});};},
 };
}
function snapshot(id){
 let held=snapshots.get(id);if(held)return held;
 const request=(method,args={})=>call(method,{snapshot:id,...args});
 held={installed:()=>request("installed").map(id=>objects.get(id)),extension:name=>objects.get(request("extension",{name})),
  tools:()=>request("tools").map(row=>({extension:objects.get(row.extension),tool:objects.get(row.tool)})),
  sections:()=>request("sections").map(row=>({extension:objects.get(row.extension),section:objects.get(row.section)})),
  tasks:()=>request("tasks").map(id=>objects.get(id)),task:name=>objects.get(request("task",{name})),
 };
 snapshots.set(id,held);ids.set(held,id);return held;
}
function settingsInput(settings){return {...settings,...(settings?.extensions===undefined?{}:{extensions:settings.extensions.map(extension=>extension.name)})};}
export function resolveSettings(settings){
 const result=call("settings",{settings:settingsInput(settings)});
 if(settings?.extensions!==undefined)result.extensions=settings.extensions;
 return result;
}
export function resolveAgent(state,snapshot,settings,report){
 const result=call("resolve",{snapshot:ids.get(snapshot),state:state??null,settings:settingsInput(settings),report:handle(report)});
 const {id,...fields}=result;
 const agent={...fields,extensions:fields.extensions.map(id=>objects.get(id)),tools:fields.tools.map(id=>objects.get(id)),callable:fields.callable.map(id=>objects.get(id)),deferred:fields.deferred.map(id=>objects.get(id)),
  sections:fields.sections.map(section=>Object.hasOwn(section,"id")?objects.get(section.id):{key:"instructions",render:()=>section.instructions}),
 };
 agents.set(agent,id);return agent;
}
export function agentHooks(agent,task){return call("hooks",{agent:agents.get(agent),task}).map(hook=>({...hook,handlers:objects.get(hook.handlers)}));}
export function sectionExtension(agent,key){return call("sectionExtension",{agent:agents.get(agent),key});}
export function toolExtension(agent,name){return call("toolExtension",{agent:agents.get(agent),name});}
export async function shutdownRegistry(){
 if(!worker)throw new Error("Stock registry tests did not reach Rust");
 const native=call("stats");
 if(native.operations===0||native.resolutions===0||native.wrappers===0||callbacks.listeners===0||callbacks.wrappers!==native.wrappers)throw new Error("Native registry or callback execution was bypassed");
 if(responses.size)throw new Error(`Unobserved native registry responses: ${responses.size}`);
 port.postMessage({type:"transport.close"});const deadline=Date.now()+10000;
 while(!closed){const observed=Atomics.load(signal,0);drain();if(transportFailure)throw transportFailure;if(Date.now()>deadline)throw new Error("Native registry bridge failed to close");if(!closed)Atomics.wait(signal,0,observed,10);}
 port.close();await worker.terminate();
 if(process.env.PI_DURABLE_NATIVE_REGISTRY_STATS)writeFileSync(process.env.PI_DURABLE_NATIVE_REGISTRY_STATS,JSON.stringify({...native,callbacks}));
}
