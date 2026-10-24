// Test transport only. The native process owns task state, invocation lifetime,
// reservations and the Session line. These callbacks run the original JS tasks.
import {writeFileSync} from "node:fs";
import {BACKGROUND_CONTEXT,withAbortSignal,withoutAbortSignal} from "@earendil-works/chord/context";
import {track} from "@earendil-works/chord/delta";
import {nativeTransport,failure as error} from "./durable-native-transport.mjs";
import {handle,describeTask,describeDoc,objectFor} from "./durable-native-handles.mjs";
import {settingsRequest} from "./durable-settings-wire.mjs";
import {nativeRegistryId,nativeSnapshotId,snapshot,hydrateAgent} from "./durable-registry-adapter.mjs";
let storageNext=1,callbackNext=1;
const storages=new Map(),callbacks=new Map(),runtimes=new Map(),harnesses=new Map(),graphRevisions=new Map();
const drafts=new Map();
function graphValue(snapshot){if(!graphRevisions.has(snapshot.revision))graphRevisions.set(snapshot.revision,snapshot.value);return graphRevisions.get(snapshot.revision);}
function watchValue(snapshot,document){const value=graphValue(snapshot);return document&&value===null?undefined:value;}
nativeTransport.register("graphFrame",args=>objectFor(args.callback)(watchValue(args.snapshot,args.document),args.ops,BACKGROUND_CONTEXT));
nativeTransport.afterCallback("graphFrame",frame=>nativeTransport.sync("graph.turn",{watch:frame.args.watch,serial:frame.args.serial}));
nativeTransport.afterCallback("phase",frame=>nativeTransport.sync("phase.turn",{runtime:frame.args[3],serial:frame.args[4]}));
function graphWatch(descriptor){if(descriptor===undefined)return undefined;const watch=descriptor.watch,closed=call("graph.closed",{watch});closed.catch(()=>{});return {get value(){return watchValue(nativeTransport.sync("graph.value",{watch}),descriptor.document);},closed,start:listener=>nativeTransport.sync("graph.start",{watch,callback:handle(listener)}),stop:()=>nativeTransport.sync("graph.stop",{watch}),cancel:()=>nativeTransport.sync("graph.cancel",{watch})};}
function docArguments(token,args){const definition=describeDoc(token);let index=0;const scope=definition.scope==="session"?{kind:"session"}:definition.scope==="task"?{kind:"task",taskId:args[index++]}:{kind:"conversation",conversationId:args[index++]};const address={kind:definition.kind,scope};if(definition.family)address.key=args[index++];return {definition,address,index};}
function documents(owner){return {
 snapshot:(token,...args)=>{const {index,...document}=docArguments(token,args);return call("doc.snapshot",{...owner,...document},args[index]).then(value=>value===undefined?undefined:graphValue(value));},
 snapshotAsOf:(token,...args)=>{const {index,...document}=docArguments(token,args);return call("doc.snapshotAsOf",{...owner,...document,at:args[index]},args[index+1]).then(value=>value===undefined?undefined:graphValue(value));},
 watchDoc:(token,...args)=>{const {index,...document}=docArguments(token,args);return call("doc.watch",{...owner,...document},args[index]).then(graphWatch);},
};}
function inspect(value){for(const entry of value.tasks)if(entry.state.error)entry.state.error=error(entry.state.error);return value;}
const stats={phases:0,commits:0,storageCalls:0};
const call=(method,args={},context)=>nativeTransport.request(method,args,context);
const send=frame=>nativeTransport.notify(frame);
nativeTransport.event("signal",frame=>runtime(frame.runtime).controller.abort(new Error(frame.message)));
nativeTransport.event("report",frame=>harnesses.get(frame.harness)?.onReport?.(error(frame.error),frame.info));
nativeTransport.register("now",id=>harnesses.get(id)?.options.now?.()??Date.now());
nativeTransport.register("settings",id=>{const input=harnesses.get(id)?.options.settings;return settingsRequest(typeof input==="function"?input():input);});
nativeTransport.register("readerSnapshot",id=>nativeSnapshotId(objectFor(id).snapshot()));
nativeTransport.register("readerSubscribe",([id,listener])=>handle(objectFor(id).subscribe(()=>nativeTransport.sync("registry.notifyReader",{listener}))));
nativeTransport.register("readerUnsubscribe",id=>objectFor(id)());
nativeTransport.register("hookMember",({handlers,name})=>{const owner=objectFor(handlers),member=owner[name];return typeof member==="function"?handle(member.bind(owner)):null;});
nativeTransport.register("hookInvoke",({callback,handler})=>objectFor(callback)(objectFor(handler)));
for(const method of ["phase","change","storage"])nativeTransport.register(method,(args,context,frame)=>invoke(method,args,frame.abortable?(context??BACKGROUND_CONTEXT):withoutAbortSignal(context??BACKGROUND_CONTEXT)));
function change(method,args,callback,context){const id=callbackNext++;callbacks.set(id,callback);const operation=call(method,{...args,callback:id},context).finally(()=>callbacks.delete(id));operation.catch(()=>{});return operation;}
function transaction(id){return{
 doc:async(token,...args)=>{const {index,...document}=docArguments(token,args);const acquired=await call("doc.acquire",{tx:id,...document,seed:token.definition.family?args[index]:undefined});let local=drafts.get(id);if(!local){local=new Map();drafts.set(id,local);}let held=local.get(acquired.draft);if(!held){const tracker=track(acquired.value);held={tracker,change:tracker.beginChange()};local.set(acquired.draft,held);}return held.change.state;},
 createConversation:options=>call("tx.createConversation",{tx:id,options}),
 forkConversation:(conversation,at,options)=>call("tx.forkConversation",{tx:id,conversation,at,options}),
 createTask:(task,input,options)=>call("createTask",{tx:id,definition:describeTask(task),input,options}),
 appendEntry:(first,second,third)=>typeof first==="number"?call("appendEntry",{tx:id,conversation:first,entry:second}):call("appendEntry",{tx:id,conversation:second,entry:{...third,kind:first.kind}}),
 entry:(first,second)=>typeof first==="number"?call("entry",{tx:id,entry:first}):call("entry",{tx:id,kind:first.kind,entry:second}),
 createSubmission:record=>call("createSubmission",{tx:id,record}),
 settleSubmission:(submission,settlement)=>call("settleSubmission",{tx:id,submission,settlement}),
 scanEntries:(query,limit,cursor)=>call("scanEntries",{tx:id,query,limit,cursor}),
 scanTasks:(query,limit,cursor)=>call("scanTasks",{tx:id,query,limit,cursor}),
};}
function runtime(id){
 let held=runtimes.get(id);if(held)return held;
 const controller=new AbortController();
 held={controller,signal:controller.signal,agent:context=>call("runtimeAgent",{runtime:id},context).then(hydrateAgent),hooks:{each:(name,invoke)=>call("runtimeHooks",{runtime:id,name,callback:handle(invoke)})},get registry(){return snapshot(nativeTransport.sync("runtimeRegistry",{runtime:id}));},commit:(callback,context)=>change("commit",{runtime:id},callback,context),
  memo:(name,...args)=>args.length===1?call("memo",{runtime:id,name},args[0]):call("memo",{runtime:id,name,candidate:args[0]},args[1]),
  getTask:(task,context)=>call("runtimeGetTask",{runtime:id,task},context),
  outcomes:(tasks,context)=>call("outcomes",{runtime:id,tasks},context),
  ...documents({runtime:id}),
  context:(conversation,context,options)=>call("runtimeContext",{runtime:id,conversation,at:options?.at},context),
  now:()=>nativeTransport.sync("runtimeNow",{runtime:id}),
  report:failure=>nativeTransport.sync("runtimeReport",{runtime:id,message:failure.message??String(failure)}),
  sleepUntil:(until,checkpoint,context)=>call("sleepUntil",{runtime:id,until,checkpoint},context),
 };
 runtimes.set(id,held);return held;
}
async function invoke(method,args,context){
 if(method==="phase"){
  stats.phases++;const[id,phase,task,runtimeId]=args;const current=runtime(runtimeId);current.taskId=task.id;current.conversationId=task.conversationId;
  const context=withAbortSignal(current.signal,BACKGROUND_CONTEXT);const definition=objectFor(id).definition;
  return (phase==="$abort"?definition.abort:definition.phases[phase])(task,current,context);
 }
 if(method==="change"){
  try{
   const result=await callbacks.get(args.callback)(transaction(args.tx),args.current??undefined);
   // JS captures edits to its proxy. Rust owns replay, validation, prepare,
   // persistence, adoption and publication of every document revision.
   for(const [draft,held] of drafts.get(args.tx)??[]){const prepared=held.change.prepare();await call("doc.apply",{draft,ops:prepared.ops});}
   return result;
  }finally{if(drafts.has(args.tx)){for(const held of drafts.get(args.tx).values())held.change.abort();drafts.delete(args.tx);nativeTransport.sync("doc.release",{tx:args.tx});}}
 }
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
 const registry=nativeRegistryId(options.registry);const reader=registry===undefined?handle(options.registry):undefined;
 const expected=nativeTransport.nextId;harnesses.set(expected,{options,onReport:options.onReport});
 const id=await call("open",{storage:storageId,registry,reader,retention:Number(process.env.PI_DURABLE_DOCUMENT_RETENTION_MS??600000)},context);
 const closed=call("closed",{harness:id}).then(end=>end.error?{...end,error:error(end.error)}:end);
 const harness={options,onReport:options.onReport,
  submission:(submission,context)=>call("submission.get",{harness:id,submission},context).then(submission=>submission===undefined?undefined:submissionHandle(submission)),
  abortSubmission:(submission,context,conversation)=>call("submission.abort",{harness:id,submission,conversation},context),
  inspect:context=>call("inspect",{harness:id},context).then(inspect),
  watchTaskGraph:context=>call("graph.watch",{harness:id},context).then(graphWatch),
  ...documents({harness:id}),
  conversation:async(idToFind,context)=>{const record=await call("conversation",{harness:id,conversation:idToFind},context);return record?conversation(record.id):undefined;},
  root:async(context,options)=>conversation(await call("root",{harness:id,options},context)),
  resume:()=>{nativeTransport.sync("resume",{harness:id});},
  createConversation:async(options,context)=>conversation(await call("createConversation",{harness:id,options},context)),
  waitForIdle:context=>call("waitForIdle",{harness:id,conversation:null},context),
  getTask:(task,context)=>call("getTask",{harness:id,task},context),
  waitForTask:(task,context)=>call("waitForTask",{harness:id,task},context),
  abortTask:(task,context)=>call("abortTask",{harness:id,task},context),
  commit:(callback,context)=>change("commit",{harness:id,conversation:null},callback,context),
  commitWith:(callback,context)=>change("commit",{harness:id,conversation:null},callback,context),
  closed,close:context=>nativeTransport.requestAdmitted("close",{harness:id},context),
 };
 function submissionHandle(submission){return {id:submission,status:context=>call("submission.status",{harness:id,submission},context),wait:context=>call("submission.wait",{harness:id,submission},context),abort:context=>call("submission.abort",{harness:id,submission,handle:true},context)};}
 function conversation(conversationId){return{id:conversationId,submit:(draft,context)=>call("submission.submit",{harness:id,conversation:conversationId,draft},context).then(submissionHandle),entries:(query,limit,cursor,context)=>call("entries",{harness:id,conversation:conversationId,query,limit,cursor},context),configure:(change,context)=>call("configure",{harness:id,conversation:conversationId,change},context),waitForIdle:context=>call("waitForIdle",{harness:id,conversation:conversationId},context),commit:(callback,context)=>change("commit",{harness:id,conversation:conversationId},callback,context)};}
 harnesses.set(id,harness);return harness;
}};
const storageIds=new WeakMap();let shuttingDown=false;
export const transport={request:call,storage(storage){let id=storageIds.get(storage);if(id===undefined){id=storageNext++;storageIds.set(storage,id);storages.set(id,storage);}return id;},release(range){if(!shuttingDown&&nativeTransport.started)send({type:"context.release",range});}};
async function finish(contextOnly,submissionsOnly=false){
 shuttingDown=true;
 if(!nativeTransport.started)throw new Error("Stock task tests did not reach Rust");
 const native=await call("stats");
 if(contextOnly){if(native.contextReads===0||stats.storageCalls===0)throw new Error("Native context implementation was bypassed");}
 else if(submissionsOnly){if(native.submissionAdmissions===0||native.submissionReads===0||native.submissionWaits===0||stats.storageCalls===0)throw new Error("Native submissions were bypassed");}
 else if(native.phases!==stats.phases||native.phases===0||native.commits===0||stats.storageCalls===0)throw new Error("Native scheduler was bypassed");
 Object.assign(stats,native);await nativeTransport.close();
 if(process.env.PI_DURABLE_NATIVE_TASK_STATS)writeFileSync(process.env.PI_DURABLE_NATIVE_TASK_STATS,JSON.stringify(stats));
}
export async function shutdown(){await finish(false);}
export async function shutdownContextTransport(){await finish(true);}
export async function shutdownSubmissions(){await finish(false,true);}
