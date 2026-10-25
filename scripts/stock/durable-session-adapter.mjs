// Declared independently so reference class definitions can extend the binding
// without importing the original Session kernel or entering a package cycle.
let bind,defaultRetention=600_000;
function operation(start){let result;try{result=Promise.resolve(start());}catch(error){result=Promise.reject(error);}result.catch(()=>{});return result;}
export function setDefaultDocumentRetention(value){defaultRetention=value;}
export function installSessionBinding(binding){bind=binding;}
export class SessionImpl {
 constructor(storage,now=Date.now,retention=()=>undefined,setTimer,readOnly=false){
  if(bind===undefined)throw new Error("Native Session transport was not installed");
  this.binding=bind(this,storage,now,()=>retention()??defaultRetention,setTimer,readOnly);
 }
 commit(callback,context){return this.commitWith(callback,context);}
 commitWith(callback,context,scope){return operation(()=>this.binding.commitWith(callback,context,scope));}
 snapshot(...args){return operation(()=>this.binding.snapshot(...args));}
 snapshotAsOf(...args){return operation(()=>this.binding.snapshotAsOf(...args));}
 watchDoc(...args){return operation(()=>this.binding.watchDoc(...args));}
 subscribeClose(callback){return this.binding.subscribeClose(callback);}
 subscribeCommits(callback){return this.binding.subscribeCommits(callback);}
 get closed(){return this.binding.closed();}
 close(context){return operation(()=>this.binding.close(context));}
 unloadDocuments(){return operation(()=>this.binding.unloadDocuments());}
 get documentCacheSizes(){return this.binding.cache();}
 async conversationCreated(){}
 async beforeClose(){}
 report(){}
}
export function createSession(storage){return new SessionImpl(storage);}
