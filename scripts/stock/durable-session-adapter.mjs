// Declared independently so reference class definitions can extend the binding
// without importing the original Session kernel or entering a package cycle.
let bind,defaultRetention=600_000;
export function setDefaultDocumentRetention(value){defaultRetention=value;}
export function installSessionBinding(binding){bind=binding;}
export class SessionImpl {
 constructor(storage,now=Date.now,retention=()=>undefined,setTimer,readOnly=false){
  if(bind===undefined)throw new Error("Native Session transport was not installed");
  this.binding=bind(this,storage,now,()=>retention()??defaultRetention,setTimer,readOnly);
 }
 commit(callback,context){return this.commitWith(callback,context);}
 async commitWith(callback,context,scope){return this.binding.commitWith(callback,context,scope);}
 snapshot(...args){return this.binding.snapshot(...args);}
 snapshotAsOf(...args){return this.binding.snapshotAsOf(...args);}
 watchDoc(...args){return this.binding.watchDoc(...args);}
 subscribeClose(callback){return this.binding.subscribeClose(callback);}
 subscribeCommits(callback){return this.binding.subscribeCommits(callback);}
 get closed(){return this.binding.closed();}
 close(context){return this.binding.close(context);}
 unloadDocuments(){return this.binding.unloadDocuments();}
 get documentCacheSizes(){return this.binding.cache();}
 async conversationCreated(){}
 async beforeClose(){}
 report(){}
}
export function createSession(storage){return new SessionImpl(storage);}
