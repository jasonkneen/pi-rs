// Opaque capabilities carry original object identity across the test transport.
import {nativeTransport} from "./durable-native-transport.mjs";
let next=1;
const objects=new Map(),ids=new WeakMap();
export function handle(value){let id=ids.get(value);if(id===undefined){id=next++;ids.set(value,id);objects.set(id,value);}return id;}
export function objectFor(id){return objects.get(id);}
export function describeTask(task){const {name,version,phases,migrate}=task.definition;return{id:handle(task),name,version,phases:Object.keys(phases),migrate:migrate!==undefined};}
nativeTransport.register("initial",([id,input])=>objectFor(id).definition.initial(input));
nativeTransport.register("migrate",([id,input,checkpoint,version])=>objectFor(id).definition.migrate(input,checkpoint,version));
