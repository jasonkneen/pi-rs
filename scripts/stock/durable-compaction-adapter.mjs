// Rust supplies range decisions and transcript serialization. Original task
// definitions remain linked for suites that have not yet been bound to Rust.
export {CompactionTask,createCompaction,compactionPolicy,estimateContext,summarizedMessages} from "pi-durable-stock-reference-compaction";
import {nativeTransport} from "./durable-native-transport.mjs";
import {writeFileSync} from "node:fs";
export function selectCut(view,budget){return nativeTransport.sync("compaction.select",{view:{...view,head:view.head??null},budget});}
export function serializeConversation(messages){return nativeTransport.sync("compaction.serialize",{messages});}
export async function shutdownCompaction(){
 const native=await nativeTransport.request("stats");
 if(!(native.compactionReads>0))throw new Error("Native compaction planning was bypassed");
 await nativeTransport.close();
 if(process.env.PI_DURABLE_NATIVE_TASK_STATS)writeFileSync(process.env.PI_DURABLE_NATIVE_TASK_STATS,JSON.stringify(native));
}
