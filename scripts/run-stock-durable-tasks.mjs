// Stock task, tool, recovery, submission, graph and inspection gates. Keep the tests and assertions;
// expand each explicit selection as the remaining harness services are ported.
import assert from "node:assert/strict";
import {execFileSync,spawn} from "node:child_process";
import {mkdtemp,writeFile,readFile,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {resolve,join} from "node:path";
import {fileURLToPath} from "node:url";
import {createHash} from "node:crypto";
const root=fileURLToPath(new URL("../",import.meta.url));
const reference=resolve(process.argv[2]??"../pi-mono");
const pin="5e63241d0294ed3fcfc4388c737a8ea27e09c74e";
assert.equal(execFileSync("git",["rev-parse","HEAD"],{cwd:reference,encoding:"utf8"}).trim(),pin);
const suite=process.argv.find(arg=>arg.startsWith("--suite="))?.slice(8)??"tasks";
assert.ok(["tasks","graph","inspect","submissions","tools","tool-recovery","nested","nested-restart","view","events"].includes(suite));
const filename=suite==="view"?"harness-view.test.ts":suite==="events"?"harness-events.test.ts":suite==="nested-restart"?"harness-nested-tools-restart.test.ts":suite==="nested"?"harness-nested-tools.test.ts":suite==="tool-recovery"?"harness-tools-recovery.test.ts":suite==="tools"?"harness-tools.test.ts":suite==="graph"?"harness-task-graph.test.ts":suite==="inspect"?"harness-inspect.test.ts":suite==="submissions"?"harness-submissions.test.ts":"harness-tasks.test.ts";
const test=join(reference,"packages/durable/test",filename);const before=await readFile(test);
assert.deepEqual(before,execFileSync("git",["show",`${pin}:packages/durable/test/${filename}`],{cwd:reference}),"Stock task tests differ from pin");
const selected=["continues one invocation through checkpoint progress and completes with a typed result","faults a phase without durable progress and a throwing phase","keeps a committed terminal outcome when the handler throws afterwards","compares checkpoints by value, including arrays"];
selected.push("resumes a waiting task once every task in `on` is terminal, whatever the outcome","rejects the run's commits and memo writes after the mark, and settles through a fresh abort invocation","starts no further phase after a mark that lands during a phase with progress","signals and joins the run before returning, then runs the abort handler","aborts waiting work before its wait ends and faults abort handlers that throw or settle nothing","does not signal a running abort handler when aborted again, and a cancelled caller leaves the mark durable","keeps a terminal outcome committed by an abort handler that throws afterwards","never starts phase one when the abort lands while the reservation settles","lets an abort mark win over a fault that races it","stops without outcomes and starts no fresh phase or abort invocation while closing");
selected.push("starts no abort handler whose reservation settles while closing","joins a reservation that settles while closing without starting its handler");
selected.push("keeps one registry snapshot per phase and refreshes it at the phase boundary");
selected.push("waits for Harness and conversation idleness, counting blocked work and ignoring background tasks","pages the newest tasks first without scanning older ones","rejects unknown tasks and reports terminal tasks","rejects task waits cancelled or closed while queued on the line, and pending waits on close");
selected.push("orders runtime commits against the step: one queued before it lands, one after it rejects","writes no fault when close seals while the step after a failed phase is queued","rejects a runtime commit that was queued on the line when close sealed it");
selected.push("resolves the agent at first use in a phase from its snapshot, keeps it for the phase, and anew at the next","rejects an agent() wait whose caller context is cancelled, without affecting the phase's resolution","observes a failed agent resolution whose only caller stopped waiting");
selected.push("stamps startedAt at the first run and endedAt when terminal, keeping startedAt through a wait");
selected.push("commits results with entries atomically, keeps memos until terminal, and retires task documents","rejects runtime operations after the invocation ends and stops its watches","reads committed documents and context through the runtime, and forwards the clock and reports","stops a watch whose acquisition finishes after the invocation ended","seals admission before signalling handlers and stops watches before joining them");
selected.push("fails the Harness instead of running a task again when its fault write fails","starts no next phase when close seals during a step that decided to continue");
selected.push("fails the Harness on a failed reservation commit; reopening runs the task");
if(["tools","tool-recovery","nested","nested-restart","view","events"].includes(suite))selected.splice(0,selected.length,...[...before.toString().matchAll(/\bit\("([^"\n]+)"/g)].map(match=>match[1]));
if(suite==="graph")selected.splice(0,selected.length,...["follows every live task through its statuses, owner edges, and owned conversations","builds from committed tasks, shows surviving tasks as pending after reopen, and marks aborts","lists owned conversations in ID order whatever order one commit creates them in","registers nothing for an acquisition cancelled while it waits for the line","publishes no revision for a commit that changes no node, and shares one mount between observers"]);
if(suite==="inspect")selected.splice(0,selected.length,"derives every live task's state without running task code");
if(suite==="submissions")selected.splice(0,selected.length,"appends an idle write and settles it done without a turn","settles submissions by their current record in the transaction","appends and reads typed entries through tokens");
execFileSync("cargo",["build","--locked","-q","-p","pi-durable","--example","task_stock_bridge"],{cwd:root,stdio:"inherit"});
const target=process.env.CARGO_TARGET_DIR??JSON.parse(execFileSync("cargo",["metadata","--no-deps","--format-version","1"],{cwd:root,encoding:"utf8"})).target_directory;
const temporary=await mkdtemp(join(tmpdir(),"pi-durable-stock-tasks-"));
// Resolve the adapter's context import to the pinned reference. Nothing under
// the pinned test/source tree is changed.
const adapter=fileURLToPath(new URL("stock/durable-task-adapter.mjs",import.meta.url));
const hook=join(temporary,"setup.mjs");
await writeFile(hook,`import {afterAll} from ${JSON.stringify(join(reference,"node_modules/vitest/dist/index.js"))};import {${suite==="submissions"?"shutdownSubmissions":"shutdown"} as shutdown} from ${JSON.stringify(adapter)};afterAll(shutdown,60000);`);
const index=join(reference,"packages/durable/src/index.ts");const config=join(temporary,"vitest.config.mjs");
const barrel=join(temporary,"native-index.mjs");
const memoryExport=["tasks","view","events"].includes(suite)?`import {MemoryStorage as ReferenceMemoryStorage} from ${JSON.stringify(join(reference,"packages/durable/src/storage/memory.ts"))};import {createNativeMemoryAdapter} from ${JSON.stringify(fileURLToPath(new URL("stock/durable-memory-adapter.mjs",import.meta.url)))};export const MemoryStorage=createNativeMemoryAdapter(ReferenceMemoryStorage);`:"";
await writeFile(barrel,`${memoryExport}export * from ${JSON.stringify(index)};export {Harness,watchEvents} from ${JSON.stringify(adapter)};export {createRegistry} from ${JSON.stringify(fileURLToPath(new URL("stock/durable-registry-adapter.mjs",import.meta.url)))};`);
await writeFile(config,`import original from ${JSON.stringify(join(reference,"packages/durable/vitest.config.ts"))};import {existsSync} from "node:fs";
export default {...original,resolve:{...original.resolve,alias:original.resolve.alias.map(alias=>alias.find.test("@earendil-works/pi-durable")?{...alias,replacement:${JSON.stringify(barrel)}}:alias)},plugins:[{name:"rust-task-service-under-stock-tests",enforce:"pre",resolveId(source,importer){
const match=/^@earendil-works\\/(pi-ai|pi-telemetry|pi-codemode|chord)(?:\\/(.*))?$/.exec(source);
if(match){const directory=match[1].replace(/^pi-/,"");const path=${JSON.stringify(join(reference,"packages"))}+"/"+directory+"/src/"+(match[2]??"index");return [path+".ts",path+"/index.ts"].find(existsSync);}
}}],test:{...original.test,include:[${JSON.stringify(test)}],setupFiles:[...original.test.setupFiles,${JSON.stringify(hook)}],maxWorkers:1,fileParallelism:false}};`);
const summaries=[];let passed=false;
try{
 for(const retention of ["600000","0"]){
  const report=join(temporary,`report-${retention}.json`),stats=join(temporary,`native-${retention}.json`);
  const code=await new Promise((done,reject)=>{const child=spawn(process.execPath,[join(reference,"node_modules/vitest/vitest.mjs"),"run","--config",config,"--root",join(reference,"packages/durable"),"--reporter=json",`--outputFile=${report}`,"-t",selected.map(name=>name.replace(/[.*+?^${}()|[\]\\]/g,"\\$&")).join("|")],{cwd:join(reference,"packages/durable"),env:{...process.env,PI_DURABLE_NATIVE_TOOL_TASK:["tools","tool-recovery","nested","nested-restart","events","view"].includes(suite)?"1":"0",PI_DURABLE_NATIVE_TASK_BRIDGE:join(target,"debug/examples/task_stock_bridge"),PI_DURABLE_NATIVE_TASK_STATS:stats,PI_DURABLE_DOCUMENT_RETENTION_MS:retention},stdio:"inherit"});child.once("error",reject);child.once("exit",done);});
  const result=JSON.parse(await readFile(report,"utf8"));
  if(code!==0||!result.success){for(const suite of result.testResults??[]){if(suite.message)console.error(suite.message);for(const test of suite.assertionResults??[])if(test.status==="failed")console.error(JSON.stringify(test,null,2));}throw new Error("Stock task tests failed against Rust");}
  assert.equal(result.numPassedTests,selected.length);assert.deepEqual(result.testResults.flatMap(suite=>suite.assertionResults.filter(test=>test.status==="passed").map(test=>test.title)).sort(),[...selected].sort(),"Stock selection differs from its literal test names");const native=JSON.parse(await readFile(stats,"utf8"));
  if(suite==="tools"||suite==="tool-recovery"||suite==="nested"||suite==="nested-restart"){assert.equal(result.numPendingTests,0,"A full stock tool file left tests unselected");assert.ok(native.nativeToolCalls>0&&native.nativeToolHooks>0,"Native tool execution or hooks were bypassed");}
  if(suite==="nested"||suite==="nested-restart")assert.ok(native.viewReads>0&&native.nativeEventFrames>0,"Native conversation views were bypassed");
  if(["nested","nested-restart","events"].includes(suite))assert.ok(native.nativeEventReads>0,"Native event translation was bypassed");
  if(["view","events"].includes(suite)){assert.equal(result.numPendingTests,0);assert.ok(native.viewReads>0&&native.nativeMemoryOpens>0&&native.nativeMemoryCommits>0);}
  if(suite!=="submissions")assert.ok(native.phases>0&&native.commits>0&&(native.storageCalls>0||native.nativeMemoryCalls>0));
  if(suite==="submissions")assert.ok(native.submissionAdmissions>0&&native.submissionReads>0&&native.submissionWaits>0&&native.commits>0&&native.storageCalls>0);
  if(suite==="tasks")assert.ok(native.nativeMemoryOpens>0&&native.nativeMemoryCalls>0&&native.nativeMemoryCommits>0&&native.registryReads>0&&native.agentReads>0&&native.agentResolutions>0&&native.hookCalls>0&&native.inspections>0&&native.documentReads>0&&native.documentWrites>0&&native.documentWatches>0&&native.contextReads>0&&native.readerSnapshots>0&&native.readerSubscriptions>0&&native.submissionAdmissions>0);
  if(suite==="graph")assert.ok(native.graphReads>0&&native.graphFrames>0&&native.inspections>0);
  if(suite==="inspect")assert.ok(native.inspections>0);
  console.log(`Stock ${filename} retention=${retention}: ${result.numPassedTests} selected cases passed, ${result.numPendingTests} remaining cases not selected; native=${JSON.stringify(native)}`);
  summaries.push({retention,passed:result.numPassedTests,notSelected:result.numPendingTests,native,tests:result.testResults.flatMap(suite=>suite.assertionResults.filter(test=>test.status!=="pending").map(test=>({name:test.fullName,status:test.status,durationMs:test.duration})))});
 }
 assert.deepEqual(await readFile(test),before,"Stock task test file was modified");
 const nativeScope=["view","events"].includes(suite)?"native Session, scheduler, registry, memory backend, conversation views and event translation; original JS generation handlers, SDK provider and tool callbacks":(suite==="tools"||suite==="tool-recovery"||suite==="nested"||suite==="nested-restart")?"native Session, scheduler, registry and pi.tool execution; original JS generation handlers, tool callbacks, schema validation, provider, environment and backend capabilities"+((suite==="nested"||suite==="nested-restart")?"; native conversation view mounts, commit publications and event translation; direct JS definition helper checks":""):suite==="submissions"?"native Session, submission controller, built-in documents and entry operations; original JS backend, token definitions and user callbacks; generation-dependent cases not selected":suite==="tasks"?"native Session, scheduler, registry, documents, submissions, runtime observations, lifecycle and plain memory stores; original JS task code, mutable draft capture and controlled/decorated backend callbacks":`native Session, scheduler, registry${suite==="graph"?", task graph and watches":" and inspection"}; original JS task code and backend callbacks`;
 const report=process.argv.find(arg=>arg.startsWith("--report="))?.slice(9);if(report)await writeFile(resolve(report),JSON.stringify({reference:pin,test:`packages/durable/test/${filename}`,sha256:createHash("sha256").update(before).digest("hex"),nativeScope,selected,modes:summaries},null,2)+"\n");passed=true;
}finally{if(passed)await rm(temporary,{recursive:true,force:true});else console.error(`Failure artifacts retained: ${temporary}`);}
