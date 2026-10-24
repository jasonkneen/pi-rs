// Stock task, graph and inspection gates. Keep the tests and assertions;
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
assert.ok(["tasks","graph","inspect"].includes(suite));
const filename=suite==="graph"?"harness-task-graph.test.ts":suite==="inspect"?"harness-inspect.test.ts":"harness-tasks.test.ts";
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
if(suite==="graph")selected.splice(0,selected.length,...["follows every live task through its statuses, owner edges, and owned conversations","builds from committed tasks, shows surviving tasks as pending after reopen, and marks aborts","lists owned conversations in ID order whatever order one commit creates them in","registers nothing for an acquisition cancelled while it waits for the line","publishes no revision for a commit that changes no node, and shares one mount between observers"]);
if(suite==="inspect")selected.splice(0,selected.length,"derives every live task's state without running task code");
execFileSync("cargo",["build","--locked","-q","-p","pi-durable","--example","task_stock_bridge"],{cwd:root,stdio:"inherit"});
const target=process.env.CARGO_TARGET_DIR??JSON.parse(execFileSync("cargo",["metadata","--no-deps","--format-version","1"],{cwd:root,encoding:"utf8"})).target_directory;
const temporary=await mkdtemp(join(tmpdir(),"pi-durable-stock-tasks-"));
// Resolve the adapter's context import to the pinned reference. Nothing under
// the pinned test/source tree is changed.
const adapter=fileURLToPath(new URL("stock/durable-task-adapter.mjs",import.meta.url));
const hook=join(temporary,"setup.mjs");
await writeFile(hook,`import {afterAll} from ${JSON.stringify(join(reference,"node_modules/vitest/dist/index.js"))};import {shutdown} from ${JSON.stringify(adapter)};afterAll(shutdown,60000);`);
const index=join(reference,"packages/durable/src/index.ts");const config=join(temporary,"vitest.config.mjs");
const barrel=join(temporary,"native-index.mjs");
await writeFile(barrel,`export * from ${JSON.stringify(index)};export {Harness} from ${JSON.stringify(adapter)};export {createRegistry} from ${JSON.stringify(fileURLToPath(new URL("stock/durable-registry-adapter.mjs",import.meta.url)))};`);
await writeFile(config,`import original from ${JSON.stringify(join(reference,"packages/durable/vitest.config.ts"))};import {existsSync} from "node:fs";
export default {...original,resolve:{...original.resolve,alias:original.resolve.alias.map(alias=>alias.find.test("@earendil-works/pi-durable")?{...alias,replacement:${JSON.stringify(barrel)}}:alias)},plugins:[{name:"rust-task-service-under-stock-tests",enforce:"pre",resolveId(source){
const match=/^@earendil-works\\/(pi-ai|pi-telemetry|pi-codemode|chord)(?:\\/(.*))?$/.exec(source);
if(match){const directory=match[1].replace(/^pi-/,"");const path=${JSON.stringify(join(reference,"packages"))}+"/"+directory+"/src/"+(match[2]??"index");return [path+".ts",path+"/index.ts"].find(existsSync);}
}}],test:{...original.test,include:[${JSON.stringify(test)}],setupFiles:[...original.test.setupFiles,${JSON.stringify(hook)}],maxWorkers:1,fileParallelism:false}};`);
const summaries=[];let passed=false;
try{
 for(const retention of ["600000","0"]){
  const report=join(temporary,`report-${retention}.json`),stats=join(temporary,`native-${retention}.json`);
  const code=await new Promise((done,reject)=>{const child=spawn(process.execPath,[join(reference,"node_modules/vitest/vitest.mjs"),"run","--config",config,"--root",join(reference,"packages/durable"),"--reporter=json",`--outputFile=${report}`,"-t",selected.map(name=>name.replace(/[.*+?^${}()|[\]\\]/g,"\\$&")).join("|")],{cwd:join(reference,"packages/durable"),env:{...process.env,PI_DURABLE_NATIVE_TASK_BRIDGE:join(target,"debug/examples/task_stock_bridge"),PI_DURABLE_NATIVE_TASK_STATS:stats,PI_DURABLE_DOCUMENT_RETENTION_MS:retention},stdio:"inherit"});child.once("error",reject);child.once("exit",done);});
  const result=JSON.parse(await readFile(report,"utf8"));
  if(code!==0||!result.success){for(const suite of result.testResults??[]){if(suite.message)console.error(suite.message);for(const test of suite.assertionResults??[])if(test.status==="failed")console.error(JSON.stringify(test,null,2));}throw new Error("Stock task tests failed against Rust");}
  assert.equal(result.numPassedTests,selected.length);assert.deepEqual(result.testResults.flatMap(suite=>suite.assertionResults.filter(test=>test.status==="passed").map(test=>test.title)).sort(),[...selected].sort(),"Stock selection differs from its literal test names");const native=JSON.parse(await readFile(stats,"utf8"));
  assert.ok(native.phases>0&&native.commits>0&&native.storageCalls>0);
  if(suite==="tasks")assert.ok(native.registryReads>0&&native.agentReads>0&&native.agentResolutions>0&&native.hookCalls>0&&native.inspections>0&&native.documentReads>0&&native.documentWrites>0&&native.documentWatches>0&&native.contextReads>0&&native.readerSnapshots>0&&native.readerSubscriptions>0);
  if(suite==="graph")assert.ok(native.graphReads>0&&native.graphFrames>0&&native.inspections>0);
  if(suite==="inspect")assert.ok(native.inspections>0);
  console.log(`Stock ${filename} retention=${retention}: ${result.numPassedTests} selected cases passed, ${result.numPendingTests} remaining cases not selected; native=${JSON.stringify(native)}`);
  summaries.push({retention,passed:result.numPassedTests,notSelected:result.numPendingTests,native,tests:result.testResults.flatMap(suite=>suite.assertionResults.filter(test=>test.status!=="pending").map(test=>({name:test.fullName,status:test.status,durationMs:test.duration})))});
 }
 assert.deepEqual(await readFile(test),before,"Stock task test file was modified");
 const nativeScope=suite==="tasks"?"native Session, scheduler, registry, documents, runtime observations and lifecycle; original JS task code, mutable draft capture and backend callbacks":`native Session, scheduler, registry${suite==="graph"?", task graph and watches":" and inspection"}; original JS task code and backend callbacks`;
 const report=process.argv.find(arg=>arg.startsWith("--report="))?.slice(9);if(report)await writeFile(resolve(report),JSON.stringify({reference:pin,test:`packages/durable/test/${filename}`,sha256:createHash("sha256").update(before).digest("hex"),nativeScope,selected,modes:summaries},null,2)+"\n");passed=true;
}finally{if(passed)await rm(temporary,{recursive:true,force:true});else console.error(`Failure artifacts retained: ${temporary}`);}
