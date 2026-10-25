// Stock environment coverage expands as each native capability is completed.
import assert from "node:assert/strict";
import {execFileSync,spawn} from "node:child_process";
import {mkdtemp,readFile,writeFile,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {resolve,join} from "node:path";
import {fileURLToPath} from "node:url";
import {createHash} from "node:crypto";
const root=fileURLToPath(new URL("../",import.meta.url)),reference=resolve(process.argv[2]??"../pi-mono"),pin="5e63241d0294ed3fcfc4388c737a8ea27e09c74e";
assert.equal(execFileSync("git",["rev-parse","HEAD"],{cwd:reference,encoding:"utf8"}).trim(),pin);
const readers=process.argv.includes("--suite=readers"),filename=readers?"env-node-conformance.test.ts":"env-node.test.ts",relative=`packages/durable/test/${filename}`,test=join(reference,relative),before=await readFile(test);
assert.deepEqual(before,execFileSync("git",["show",`${pin}:${relative}`],{cwd:reference}),"Stock environment tests differ from pin");
const conformancePath="packages/durable/src/testing/env-conformance.ts",conformance=readers?await readFile(join(reference,conformancePath)):undefined;
if(readers)assert.deepEqual(conformance,execFileSync("git",["show",`${pin}:${conformancePath}`],{cwd:reference}));
const readerCases=readers?[...conformance.toString().split('\n\t\twatchCase("')[0].matchAll(/\bcreateCase\("([^"\n]+)"/g)].map(match=>match[1]):[];
const selected=readers?[...readerCases,...readerCases,"reads ranges spanning several internal chunks exactly","refuses a FIFO without waiting for a writer"]:[...before.toString().split('describe("NodeExecutionEnv shell"')[0].matchAll(/\bit\("([^"\n]+)"/g)].map(match=>match[1]);
execFileSync("cargo",["build","--locked","-q","-p","pi-durable","--example","env_stock_bridge"],{cwd:root,stdio:"inherit"});
const target=process.env.CARGO_TARGET_DIR??JSON.parse(execFileSync("cargo",["metadata","--no-deps","--format-version","1"],{cwd:root,encoding:"utf8"})).target_directory;
const temporary=await mkdtemp(join(tmpdir(),"pi-durable-stock-env-")),adapter=fileURLToPath(new URL("stock/durable-env-adapter.mjs",import.meta.url));
const config=join(temporary,"vitest.config.mjs"),setup=join(temporary,"setup.mjs");
await writeFile(setup,`import {afterAll} from ${JSON.stringify(join(reference,"node_modules/vitest/dist/index.js"))};import {writeFileSync} from "node:fs";import {shutdown} from ${JSON.stringify(adapter)};afterAll(async()=>writeFileSync(process.env.PI_DURABLE_ENV_STATS,JSON.stringify(await shutdown())),60000);`);
await writeFile(config,`import original from ${JSON.stringify(join(reference,"packages/durable/vitest.config.ts"))};import {resolve,dirname} from "node:path";
export default {...original,plugins:[{name:"native-rust-env",enforce:"pre",resolveId(source,importer){if(source==="@pi-durable-stock-env-contract")return ${JSON.stringify(join(reference,"packages/durable/src/env/index.ts"))};if(importer&&resolve(dirname(importer),source)===${JSON.stringify(join(reference,"packages/durable/src/env/node.ts"))})return ${JSON.stringify(adapter)};}}],test:{...original.test,include:[${JSON.stringify(test)}],setupFiles:[...original.test.setupFiles,${JSON.stringify(setup)}],maxWorkers:1,fileParallelism:false}};`);
let passed=false;const modes=[];
try{
 for(const retention of ["600000","0"]){
  const report=join(temporary,`report-${retention}.json`),stats=join(temporary,`stats-${retention}.json`);
  const code=await new Promise((done,reject)=>{const child=spawn(process.execPath,[join(reference,"node_modules/vitest/vitest.mjs"),"run","--config",config,"--root",join(reference,"packages/durable"),"--reporter=json",`--outputFile=${report}`,"-t",readers?"NodeExecutionEnv readers|binary reader (reads|scans|keeps|refuses)|directory reader":"NodeExecutionEnv filesystem|NodeExecutionEnv text line reader"],{cwd:join(reference,"packages/durable"),env:{...process.env,PI_DURABLE_NATIVE_TASK_BRIDGE:join(target,"debug/examples/env_stock_bridge"),PI_DURABLE_ENV_STATS:stats,PI_DURABLE_NATIVE_ENV_HOST_IO:readers?"0":"1",PI_DURABLE_DOCUMENT_RETENTION_MS:retention},stdio:"inherit"});child.once("error",reject);child.once("exit",done);});
  const result=JSON.parse(await readFile(report,"utf8"));
  if(code!==0||!result.success){for(const suite of result.testResults??[]){if(suite.message)console.error(suite.message);for(const item of suite.assertionResults??[])if(item.status==="failed")console.error(JSON.stringify(item,null,2));}throw new Error("Stock environment tests failed against Rust");}
  const tests=result.testResults.flatMap(suite=>suite.assertionResults.filter(item=>item.status==="passed"));assert.equal(tests.length,selected.length);assert.deepEqual(tests.map(item=>item.title).sort(),[...selected].sort());
  const native=JSON.parse(await readFile(stats,"utf8"));assert.ok(native.opens>0&&native.calls>0);if(readers)assert.ok(native.binaryReads>0);else assert.ok(native.textReads>0&&native.hostFileCalls>0);
  console.log(`Stock ${filename} retention=${retention}: ${tests.length} selected cases passed, ${result.numPendingTests} not selected; native=${JSON.stringify(native)}`);
  modes.push({retention,passed:tests.length,notSelected:result.numPendingTests,native,tests:tests.map(item=>({name:item.fullName,status:item.status,durationMs:item.duration}))});
 }
 assert.deepEqual(await readFile(test),before);if(readers)assert.deepEqual(await readFile(join(reference,conformancePath)),conformance);const report=process.argv.find(arg=>arg.startsWith("--report="))?.slice(9);if(report)await writeFile(resolve(report),JSON.stringify({reference:pin,test:relative,sha256:createHash("sha256").update(before).digest("hex"),selected,nativeScope:readers?"native filesystem, positional binary readers and directory pagination over real local handles; original conformance cases in both registrations; shell-dependent symlink and watch cases remain unselected":"native filesystem, LF text reader state, UTF-8 decoding, cancellation and fsync lifecycle; stock opened-file callbacks preserve original fsync/read failure injections",modes},null,2)+"\n");passed=true;
}finally{if(passed)await rm(temporary,{recursive:true,force:true});else console.error(`Failure artifacts retained: ${temporary}`);}
