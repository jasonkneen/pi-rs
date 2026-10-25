// Filesystem test bindings. Policy and reader state execute in Rust; optional
// opened-file callbacks preserve the stock tests' original I/O fault injection.
import {open} from "node:fs/promises";
import {constants} from "node:fs";
import {basename} from "node:path";
import {FileError} from "@pi-durable-stock-env-contract";
import {nativeTransport} from "./durable-native-transport.mjs";
const files=new Map();let nextFile=1,hostCalls=0;
const scalar=value=>Number.isFinite(value)?value:String(value);
const code=error=>({ABORT_ERR:"aborted",ENOENT:"not_found",EACCES:"permission_denied",EPERM:"permission_denied",ENOTDIR:"not_directory",EISDIR:"is_directory",EINVAL:"invalid",ELOOP:"invalid",EMLINK:"invalid"})[error?.code]??"unknown";
function host(method,callback){nativeTransport.register(method,async args=>{hostCalls++;try{return {ok:true,value:await callback(args)};}catch(error){return {ok:false,error:{code:code(error),message:error.message,path:error.path??args.path??files.get(args.file)?.path}};}});}
host("file.open",async args=>{let flags=args.write?constants.O_RDWR:constants.O_RDONLY;if(args.regular)flags|=constants.O_NONBLOCK??0;if(args.noFollow)flags|=constants.O_NOFOLLOW??0;const file=await open(args.path,flags),id=nextFile++;files.set(id,{file,path:args.path});return id;});
host("file.info",async args=>{const {file,path}=files.get(args.file),info=await file.stat();return {name:basename(path),path,kind:info.isFile()?"file":info.isDirectory()?"directory":info.isSymbolicLink()?"symlink":"other",size:info.size,mtimeMs:info.mtimeMs};});
host("file.read",async args=>{const bytes=Buffer.alloc(args.length),{bytesRead}=await files.get(args.file).file.read(bytes,0,bytes.length,args.offset);return bytes.subarray(0,bytesRead).toString("base64");});
host("file.sync",args=>files.get(args.file).file.sync());
host("file.close",async args=>{const file=files.get(args.file);if(file){try{await file.file.close();}finally{files.delete(args.file);}}});
nativeTransport.error("file",value=>new FileError(value.code,value.message,value.path??undefined));
function call(method,args,context,convert=value=>value){return nativeTransport.request(method,args,context).then(value=>({ok:true,value:convert(value)}),error=>{if(!(error instanceof FileError))throw error;return {ok:false,error};});}
const voidValue=()=>undefined;
function reader(id,kind){const args={reader:id};return {close:context=>nativeTransport.request("reader.close",args,context).then(voidValue),...(kind==="text"?{readLine:context=>call("reader.line",args,context,value=>value??undefined)}:kind==="binary"?{info:context=>call("reader.info",args,context),read:(offset,length,context)=>call("reader.read",{...args,offset:scalar(offset),length:scalar(length)},context,value=>new Uint8Array(Buffer.from(value,"base64"))),scanLines:(options,context)=>call("reader.scan",{...args,start:scalar(options.startLine),...(options.endLine===undefined?{}:{end:scalar(options.endLine)})},context)}:{next:(max,context)=>call("reader.next",{...args,max:scalar(max)},context)})};}
export class NodeExecutionEnv {
 constructor(options){const opened=nativeTransport.sync("fs.new",{cwd:options.cwd,hostIo:process.env.PI_DURABLE_NATIVE_ENV_HOST_IO==="1"});this.native=opened.id;this.id=opened.namespace;this.directory=options.cwd;}
 get cwd(){return this.directory;}
 set cwd(path){nativeTransport.sync("fs.cwd",{fs:this.native,path});this.directory=path;}
 invoke(method,args,context,convert){return call(method,{fs:this.native,...args},context,convert);}
 absolutePath(path,context){return this.invoke("fs.absolute",{path},context);}
 joinPath(parts,context){return this.invoke("fs.join",{parts},context);}
 readTextFile(path,context){return this.invoke("fs.readText",{path},context);}
 readTextLines(path,options,context){return this.invoke("fs.readLines",{path,...(options?.maxLines===undefined?{}:{max:scalar(options.maxLines)})},context);}
 readBinaryFile(path,context){return this.invoke("fs.readBinary",{path},context,value=>new Uint8Array(Buffer.from(value,"base64")));}
 openTextLineReader(path,context){return this.invoke("fs.openText",{path},context,id=>reader(id,"text"));}
 openBinaryReader(path,options,context){return this.invoke("fs.openBinary",{path,noFollow:options?.noFollow},context,id=>reader(id,"binary"));}
 openDirReader(path,context){return this.invoke("fs.openDir",{path},context,id=>reader(id,"directory"));}
 writeFile(path,content,context){return this.invoke("fs.write",{path,data:Buffer.from(content).toString("base64")},context,voidValue);}
 appendFile(path,content,context){return this.invoke("fs.append",{path,data:Buffer.from(content).toString("base64")},context,voidValue);}
 truncateFile(path,size,context){return this.invoke("fs.truncate",{path,size:scalar(size)},context,voidValue);}
 flushFile(path,context){return this.invoke("fs.flush",{path},context,voidValue);}
 renameFile(path,destination,context){return this.invoke("fs.rename",{path,destination},context,voidValue);}
 fileInfo(path,context){return this.invoke("fs.info",{path},context);}
 listDir(path,context){return this.invoke("fs.list",{path},context);}
 canonicalPath(path,context){return this.invoke("fs.canonical",{path},context);}
 exists(path,context){return this.invoke("fs.exists",{path},context);}
 createDir(path,options,context){return this.invoke("fs.mkdir",{path,recursive:options?.recursive},context,voidValue);}
 remove(path,options,context){return this.invoke("fs.remove",{path,...options},context,voidValue);}
 createTempDir(prefix,context){return this.invoke("fs.tempDir",{prefix},context);}
 createTempFile(options,context){return this.invoke("fs.tempFile",{...options},context);}
 cleanup(context){return nativeTransport.request("fs.cleanup",{fs:this.native},context).then(voidValue);}
}
export async function shutdown(){const native=await nativeTransport.request("stats");if(native.opens===0||native.calls===0)throw new Error("Native filesystem was bypassed");if(files.size)throw new Error(`Host file handles still open: ${files.size}`);await nativeTransport.close();return {...native,hostFileCalls:hostCalls};}
