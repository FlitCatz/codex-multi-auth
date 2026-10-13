import {createHash} from "node:crypto";
import {afterEach,expect,it,vi} from "vitest";
import {promises as fs} from "node:fs";
import {mkdtemp,readFile,writeFile} from "node:fs/promises";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {removeWithRetry} from "./helpers/remove-with-retry.js";
import {runAutomaticApiChecks,startAutomaticApiChecks,AUTOMATIC_API_CHECK_INTERVAL_MS} from "../lib/runtime/automatic-api-checks.js";
import type {ApiRouteCredential} from "../lib/api-route-store.js";
const roots:string[]=[];
afterEach(async()=>{vi.useRealTimers();for(const root of roots.splice(0))await removeWithRetry(root,{recursive:true,force:true});});
const route=(id:string, enabled=true,probeCapabilities=true):ApiRouteCredential=>({id,label:id,kind:"api",apiKey:`secret-${id}`,enabled,probeCapabilities,priority:0,visibleModels:["chat-test"]});
async function fixture(){const root=await mkdtemp(join(tmpdir(),"api-checks-"));roots.push(root);return join(root,"attempts.json");}
it("checks only opted-in enabled routes once per day, including competing runners",async()=>{
 const path=await fixture(),routes=[route("on"),route("off",false),route("no",true,false)],check=vi.fn(async()=>{});
 const options={path,loadRoutes:async()=>routes,check,now:()=>1000};
 await Promise.all([runAutomaticApiChecks(options),runAutomaticApiChecks(options)]);
 expect(check).toHaveBeenCalledTimes(1);expect(check.mock.calls[0]?.[0]).toEqual(routes[0]);
 expect(await readFile(path,"utf8")).not.toContain("secret");
 await runAutomaticApiChecks({...options,now:()=>1000+AUTOMATIC_API_CHECK_INTERVAL_MS});expect(check).toHaveBeenCalledTimes(2);
});
it("records failed attempts before requests",async()=>{
 const path=await fixture(),check=vi.fn(async()=>{throw Error("fixture");}),options={path,loadRoutes:async()=>[route("on")],check,now:()=>1000};
 await runAutomaticApiChecks(options);await runAutomaticApiChecks(options);expect(check).toHaveBeenCalledTimes(1);
});
it("defers startup, prevents overlap, and aborts/drains on stop",async()=>{
 vi.useFakeTimers();let release!:()=>void;const gate=new Promise<void>(r=>{release=r;});let signal:AbortSignal|undefined;
 const run=vi.fn(async(s:AbortSignal)=>{signal=s;await gate;});const scheduler=startAutomaticApiChecks(run);
 expect(run).not.toHaveBeenCalled();await vi.advanceTimersByTimeAsync(5000);expect(run).toHaveBeenCalledTimes(1);
 await vi.advanceTimersByTimeAsync(AUTOMATIC_API_CHECK_INTERVAL_MS*2);expect(run).toHaveBeenCalledTimes(1);
 const stop=scheduler.stop();expect(signal?.aborted).toBe(true);release();await stop;
 await vi.advanceTimersByTimeAsync(AUTOMATIC_API_CHECK_INTERVAL_MS*2);expect(run).toHaveBeenCalledTimes(1);
});

it("checks changed credentials and skips routes disabled before their turn",async()=>{
 const path=await fixture();let routes=[route("first"),route("second")];
 const check=vi.fn(async(r:ApiRouteCredential)=>{if(r.id==="first")routes=routes.map(x=>x.id==="second"?{...x,enabled:false}:x);});
 const options={path,loadRoutes:async()=>routes,check,now:()=>1000};
 await runAutomaticApiChecks(options);expect(check).toHaveBeenCalledTimes(1);
 routes=routes.map(x=>({...x,apiKey:x.apiKey+"-rotated"}));
 await runAutomaticApiChecks(options);expect(check).toHaveBeenCalledTimes(2);
});
it("schedules the next daily check after a failure finishes",async()=>{
 vi.useFakeTimers();const run=vi.fn(async()=>{throw Error("fixture");});const scheduler=startAutomaticApiChecks(run);
 try{await vi.advanceTimersByTimeAsync(5000);expect(run).toHaveBeenCalledTimes(1);await vi.advanceTimersByTimeAsync(AUTOMATIC_API_CHECK_INTERVAL_MS-1);expect(run).toHaveBeenCalledTimes(1);await vi.advanceTimersByTimeAsync(1);expect(run).toHaveBeenCalledTimes(2);}finally{await scheduler.stop();}
});

it("revalidates opt-in immediately before each POST and keeps shutdown linked to response bodies",async()=>{
 const api=await import("../lib/api-route-store.js"),discovery=await import("../lib/runtime/model-discovery-status.js");
 const {createAutomaticApiCheck}=await import("../lib/runtime/automatic-api-checks.js");
 let enabled=true;const credential=route("before-post");const controller=new AbortController();let observed:AbortSignal|null|undefined;
 const load=vi.spyOn(api,"loadApiRoutes").mockImplementation(async()=>[{...credential,enabled}]);
 const network=vi.spyOn(globalThis,"fetch").mockImplementation(async(_input,init)=>{observed=init?.signal;return new Response("ok");});
 const save=vi.spyOn(discovery,"saveModelInventory").mockResolvedValue();
 const discover=vi.spyOn(discovery,"discoverModelInventory").mockImplementation(async(storage,routes,options)=>{
  expect(storage).toBeNull();expect(routes).toEqual([credential]);expect(options?.forceProbes).toBeUndefined();
  await options!.fetchImpl!("https://api.example/responses",{method:"POST"});
  enabled=false;
  await expect(options!.fetchImpl!("https://api.example/responses",{method:"POST"})).rejects.toThrow("route changed");
  controller.abort();expect(observed?.aborted).toBe(true);
  return {version:1,checkedAt:1000,entries:[]};
 });
 try{await createAutomaticApiCheck()(controller.signal);expect(network).toHaveBeenCalledTimes(1);expect(save).not.toHaveBeenCalled();}finally{load.mockRestore();network.mockRestore();save.mockRestore();discover.mockRestore();}
});


it.each(["not-json", JSON.stringify({oldFormat:true})])("repairs corrupt history without an immediate duplicate billable pass: %s",async(raw)=>{
 const path=await fixture(),routes=[route("one"),route("two")],check=vi.fn(async()=>{});
 await writeFile(path,raw);
 const options={path,loadRoutes:async()=>routes,check,now:()=>1000};
 await Promise.all([runAutomaticApiChecks(options),runAutomaticApiChecks(options)]);
 expect(check).not.toHaveBeenCalled();
 const repaired=JSON.parse(await readFile(path,"utf8"));expect(Object.keys(repaired)).toHaveLength(2);expect(Object.values(repaired)).toEqual([1000,1000]);
 expect(JSON.stringify(repaired)).not.toContain("secret");
 await Promise.all([runAutomaticApiChecks({...options,now:()=>1000+AUTOMATIC_API_CHECK_INTERVAL_MS}),runAutomaticApiChecks({...options,now:()=>1000+AUTOMATIC_API_CHECK_INTERVAL_MS})]);
 expect(check).toHaveBeenCalledTimes(2);
});
it("does not replace an unreadable history or send probes",async()=>{
 const path=await fixture(),original=fs.readFile,check=vi.fn(async()=>{});
 await writeFile(path,"preserve unreadable history");
 const read=vi.spyOn(fs,"readFile").mockImplementation(((file,...args)=>String(file)===path?Promise.reject(Object.assign(Error("denied"),{code:"EACCES"})):original(file,...args)) as typeof fs.readFile);
 try{await expect(runAutomaticApiChecks({path,loadRoutes:async()=>[route("one")],check})).rejects.toMatchObject({code:"EACCES"});expect(check).not.toHaveBeenCalled();}finally{read.mockRestore();}
 expect(await readFile(path,"utf8")).toBe("preserve unreadable history");
});

it("includes newly enabled routes when repairing history under the lock",async()=>{
 const path=await fixture(),one=route("one"),two=route("two"),check=vi.fn(async()=>{});
 await writeFile(path,"corrupt");let loads=0;
 const options={path,loadRoutes:async()=>++loads===1?[one]:[one,two],check,now:()=>1000};
 await runAutomaticApiChecks(options);await runAutomaticApiChecks(options);
 expect(check).not.toHaveBeenCalled();
 expect(Object.keys(JSON.parse(await readFile(path,"utf8")))).toHaveLength(2);
});

it("probes newly discovered visible models and saves their inventory without repeating after restart",async()=>{
 const api=await import("../lib/api-route-store.js"),discovery=await import("../lib/runtime/model-discovery-status.js");
 const {createAutomaticApiCheck}=await import("../lib/runtime/automatic-api-checks.js");
 const root=await fixture();const previous=process.env.CODEX_MULTI_AUTH_DIR;process.env.CODEX_MULTI_AUTH_DIR=join(root,"..");
 let posts=0;const models:string[]=[];
 const network=vi.spyOn(globalThis,"fetch").mockImplementation(async(input,init)=>{
  if(init?.method==="POST"){posts++;models.push(JSON.parse(String(init.body)).model);return Response.json({output:[],service_tier:"default"});}
  if(String(input).endsWith("/models"))return Response.json({data:[{id:"gpt-test-old"},{id:"gpt-test-new"}]});
  return new Response("",{status:404});
 });
 try{
  await api.saveApiRoutes([{...route("discover"),visibleModels:["gpt-test-old"],knownModels:["gpt-test-old"]}]);
  await createAutomaticApiCheck()(new AbortController().signal);
  expect(posts).toBeGreaterThan(0);expect(models).toContain("gpt-test-new");
  expect((await api.loadApiRoutes())[0]?.visibleModels).toContain("gpt-test-new");
  expect((await discovery.loadModelInventory())?.entries[0]?.visibleModels).toContain("gpt-test-new");
  const before=posts;await createAutomaticApiCheck()(new AbortController().signal);expect(posts).toBe(before);
 }finally{network.mockRestore();if(previous===undefined)delete process.env.CODEX_MULTI_AUTH_DIR;else process.env.CODEX_MULTI_AUTH_DIR=previous;}
});

it("honors the legacy visibility-scoped daily claim",async()=>{
 const path=await fixture(),credential=route("legacy"),check=vi.fn(async()=>{});
 const oldKey=createHash("sha256").update(JSON.stringify([credential.id,credential.kind,credential.apiKey,credential.visibleModels])).digest("hex");
 await writeFile(path,JSON.stringify({[oldKey]:1000}));
 await runAutomaticApiChecks({path,loadRoutes:async()=>[credential],check,now:()=>2000});expect(check).not.toHaveBeenCalled();
});
it("does not adopt concurrent policy changes returned by discovery",async()=>{
 const api=await import("../lib/api-route-store.js"),discovery=await import("../lib/runtime/model-discovery-status.js");
 const {createAutomaticApiCheck}=await import("../lib/runtime/automatic-api-checks.js");
 const root=await fixture(),previous=process.env.CODEX_MULTI_AUTH_DIR;process.env.CODEX_MULTI_AUTH_DIR=join(root,"..");
 const original=api.updateApiModelDiscovery;let posts=0;
 const network=vi.spyOn(globalThis,"fetch").mockImplementation(async(input,init)=>{if(init?.method==="POST"){posts++;return Response.json({output:[]});}if(String(input).endsWith("/models"))return Response.json({data:[{id:"gpt-test-new"}]});return new Response("",{status:404});});
 const update=vi.spyOn(api,"updateApiModelDiscovery").mockImplementation(async(route,models)=>{await api.saveApiRoutes([{...route,priority:9}]);return original(route,models);});
 try{await api.saveApiRoutes([{...route("policy-race"),visibleModels:[],knownModels:[]}]);await createAutomaticApiCheck()(new AbortController().signal);expect(posts).toBe(0);expect(await discovery.loadModelInventory()).toBeNull();expect((await api.loadApiRoutes())[0]?.priority).toBe(9);}
 finally{update.mockRestore();network.mockRestore();if(previous===undefined)delete process.env.CODEX_MULTI_AUTH_DIR;else process.env.CODEX_MULTI_AUTH_DIR=previous;}
});


it("refreshes expired program evidence before consuming the daily pass despite fresh general probes",async()=>{
 const api=await import("../lib/api-route-store.js"),discovery=await import("../lib/runtime/model-discovery-status.js");
 const {createAutomaticApiCheck}=await import("../lib/runtime/automatic-api-checks.js");
 const path=await fixture(),previous=process.env.CODEX_MULTI_AUTH_DIR;
 process.env.CODEX_MULTI_AUTH_DIR=join(path,"..");
 let now=1000,programPosts=0;
 const clock=vi.spyOn(Date,"now").mockImplementation(()=>now);
 const network=vi.spyOn(globalThis,"fetch").mockImplementation(async(input,init)=>{
  if(String(input).endsWith("/models"))return Response.json({data:[{id:"fixture"}]});
  if(init?.method!=="POST")return new Response("missing",{status:404});
  const body=JSON.parse(String(init.body));
  if(body.access_programs)programPosts++;
  return Response.json({status:"completed",reasoning:body.reasoning,access_programs:body.access_programs});
 });
 try{
  const credential={...route("expiry"),visibleModels:["fixture"],accessPrograms:{cyber:["daybreak_blue"]}};
  await api.saveApiRoutes([credential]);
  const discover=()=>discovery.discoverModelInventory(null,[credential],{persistProbes:true,updateApiDiscovery:api.updateApiModelDiscovery});
  await discover();
  now=AUTOMATIC_API_CHECK_INTERVAL_MS;await discover();
  expect(programPosts).toBe(1);
  now=1001+AUTOMATIC_API_CHECK_INTERVAL_MS;
  await createAutomaticApiCheck()(new AbortController().signal);
  expect(programPosts).toBe(2);
  expect((await discovery.loadModelInventory())?.entries[0]?.entitlements?.[0]?.programCheckedAt).toBe(now);
  await createAutomaticApiCheck()(new AbortController().signal);
  expect(programPosts).toBe(2);
 }finally{clock.mockRestore();network.mockRestore();if(previous===undefined)delete process.env.CODEX_MULTI_AUTH_DIR;else process.env.CODEX_MULTI_AUTH_DIR=previous;}
});

it("stops daily discovery waiting on a real probe lease without paid or late work", async () => {
 const {withFileTransactionLock}=await import("../lib/storage/file-lock.js");
 const {saveApiRoutes}=await import("../lib/api-route-store.js");
 const {createAutomaticApiCheck}=await import("../lib/runtime/automatic-api-checks.js");
 const root=await mkdtemp(join(tmpdir(),"daily-lease-stop-"));roots.push(root);
 const previous=process.env.CODEX_MULTI_AUTH_DIR;process.env.CODEX_MULTI_AUTH_DIR=root;
 const credential={...route("fixture"),visibleModels:["fixture"],knownModels:["fixture"]};
 const key=createHash("sha256").update(credential.id).update("\0").update(credential.apiKey).update("\0fixture").digest("hex");
 const lease=join(root,"api-probe-locks",key);
 let release!:()=>void,entered!:()=>void;const gate=new Promise<void>(r=>{release=r;}),started=new Promise<void>(r=>{entered=r;});
 let posts=0;
 const mock=vi.spyOn(globalThis,"fetch").mockImplementation(async(input,init)=>{
  if(init?.method==="POST"){posts++;return Response.json({output:[]});}
  if(String(input).includes("/models"))return Response.json({data:[{id:"fixture"}]});
  return new Response("missing",{status:404});
 });
 const owner=withFileTransactionLock(lease,async()=>{entered();await gate;});await started;
 let scheduler:ReturnType<typeof startAutomaticApiChecks>|undefined,timer:ReturnType<typeof setTimeout>|undefined;
 let stop:Promise<void>|undefined;
 try{
  await saveApiRoutes([credential]);
  vi.useFakeTimers();scheduler=startAutomaticApiChecks(createAutomaticApiCheck());await vi.advanceTimersByTimeAsync(5000);vi.useRealTimers();
  await vi.waitFor(async()=>expect((await fs.readdir(join(root,"api-probe-locks"))).some(name=>name.includes("candidate"))).toBe(true));
  stop=scheduler.stop();
  expect(await Promise.race([stop.then(()=>"stopped"),new Promise(resolve=>{timer=setTimeout(()=>resolve("blocked"),1000);})])).toBe("stopped");
  expect(posts).toBe(0);
 }finally{
  clearTimeout(timer);vi.useRealTimers();release();await owner;await(stop??scheduler?.stop());mock.mockRestore();
  if(previous===undefined)delete process.env.CODEX_MULTI_AUTH_DIR;else process.env.CODEX_MULTI_AUTH_DIR=previous;
 }
 expect(posts).toBe(0);
 expect((await fs.readdir(join(root,"api-probe-locks"))).filter(name=>name.includes("candidate")||name.endsWith("write-lock"))).toEqual([]);
});
