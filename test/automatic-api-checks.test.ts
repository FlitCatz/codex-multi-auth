import {afterEach,expect,it,vi} from "vitest";
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
it("records failed attempts before requests and rejects malformed history",async()=>{
 const path=await fixture(),check=vi.fn(async()=>{throw Error("fixture");}),options={path,loadRoutes:async()=>[route("on")],check,now:()=>1000};
 await runAutomaticApiChecks(options);await runAutomaticApiChecks(options);expect(check).toHaveBeenCalledTimes(1);
 await writeFile(path,"not-json");await expect(runAutomaticApiChecks(options)).rejects.toThrow();expect(check).toHaveBeenCalledTimes(1);
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
