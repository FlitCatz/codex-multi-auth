import {createHash} from "node:crypto";
import {promises as fs} from "node:fs";
import {dirname,join} from "node:path";
import {z} from "zod";
import {loadApiRoutes,updateApiModelDiscovery,type ApiRouteCredential} from "../api-route-store.js";
import {getCodexMultiAuthDir} from "../runtime-paths.js";
import {withFileTransactionLock} from "../storage/file-lock.js";
import {tempPathFor} from "../temp-path.js";
import {withRetry} from "../fs-retry.js";
import {combineSignals} from "../utils.js";
import {logWarn} from "../logger.js";
import {discoverModelInventory,saveModelInventory} from "./model-discovery-status.js";
export const AUTOMATIC_API_CHECK_INTERVAL_MS=24*60*60_000;
const schema=z.record(z.string().regex(/^[a-f0-9]{64}$/),z.number().finite().nonnegative());
const retry={maxAttempts:6,backoffMs:25};
function key(route:ApiRouteCredential){return createHash("sha256").update(JSON.stringify([route.id,route.kind,route.apiKey,route.visibleModels])).digest("hex");}
const eligible=(route:ApiRouteCredential)=>route.enabled&&route.probeCapabilities===true;
// Visibility can expand during discovery; daily spending remains credential-scoped.
function claimKey(route:ApiRouteCredential){return createHash("sha256").update(JSON.stringify([route.id,route.kind,route.apiKey])).digest("hex");}
function sameRoute(a:ApiRouteCredential,b:ApiRouteCredential){return JSON.stringify(Object.entries(a).sort(([x],[y])=>x.localeCompare(y)))===JSON.stringify(Object.entries(b).sort(([x],[y])=>x.localeCompare(y)));}

export async function runAutomaticApiChecks(options:{path:string;loadRoutes:()=>Promise<ApiRouteCredential[]>;check:(route:ApiRouteCredential,signal:AbortSignal)=>Promise<void>;now?:()=>number;signal?:AbortSignal}){
 const signal=options.signal??new AbortController().signal;
 if(signal.aborted)return;
 const routes=(await options.loadRoutes()).filter(eligible);
 if(!routes.length)return;
 for(const route of routes){
  if(signal.aborted)return;
  const current=(await options.loadRoutes()).find(r=>eligible(r)&&key(r)===key(route));
  if(!current)continue;
  // Commit the claim before network I/O. Concurrent routers and restarts share it.
  const claimed=await withFileTransactionLock(options.path,async()=>{
   let attempts:Record<string,number>={};
   let raw:string|undefined,repairHistory=false;
   try{raw=await withRetry(()=>fs.readFile(options.path,"utf8"),retry);}catch(error){if((error as NodeJS.ErrnoException).code!=="ENOENT")throw error;}
   if(raw!==undefined)try{if(Buffer.byteLength(raw)>1024*1024)throw Error("API-check history too large");attempts=schema.parse(JSON.parse(raw));}catch{repairHistory=true;}
   const now=options.now?.()??Date.now(),id=claimKey(current);
   // Unknown prior attempts must not trigger another billable pass on repair.
   // Seed every eligible route so a restart can resume normally the next day.
   if(repairHistory)attempts=Object.fromEntries((await options.loadRoutes()).filter(eligible).map(route=>[claimKey(route),now]));
   const last=attempts[id] ?? attempts[key(current)];
   if(signal.aborted||(!repairHistory&&last!==undefined&&last<=now+300_000&&now-last<AUTOMATIC_API_CHECK_INTERVAL_MS))return false;
   // Keep other processes' claims; prune only the oldest history at a fixed bound.
   for(const [old] of Object.entries(attempts).sort((a,b)=>b[1]-a[1]).slice(999))delete attempts[old];
   attempts[id]=now;const temp=tempPathFor(options.path);
   await fs.mkdir(dirname(options.path),{recursive:true,mode:0o700});
   try{await fs.writeFile(temp,JSON.stringify(attempts)+"\n",{mode:0o600,flag:"wx"});await withRetry(()=>fs.rename(temp,options.path),retry);}finally{await withRetry(()=>fs.rm(temp,{force:true}),retry);}
   if(repairHistory)logWarn("Automatic API check history repaired; billable checks deferred for one daily interval.");
   return !repairHistory;
  });
  if(!claimed||signal.aborted)continue;
  try{await options.check(current,signal);}catch{if(!signal.aborted)logWarn("Automatic API capability check failed; retry deferred until the next daily interval.");}
 }
}
export function createAutomaticApiCheck(){return async(signal:AbortSignal)=>runAutomaticApiChecks({
 path:join(getCodexMultiAuthDir(),"automatic-api-checks.json"),loadRoutes:loadApiRoutes,signal,
 check:async(route,stop)=>{
  let acceptedRoute=route, discoveryAccepted=true;
  const updateDiscovery:typeof updateApiModelDiscovery=async(original,models)=>{
   discoveryAccepted=false;
   const known=new Set(original.knownModels??models),visible=new Set(original.visibleModels);
   for(const model of models){if(!known.has(model)&&/^gpt-/i.test(model))visible.add(model);known.add(model);}
   const expected={...original,knownModels:[...known],visibleModels:[...visible]};
   const updated=await updateApiModelDiscovery(original,models);
   // Accept this discovery's expansion only, never a concurrent policy edit.
   if(!sameRoute(updated,expected))throw Error("API check route changed");
   acceptedRoute=updated;discoveryAccepted=true;return updated;
  };
  // Link shutdown to each existing bounded request without weakening its timeout.
  const fetcher:typeof fetch=async(input,init)=>{
   const signal=combineSignals(stop,init?.signal);
   signal.throwIfAborted();
   if(init?.method?.toUpperCase()==="POST" && (!discoveryAccepted || !(await loadApiRoutes()).some(r=>eligible(r)&&sameRoute(r,acceptedRoute))))throw Error("API check route changed");
   signal.throwIfAborted();
   return fetch(input,{...init,signal});
  };
  const inventory=await discoverModelInventory(null,[route],{fetchImpl:fetcher,persistProbes:true,updateApiDiscovery:updateDiscovery});
  if(stop.aborted||!discoveryAccepted)return;
  const current=(await loadApiRoutes()).find(r=>eligible(r)&&sameRoute(r,acceptedRoute));
  if(current)await saveModelInventory(inventory,{mergeEntries:true});
 },
});}
/** Daily checks run off startup, never overlap, and drain on shutdown. */
export function startAutomaticApiChecks(run:(signal:AbortSignal)=>Promise<void>){
 const controller=new AbortController();let pending:Promise<void>|undefined;let timer:ReturnType<typeof setTimeout>|undefined;
 const schedule=(delay:number)=>{if(!controller.signal.aborted){timer=setTimeout(tick,delay);timer.unref();}};
 const tick=()=>{timer=undefined;if(controller.signal.aborted)return;pending=Promise.resolve().then(()=>run(controller.signal)).catch(()=>{if(!controller.signal.aborted)logWarn("Automatic API capability checks unavailable; retry deferred.");}).finally(()=>{pending=undefined;schedule(AUTOMATIC_API_CHECK_INTERVAL_MS);});};
 schedule(5000);return{async stop(){clearTimeout(timer);controller.abort();await pending;}};
}
