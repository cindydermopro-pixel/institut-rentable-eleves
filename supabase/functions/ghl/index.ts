import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

// Edge function "ghl" — passerelle entre l'appli élèves et GoHighLevel.
// Actions : sync (récupère les opportunités d'une élève), stage (change l'étape d'une opportunité),
//           test (vérifie la clé), pipelines (recharge les étapes).
// La clé d'agence GHL est stockée dans ir_config (admin seulement) et lue ici avec la clé service.

const GHL = "https://services.leadconnectorhq.com";
const VERSION = "2021-07-28";
const cors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};
const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: { ...cors, "Content-Type": "application/json" } });

async function ghlFetch(token: string, path: string, init: RequestInit = {}) {
  const r = await fetch(GHL + path, { ...init, headers: { Authorization: "Bearer " + token, Version: VERSION, Accept: "application/json", "Content-Type": "application/json", ...(init.headers || {}) } });
  const text = await r.text(); let body: any = null; try { body = JSON.parse(text); } catch { body = { raw: text }; }
  if (!r.ok) throw new Error(`GHL ${r.status} ${path} : ${body?.message || body?.error || text.slice(0, 200)}`);
  return body;
}

// Token utilisable pour un sous-compte : token propre à l'élève, sinon token de localisation généré depuis la clé d'agence.
const locTokenCache = new Map<string, { token: string; exp: number }>();
async function locationToken(admin: any, eleve: any): Promise<string> {
  if (eleve.ghl_token) return eleve.ghl_token;
  const c = locTokenCache.get(eleve.ghl_location_id); if (c && c.exp > Date.now()) return c.token;
  const { data: cfg } = await admin.from("ir_config").select("key,value").in("key", ["ghl_agency_token", "ghl_company_id"]);
  const conf: Record<string, string> = {}; (cfg || []).forEach((r: any) => conf[r.key] = r.value);
  if (!conf.ghl_agency_token) throw new Error("Clé GHL non configurée (Réglages → GoHighLevel).");
  // Clé d'agence de type Private Integration : on génère un token de sous-compte.
  if (conf.ghl_company_id) {
    try {
      const r = await ghlFetch(conf.ghl_agency_token, "/oauth/locationToken", { method: "POST", body: JSON.stringify({ companyId: conf.ghl_company_id, locationId: eleve.ghl_location_id }) });
      const tok = r.access_token; if (tok) { locTokenCache.set(eleve.ghl_location_id, { token: tok, exp: Date.now() + 50 * 60 * 1000 }); return tok; }
    } catch (_e) { /* on retombe sur la clé telle quelle */ }
  }
  return conf.ghl_agency_token;
}

async function loadPipelines(admin: any, eleve: any, token: string) {
  const r = await ghlFetch(token, `/opportunities/pipelines?locationId=${encodeURIComponent(eleve.ghl_location_id)}`);
  const rows = (r.pipelines || []).map((p: any) => ({ eleve_id: eleve.id, pipeline_id: p.id, nom: p.name || "", stages: (p.stages || []).sort((a: any, b: any) => (a.position ?? 0) - (b.position ?? 0)).map((s: any) => ({ id: s.id, nom: s.name })) }));
  if (rows.length) await admin.from("ir_pipelines").upsert(rows);
  return rows;
}

async function sync(admin: any, eleve: any) {
  const token = await locationToken(admin, eleve);
  const pipes = await loadPipelines(admin, eleve, token);
  const stageName: Record<string, string> = {}; pipes.forEach((p: any) => p.stages.forEach((s: any) => stageName[s.id] = s.nom));
  const all: any[] = []; let page = 1; let startAfter = "", startAfterId = "";
  for (let i = 0; i < 20; i++) {
    let path = `/opportunities/search?location_id=${encodeURIComponent(eleve.ghl_location_id)}&limit=100`;
    if (eleve.ghl_pipeline_id) path += `&pipeline_id=${encodeURIComponent(eleve.ghl_pipeline_id)}`;
    if (startAfter) path += `&startAfter=${startAfter}&startAfterId=${startAfterId}`; else path += `&page=${page}`;
    const r = await ghlFetch(token, path);
    const ops = r.opportunities || []; all.push(...ops);
    const m = r.meta || {}; if (!ops.length || ops.length < 100) break;
    if (m.startAfter && m.startAfterId) { startAfter = m.startAfter; startAfterId = m.startAfterId; } else page++;
  }
  const rows = all.map((o: any) => ({
    id: o.id, eleve_id: eleve.id, contact_id: o.contact?.id || o.contactId || null,
    nom: o.contact?.name || [o.contact?.firstName, o.contact?.lastName].filter(Boolean).join(" ") || o.name || "",
    tel: o.contact?.phone || "", email: o.contact?.email || "",
    prestation: o.name || "", source: o.source || "", valeur: o.monetaryValue ?? null,
    pipeline_id: o.pipelineId || null, stage_id: o.pipelineStageId || null, stage_nom: stageName[o.pipelineStageId] || "",
    statut: o.status || "open", cree_le: o.createdAt || null, maj_le: o.updatedAt || null, synced_at: new Date().toISOString(),
  }));
  if (rows.length) { const { error } = await admin.from("ir_prospects").upsert(rows, { onConflict: "id" }); if (error) throw error; }
  await admin.from("ir_eleves").update({ ghl_last_sync: new Date().toISOString() }).eq("id", eleve.id);
  return { count: rows.length, pipelines: pipes };
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  try {
    const auth = req.headers.get("Authorization") || "";
    const url = Deno.env.get("SUPABASE_URL")!;
    const user = createClient(url, Deno.env.get("SUPABASE_ANON_KEY")!, { global: { headers: { Authorization: auth } } });
    const admin = createClient(url, Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!);
    const { data: { user: u } } = await user.auth.getUser(); if (!u) return json({ error: "Non connectée" }, 401);
    const { data: isAdmin } = await user.rpc("ir_is_admin");
    const body = await req.json().catch(() => ({}));
    const action = body.action;

    // Quelle élève ? L'admin choisit ; l'élève est forcément elle-même.
    let eleve: any = null;
    if (isAdmin && body.eleve_id) { const { data } = await admin.from("ir_eleves").select("*").eq("id", body.eleve_id).single(); eleve = data; }
    else if (!isAdmin) { const { data } = await admin.from("ir_eleves").select("*").ilike("email", u.email || "").maybeSingle(); eleve = data; }

    if (action === "test") {
      if (!isAdmin) return json({ error: "Réservé à l'admin" }, 403);
      const { data: cfg } = await admin.from("ir_config").select("key,value").eq("key", "ghl_agency_token").maybeSingle();
      if (!cfg?.value) return json({ ok: false, message: "Aucune clé enregistrée." });
      try { const r = await ghlFetch(cfg.value, "/locations/search?limit=5"); const locs = (r.locations || []).map((l: any) => ({ id: l.id, name: l.name })); return json({ ok: true, locations: locs }); }
      catch (e) { try { const r2 = await ghlFetch(cfg.value, `/locations/${encodeURIComponent(body.location_id || "")}`); return json({ ok: true, locations: [{ id: r2.location?.id, name: r2.location?.name }] }); } catch { return json({ ok: false, message: String(e.message || e) }); } }
    }
    if (!eleve) return json({ error: "Dossier élève introuvable" }, 404);
    if (!eleve.ghl_location_id) return json({ error: "Aucun sous-compte GoHighLevel rattaché à cette élève." }, 400);

    if (action === "sync") return json(await sync(admin, eleve));
    if (action === "pipelines") { const token = await locationToken(admin, eleve); return json({ pipelines: await loadPipelines(admin, eleve, token) }); }
    if (action === "stage") {
      const { opportunity_id, stage_id, status, montant } = body; if (!opportunity_id) return json({ error: "opportunity_id manquant" }, 400);
      const { data: p } = await admin.from("ir_prospects").select("id,eleve_id").eq("id", opportunity_id).single();
      if (!p || p.eleve_id !== eleve.id) return json({ error: "Prospect introuvable" }, 404);
      const token = await locationToken(admin, eleve);
      const patch: any = {}; if (stage_id) patch.pipelineStageId = stage_id; if (status) patch.status = status; if (montant != null && !isNaN(Number(montant))) patch.monetaryValue = Number(montant);
      await ghlFetch(token, `/opportunities/${encodeURIComponent(opportunity_id)}`, { method: "PUT", body: JSON.stringify(patch) });
      const { data: pipes } = await admin.from("ir_pipelines").select("stages").eq("eleve_id", eleve.id);
      let stageNom = ""; (pipes || []).forEach((pp: any) => (pp.stages || []).forEach((s: any) => { if (s.id === stage_id) stageNom = s.nom; }));
      const up: any = { maj_le: new Date().toISOString() }; if (montant != null && !isNaN(Number(montant))) { up.montant = Number(montant); up.valeur = Number(montant); } if (stage_id) { up.stage_id = stage_id; up.stage_nom = stageNom; } if (status) up.statut = status;
      await admin.from("ir_prospects").update(up).eq("id", opportunity_id);
      return json({ ok: true, stage_nom: stageNom });
    }
    return json({ error: "Action inconnue" }, 400);
  } catch (e) { return json({ error: String(e?.message || e) }, 500); }
});
