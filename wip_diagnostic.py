#!/usr/bin/env python3
"""
wip_diagnostic.py  -  why do the WIP tiles and the WIP trend chart disagree?

What it does
  1. Calls YOUR app's own endpoints (same code the page uses) to get exactly what the page shows for the month:
       tiles  : /analytics/wip   (Segmentation, PSP/Design, Surgeon Approval, Peer Review, Manufacturing)
       chart  : /analytics/report/wip-trend (weekly + monthly points) and the case list behind each point
  2. Pulls one row per case straight from BigQuery (raw Case table, so cases that are COMPLETED today are included)
     plus the on-hold event history.
  3. Rebuilds the tiles and the chart in pandas, checks the rebuild equals what the page shows ("replication check";
     both the rules BEFORE and AFTER the WIP fix are rebuilt, so it works on either version of the app),
     then rebuilds a REFERENCE that also counts cases outside vw_fact_case.
  4. Reports, case by case, WHY the numbers differ:
       - tiles vs chart, today
       - chart vs reference for every snapshot of the month (a "bridge" of each quirk)

Run from the report-generator-app folder (needs report_generator_app.py next to it):
    python wip_diagnostic.py --key C:\\Users\\rohit.jagini\\bq-key.json --month 2026-10
Optional filters (same ones you would pick on the page):
    --product "Reverse Total Shoulder Arthroplasty"     (comma separated)
    --case-type "Custom rTSA - Custom Glenoid + Standard Stem"
    --asof 2026-10-28      snapshot used for "today" (default: today)
    --out wip_diag         output folder (CSVs + SUMMARY.txt)
Offline replay (no BigQuery):  --offline DIR   (DIR has cases.csv and hold_events.csv, optional page.json)
"""
import argparse
import calendar
import datetime as dt
import json
import os
import sys

import pandas as pd

INTAKE = ['Case Intake', 'Restor3d MedEd', 'Cody Case Intake']       # build_where_clause() default intake filter
DATE_COLS = ['first_scan', 'ship', 'seg', 'first_psp', 'surg_app', 'peer', 'scan_assess', 'dcp', 'call']
BOOL_COLS = ['in_view', 'deleted', 'on_hold', 'canceled', 'reject']
SNAP_OUT = []          # lines of SUMMARY.txt
FAR_FUTURE = pd.Timestamp('2100-01-01')
TODAY = pd.Timestamp(dt.date.today())


def say(line=""):
    print(line)
    SNAP_OUT.append(line)


# ───────────────────────────── data ─────────────────────────────
CASES_SQL = """
WITH {signoff},
design_call AS (
    SELECT caseId, MIN(DATE(CAST(startTime AS TIMESTAMP))) AS call_date
    FROM {design_call_tbl}
    WHERE LOWER(COALESCE(CAST(cancelled AS STRING), 'false')) = 'false'
      AND LOWER(COALESCE(CAST(deleted AS STRING), 'false')) = 'false' AND startTime IS NOT NULL
    GROUP BY caseId
),
case_pipeline AS (
    SELECT w.caseId,
        MIN(CASE WHEN wm.name = 'Scan Assessment' AND w.workModuleUserType = 'WORKER'
                  AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) AS scan_assess,
        MAX(CASE WHEN wm.name = 'Design Call Prep' AND w.workModuleUserType = 'REVIEWER'
                  AND w.workModuleSignatureType = 'ACCEPT' THEN 1 ELSE 0 END) AS dcp_done
    FROM {signoff_tbl} w
    JOIN {instance_tbl} wi ON w.workModuleInstanceId = wi.id
    JOIN {module_tbl} wm ON wi.workModuleId = wm.id
    WHERE w.deleted = false
    GROUP BY w.caseId
)
SELECT base.id AS caseId,
    COALESCE(CONCAT(f.count, '_', f.alias), CAST(base.id AS STRING)) AS alias,
    cc.name AS product, ct.name AS case_type,
    (f.id IS NOT NULL) AS in_view,
    COALESCE(f.phase, base.phase) AS phase,
    COALESCE(f.onHold, base.onHold) AS on_hold,
    COALESCE(f.canceled, base.canceled) AS canceled,
    base.deleted AS deleted,
    f.Case_In_Take AS intake, f.work_queue,
    f.phy_nameFirst, f.phy_nameLast,
    s.first_scan_upload_date AS first_scan,
    DATE(CAST(s.ship_wrk_comp_date AS TIMESTAMP)) AS ship,
    sd.seg_review_date AS seg, sd.first_psp_review_date AS first_psp,
    sd.surgeon_approval_date AS surg_app, sd.peer_review_date AS peer,
    COALESCE(sd.surgeon_last_action_reject, false) AS reject,
    cp.scan_assess, COALESCE(cp.dcp_done, 0) AS dcp_done,
    sd.design_call_prep_date AS dcp, dc.call_date AS call
FROM {case_tbl} base
JOIN {category_tbl} cc ON base.caseCategoryId = cc.id
LEFT JOIN {type_tbl} ct ON base.caseTypeId = ct.id
LEFT JOIN {view_tbl} f ON base.id = f.id
LEFT JOIN {stage_tbl} s ON base.id = s.caseId
LEFT JOIN signoff_dates sd ON base.id = sd.caseId
LEFT JOIN case_pipeline cp ON base.id = cp.caseId
LEFT JOIN design_call dc ON base.id = dc.caseId
WHERE base.deleted = false
  AND (s.first_scan_upload_date IS NOT NULL OR cp.scan_assess IS NOT NULL OR sd.seg_review_date IS NOT NULL
       OR sd.first_psp_review_date IS NOT NULL OR COALESCE(f.phase, base.phase) = 'MANUFACTURING')
  AND (LEAST(COALESCE(sd.peer_review_date, DATE '9999-12-31'),
             COALESCE(DATE(CAST(s.ship_wrk_comp_date AS TIMESTAMP)), DATE '9999-12-31')) > DATE '{first_snap}'
       OR COALESCE(f.phase, base.phase) = 'MANUFACTURING')
"""
HOLD_SQL = """
SELECT refId AS caseId, type, CAST(timestamp AS TIMESTAMP) AS ts
FROM {event_tbl}
WHERE type IN ('r3idCasePutOnHold', 'r3idCaseRemovedFromOnHold')
ORDER BY refId, timestamp
"""


def load_live(a, first_snap):
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS_JSON", open(a.key, encoding="utf-8").read())
    sys.path.insert(0, os.path.abspath(a.app_dir))
    import report_generator_app as app            # the page's own backend
    t = app.tbl
    sql = CASES_SQL.format(
        signoff=app.signoff_cte(), signoff_tbl=t('WorkModuleSignoff'), instance_tbl=t('WorkModuleInstance'),
        module_tbl=t('WorkModule'), case_tbl=t('Case'), category_tbl=t('CaseCategory'), type_tbl=t('CaseType'),
        view_tbl=t('vw_fact_case'), stage_tbl=t('vw_lkup_stage_log_dates'), design_call_tbl=t('DesignCall'), first_snap=first_snap)
    print("Pulling one row per case from BigQuery ...")
    cases = app.client.query(sql).to_dataframe()
    print(f"  {len(cases):,} cases")
    holds = app.client.query(HOLD_SQL.format(event_tbl=t('vw_event_log_rank'))).to_dataframe()
    print(f"  {len(holds):,} on-hold events")
    return cases, holds, app


def load_offline(d):
    cases = pd.read_csv(os.path.join(d, "cases.csv"))
    holds = pd.read_csv(os.path.join(d, "hold_events.csv"))
    page = None
    p = os.path.join(d, "page.json")
    if os.path.exists(p):
        page = json.load(open(p, encoding="utf-8"))
    return cases, holds, page


def normalise(cases):
    df = cases.copy()
    for c in DATE_COLS:
        df[c] = pd.to_datetime(df[c], errors="coerce").dt.normalize()
    for c in BOOL_COLS:
        df[c] = df[c].astype("boolean")
    df["dcp_done"] = pd.to_numeric(df["dcp_done"], errors="coerce").fillna(0).astype(int)
    for c in ["phase", "intake", "work_queue", "phy_nameFirst", "phy_nameLast", "product", "case_type", "alias"]:
        if c in df.columns:
            df[c] = df[c].astype("object").where(df[c].notna(), None)
    df["marketing"] = ((df["phy_nameFirst"].fillna("") + " " + df["phy_nameLast"].fillna(""))
                       .str.lower().str.contains("marketing"))
    df["exit"] = df[["peer", "ship"]].min(axis=1)                     # LEAST(peer review, ship) as the chart does
    df["start"] = df[["first_scan", "scan_assess", "seg"]].min(axis=1)  # chart start after the fix
    return df.set_index("caseId", drop=False)


def false_(s):                       # SQL "col = false": NULL counts as not matching
    return s.eq(False).fillna(False).astype(bool)


def true_(s):
    return s.eq(True).fillna(False).astype(bool)


def apply_filters(df, a):
    keep = pd.Series(True, index=df.index)
    if a.product:
        keep &= df["product"].isin([p.strip() for p in a.product.split(",")])
    if a.case_type:
        keep &= df["case_type"].isin([p.strip() for p in a.case_type.split(",")])
    return df[keep]


# ───────────────────────────── holds ─────────────────────────────
def hold_intervals(holds, now):
    out = {}
    if holds is None or len(holds) == 0:
        return out
    h = holds.copy()
    h["ts"] = pd.to_datetime(h["ts"], utc=True).dt.tz_localize(None)
    for cid, g in h.sort_values(["caseId", "ts"]).groupby("caseId"):
        start, lst = None, []
        for typ, ts in zip(g["type"], g["ts"]):
            if typ == "r3idCasePutOnHold":
                if start is None:
                    start = ts
            elif start is not None:
                lst.append((start, ts))
                start = None
        if start is not None:
            lst.append((start, FAR_FUTURE))          # still on hold: stays held on every later date too
        out[cid] = lst
    return out


def on_hold_at(df, intervals, snap):
    """True for cases that are on hold at the END of day `snap`."""
    end_of_day = pd.Timestamp(snap) + pd.Timedelta(days=1)
    held = [cid for cid, lst in intervals.items() if any(st < end_of_day < en for st, en in lst)]
    return pd.Series(df.index.isin(held), index=df.index)


# ───────────────────────────── the page's logic, rebuilt ─────────────────────────────
def tile_flags(df):
    """The five tile rules from analytics_wip() AFTER the WIP fix (one predicate per tile, mutually exclusive)."""
    f = df[df["in_view"] & false_(df["deleted"]) & df["intake"].isin(INTAKE)]
    past_psp = f["first_psp"].notna() | f["peer"].notna() | f["ship"].notna()
    awaiting = (f["dcp_done"] == 1) & (f["work_queue"] == "PLANNING_REVIEW")
    ok = false_(f["on_hold"]) & false_(f["canceled"])
    phase_dp = ~f["phase"].isin(["SHIPPING", "SURGERY", "MANUFACTURING"])
    rej = true_(f["reject"])
    mk = ~f["marketing"]
    first_psp_wait = f["first_psp"].notna() & f["surg_app"].isna() & f["peer"].isna() & f["ship"].isna()
    t = pd.DataFrame(index=f.index)
    t["segmentation"] = f["first_scan"].notna() & f["seg"].isna() & ~past_psp & ok & phase_dp & mk
    t["psp_design"] = ((f["seg"].notna() & ~past_psp & ok & phase_dp & ~awaiting & mk)
                       | (first_psp_wait & rej & ok & phase_dp & mk))
    t["surgeon_approval"] = ((first_psp_wait & ~rej & ok & phase_dp & mk)
                             | (f["seg"].notna() & ~past_psp & ok & phase_dp & awaiting & mk))
    t["peer_review"] = (past_psp & f["peer"].isna() & f["ship"].isna() & ok & phase_dp
                        & ~(f["first_psp"].notna() & f["surg_app"].isna()) & mk)
    t["manufacturing"] = (f["phase"] == "MANUFACTURING") & ok & f["ship"].isna() & mk
    return t


def surgeon_wait_at(df, snap):
    """Waiting on the surgeon after PSP review, on that date (timestamps only)."""
    s = pd.Timestamp(snap)
    return (df["first_psp"].notna() & (df["first_psp"] <= s) & (df["surg_app"].isna() | (df["surg_app"] > s))
            & (df["peer"].isna() | (df["peer"] > s)))


def old_surgeon_wait_at(df, snap):
    return surgeon_wait_at(df, snap) & ~true_(df["reject"])


def chart_members(df, snap, intervals, in_view_only=True):
    """The chart AFTER the WIP fix (report_wip_trend). in_view_only=False also counts cases outside vw_fact_case
    (intake / marketing / work queue are unknown for those, so those rules are skipped) = the REFERENCE."""
    s = pd.Timestamp(snap)
    now = s >= TODAY
    r = df[df["in_view"]] if in_view_only else df
    known = r["in_view"]
    r = r[false_(r["deleted"]) & false_(r["canceled"]) & (~known | r["intake"].isin(INTAKE)) & ~r["marketing"] & r["start"].notna()]
    r = r[(r["start"] <= s) & (r["exit"].isna() | (r["exit"] > s))]
    r = r[~(r["exit"].isna() & r["phase"].isin(["SHIPPING", "SURGERY", "MANUFACTURING"]))]
    r = r[~(true_(r["on_hold"]) if now else on_hold_at(r, intervals, snap))]
    r = r[~surgeon_wait_at(r, snap)]
    queue = (r["work_queue"] == "PLANNING_REVIEW")
    past_psp = r["first_psp"].notna() | r["peer"].notna() | r["ship"].notna()
    if now:
        pre = r["seg"].notna() & ~past_psp & r["dcp"].notna() & queue
    else:
        pre = (r["dcp"].notna() & (r["dcp"] <= s) & (r["first_psp"].isna() | (r["first_psp"] > s))
               & ((r["call"] > s) | (r["call"].isna() & r["seg"].notna() & ~past_psp & queue)))
    return r[~pre].index


def old_chart_members(df, snap):
    """The chart BEFORE the WIP fix: vw_fact_case only, today's hold flag, no intake / phase rule, Scan Assessment start."""
    v = df[df["in_view"] & false_(df["deleted"]) & false_(df["canceled"]) & false_(df["on_hold"])
           & ~df["marketing"] & df["scan_assess"].notna()]
    s = pd.Timestamp(snap)
    inwip = (v["scan_assess"] <= s) & (v["exit"].isna() | (v["exit"] > s))
    return v[inwip & ~old_surgeon_wait_at(v, snap)].index


# ───────────────────────────── explanations ─────────────────────────────
def why_not_on_chart(row, snap):
    """Why a case that is in the tiles is not on the chart (new rules; first failing rule)."""
    s = pd.Timestamp(snap)
    if not row["in_view"]:
        return "not in vw_fact_case"
    if row["intake"] not in INTAKE:
        return f"Case_In_Take = {row['intake']!r}"
    if row["marketing"]:
        return "marketing surgeon"
    if pd.isna(row["start"]):
        return "no start date (no first scan / scan assessment / segmentation)"
    if row["start"] > s:
        return "entered after the snapshot"
    if pd.notna(row["exit"]) and row["exit"] <= s:
        return "already exited (peer review / ship date on or before the snapshot)"
    if pd.isna(row["exit"]) and row["phase"] in ("SHIPPING", "SURGERY", "MANUFACTURING"):
        return f"phase = {row['phase']} with no peer review / ship date"
    return "other"


def which_tiles(tf):
    return tf.apply(lambda r: [k for k in tf.columns if r[k]], axis=1)


def why_not_in_dp_tiles(row, tiles_row_names):
    if tiles_row_names:
        return "counted in the " + "/".join(tiles_row_names) + " tile (not a digital-production tile)"
    if not row["in_view"]:
        return "not in vw_fact_case"
    if row["intake"] not in INTAKE:
        return f"Case_In_Take = {row['intake']!r} (tiles only count {', '.join(INTAKE)})"
    if row["phase"] in ("SHIPPING", "SURGERY", "MANUFACTURING"):
        return f"phase = {row['phase']} with no peer review / ship date"
    return "matches none of the tile rules"


# ───────────────────────────── page numbers (live) ─────────────────────────────
def app_periods(date_from, date_to, gran):
    """Mirror of the SQL GENERATE_DATE_ARRAY / DATE_TRUNC / period_end logic in report_wip_trend()."""
    d0, d1 = pd.Timestamp(date_from), pd.Timestamp(date_to)
    out, d = [], d0
    while d <= d1:
        if gran == "weekly":
            start = d - pd.Timedelta(days=int(d.weekday()))
            end = start + pd.Timedelta(days=7)
            d += pd.Timedelta(days=7)
        else:
            start = d.replace(day=1)
            end = start + pd.DateOffset(months=1)
            d = d + pd.DateOffset(months=1)
        if not out or out[-1][0] != start:
            out.append((start, end))
    return out


def page_numbers(app, a, date_from, date_to):
    c = app.app.test_client()
    qs = ""
    if a.product:
        qs += "&product=" + a.product.replace(" ", "%20")
    if a.case_type:
        qs += "&case_type=" + a.case_type.replace(" ", "%20")
    page = {"tiles": c.get("/analytics/wip?" + qs.lstrip("&")).get_json(), "chart": {}, "lists": {}}
    for gran in ("weekly", "monthly"):
        j = c.get(f"/analytics/report/wip-trend?granularity={gran}&date_from={date_from}&date_to={date_to}{qs}").get_json()
        page["chart"][gran] = dict(zip(j["periods"], j["wip"]))
    for step in ("segmentation", "psp_design", "surgeon_approval", "peer_review", "manufacturing"):
        j = c.get(f"/analytics/wip?fetch_step={step}{qs}").get_json()
        page["lists"]["tile_" + step] = [x["id"] for x in j.get("cases", [])] + (["…500+"] if j.get("count", 0) >= 500 else [])
    return page, c, qs


def chart_list(client, gran, snap, qs, df_range):
    j = client.get(f"/analytics/report/wip-trend?granularity={gran}&fetch_period={snap}&date_from={df_range[0]}&date_to={df_range[1]}{qs}").get_json()
    return [x["id"] for x in j.get("cases", [])], j.get("count", 0)


# ───────────────────────────── report ─────────────────────────────
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--key")
    ap.add_argument("--app-dir", default=".")
    ap.add_argument("--month", default=dt.date.today().strftime("%Y-%m"))
    ap.add_argument("--product", default="")
    ap.add_argument("--case-type", default="")
    ap.add_argument("--asof", default="")
    ap.add_argument("--out", default="wip_diag")
    ap.add_argument("--offline", default="")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    y, m = map(int, a.month.split("-"))
    date_from = f"{y}-{m:02d}-01"
    date_to = f"{y}-{m:02d}-{calendar.monthrange(y, m)[1]:02d}"
    asof = pd.Timestamp(a.asof) if a.asof else pd.Timestamp(dt.date.today())
    snaps = {g: app_periods(date_from, date_to, g) for g in ("weekly", "monthly")}
    first_snap = (min(s for g in snaps.values() for s, _ in g) - pd.Timedelta(days=14)).strftime("%Y-%m-%d")

    app, page, client, qs = None, None, None, ""
    if a.offline:
        cases, holds, page = load_offline(a.offline)
    else:
        if not a.key:
            ap.error("--key is required (or use --offline)")
        cases, holds, app = load_live(a, first_snap)
        page, client, qs = page_numbers(app, a, date_from, date_to)
    df = apply_filters(normalise(cases), a)
    intervals = hold_intervals(holds, None)
    df.to_csv(os.path.join(a.out, "cases_table.csv"), index=False)

    say("=" * 100)
    say(f"WIP DIAGNOSTIC   month {a.month}   today = {asof.date()}   filters: product={a.product or 'all'}  case type={a.case_type or 'all'}")
    say("=" * 100)

    # ── 1. tiles ───────────────────────────────────────────────────────────────
    tf = tile_flags(df)
    mine_tiles = {k: int(tf[k].sum()) for k in tf.columns}
    say("\n1) CURRENT WIP TILES (no date filter - every case alive right now)")
    say(f"{'tile':<18}{'page':>8}{'rebuilt':>9}")
    page_tiles = (page or {}).get("tiles") or {}
    for k in tf.columns:
        pv = page_tiles.get(k, "n/a")
        say(f"{k:<18}{str(pv):>8}{mine_tiles[k]:>9}" + ("" if pv == "n/a" or pv == mine_tiles[k] else "   <-- differs (see TILE REPLICATION DETAIL in section 6)"))
    dp_tiles = int(tf[["segmentation", "psp_design", "peer_review"]].any(axis=1).sum())
    say(f"Digital-production tiles (Segmentation + PSP/Design + Peer Review): {mine_tiles['segmentation'] + mine_tiles['psp_design'] + mine_tiles['peer_review']}"
        f"   (distinct cases: {dp_tiles})")
    multi = tf.sum(axis=1)
    dbl = tf[multi > 1]
    say(f"Cases counted in MORE THAN ONE tile: {len(dbl)}" + ("   (the WIP fix makes the tiles mutually exclusive; 0 is expected after deploying it)" if len(dbl) == 0 else "   e.g. " + ", ".join(f"{i[:8]}:{'+'.join(k for k in tf.columns if dbl.loc[i, k])}" for i in dbl.index[:5])))

    # ── 2. chart ───────────────────────────────────────────────────────────────
    say("\n2) WIP TREND CHART POINTS   (old = rules before the WIP fix, new = after; the page equals whichever version is deployed)")
    rows = []
    for gran in ("weekly", "monthly"):
        say(f"  {gran}: period start -> snapshot | page | old rules | new rules | reference (incl. cases outside the view) | new - reference")
        for start, end in snaps[gran]:
            old = len(old_chart_members(df, end))
            new = len(chart_members(df, end, intervals))
            ref = len(chart_members(df, end, intervals, in_view_only=False))
            pg = ((page or {}).get("chart", {}).get(gran, {}) or {}).get(start.strftime("%Y-%m-%d"), "n/a")
            flag = "" if pg == "n/a" or pg in (old, new) else "   <-- page matches neither rebuild"
            who = "" if pg == "n/a" else ("   (page = new rules)" if pg == new and pg != old else "   (page = old rules)" if pg == old and pg != new else "")
            say(f"    {start.date()} -> {end.date()} | {str(pg):>5} | {old:>5} | {new:>5} | {ref:>5} | {(new - ref):>+5}{flag}{who}")
            rows.append({"granularity": gran, "period_start": start.date(), "snapshot": end.date(), "page": pg, "old_rules": old, "new_rules": new, "reference": ref})
    pd.DataFrame(rows).to_csv(os.path.join(a.out, "chart_points.csv"), index=False)

    # ── 3. today: tiles vs chart ───────────────────────────────────────────────
    say("\n" + "-" * 100)
    say(f"3) TODAY ({asof.date()}): DIGITAL-PRODUCTION TILES vs THE CHART'S CURRENT POINT (new rules)")
    chart_now = set(chart_members(df, asof, intervals))
    tile_dp = set(tf.index[tf[["segmentation", "psp_design", "peer_review"]].any(axis=1)])
    say(f"  tiles (Seg + PSP/Design + Peer Review, distinct cases): {len(tile_dp)}")
    say(f"  chart (Digital Production WIP now, new rules):          {len(chart_now)}")
    only_tile, only_chart = sorted(tile_dp - chart_now), sorted(chart_now - tile_dp)
    say(f"  in tiles but NOT on the chart: {len(only_tile)}      on the chart but NOT in the DP tiles: {len(only_chart)}"
        + ("      -> EQUAL" if not only_tile and not only_chart else ""))
    t_rows = []
    for cid in only_tile:
        t_rows.append({"caseId": cid, "alias": df.loc[cid, "alias"], "side": "tiles_only", "reason": why_not_on_chart(df.loc[cid], asof)})
    names = which_tiles(tf)
    for cid in only_chart:
        row = df.loc[cid]
        t_rows.append({"caseId": cid, "alias": row["alias"], "side": "chart_only", "reason": why_not_in_dp_tiles(row, names.get(cid, []))})
    td = pd.DataFrame(t_rows, columns=["caseId", "alias", "side", "reason"])
    td.to_csv(os.path.join(a.out, "today_tiles_vs_chart.csv"), index=False)
    for side, title in (("tiles_only", "In the tiles, missing from the chart"), ("chart_only", "On the chart, missing from the DP tiles")):
        sub = td[td["side"] == side]
        if len(sub):
            say(f"\n  {title}: {len(sub)}")
            for reason, n in sub["reason"].value_counts().items():
                ex = ", ".join(sub[sub["reason"] == reason]["alias"].astype(str).head(3))
                say(f"    {n:>4}  {reason}   e.g. {ex}")
    on_hold_now = int((df["in_view"] & false_(df["deleted"]) & df["intake"].isin(INTAKE) & true_(df["on_hold"])).sum())
    say(f"\n  The tile strip also holds Surgeon Approval {mine_tiles['surgeon_approval']}, Manufacturing {mine_tiles['manufacturing']}, On Hold {on_hold_now}"
        f"  -> {sum(mine_tiles.values()) + on_hold_now} in all; those are not digital production, so they are not on the chart")

    # ── 4. each snapshot: chart (new) vs reference ─────────────────────────────
    say("\n" + "-" * 100)
    say("4) EACH SNAPSHOT: CHART (new rules) vs REFERENCE (same rules, but also counting cases outside vw_fact_case)")
    bridge = []
    for gran in ("weekly", "monthly"):
        for start, end in snaps[gran]:
            page_set = set(chart_members(df, end, intervals))
            ref_set = set(chart_members(df, end, intervals, in_view_only=False))
            only_ref = sorted(ref_set - page_set)
            reasons = {}
            for cid in only_ref:
                r = df.loc[cid]
                k = ("not in vw_fact_case AND no peer review / ship date at all (never 'exits')" if pd.isna(r["exit"])
                     else "not in vw_fact_case, exited after the snapshot (completed since)")
                reasons[k] = reasons.get(k, 0) + 1
                bridge.append({"granularity": gran, "snapshot": end.date(), "caseId": cid, "alias": r["alias"], "side": "reference_only", "reason": k})
            say(f"\n  {gran} {start.date()} -> {end.date()}:  chart {len(page_set)}   reference {len(ref_set)}   gap {len(page_set) - len(ref_set):+d}")
            for k, n in sorted(reasons.items(), key=lambda kv: -kv[1]):
                say(f"      -{n:>4}  {k}")
    br = pd.DataFrame(bridge, columns=["granularity", "snapshot", "caseId", "alias", "side", "reason"])
    br.to_csv(os.path.join(a.out, "chart_vs_reference.csv"), index=False)

    # ── 6. outside the view + tile replication detail ───────────────────────────
    say("\n" + "-" * 100)
    say("6) CASES OUTSIDE vw_fact_case THAT A DATE-BASED VIEW OF WIP WOULD COUNT (today's snapshot)")
    ref_today = chart_members(df, asof, intervals, in_view_only=False)
    outside = df.loc[[c for c in ref_today if not df.loc[c, "in_view"]]]
    say(f"  {len(outside)} cases: scan assessed, no peer review / ship date, not canceled, not on hold, but not in vw_fact_case")
    if len(outside):
        for col, title in (("phase", "by phase"), ("intake", "by Case_In_Take"), ("product", "by product")):
            vc = outside[col].fillna("(none)").value_counts().head(8)
            say(f"    {title}: " + ", ".join(f"{k} {v}" for k, v in vc.items()))
        yrs = outside["start"].dt.year.fillna(0).astype(int).value_counts().sort_index()
        say("    year of first scan / scan assessment: " + ", ".join(f"{y or '?'} {n}" for y, n in yrs.items()))
        say(f"    with a PSP review date: {int(outside['first_psp'].notna().sum())}   with a peer review date: {int(outside['peer'].notna().sum())}   with a ship date: {int(outside['ship'].notna().sum())}")
        outside.drop(columns=["exit"]).to_csv(os.path.join(a.out, "outside_view_cases.csv"), index=False)
    say("\n  TILE REPLICATION DETAIL (page's case list vs my rebuild; lists stop at 500 rows)")
    lists = (page or {}).get("lists", {}) or {}
    for k in tf.columns:
        pl = lists.get("tile_" + k)
        if not pl:
            continue
        if any(str(x).startswith("…") for x in pl):
            say(f"    {k}: page list truncated at 500, skipped")
            continue
        pids, mine = set(pl), set(tf.index[tf[k]])
        extra_page, extra_mine = sorted(pids - mine), sorted(mine - pids)
        if not extra_page and not extra_mine:
            say(f"    {k}: lists identical ({len(pids)} cases)")
            continue
        missing = [c for c in extra_page if c not in df.index]
        present = [c for c in extra_page if c in df.index]
        say(f"    {k}: page has {len(extra_page)} cases my rebuild lacks ({len(missing)} not in my pull, {len(present)} in my pull but failing my rule); "
            f"my rebuild has {len(extra_mine)} the page lacks   (if the WIP fix is not deployed yet this is expected)")
        for c in present[:3]:
            r = df.loc[c]
            say(f"        {r['alias']}: phase={r['phase']} on_hold={r['on_hold']} canceled={r['canceled']} intake={r['intake']} peer={r['peer']} ship={r['ship']} marketing={r['marketing']}")

    # ── findings ────────────────────────────────────────────────────────────────
    say("\n" + "-" * 100)
    say("FINDINGS")
    cp = pd.DataFrame(rows)
    wk = cp[cp["granularity"] == "weekly"]
    if len(wk):
        say(f"  * Weekly points, old rules -> new rules: " + ", ".join(f"{int(r.old_rules)}->{int(r.new_rules)}" for r in wk.itertuples()))
    say(f"  * Today: digital-production tiles {len(tile_dp)} vs chart {len(chart_now)}" + ("  (equal)" if tile_dp == chart_now else f"  ({len(only_tile)} tiles-only, {len(only_chart)} chart-only, listed above)"))
    nev = int(br["reason"].str.contains("never 'exits'", regex=False).groupby(br["snapshot"]).sum().max()) if len(br) else 0
    if nev:
        say(f"  * About {nev} cases outside vw_fact_case have no exit date at all; the chart does not count them (see section 6 for what they are).")

    # ── 5. definitions ──────────────────────────────────────────────────────────
    say("\n" + "-" * 100)
    say("5) THE DEFINITION (after the WIP fix)")
    say("  Tiles and chart share one rule set: vw_fact_case, not deleted / canceled, Case_In_Take in (Case Intake, Restor3d MedEd, Cody Case Intake),")
    say("  marketing surgeons excluded, not on hold, phase not SHIPPING / SURGERY / MANUFACTURING, not waiting on the surgeon (before or after PSP review).")
    say("  Chart start = earliest of first scan upload / Scan Assessment accept / Segmentation accept; exit = peer review accept or ship date.")
    say("  Past dates use sign-off dates plus hold and design-call events; today's point uses the live flags, so it equals the tiles.")
    open(os.path.join(a.out, "SUMMARY.txt"), "w", encoding="utf-8").write("\n".join(SNAP_OUT))
    say(f"\nWrote CSVs and SUMMARY.txt to {os.path.abspath(a.out)}")


if __name__ == "__main__":
    main()
