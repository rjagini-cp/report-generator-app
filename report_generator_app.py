from flask import Flask, jsonify, request, send_from_directory
from werkzeug.exceptions import HTTPException

from flask_cors import CORS

from google.cloud import bigquery

from google.oauth2 import service_account

import os

import re

import json

import math

import datetime

import traceback

import pandas as pd

import anthropic

import logging
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger("datagenie")

creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")

if creds_json:
    _bq_credentials = service_account.Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
elif not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    raise RuntimeError(
        "BigQuery credentials not configured. Set GOOGLE_APPLICATION_CREDENTIALS_JSON "
        "(preferred, JSON content) or GOOGLE_APPLICATION_CREDENTIALS (path to file)."
    )
else:
    _bq_credentials = None  # use GOOGLE_APPLICATION_CREDENTIALS file path

app = Flask(__name__)

CORS(app)

@app.route("/")
@app.route("/report_generator.html")
def serve_frontend():
    """Serve the DataGenie frontend."""
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), "report_generator.html")

client = bigquery.Client(credentials=_bq_credentials) if _bq_credentials else bigquery.Client()

_anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY")

_anthropic_client = anthropic.Anthropic(api_key=_anthropic_api_key) if _anthropic_api_key else None

if not _anthropic_client:
    logger.warning("ANTHROPIC_API_KEY not set — /analytics/nlq will be unavailable.")

def handle_error(endpoint_name: str, e: Exception):
    """Log exception server-side; return a safe generic error to the client."""
    logger.exception(f"Error in {endpoint_name}: {e}")
    return jsonify({"error": "An internal error occurred. Check server logs."}), 500

@app.errorhandler(Exception)
def handle_unhandled_exception(e):
    """Catch any unhandled exception and return JSON instead of HTML 500.
    Must NOT swallow routing errors (404, 405, etc.) — those are HTTPException
    subclasses too, and re-labeling a genuine 404 as 500 hides real missing-route
    bugs behind a misleading 'internal error occurred' message."""
    if isinstance(e, HTTPException):
        return e
    logger.exception(f"Unhandled exception: {e}")
    return jsonify({"error": "An internal error occurred."}), 500

DATABASE_INDEX_SHEET_ID = "1plJGqLHT-9u90ojvAKnrtA0edTmTr45suMV2OnQfx_c"

DATABASE_INDEX_ACCOUNTS_GID = "1162564586"

_user_groups_cache = {"data": None, "fetched_at": None}

_USER_GROUPS_CACHE_TTL_SECONDS = 600  # 10 minutes

def get_user_groups(force_refresh=False):
    """Fetches the Database_Index 'Accounts' tab's System / Team / System UserID
    columns and returns {"bigquery": {team: [userIds]}, "camstar": {team: [userIds]}}.
    Cached in memory for _USER_GROUPS_CACHE_TTL_SECONDS to avoid hitting the
    sheet on every request. No credentials needed — reads the sheet's public
    CSV export (same access level as viewing it in a browser)."""
    now = datetime.datetime.utcnow()
    cached = _user_groups_cache["data"]
    fetched_at = _user_groups_cache["fetched_at"]
    if (not force_refresh and cached is not None and fetched_at is not None
            and (now - fetched_at).total_seconds() < _USER_GROUPS_CACHE_TTL_SECONDS):
        return cached

    csv_url = (f"https://docs.google.com/spreadsheets/d/{DATABASE_INDEX_SHEET_ID}"
               f"/export?format=csv&gid={DATABASE_INDEX_ACCOUNTS_GID}")
    try:
        df = pd.read_csv(csv_url)
    except Exception as e:
        logger.error(f"Failed to fetch Database_Index sheet: {e}")
        return cached if cached is not None else {"bigquery": {}, "camstar": {}}

    df.columns = [str(c).strip() for c in df.columns]
    result = {"bigquery": {}, "camstar": {}}
    for _, row in df.iterrows():
        system = str(row.get("System", "")).strip()
        team = str(row.get("Team", "")).strip()
        user_id = str(row.get("System UserID", "")).strip()
        if system not in ("BigQuery", "Camstar") or not team or not user_id or user_id.lower() == "nan":
            continue
        bucket = "bigquery" if system == "BigQuery" else "camstar"
        result[bucket].setdefault(team, []).append(user_id)

    _user_groups_cache["data"] = result
    _user_groups_cache["fetched_at"] = now
    return result

@app.route("/analytics/user-groups", methods=["GET"])
def user_groups():
    try:
        force_refresh = request.args.get("refresh", "").lower() == "true"
        return jsonify(get_user_groups(force_refresh=force_refresh))
    except Exception as e:
        return handle_error(request.endpoint, e)

PROJECT = "restor3d-data-warehouse"

DATASET = "production3_r3id_public"

def tbl(name):
    return f"`{PROJECT}.{DATASET}.{name}`"

PRODUCT_GROUPS = {
    'Upper Extremity': [
        'Anatomic Shoulder Arthroplasty','Hemiarthroplasty Shoulder',
        'Reverse Shoulder Arthroplasty - Glenoid Baseplate',
        'Reverse Shoulder Arthroplasty - Glenosphere Only',
        'Custom rTSA - Custom Glenoid + Veritas OTS Components',
        'Reverse Total Shoulder Arthroplasty',
        'Reverse Shoulder Arthroplasty - Proximal Humerus',
        'Custom rTSA - Other',
        'Acromion','Clavicle','Shoulder',
        'Hemiarthroplasty Elbow','Total Elbow Arthroplasty','Elbow Fusion',
        'Total Wrist Arthroplasty','Hand/Wrist Fusion',
        'Hemiarthroplasty Hand/Wrist','Carpal Replacement',
        'Corrective Osteotomy Hand/Wrist','Corrective Osteotomy - Arm',
        'Segmental Defect - Arm','Bone Models - Arm','Prosthetic - Arm',
    ],
    'Lower Extremity': [
        'Total Ankle Replacement','PSR','Ankle Fusion',
        'Corrective Osteotomy Ankle','Corrective Osteotomy Foot',
        'Corrective Osteotomy - Leg','Hindfoot Fusion','Midfoot','MPJ/MTP',
        'Total Talus and Other Arthroplasty','Hemitalus',
        'Antibiotic Spacer','Hemiarthroplasty Ankle','Unknown - Foot/Ankle',
        'Segmental Defect - Leg','Prosthetic - Leg',
        'Bone Models - Ankle','Bone Models - Foot',
        'Corrective Osteotomy','Prosthetic','Segmental Defect',
    ],
    'Knee': ['Total Knee Arthroplasty','TKA','Hemiarthroplasty Knee'],
    'Hip': ['Hip Arthroplasty','Hip Hemipelvis','Hip Hemiarthroplasty'],
    'Craniofacial': ['Mandible','Maxilla','General Reconstruction'],
    'Spine': ['Lumbar'],
}

CLEARED_PRODUCTS = [
    'Reverse Shoulder Arthroplasty - Glenoid Baseplate',
    'Reverse Shoulder Arthroplasty - Glenosphere Only',
    'Reverse Total Shoulder Arthroplasty',
    'Total Ankle Replacement',
]

# ══════════════════════════════════════════════════════════════
# Ported from r3id_app.py — design-time lookups needed by
# /analytics/process-efficiency and /analytics/process-efficiency/exclusions.
# ══════════════════════════════════════════════════════════════

TAR_PRODUCTS = ['Total Ankle Replacement', 'PSR']

RTSA_PRODUCTS = [
    'Reverse Total Shoulder Arthroplasty',
    'Reverse Shoulder Arthroplasty - Glenosphere Only',
    'Reverse Shoulder Arthroplasty - Glenoid Baseplate',
]

SCAN_TIMES = {
    'Corrective Osteotomy Hand/Wrist':7,'Hemiarthroplasty Hand/Wrist':7,'Carpal Replacement':7,
    'Total Wrist Arthroplasty':7,'Hand/Wrist Fusion':7,'Total Elbow Arthroplasty':7,
    'Hemiarthroplasty Elbow':7,'Elbow Fusion':7,'Clavicle':7,'Hemiarthroplasty Shoulder':7,
    'Acromion':7,'Anatomic Shoulder Arthroplasty':7,'Reverse Total Shoulder Arthroplasty':7,
    'Reverse Shoulder Arthroplasty - Glenosphere Only':7,'Reverse Shoulder Arthroplasty - Glenoid Baseplate':7,
    'Reverse Shoulder Arthroplasty - Proximal Humerus':7,'Shoulder':7,
    'Hemiarthroplasty Knee':7,'Total Knee Arthroplasty':7,'TKA':7,
    'Mandible':7,'Maxilla':7,'General Reconstruction':7,
    'Corrective Osteotomy Foot':15,'Bone Models - Foot':15,'Midfoot':15,'MPJ/MTP':15,'Hindfoot Fusion':15,
    'Corrective Osteotomy Ankle':15,'Bone Models - Ankle':15,'Ankle Fusion':15,'Antibiotic Spacer':15,
    'Hemiarthroplasty Ankle':15,'Hemitalus':15,'Unknown - Foot/Ankle':15,
    'Total Ankle Replacement':15,'Total Talus and Other Arthroplasty':15,'PSR':15,
    'Bone Models - Arm':7,'Prosthetic - Arm':7,'Segmental Defect - Arm':7,'Corrective Osteotomy - Arm':7,
    'Prosthetic - Leg':7,'Corrective Osteotomy - Leg':7,'Segmental Defect - Leg':7,
    'Corrective Osteotomy':7,'Prosthetic':7,'Segmental Defect':7,
    'Hip Hemipelvis':7,'Hip Arthroplasty':7,'Hip Hemiarthroplasty':7,
}

SEG_TIMES = {
    'Corrective Osteotomy Hand/Wrist':(240,20),'Hemiarthroplasty Hand/Wrist':(240,20),'Carpal Replacement':(240,20),
    'Total Wrist Arthroplasty':(240,20),'Hand/Wrist Fusion':(240,20),'Total Elbow Arthroplasty':(240,20),
    'Hemiarthroplasty Elbow':(240,20),'Elbow Fusion':(300,20),'Clavicle':(120,20),
    'Hemiarthroplasty Shoulder':(150,30),'Acromion':(150,30),'Anatomic Shoulder Arthroplasty':(150,30),
    'Reverse Total Shoulder Arthroplasty':(150,30),'Reverse Shoulder Arthroplasty - Glenosphere Only':(150,30),
    'Reverse Shoulder Arthroplasty - Glenoid Baseplate':(150,30),'Reverse Shoulder Arthroplasty - Proximal Humerus':(150,30),
    'Shoulder':(150,30),'Hemiarthroplasty Knee':(120,20),'Total Knee Arthroplasty':(120,20),'TKA':(120,20),
    'Mandible':(200,20),'Maxilla':(200,20),'General Reconstruction':(200,20),
    'Corrective Osteotomy Foot':(200,20),'Bone Models - Foot':(200,20),'Midfoot':(200,20),'MPJ/MTP':(200,20),
    'Hindfoot Fusion':(200,20),'Corrective Osteotomy Ankle':(200,20),'Bone Models - Ankle':(200,20),
    'Ankle Fusion':(200,20),'Antibiotic Spacer':(200,20),'Hemiarthroplasty Ankle':(200,20),
    'Hemitalus':(200,20),'Unknown - Foot/Ankle':(200,20),'Total Ankle Replacement':(150,30),
    'Total Talus and Other Arthroplasty':(150,20),'PSR':(150,30),
    'Bone Models - Arm':(200,20),'Prosthetic - Arm':(200,20),'Segmental Defect - Arm':(200,20),
    'Corrective Osteotomy - Arm':(200,20),'Prosthetic - Leg':(120,20),'Corrective Osteotomy - Leg':(120,20),
    'Segmental Defect - Leg':(120,20),'Corrective Osteotomy':(120,20),'Prosthetic':(120,20),
    'Segmental Defect':(120,20),'Hip Hemipelvis':(120,20),'Hip Arthroplasty':(120,20),'Hip Hemiarthroplasty':(120,20),
}

STEP_TIMES = {
    'Planning':     {'TAR':(150,60), 'rTSA':(330,60)},
    'Design':       {'TAR':(150,60), 'rTSA':(240,60)},
    'Jigs Design':  {'rTSA':(240,60)},  # Alias — same as Design for rTSA
    'Peer Review':  {'TAR':(180,60)},   # Worker=180 min, Reviewer=60 min (from PSR pbit)
    'Proposed Surgical Plan': {'TAR':(90,30), 'rTSA':(120,30)},
    'Shipping':     {'TAR':(180,60)},  # Bone Model — TAR only
}

MINUTES_PER_DAY = 480

def get_product_group(product):
    """Map a case_category_name to a product group for design time lookup.
    (Only used by the fallback path now — see get_design_time().)"""
    if product in TAR_PRODUCTS:
        return 'TAR'
    if product in RTSA_PRODUCTS:
        return 'rTSA'
    return None

DATABASE_INDEX_DESIGN_TIMES_GID = "1507030841"

_design_times_cache = {"data": None, "fetched_at": None}

_DESIGN_TIMES_CACHE_TTL_SECONDS = 600  # 10 minutes

def get_design_times_live(force_refresh=False):
    """Fetches the live 'Design Times' tab and returns
    {(product, step_name): (design_min, review_min_or_None)}.
    Only System == 'BigQuery' rows are used here — Camstar design times (once
    added to this same tab) are a separate lookup path, not wired up yet.
    Cached in memory for _DESIGN_TIMES_CACHE_TTL_SECONDS."""
    now = datetime.datetime.utcnow()
    cached = _design_times_cache["data"]
    fetched_at = _design_times_cache["fetched_at"]
    if (not force_refresh and cached is not None and fetched_at is not None
            and (now - fetched_at).total_seconds() < _DESIGN_TIMES_CACHE_TTL_SECONDS):
        return cached

    csv_url = (f"https://docs.google.com/spreadsheets/d/{DATABASE_INDEX_SHEET_ID}"
               f"/export?format=csv&gid={DATABASE_INDEX_DESIGN_TIMES_GID}")
    try:
        df = pd.read_csv(csv_url)
    except Exception as e:
        logger.error(f"Failed to fetch Design Times sheet: {e}")
        return cached  # None triggers the hardcoded fallback in get_design_time()

    df.columns = [str(c).strip() for c in df.columns]
    result = {}
    for _, row in df.iterrows():
        system = str(row.get("System", "")).strip()
        if system != "BigQuery":
            continue
        product = str(row.get("Product (case_category_name)", "")).strip()
        step = str(row.get("Process/Step", "")).strip()
        if not product or not step or product.lower() == 'nan' or step.lower() == 'nan':
            continue
        design_val = row.get("Design", None)
        review_val = row.get("Review", None)
        design_min = float(design_val) if pd.notna(design_val) else None
        review_min = float(review_val) if pd.notna(review_val) else None
        result[(product, step)] = (design_min, review_min)

    _design_times_cache["data"] = result
    _design_times_cache["fetched_at"] = now
    return result

@app.route("/analytics/design-times", methods=["GET"])
def design_times_debug():
    """Debug/inspection endpoint — shows the live-loaded design times."""
    try:
        force_refresh = request.args.get("refresh", "").lower() == "true"
        live = get_design_times_live(force_refresh=force_refresh)
        if live is None:
            return jsonify({"error": "Live sheet unavailable, using hardcoded fallback", "live": False})
        return jsonify({"live": True, "count": len(live),
                         "data": {f"{p} | {s}": {"design": d, "review": r} for (p, s), (d, r) in live.items()}})
    except Exception as e:
        return handle_error(request.endpoint, e)

def get_design_time(step_name, user_type, product, multiplier=1):
    """Return standard design time in minutes for a step+role+product combo, or None.
    `multiplier` (1, 2, or 4) is applied ONLY to Scan Assessment and Segmentation —
    both WORKER and REVIEWER minutes — per the bilateral/revision case-complexity rule.
    Planning/Design/PSP/Peer Review/Shipping are unaffected.

    Reads live from the Design Times sheet first; falls back to the hardcoded
    SCAN_TIMES/SEG_TIMES/STEP_TIMES dicts only if the live sheet is unreachable.
    """
    live = get_design_times_live()
    if live is not None:
        design_min, review_min = live.get((product, step_name), (None, None))
        base = design_min if user_type == 'WORKER' else review_min
        if base is None:
            return None
        if step_name in ('Scan Assessment', 'Segmentation'):
            return base * multiplier
        return base

    # ── Fallback path (live sheet unreachable) ──
    if step_name == 'Scan Assessment':
        base = SCAN_TIMES.get(product) if user_type == 'WORKER' else None
        return base * multiplier if base is not None else None
    if step_name == 'Segmentation':
        times = SEG_TIMES.get(product)
        if times:
            base = times[0] if user_type == 'WORKER' else times[1]
            return base * multiplier
        return None
    if step_name in STEP_TIMES:
        pg = get_product_group(product)
        if pg and pg in STEP_TIMES[step_name]:
            times = STEP_TIMES[step_name][pg]
            return times[0] if user_type == 'WORKER' else times[1]
    return None

def get_case_time_multiplier(laterality, preoperative_state, proposed_indication, design_notes):
    """Bilateral case -> 2x, revision case -> 2x, both -> 4x, neither -> 1x.
    Revision = preoperativeState == 'REVISION_OTHER_SYSTEM' OR the word "revision"
    appears in proposedIndication or designNotes (case-insensitive).
    Applies to all BigQuery case types (not Camstar — this data doesn't exist there).
    """
    is_bilateral = (laterality or '').strip().upper() == 'BILATERAL'
    is_revision = (
        (preoperative_state or '') == 'REVISION_OTHER_SYSTEM'
        or 'revision' in (proposed_indication or '').lower()
        or 'revision' in (design_notes or '').lower()
    )
    m = 1
    if is_bilateral:
        m *= 2
    if is_revision:
        m *= 2
    return m

PRODUCT_STEPS = {
    'Reverse Total Shoulder Arthroplasty': [
        'Scan Assessment', 'Segmentation', 'Design Call Prep', 'Planning',
        'Jigs Design', 'Design', 'Proposed Surgical Plan', 'Peer Review'
    ],
    'Total Ankle Replacement': [
        'Scan Assessment', 'Segmentation', 'Planning',
        'Design', 'Proposed Surgical Plan', 'Peer Review'
    ],
    'DEFAULT': [
        'Scan Assessment', 'Segmentation', 'Design Call Prep', 'Planning',
        'Design', 'Proposed Surgical Plan', 'Peer Review'
    ],
}

ALL_STEPS_ORDER = [
    'Scan Assessment', 'Segmentation', 'Design Call Prep', 'Planning',
    'Jigs Design', 'Design', 'Proposed Surgical Plan', 'Peer Review'
]

def resolve_step_list(args):
    """Returns the ordered list of steps to use for the selected product/product_group.
    For mixed selections: union of all relevant products' step lists in canonical order.
    Falls back to DEFAULT if no specific product list configured.
    """
    product = args.get('product', '').strip()
    product_group = args.get('product_group', '').strip()

    selected_products = []
    if product:
        selected_products = [p.strip() for p in product.split(',') if p.strip()]
    elif product_group:
        for g in [g.strip() for g in product_group.split(',')]:
            selected_products.extend(PRODUCT_GROUPS.get(g, []))

    if not selected_products:
        return PRODUCT_STEPS['DEFAULT']

    # Union of step lists across all selected products
    step_set = set()
    for p in selected_products:
        step_set.update(PRODUCT_STEPS.get(p, PRODUCT_STEPS['DEFAULT']))
    # Preserve canonical order
    return [s for s in ALL_STEPS_ORDER if s in step_set]

METRIC_MAP = {
    'total_lt':   ("TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0)", "Total LT", "s.ship_wrk_comp_date"),
    'seg_lt':     ("TIMESTAMP_DIFF(sd.seg_review_date, s.first_scan_upload_date, DAY)", "Segmentation LT", "sd.seg_review_date"),
    'surgeon_lt': ("TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY)", "Surgeon Approval LT", "sd.surgeon_approval_date"),
    'digital_lt': ("GREATEST(0, LEAST(TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0) - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)), COALESCE(TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0), TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0))))", "Digital Production LT", "sd.first_psp_review_date"),
    'volume':     ("COUNT(DISTINCT f.id)", "Volume", "sd.first_psp_review_date"),
}

def on_hold_cte():
    return f"""
    on_hold_time AS (
        SELECT refId as caseId, SUM(hold_days) as total_hold_days
        FROM (
            SELECT refId,
                CASE
                    WHEN type = 'r3idCaseRemovedFromOnHold'
                        THEN TIMESTAMP_DIFF(CAST(timestamp AS DATETIME), CAST(LAG(timestamp) OVER (PARTITION BY refId ORDER BY timestamp) AS DATETIME), DAY)
                    WHEN type = 'r3idCasePutOnHold'
                        AND LEAD(type) OVER (PARTITION BY refId ORDER BY timestamp) IS NULL
                        THEN TIMESTAMP_DIFF(CAST(CURRENT_TIMESTAMP() AS DATETIME), CAST(timestamp AS DATETIME), DAY)
                    ELSE 0
                END as hold_days
            FROM {tbl('vw_event_log_rank')}
            WHERE type IN ('r3idCasePutOnHold','r3idCaseRemovedFromOnHold')
        )
        GROUP BY refId
    )"""

def on_hold_user_cte():
    return f"""
    on_hold_user AS (
        SELECT refId as caseId, userId as put_on_hold_by, timestamp as put_on_hold_at
        FROM (
            SELECT refId, userId, timestamp,
                ROW_NUMBER() OVER (PARTITION BY refId ORDER BY timestamp DESC) as rn
            FROM {tbl('vw_event_log_rank')}
            WHERE type = 'r3idCasePutOnHold'
        )
        WHERE rn = 1
    )"""

def signoff_cte():
    return f"""
    signoff_dates AS (
        SELECT
            w.caseId,
            MIN(CASE WHEN wm.name = 'Segmentation' AND w.workModuleUserType = 'REVIEWER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as seg_review_date,
            MIN(CASE WHEN wm.name = 'Design Call Prep' AND w.workModuleUserType = 'REVIEWER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as design_call_prep_date,
            MAX(CASE WHEN wm.name = 'Proposed Surgical Plan' AND w.workModuleUserType = 'APPROVER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as surgeon_approval_date,
            MAX(CASE WHEN wm.name = 'Proposed Surgical Plan' AND w.workModuleUserType = 'REVIEWER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as first_psp_review_date,
            MIN(CASE WHEN wm.name = 'Peer Review' AND w.workModuleUserType = 'REVIEWER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as peer_review_date,
            -- True if the most recent PSP APPROVER signoff is a REJECT (surgeon rejected, case sent back for rework).
            -- When true the case should NOT sit in surgeon_approval — it belongs back in psp_design.
            MAX(CASE WHEN wm.name = 'Proposed Surgical Plan' AND w.workModuleUserType = 'APPROVER' AND w.workModuleSignatureType = 'REJECT' THEN w.createdAt END) > MAX(CASE WHEN wm.name = 'Proposed Surgical Plan' AND w.workModuleUserType = 'APPROVER' AND w.workModuleSignatureType = 'ACCEPT' THEN w.createdAt END) as surgeon_last_action_reject
        FROM {tbl('WorkModuleSignoff')} w
        JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
        JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
        WHERE w.deleted = false
        GROUP BY w.caseId
    )"""

def case_select_fields(include_hold_user=False):
    hold_cols = ", hu.put_on_hold_by, hu.put_on_hold_at" if include_hold_user else ""
    return f"""
        f.id, CONCAT(f.count, '_', f.alias) as alias, f.alias as alias_short, f.count as case_number, f.case_category_name, f.phase,
        f.phy_nameFirst, f.phy_nameLast, f.fac_name, f.fac_state,
        f.laterality, f.onHold, f.createdAt,
        s.first_scan_upload_date, s.ship_wrk_comp_date,
        sd.seg_review_date, sd.first_psp_review_date, sd.surgeon_approval_date,
        COALESCE(oh.total_hold_days, 0) as onhold_days{hold_cols},
        CASE WHEN s.ship_wrk_comp_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
            ELSE NULL END as total_lt,
        CASE WHEN sd.seg_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.seg_review_date, s.first_scan_upload_date, DAY)
            ELSE NULL END as seg_lt,
        CASE WHEN sd.surgeon_approval_date IS NOT NULL AND sd.first_psp_review_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY)
            ELSE NULL END as surgeon_lt,
        CASE WHEN sd.peer_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN GREATEST(0, LEAST(
                TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY)
                    - COALESCE(oh.total_hold_days, 0)
                    - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)),
                COALESCE(
                    TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0),
                    TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
                )
            ))
            ELSE NULL END as digital_lt,
        f.onHoldComment as oh_note,
        cn.last_case_note, cn.last_case_note_author, cn.last_case_note_date,
        cn.last_internal_case_note, cn.last_internal_case_note_author, cn.last_internal_case_note_date,
        cn.last_design_case_note, cn.last_design_case_note_author, cn.last_design_case_note_date"""

def case_joins(include_hold_user=False):
    hold_join = f"\n        LEFT JOIN on_hold_user hu ON f.id = hu.caseId" if include_hold_user else ""
    return f"""
        FROM {tbl('vw_fact_case')} f
        LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON f.id = s.caseId
        LEFT JOIN signoff_dates sd ON f.id = sd.caseId
        LEFT JOIN on_hold_time oh ON f.id = oh.caseId
        LEFT JOIN {tbl('vw_lkup_last_case_notes')} cn ON f.id = cn.caseid{hold_join}"""

def fetch_cases_by_ids_query(case_ids, include_hold_user=False):
    """Return a query that fetches full case details for the given IDs.
    Uses raw Case table LEFT JOIN vw_fact_case so COMPLETED phase cases are included."""
    hold_join = f"\n        LEFT JOIN on_hold_user hu ON f.id = hu.caseId" if include_hold_user else ""
    hold_cols = ", hu.put_on_hold_by, hu.put_on_hold_at" if include_hold_user else ""
    ids_str = "','".join(str(i) for i in case_ids)
    return f"""
    WITH {signoff_cte()}, {on_hold_cte()}{',' + on_hold_user_cte() if include_hold_user else ''}
    SELECT
        base.id,
        COALESCE(CONCAT(f.count, '_', f.alias), CAST(base.id AS STRING)) as alias,
        COALESCE(f.alias, CAST(base.id AS STRING)) as alias_short,
        f.count as case_number,
        COALESCE(f.case_category_name, cc.name) as case_category_name,
        COALESCE(f.phase, base.phase) as phase,
        f.phy_nameFirst, f.phy_nameLast, f.fac_name, f.fac_state,
        COALESCE(f.laterality, base.laterality) as laterality,
        COALESCE(f.onHold, base.onHold) as onHold,
        COALESCE(f.createdAt, base.createdAt) as createdAt,
        s.first_scan_upload_date, s.ship_wrk_comp_date,
        sd.seg_review_date, sd.first_psp_review_date, sd.surgeon_approval_date,
        COALESCE(oh.total_hold_days, 0) as onhold_days{hold_cols},
        CASE WHEN s.ship_wrk_comp_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
            ELSE NULL END as total_lt,
        CASE WHEN sd.seg_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.seg_review_date, s.first_scan_upload_date, DAY)
            ELSE NULL END as seg_lt,
        CASE WHEN sd.surgeon_approval_date IS NOT NULL AND sd.first_psp_review_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY)
            ELSE NULL END as surgeon_lt,
        CASE WHEN sd.peer_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN GREATEST(0, LEAST(
                TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY)
                    - COALESCE(oh.total_hold_days, 0)
                    - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)),
                COALESCE(
                    TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0),
                    TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
                )
            ))
            ELSE NULL END as digital_lt,
        f.onHoldComment as oh_note,
        cn.last_case_note, cn.last_case_note_author, cn.last_case_note_date,
        cn.last_internal_case_note, cn.last_internal_case_note_author, cn.last_internal_case_note_date,
        cn.last_design_case_note, cn.last_design_case_note_author, cn.last_design_case_note_date
    FROM {tbl('Case')} base
    JOIN {tbl('CaseCategory')} cc ON base.caseCategoryId = cc.id
    LEFT JOIN {tbl('vw_fact_case')} f ON base.id = f.id
    LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON base.id = s.caseId
    LEFT JOIN signoff_dates sd ON base.id = sd.caseId
    LEFT JOIN on_hold_time oh ON base.id = oh.caseId
    LEFT JOIN {tbl('vw_lkup_last_case_notes')} cn ON base.id = cn.caseid{hold_join}
    WHERE base.id IN ('{ids_str}')
    ORDER BY base.createdAt DESC
    LIMIT 500
    """

def volume_case_joins():
    """For volume queries: bypass vw_fact_case (which excludes COMPLETED cases)
    and query raw Case table so completed cases are counted too."""
    return f"""
        FROM {tbl('Case')} f
        LEFT JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
        LEFT JOIN {tbl('CaseType')} ct ON f.caseTypeId = ct.id
        LEFT JOIN signoff_dates sd ON f.id = sd.caseId
        LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON f.id = s.caseId"""

def volume_where_clause(args):
    """Build WHERE clause for volume using raw Case table columns.
    No phase filter — includes COMPLETED cases for accurate volume counting.
    Includes cancelled cases — PSP completion is the milestone; post-PSP cancellation should not affect volume."""
    conditions = ["f.deleted = false"]
    # Product filter — use cc.name (CaseCategory.name) instead of f.case_category_name
    product = args.get('product', '').strip()
    product_group = args.get('product_group', '').strip()
    if product:
        products = [p.strip() for p in product.split(',')]
        joined = "','".join(products)
        conditions.append(f"cc.name IN ('{joined}')" if len(products) > 1 else f"cc.name = '{products[0]}'")
    elif product_group:
        all_prods = []
        for g in [g.strip() for g in product_group.split(',')]:
            all_prods.extend(PRODUCT_GROUPS.get(g, []))
        if all_prods:
            joined = "','".join(all_prods)
            conditions.append(f"cc.name IN ('{joined}')")
    # No product selected — no default filter, returns all products
    # Surgeon filter
    surgeon = args.get('surgeon', '').strip()
    if surgeon:
        conditions.append(f"LOWER(f.alias) LIKE LOWER('%{sanitize(surgeon)}%')")
    # Case type filter
    case_type = args.get('case_type', '').strip()
    if case_type:
        types = [t.strip() for t in case_type.split(',')]
        joined = "','".join(types)
        conditions.append(f"ct.name IN ('{joined}')" if len(types) > 1 else f"ct.name = '{types[0]}'")
    # Step + User filter via WorkModuleSignoff subquery (mirrors build_where_clause logic
    # so the User dropdown and the Designed/Reviewed toggle apply to Volume too).
    step_val = args.get('step', '').strip()
    user_val = args.get('step_user', '').strip()
    if step_val or user_val:
        sub = ["w_flt.deleted = false", "w_flt.workModuleSignatureType = 'ACCEPT'"]
        if step_val:
            steps = [s.strip() for s in step_val.split(',')]
            if len(steps) == 1:
                sub.append(f"wm_flt.name = '{steps[0]}'")
            else:
                joined = "','".join(steps)
                sub.append(f"wm_flt.name IN ('{joined}')")
        if user_val:
            users = [u.strip() for u in user_val.split(',')]
            if len(users) == 1:
                sub.append(f"w_flt.signature = '{users[0]}'")
            else:
                joined = "','".join(users)
                sub.append(f"w_flt.signature IN ('{joined}')")
            user_role = args.get('user_role', '').strip().lower()
            if user_role == 'worker':
                sub.append("w_flt.workModuleUserType = 'WORKER'")
            elif user_role == 'reviewer':
                sub.append("w_flt.workModuleUserType IN ('REVIEWER', 'APPROVER')")
        sub_where = " AND ".join(sub)
        conditions.append(f"""f.id IN (
            SELECT DISTINCT w_flt.caseId
            FROM `{PROJECT}.{DATASET}.WorkModuleSignoff` w_flt
            LEFT JOIN `{PROJECT}.{DATASET}.WorkModuleInstance` wi_flt ON w_flt.workModuleInstanceId = wi_flt.id
            LEFT JOIN `{PROJECT}.{DATASET}.WorkModule` wm_flt ON wi_flt.workModuleId = wm_flt.id
            WHERE {sub_where}
        )""")
    return " AND ".join(conditions)

def format_cases(df):
    return df.astype(str).replace('nan','').replace('NaT','').replace('None','').replace('<NA>','').to_dict(orient='records')

def shorten_product(s):
    return (str(s)
        .replace('Reverse Total Shoulder Arthroplasty','RTSA')
        .replace('Total Ankle Replacement','TAR')
        .replace('Total Knee Arthroplasty','TKA')
        .replace('Total Talus and Other Arthroplasty','Total Talus')
        .replace('Hip Arthroplasty','Hip'))

def build_where_clause(args, alias='f'):
    conditions = [f"{alias}.deleted = false", f"{alias}.Case_In_Take IN ('Case Intake','Restor3d MedEd','Cody Case Intake')"]
    if args.get('product'):
        products = [p.strip() for p in args.get('product').split(',')]
        joined = "','".join(products)
        if len(products) == 1:
            conditions.append(f"{alias}.case_category_name = '{products[0]}'")
        else:
            conditions.append(f"{alias}.case_category_name IN ('{joined}')")
    if args.get('product_group'):
        all_products = []
        for g in [g.strip() for g in args.get('product_group').split(',')]:
            all_products.extend(PRODUCT_GROUPS.get(g, []))
        if all_products:
            joined = "','".join(all_products)
            conditions.append(f"{alias}.case_category_name IN ('{joined}')")
    if args.get('joint'):
        joints = [j.strip() for j in args.get('joint').split(',')]
        joined = "','".join(joints)
        conditions.append(f"{alias}.anatomy_name IN ('{joined}')")
    if args.get('regulatory'):
        regs = [r.strip() for r in args.get('regulatory').split(',')]
        joined = "','".join(CLEARED_PRODUCTS)
        if 'Cleared' in regs and 'Custom' not in regs:
            conditions.append(f"{alias}.case_category_name IN ('{joined}')")
        elif 'Custom' in regs and 'Cleared' not in regs:
            conditions.append(f"{alias}.case_category_name NOT IN ('{joined}')")
    if args.get('surgeon'):
        conditions.append(f"LOWER({alias}.phy_nameLast) LIKE LOWER('%{sanitize(args.get('surgeon'))}%')")
    if args.get('laterality'):
        lats = [l.strip() for l in args.get('laterality').split(',')]
        joined = "','".join(lats)
        conditions.append(f"{alias}.laterality IN ('{joined}')")
    if args.get('facility'):
        conditions.append(f"LOWER({alias}.fac_name) LIKE LOWER('%{sanitize(args.get('facility'))}%')")
    if args.get('state'):
        conditions.append(f"UPPER({alias}.fac_state) = UPPER('{sanitize(args.get('state'))}')")
    for flag, field in {'surgery_scheduled':'isSurgeryScheduled','oncology':'isOncologyCase','trauma':'isTraumaCase','pediatric':'isPediatricCase','preop':'preoperativePlanningOnly','research':'researchCase'}.items():
        if args.get(flag) == 'true':
            conditions.append(f"{alias}.{field} = true")
    # Case type filter
    case_type = args.get('case_type', '').strip()
    if case_type:
        types = [t.strip() for t in case_type.split(',')]
        if len(types) == 1:
            conditions.append(f"{alias}.case_type_name = '{types[0]}'")
        else:
            joined = "','".join(types)
            conditions.append(f"{alias}.case_type_name IN ('{joined}')")
    # Shoulder/ankle case-type selection now goes through the generic case_type filter above —
    # no per-product grouping dicts needed.
    # Step + User filter via WorkModuleSignoff subquery
    step_val = args.get('step', '').strip()
    user_val = args.get('step_user', '').strip()
    if step_val or user_val:
        sub = ["w_flt.deleted = false", "w_flt.workModuleSignatureType = 'ACCEPT'"]
        if step_val:
            steps = [s.strip() for s in step_val.split(',')]
            if len(steps) == 1:
                sub.append(f"wm_flt.name = '{steps[0]}'")
            else:
                joined = "','".join(steps)
                sub.append(f"wm_flt.name IN ('{joined}')")
        if user_val:
            users = [u.strip() for u in user_val.split(',')]
            if len(users) == 1:
                sub.append(f"w_flt.signature = '{users[0]}'")
            else:
                joined = "','".join(users)
                sub.append(f"w_flt.signature IN ('{joined}')")
        sub_where = " AND ".join(sub)
        conditions.append(f"""{alias}.id IN (
            SELECT DISTINCT w_flt.caseId
            FROM `{PROJECT}.{DATASET}.WorkModuleSignoff` w_flt
            LEFT JOIN `{PROJECT}.{DATASET}.WorkModuleInstance` wi_flt ON w_flt.workModuleInstanceId = wi_flt.id
            LEFT JOIN `{PROJECT}.{DATASET}.WorkModule` wm_flt ON wi_flt.workModuleId = wm_flt.id
            WHERE {sub_where}
        )""")
    return " AND ".join(conditions)

def sanitize(value: str) -> str:
    """Strip characters that could be used for SQL injection in LIKE/equality clauses."""
    return re.sub(r"['\";\\]", "", value).strip()

_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

def validate_date(value: str) -> str:
    """Return value if it matches YYYY-MM-DD, otherwise return empty string."""
    v = (value or '').strip()
    return v if _DATE_RE.match(v) else ''

def date_range_filter(date_field, date_from, date_to):
    date_from = validate_date(date_from)
    date_to   = validate_date(date_to)
    conds = [f"{date_field} IS NOT NULL"]
    if date_from:
        conds.append(f"CAST({date_field} AS DATETIME) >= CAST('{date_from}' AS DATETIME)")
    if date_to:
        conds.append(f"CAST({date_field} AS DATETIME) <= CAST('{date_to} 23:59:59' AS DATETIME)")
    return " AND ".join(conds)

def outlier_exclusion_sql(avg_digital_lt=None):
    parts = ["(s.ship_wrk_comp_date IS NULL OR TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0) <= 50)"]
    if avg_digital_lt and avg_digital_lt > 0:
        threshold = round(avg_digital_lt * 2, 1)
        parts.append(f"""(sd.peer_review_date IS NULL OR
            TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0)
            - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)) <= {threshold})""")
    return " AND ".join(parts)

_LOG = "`restor3d-data-warehouse.production3_logging_public.Log`"


@app.route("/config", methods=["GET"])
def config():
    try:
        # Get live product counts from BigQuery (all time)
        # Pull products directly from live BigQuery — no static list, no duplicates
        df = client.query(f"""
            SELECT case_category_name, COUNT(*) as case_count
            FROM {tbl('vw_fact_case')}
            WHERE deleted = false AND case_category_name IS NOT NULL
            GROUP BY case_category_name ORDER BY case_count DESC
        """).to_dataframe()
        live_products = df['case_category_name'].tolist()  # already sorted by count

        # Build product groups — only include products that actually exist in BigQuery
        live_set = set(live_products)
        groups = {}
        for g, prods in PRODUCT_GROUPS.items():
            matched = [p for p in prods if p in live_set]
            if matched:
                groups[g] = matched

        return jsonify({
            "products": live_products,
            "product_groups": groups,
            "cleared": [p for p in live_products if p in CLEARED_PRODUCTS],
            "custom": [p for p in live_products if p not in CLEARED_PRODUCTS],
            "live_products": live_products,
        })
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/case-types", methods=["GET"])
def case_types():
    """Return distinct case_type_name values for the case type filter dropdown."""
    try:
        args = request.args
        product = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()

        conditions = ["f.deleted = false", "f.case_type_name IS NOT NULL"]
        if product:
            prods = [p.strip() for p in product.split(',')]
            joined = "','".join(prods)
            conditions.append(f"f.case_category_name IN ('{joined}')")
        elif product_group:
            all_prods = []
            for g in [g.strip() for g in product_group.split(',')]:
                all_prods.extend(PRODUCT_GROUPS.get(g, []))
            if all_prods:
                joined = "','".join(all_prods)
                conditions.append(f"f.case_category_name IN ('{joined}')")

        where_sql = " AND ".join(conditions)
        query = f"""
            SELECT f.case_type_name, COUNT(*) as case_count
            FROM {tbl('vw_fact_case')} f
            WHERE {where_sql}
            GROUP BY f.case_type_name
            ORDER BY case_count DESC
        """
        rows = client.query(query).to_dataframe().astype(str).to_dict(orient="records")
        return jsonify({"case_types": rows})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/users-by-step", methods=["GET"])
def users_by_step():
    """Return distinct step names and/or userIds for populating filter dropdowns."""
    try:
        args = request.args
        step = args.get('step', '').strip()
        mode = args.get('mode', '').strip()  # 'users_only' to force user list
        where_parts = ["w.deleted = false", "w.workModuleSignatureType = 'ACCEPT'"]
        # Apply product filter via case join
        product = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()
        case_join = ""
        if product or product_group:
            case_join = f"JOIN {tbl('vw_fact_case')} f ON w.caseId = f.id"
            if product:
                prods = [p.strip() for p in product.split(',')]
                joined = "','".join(prods)
                where_parts.append(f"f.case_category_name IN (\'{joined}\')")
            if product_group:
                all_prods = []
                for g in [g.strip() for g in product_group.split(',')]:
                    all_prods.extend(PRODUCT_GROUPS.get(g, []))
                if all_prods:
                    joined = "','".join(all_prods)
                    where_parts.append(f"f.case_category_name IN (\'{joined}\')")

        if step:
            steps = [s.strip() for s in step.split(',')]
            if len(steps) == 1:
                where_parts.append(f"wm.name = '{steps[0]}'")
            else:
                joined = "','".join(steps)
                where_parts.append(f"wm.name IN ('{joined}')")

        if step or mode == 'users_only':
            # Return users (optionally filtered by step and/or product)
            where_sql = " AND ".join(where_parts)
            query = f"""
                SELECT w.signature as userId, COUNT(DISTINCT w.caseId) as case_count
                FROM {tbl('WorkModuleSignoff')} w
                LEFT JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
                LEFT JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
                {case_join}
                WHERE {where_sql} AND w.signature IS NOT NULL AND w.signature != ''
                GROUP BY w.signature ORDER BY case_count DESC
            """
            rows = client.query(query).to_dataframe().astype(str).to_dict(orient="records")
            return jsonify({"mode": "users", "step": step, "users": rows})
        else:
            # Return step names
            where_sql = " AND ".join(where_parts)
            query = f"""
                SELECT wm.name as step_name, COUNT(DISTINCT w.caseId) as case_count
                FROM {tbl('WorkModuleSignoff')} w
                LEFT JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
                LEFT JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
                {case_join}
                WHERE {where_sql} AND wm.name IS NOT NULL
                GROUP BY wm.name ORDER BY case_count DESC
            """
            rows = client.query(query).to_dataframe().astype(str).to_dict(orient="records")
            return jsonify({"mode": "steps", "steps": rows})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/cases", methods=["GET"])
def analytics_cases():
    try:
        args = request.args
        where = build_where_clause(args)
        if args.get('milestone_field') and (args.get('date_from') or args.get('date_to')):
            mf = args.get('milestone_field')
            df_filter = date_range_filter(mf, args.get('date_from',''), args.get('date_to',''))
            query = f"WITH {signoff_cte()}, {on_hold_cte()} SELECT {case_select_fields()} {case_joins()} WHERE {where} AND {df_filter} ORDER BY f.createdAt DESC LIMIT 500"
        else:
            if args.get('date_from'):
                where += f" AND f.createdAt >= '{validate_date(args.get('date_from'))}'"
            if args.get('date_to'):
                where += f" AND f.createdAt <= '{validate_date(args.get('date_to'))} 23:59:59'"
            query = f"WITH {signoff_cte()}, {on_hold_cte()} SELECT {case_select_fields()} {case_joins()} WHERE {where} ORDER BY f.createdAt DESC LIMIT 500"
        df = client.query(query).to_dataframe()
        return jsonify({"cases": format_cases(df), "count": len(df)})
    except Exception as e:
        return handle_error(request.endpoint, e)


@app.route("/analytics/report/wip-trend", methods=["GET"])
def report_wip_trend():
    """Digital Production WIP trend — cases in pipeline at end of each period.
    Matches Power BI WIP Daily logic:
      Start = Scan Assessment WORKER ACCEPT (entered digital production)
      End   = Peer Review REVIEWER ACCEPT (left digital production)
      WIP at period end = cases where start <= period_end AND (end IS NULL OR end > period_end)
    Excludes cases sitting in Surgeon Approval as of period end (PSP-reviewed, not yet
    surgeon-approved, not sent back for rework, not yet at peer review) — Surgeon Approval
    is tracked as its own separate stage (see analytics_wip), not part of Digital Production WIP.
    NOTE: this only covers the "post-PSP-review" surgeon-approval path, using timestamped
    signoff events, so it's accurate for every historical period. analytics_wip's surgeon_approval
    bucket also has a rarer pre-PSP path (Design Call Prep + current work_queue='PLANNING_REVIEW')
    that depends on a live status field with no historical timestamp — it can't be reconstructed
    for past periods and is intentionally not excluded here.
    Uses raw Case table (not vw_fact_case) so completed cases are included.
    On-hold excluded via vw_event_log_rank snapshot per case.
    Config: DataGenie_Report_Config.yaml → wip_trend
    """
    try:
        args        = request.args
        granularity = args.get('granularity', 'weekly')
        date_from   = args.get('date_from', '')
        date_to     = args.get('date_to', '')
        product     = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()

        trunc = 'WEEK' if granularity == 'weekly' else 'MONTH'

        if not date_from:
            date_from = (datetime.datetime.utcnow() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
        if not date_to:
            date_to = datetime.datetime.utcnow().strftime('%Y-%m-%d')

        validate_date(date_from)
        validate_date(date_to)

        # Product filter — defined early so fetch_period branch can use it too
        prod_join = ""
        prod_cond = ""
        if product:
            prods = [p.strip() for p in product.split(',')]
            joined = "','".join(prods)
            prod_join = f"JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id"
            prod_cond = f"AND cc.name IN ('{joined}')"
        elif product_group:
            all_prods = []
            for g in [g.strip() for g in product_group.split(',')]:
                all_prods.extend(PRODUCT_GROUPS.get(g, []))
            if all_prods:
                joined = "','".join(all_prods)
                prod_join = f"JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id"
                prod_cond = f"AND cc.name IN ('{joined}')"

        # ── Fetch cases branch: return cases that were in WIP at a specific snapshot ──
        # Used when user clicks a bar in the WIP trend chart.
        fetch_period = args.get('fetch_period', '').strip()
        case_type = args.get('case_type', '').strip()

        # Product + case_type filter — defined early so fetch_period branch can use it
        vw_prod_cond = ""
        if product:
            prods = [p.strip() for p in product.split(',')]
            joined = "','".join(prods)
            vw_prod_cond = f"AND f.case_category_name IN ('{joined}')"
        elif product_group:
            all_prods = []
            for g in [g.strip() for g in product_group.split(',')]:
                all_prods.extend(PRODUCT_GROUPS.get(g, []))
            if all_prods:
                joined = "','".join(all_prods)
                vw_prod_cond = f"AND f.case_category_name IN ('{joined}')"
        if case_type:
            types = [t.strip() for t in case_type.split(',')]
            joined_ct = "','".join(types)
            vw_prod_cond += f" AND f.case_type_name IN ('{joined_ct}')"

        if fetch_period:
            validate_date(fetch_period)
            snapshot_clause = f"DATE '{fetch_period}'"
            fetch_query = f"""
            WITH case_pipeline AS (
                SELECT
                    w.caseId,
                    MIN(CASE WHEN wm.name = 'Scan Assessment'
                                 AND w.workModuleUserType = 'WORKER'
                                 AND w.workModuleSignatureType = 'ACCEPT'
                        THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) AS scan_date,
                    MIN(CASE WHEN wm.name = 'Peer Review'
                                 AND w.workModuleUserType = 'REVIEWER'
                                 AND w.workModuleSignatureType = 'ACCEPT'
                        THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) AS peer_review_date
                FROM {tbl('WorkModuleSignoff')} w
                JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
                JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
                WHERE w.deleted = false
                  AND wm.name IN ('Scan Assessment', 'Peer Review')
                GROUP BY w.caseId
            ),
            case_pipeline_with_exit AS (
                SELECT
                    cp.caseId, cp.scan_date,
                    CASE
                        WHEN cp.peer_review_date IS NOT NULL AND s.ship_wrk_comp_date IS NOT NULL
                            THEN LEAST(cp.peer_review_date, DATE(CAST(s.ship_wrk_comp_date AS TIMESTAMP)))
                        WHEN cp.peer_review_date IS NOT NULL
                            THEN cp.peer_review_date
                        WHEN s.ship_wrk_comp_date IS NOT NULL
                            THEN DATE(CAST(s.ship_wrk_comp_date AS TIMESTAMP))
                        ELSE NULL
                    END AS exit_date
                FROM case_pipeline cp
                LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON cp.caseId = s.caseId
            ),
            {signoff_cte()},
            wip_at_snapshot AS (
                SELECT cp.caseId
                FROM case_pipeline_with_exit cp
                JOIN {tbl('vw_fact_case')} f ON cp.caseId = f.id
                LEFT JOIN signoff_dates sd ON cp.caseId = sd.caseId
                WHERE f.deleted = false
                  AND f.canceled = false
                  AND f.onHold = false
                  AND (LOWER(COALESCE(f.phy_nameFirst,'') || ' ' || COALESCE(f.phy_nameLast,'')) NOT LIKE '%marketing%')
                  AND cp.scan_date IS NOT NULL
                  AND cp.scan_date <= {snapshot_clause}
                  AND (cp.exit_date IS NULL OR cp.exit_date > {snapshot_clause})
                  -- Exclude cases currently sitting in Surgeon Approval (post-PSP-review, awaiting
                  -- surgeon signoff) as of this snapshot date. Mirrors analytics_wip's surgeon_approval
                  -- bucket's primary (timestamped) path — see report_wip_trend's periods query for notes.
                  AND NOT (
                        sd.first_psp_review_date IS NOT NULL AND sd.first_psp_review_date <= {snapshot_clause}
                        AND (sd.surgeon_approval_date IS NULL OR sd.surgeon_approval_date > {snapshot_clause})
                        AND COALESCE(sd.surgeon_last_action_reject, false) = false
                        AND (sd.peer_review_date IS NULL OR sd.peer_review_date > {snapshot_clause})
                  )
                  {vw_prod_cond}
            ),
            {on_hold_cte()}
            SELECT {case_select_fields()}
            {case_joins()}
            WHERE f.id IN (SELECT caseId FROM wip_at_snapshot)
            ORDER BY f.createdAt DESC LIMIT 500
            """
            df = client.query(fetch_query).to_dataframe()
            return jsonify({"cases": format_cases(df), "count": len(df), "snapshot": fetch_period})

        # trunc expression: use WEEK(MONDAY) for weekly so periods start on Monday
        trunc_expr = 'WEEK(MONDAY)' if granularity == 'weekly' else 'MONTH'
        period_add = 'INTERVAL 7 DAY' if granularity == 'weekly' else 'INTERVAL 1 MONTH'
        interval_unit = 'WEEK' if granularity == 'weekly' else 'MONTH'

        query = f"""
        WITH
        case_pipeline AS (
            SELECT
                w.caseId,
                MIN(CASE WHEN wm.name = 'Scan Assessment'
                             AND w.workModuleUserType = 'WORKER'
                             AND w.workModuleSignatureType = 'ACCEPT'
                    THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) AS scan_date,
                MIN(CASE WHEN wm.name = 'Peer Review'
                             AND w.workModuleUserType = 'REVIEWER'
                             AND w.workModuleSignatureType = 'ACCEPT'
                    THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) AS peer_review_date
            FROM {tbl('WorkModuleSignoff')} w
            JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
            JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
            WHERE w.deleted = false
              AND wm.name IN ('Scan Assessment', 'Peer Review')
            GROUP BY w.caseId
        ),
        -- Join ship_wrk_comp_date as an alternative exit signal (implicit-PSP rule).
        -- A case exits digital production WIP when EITHER peer review OR ship completes.
        -- This matches the effective_psp_date logic used across all other metrics.
        case_pipeline_with_exit AS (
            SELECT
                cp.caseId,
                cp.scan_date,
                -- Exit = earliest of peer_review_date or ship_wrk_comp_date
                CASE
                    WHEN cp.peer_review_date IS NOT NULL AND s.ship_wrk_comp_date IS NOT NULL
                        THEN LEAST(cp.peer_review_date, DATE(CAST(s.ship_wrk_comp_date AS TIMESTAMP)))
                    WHEN cp.peer_review_date IS NOT NULL
                        THEN cp.peer_review_date
                    WHEN s.ship_wrk_comp_date IS NOT NULL
                        THEN DATE(CAST(s.ship_wrk_comp_date AS TIMESTAMP))
                    ELSE NULL
                END AS exit_date
            FROM case_pipeline cp
            LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON cp.caseId = s.caseId
        ),
        -- Use vw_fact_case so we have phy_nameFirst/phy_nameLast for marketing filter.
        -- Excludes: on-hold cases, marketing surgeon cases, deleted/cancelled.
        -- Exit signal: LEAST(peer_review_date, ship_wrk_comp_date) — implicit-PSP rule.
        {signoff_cte()},
        valid_cases AS (
            SELECT
                cp.caseId, cp.scan_date, cp.exit_date,
                sd.first_psp_review_date, sd.surgeon_approval_date, sd.peer_review_date,
                COALESCE(sd.surgeon_last_action_reject, false) AS surgeon_last_action_reject
            FROM case_pipeline_with_exit cp
            JOIN {tbl('vw_fact_case')} f ON cp.caseId = f.id
            LEFT JOIN signoff_dates sd ON cp.caseId = sd.caseId
            WHERE f.deleted = false
              AND f.canceled = false
              AND f.onHold = false
              AND (LOWER(COALESCE(f.phy_nameFirst,'') || ' ' || COALESCE(f.phy_nameLast,'')) NOT LIKE '%marketing%')
              AND cp.scan_date IS NOT NULL
              {vw_prod_cond}
        ),
        periods AS (
            SELECT
                DATE_TRUNC(d, {trunc_expr}) AS period_start,
                DATE_ADD(DATE_TRUNC(d, {trunc_expr}), {period_add}) AS period_end
            FROM UNNEST(
                GENERATE_DATE_ARRAY(DATE('{date_from}'), DATE('{date_to}'), INTERVAL 1 {interval_unit})
            ) AS d
            GROUP BY 1, 2
        )
        SELECT
            p.period_start AS period,
            COUNT(DISTINCT CASE
                WHEN vc.scan_date <= p.period_end
                 AND (vc.exit_date IS NULL OR vc.exit_date > p.period_end)
                 -- Exclude cases sitting in Surgeon Approval as of this period end: PSP-reviewed,
                 -- not yet surgeon-approved, not sent back for rework, not yet at peer review.
                 AND NOT (
                       vc.first_psp_review_date IS NOT NULL AND vc.first_psp_review_date <= p.period_end
                       AND (vc.surgeon_approval_date IS NULL OR vc.surgeon_approval_date > p.period_end)
                       AND vc.surgeon_last_action_reject = false
                       AND (vc.peer_review_date IS NULL OR vc.peer_review_date > p.period_end)
                 )
                THEN vc.caseId END) AS wip_count
        FROM periods p
        LEFT JOIN valid_cases vc ON TRUE
        GROUP BY p.period_start
        ORDER BY p.period_start ASC
        """

        df = client.query(query).to_dataframe()
        if df.empty:
            return jsonify({"periods": [], "wip": [], "avg": 0, "granularity": granularity})

        df['period']    = df['period'].astype(str)
        df['wip_count'] = df['wip_count'].fillna(0).astype(int)

        periods  = df['period'].tolist()
        wip_vals = df['wip_count'].tolist()
        avg_wip  = round(sum(wip_vals) / len(wip_vals), 1) if wip_vals else 0

        return jsonify({
            "periods":     periods,
            "wip":         wip_vals,
            "avg":         avg_wip,
            "max":         max(wip_vals) if wip_vals else 0,
            "granularity": granularity
        })
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/report/process-counts", methods=["GET"])
def report_process_counts():
    """Distinct cases completed per step in the date range.

    Steps are determined dynamically from the selected product(s) via PRODUCT_STEPS.
    Implicit-completion rule: if a case has no signoff for step X but has a signoff
    for any later step in the workflow, X is treated as implicitly complete with
    completion date = MIN of all later step reviewer-accept dates.

    Rules:
    - Scan Assessment: WORKER ACCEPT (no reviewer exists for this step)
    - All other steps: REVIEWER ACCEPT only
    - One case counted once per step (COUNT DISTINCT caseId)
    - Implicit completion fills in missing intermediate signoffs from later steps
    Config: app.py -> PRODUCT_STEPS
    """
    try:
        args = request.args
        date_from = args.get('date_from', '')
        date_to   = args.get('date_to', '')
        product   = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()
        case_type = args.get('case_type', '').strip()
        surgeon   = args.get('surgeon', '').strip()

        # Resolve step list based on selected product(s)
        active_steps = resolve_step_list(args)
        # Split into worker vs reviewer steps
        WORKER_STEPS = ['Scan Assessment']
        worker_active   = [s for s in active_steps if s in WORKER_STEPS]
        reviewer_active = [s for s in active_steps if s not in WORKER_STEPS]

        # Include cancelled cases — step completion is the milestone
        case_conds = ["f.deleted = false"]
        if product:
            prods = [p.strip() for p in product.split(',')]
            joined = "','".join(prods)
            case_conds.append(f"cc.name IN ('{joined}')")
        elif product_group:
            all_prods = []
            for g in [g.strip() for g in product_group.split(',')]:
                all_prods.extend(PRODUCT_GROUPS.get(g, []))
            if all_prods:
                joined = "','".join(all_prods)
                case_conds.append(f"cc.name IN ('{joined}')")
        if case_type:
            types = [t.strip() for t in case_type.split(',')]
            joined = "','".join(types)
            case_conds.append(f"ct.name IN ('{joined}')" if len(types) > 1 else f"ct.name = '{types[0]}'")
        if surgeon:
            case_conds.append(f"LOWER(f.alias) LIKE LOWER('%{sanitize(surgeon)}%')")
        case_where = " AND ".join(case_conds)

        if date_from: validate_date(date_from)
        if date_to:   validate_date(date_to)

        reviewer_list = "','".join(reviewer_active)
        worker_list   = "','".join(worker_active) if worker_active else ''

        # Build per-step columns for the pivot (real reviewer dates per step)
        # We'll select MIN(createdAt) for each step into its own column,
        # then use COALESCE with MIN of later steps' dates for implicit completion.
        step_cols = []
        for s in active_steps:
            safe = s.lower().replace(' ', '_').replace('/', '_')
            if s == 'Scan Assessment':
                # WORKER signoff for scan assessment
                step_cols.append(
                    f"MIN(CASE WHEN wm.name = 'Scan Assessment' AND w.workModuleUserType = 'WORKER' "
                    f"AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) AS {safe}_date"
                )
            else:
                # For PSP, use MAX (last accept) to count reworked cases correctly
                agg = 'MAX' if s == 'Proposed Surgical Plan' else 'MIN'
                step_cols.append(
                    f"{agg}(CASE WHEN wm.name = '{s}' AND w.workModuleUserType = 'REVIEWER' "
                    f"AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) AS {safe}_date"
                )

        # Effective completion = COALESCE(real_date, MIN of all later step dates)
        # Build effective_X = LEAST of real X date and effective_X+1 (cascading from end)
        # Easiest: for step at index i, effective = COALESCE(real[i], real[i+1], ..., real[n])
        effective_exprs = []
        for i, s in enumerate(active_steps):
            safe_i = s.lower().replace(' ', '_').replace('/', '_')
            # COALESCE this step with all later steps
            chain = [f"{active_steps[j].lower().replace(' ', '_').replace('/', '_')}_date" for j in range(i, len(active_steps))]
            effective_exprs.append(f"COALESCE({', '.join(chain)}) AS {safe_i}_eff")

        step_cols_sql = ",\n            ".join(step_cols)
        effective_sql = ",\n            ".join(effective_exprs)

        # Build SELECT counts per step with date filter
        step_counts = []
        for s in active_steps:
            safe = s.lower().replace(' ', '_').replace('/', '_')
            cond_parts = [f"{safe}_eff IS NOT NULL"]
            if date_from: cond_parts.append(f"{safe}_eff >= '{date_from}'")
            if date_to:   cond_parts.append(f"{safe}_eff <= '{date_to}'")
            cond_sql = " AND ".join(cond_parts)
            step_counts.append(f"COUNT(DISTINCT CASE WHEN {cond_sql} THEN caseId END) AS {safe}_count")
        step_counts_sql = ",\n            ".join(step_counts)

        # Steps to include in the IN-clause for the signoff scan
        all_step_names = active_steps[:]  # copy
        step_in_list = "','".join(all_step_names)

        query = f"""
        WITH case_step_dates AS (
            SELECT
                w.caseId,
                {step_cols_sql}
            FROM {tbl('WorkModuleSignoff')} w
            JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
            JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
            JOIN {tbl('Case')} f ON w.caseId = f.id
            JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
            LEFT JOIN {tbl('CaseType')} ct ON f.caseTypeId = ct.id
            WHERE w.deleted = false
              AND w.workModuleSignatureType = 'ACCEPT'
              AND wm.name IN ('{step_in_list}')
              AND {case_where}
            GROUP BY w.caseId
        ),
        case_effective AS (
            SELECT caseId,
                {effective_sql}
            FROM case_step_dates
        )
        SELECT {step_counts_sql}
        FROM case_effective
        """
        df = client.query(query).to_dataframe()
        if df.empty:
            return jsonify({"steps": []})
        row = df.iloc[0]

        results = []
        for s in active_steps:
            safe = s.lower().replace(' ', '_').replace('/', '_')
            count = int(row.get(f"{safe}_count", 0))
            # Drop bars with 0 cases (per spec)
            if count > 0:
                results.append({"step": s, "count": count})
        return jsonify({"steps": results})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/report/lt-by-step", methods=["GET"])
def report_lt_by_step():
    """Average lead time per step using real signoff dates.

    Steps are determined dynamically from selected product(s) via PRODUCT_STEPS.
    Each step's LT boundary is from the PREVIOUS step's effective completion date
    to THIS step's effective completion date (using implicit-completion fallback).

    Special boundaries:
    - Scan Assessment LT: Case.createdAt → Scan Assessment WORKER ACCEPT
    - Peer Review LT: PSP APPROVER ACCEPT → Peer Review REVIEWER ACCEPT
      (surgeon wait excluded; falls back to previous step if no PSP approver)
    - Surgeon Approval LT: PSP REVIEWER ACCEPT → PSP APPROVER ACCEPT (separate, not in step chain)

    All other steps use effective completion dates from the implicit-completion rule:
    if a step has no real reviewer signoff but later steps do, use MIN of later step dates.
    Config: app.py -> PRODUCT_STEPS
    """
    try:
        args = request.args
        date_from = args.get('date_from', '')
        date_to   = args.get('date_to', '')
        product   = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()
        case_type = args.get('case_type', '').strip()
        surgeon   = args.get('surgeon', '').strip()

        active_steps = resolve_step_list(args)
        if not active_steps:
            return jsonify({"steps": []})

        case_conds = ["f.deleted = false", "f.canceled = false"]
        if product:
            prods = [p.strip() for p in product.split(',')]
            joined = "','".join(prods)
            case_conds.append(f"cc.name IN ('{joined}')")
        elif product_group:
            all_prods = []
            for g in [g.strip() for g in product_group.split(',')]:
                all_prods.extend(PRODUCT_GROUPS.get(g, []))
            if all_prods:
                joined = "','".join(all_prods)
                case_conds.append(f"cc.name IN ('{joined}')")
        if case_type:
            types = [t.strip() for t in case_type.split(',')]
            joined = "','".join(types)
            case_conds.append(f"ct.name IN ('{joined}')" if len(types) > 1 else f"ct.name = '{types[0]}'")
        if surgeon:
            case_conds.append(f"LOWER(f.alias) LIKE LOWER('%{sanitize(surgeon)}%')")
        case_where = " AND ".join(case_conds)

        if date_from: validate_date(date_from)
        if date_to:   validate_date(date_to)

        # Per-step DATETIME columns (use DATETIME for accurate TIMESTAMP_DIFF; cast at the end)
        step_cols = []
        for s in active_steps:
            safe = s.lower().replace(' ', '_').replace('/', '_')
            if s == 'Scan Assessment':
                step_cols.append(
                    f"MIN(CASE WHEN wm.name = 'Scan Assessment' AND w.workModuleUserType = 'WORKER' "
                    f"AND w.workModuleSignatureType = 'ACCEPT' THEN CAST(w.createdAt AS DATETIME) END) AS {safe}_dt"
                )
            else:
                step_cols.append(
                    f"MIN(CASE WHEN wm.name = '{s}' AND w.workModuleUserType = 'REVIEWER' "
                    f"AND w.workModuleSignatureType = 'ACCEPT' THEN CAST(w.createdAt AS DATETIME) END) AS {safe}_dt"
                )
        # PSP approver — needed for Peer Review boundary
        step_cols.append(
            "MIN(CASE WHEN wm.name = 'Proposed Surgical Plan' AND w.workModuleUserType = 'APPROVER' "
            "AND w.workModuleSignatureType = 'ACCEPT' THEN CAST(w.createdAt AS DATETIME) END) AS psp_approver_dt"
        )

        # Effective completion datetimes — COALESCE this step's dt with later steps' dts
        effective_exprs = []
        for i, s in enumerate(active_steps):
            safe_i = s.lower().replace(' ', '_').replace('/', '_')
            chain = [f"{active_steps[j].lower().replace(' ', '_').replace('/', '_')}_dt" for j in range(i, len(active_steps))]
            effective_exprs.append(f"COALESCE({', '.join(chain)}) AS {safe_i}_eff")
        effective_exprs.append("psp_approver_dt")  # passthrough

        # Build LT computation per step
        # Each step's LT = TIMESTAMP_DIFF(this_eff, prev_eff_or_special, DAY)
        # Population for each step = cases with effective date in range
        step_lt_branches = []
        for i, s in enumerate(active_steps):
            safe = s.lower().replace(' ', '_').replace('/', '_')
            # Date range filter on this step's effective date
            range_parts = [f"{safe}_eff IS NOT NULL"]
            if date_from: range_parts.append(f"DATE({safe}_eff) >= '{date_from}'")
            if date_to:   range_parts.append(f"DATE({safe}_eff) <= '{date_to}'")
            range_sql = " AND ".join(range_parts)

            if s == 'Scan Assessment':
                # Scan Assessment: case_created → scan_worker (the only "real" date, no fallback chain needed)
                branch = f"""
                SELECT '{s}' AS step, caseId,
                       TIMESTAMP_DIFF({safe}_eff, case_created, DAY) AS lt_days
                FROM case_effective
                WHERE {range_sql} AND case_created IS NOT NULL"""
            elif s == 'Peer Review':
                # Peer Review LT: use psp_approver if present (excludes surgeon wait),
                # else fall back to previous step's effective date
                prev_safe = active_steps[i-1].lower().replace(' ', '_').replace('/', '_') if i > 0 else None
                if prev_safe:
                    boundary = f"COALESCE(psp_approver_dt, {prev_safe}_eff)"
                else:
                    boundary = "psp_approver_dt"
                branch = f"""
                SELECT '{s}' AS step, caseId,
                       ABS(TIMESTAMP_DIFF({safe}_eff, {boundary}, DAY)) AS lt_days
                FROM case_effective
                WHERE {range_sql} AND {boundary} IS NOT NULL"""
            else:
                # Generic step: previous step's effective → this step's effective
                if i == 0:
                    continue  # already handled by Scan Assessment branch
                prev_safe = active_steps[i-1].lower().replace(' ', '_').replace('/', '_')
                branch = f"""
                SELECT '{s}' AS step, caseId,
                       ABS(TIMESTAMP_DIFF({safe}_eff, {prev_safe}_eff, DAY)) AS lt_days
                FROM case_effective
                WHERE {range_sql} AND {prev_safe}_eff IS NOT NULL"""
            step_lt_branches.append(branch)

        # Surgeon Approval is separate — not in step chain, always shown if data exists
        sa_range_parts = ["psp_approver_dt IS NOT NULL"]
        if date_from: sa_range_parts.append(f"DATE(psp_approver_dt) >= '{date_from}'")
        if date_to:   sa_range_parts.append(f"DATE(psp_approver_dt) <= '{date_to}'")
        sa_range_sql = " AND ".join(sa_range_parts)
        if 'Proposed Surgical Plan' in active_steps:
            step_lt_branches.append(f"""
                SELECT 'Surgeon Approval' AS step, caseId,
                       ABS(TIMESTAMP_DIFF(psp_approver_dt, proposed_surgical_plan_eff, DAY)) AS lt_days
                FROM case_effective
                WHERE {sa_range_sql} AND proposed_surgical_plan_eff IS NOT NULL""")

        step_cols_sql = ",\n            ".join(step_cols)
        effective_sql = ",\n            ".join(effective_exprs)
        all_branches_sql = "\n            UNION ALL\n".join(step_lt_branches)

        step_in_list = "','".join(active_steps)

        query = f"""
        WITH case_step_dates AS (
            SELECT
                w.caseId,
                CAST(f.createdAt AS DATETIME) AS case_created,
                {step_cols_sql}
            FROM {tbl('WorkModuleSignoff')} w
            JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
            JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
            JOIN {tbl('Case')} f ON w.caseId = f.id
            JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
            LEFT JOIN {tbl('CaseType')} ct ON f.caseTypeId = ct.id
            WHERE w.deleted = false
              AND w.workModuleSignatureType = 'ACCEPT'
              AND wm.name IN ('{step_in_list}', 'Proposed Surgical Plan')
              AND {case_where}
            GROUP BY w.caseId, f.createdAt
        ),
        case_effective AS (
            SELECT caseId, case_created,
                {effective_sql}
            FROM case_step_dates
        )
        SELECT step, ROUND(AVG(lt_days), 1) AS avg_lt, COUNT(*) AS case_count
        FROM (
            {all_branches_sql}
        )
        WHERE lt_days IS NOT NULL AND lt_days >= 0
        GROUP BY step
        """
        df = client.query(query).to_dataframe()

        step_map = {}
        for _, r in df.iterrows():
            v = r['avg_lt']
            try:
                fv = float(v) if v is not None else None
                if fv is not None and (fv != fv or fv == float('inf')):
                    fv = None
            except:
                fv = None
            step_map[r['step']] = {
                'avg_lt': round(fv, 1) if fv is not None else None,
                'case_count': int(r['case_count'])
            }

        # Display order: active steps + Surgeon Approval at the end if PSP is in workflow
        display_order = list(active_steps)
        if 'Proposed Surgical Plan' in active_steps:
            display_order.append('Surgeon Approval')

        steps = []
        for s in display_order:
            d = step_map.get(s)
            if not d or d.get('case_count', 0) == 0:
                continue  # drop zero-pop bars
            steps.append({"step": s, "avg_lt": d['avg_lt'], "case_count": d['case_count']})
        return jsonify({"steps": steps})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/report/fpy-by-step", methods=["GET"])
def report_fpy_by_step():
    """First Pass Yield per step.
    Population: same as process-counts for that step (effective completion date in range,
    where effective = COALESCE of real reviewer accept + later step dates per implicit-complete rule).
    Pass/fail: reviewer's first signoff at step.
      - First = ACCEPT → pass
      - First = REJECT → fail (case was reworked)
      - No reviewer signoff at all (implicitly complete via later step) → pass (option B)
    Scan Assessment has no FPY (no reviewer step).
    Config: app.py -> PRODUCT_STEPS
    """
    try:
        args = request.args
        date_from = args.get('date_from', '')
        date_to   = args.get('date_to', '')
        product   = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()
        case_type = args.get('case_type', '').strip()
        surgeon   = args.get('surgeon', '').strip()

        active_steps = resolve_step_list(args)
        # FPY excludes Scan Assessment (no reviewer signoff)
        fpy_steps = [s for s in active_steps if s != 'Scan Assessment']
        if not fpy_steps:
            return jsonify({"steps": []})

        case_conds = ["f.deleted = false", "f.canceled = false"]
        if product:
            prods = [p.strip() for p in product.split(',')]
            joined = "','".join(prods)
            case_conds.append(f"cc.name IN ('{joined}')")
        elif product_group:
            all_prods = []
            for g in [g.strip() for g in product_group.split(',')]:
                all_prods.extend(PRODUCT_GROUPS.get(g, []))
            if all_prods:
                joined = "','".join(all_prods)
                case_conds.append(f"cc.name IN ('{joined}')")
        if case_type:
            types = [t.strip() for t in case_type.split(',')]
            joined = "','".join(types)
            case_conds.append(f"ct.name IN ('{joined}')" if len(types) > 1 else f"ct.name = '{types[0]}'")
        if surgeon:
            case_conds.append(f"LOWER(f.alias) LIKE LOWER('%{sanitize(surgeon)}%')")
        case_where = " AND ".join(case_conds)

        if date_from: validate_date(date_from)
        if date_to:   validate_date(date_to)

        # Build per-step real reviewer accept dates
        step_cols = []
        for s in active_steps:
            safe = s.lower().replace(' ', '_').replace('/', '_')
            if s == 'Scan Assessment':
                # Scan Assessment uses WORKER (no reviewer); include in effective chain only
                step_cols.append(
                    f"MIN(CASE WHEN wm.name = 'Scan Assessment' AND w.workModuleUserType = 'WORKER' "
                    f"AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) AS {safe}_date"
                )
            else:
                step_cols.append(
                    f"MIN(CASE WHEN wm.name = '{s}' AND w.workModuleUserType = 'REVIEWER' "
                    f"AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) AS {safe}_date"
                )

        # Effective completion = COALESCE(this step's date, all later step dates)
        effective_exprs = []
        for i, s in enumerate(active_steps):
            safe_i = s.lower().replace(' ', '_').replace('/', '_')
            chain = [f"{active_steps[j].lower().replace(' ', '_').replace('/', '_')}_date" for j in range(i, len(active_steps))]
            effective_exprs.append(f"COALESCE({', '.join(chain)}) AS {safe_i}_eff")

        step_cols_sql = ",\n            ".join(step_cols)
        effective_sql = ",\n            ".join(effective_exprs)

        step_in_list = "','".join(active_steps)

        # Build per-step counts: total (effective date in range) and reworked
        # (case has any reviewer first-signoff REJECT at that step)
        step_metrics = []
        for s in fpy_steps:
            safe = s.lower().replace(' ', '_').replace('/', '_')
            cond_parts = [f"ce.{safe}_eff IS NOT NULL"]
            if date_from: cond_parts.append(f"ce.{safe}_eff >= '{date_from}'")
            if date_to:   cond_parts.append(f"ce.{safe}_eff <= '{date_to}'")
            cond_sql = " AND ".join(cond_parts)
            step_metrics.append(
                f"COUNT(DISTINCT CASE WHEN {cond_sql} THEN ce.caseId END) AS {safe}_total"
            )
            step_metrics.append(
                f"COUNT(DISTINCT CASE WHEN {cond_sql} AND ff.{safe}_failed = TRUE THEN ce.caseId END) AS {safe}_rework"
            )

        # Per-step failed_first columns
        failed_pivot_exprs = []
        for s in fpy_steps:
            safe = s.lower().replace(' ', '_').replace('/', '_')
            failed_pivot_exprs.append(
                f"MAX(CASE WHEN step_name = '{s}' AND first_result = 'REJECT' THEN TRUE ELSE FALSE END) AS {safe}_failed"
            )
        failed_pivot_sql = ",\n            ".join(failed_pivot_exprs)

        fpy_step_in = "','".join(fpy_steps)

        query = f"""
        WITH case_step_dates AS (
            SELECT
                w.caseId,
                {step_cols_sql}
            FROM {tbl('WorkModuleSignoff')} w
            JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
            JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
            JOIN {tbl('Case')} f ON w.caseId = f.id
            JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
            LEFT JOIN {tbl('CaseType')} ct ON f.caseTypeId = ct.id
            WHERE w.deleted = false
              AND w.workModuleSignatureType = 'ACCEPT'
              AND wm.name IN ('{step_in_list}')
              AND {case_where}
            GROUP BY w.caseId
        ),
        case_effective AS (
            SELECT caseId, {effective_sql}
            FROM case_step_dates
        ),
        -- Reviewer's FIRST signoff per case per step.
        -- CRITICAL: restricted to cases in case_effective (the denominator) only.
        -- This ensures rework is only counted for cases that completed in the date range.
        reviewer_first AS (
            SELECT
                w.caseId,
                wm.name AS step_name,
                w.workModuleSignatureType AS first_result,
                ROW_NUMBER() OVER (
                    PARTITION BY w.caseId, wm.name
                    ORDER BY w.createdAt ASC
                ) AS rn
            FROM {tbl('WorkModuleSignoff')} w
            JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
            JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
            WHERE w.deleted = false
              AND w.workModuleUserType = 'REVIEWER'
              AND w.workModuleSignatureType IN ('ACCEPT', 'REJECT')
              AND wm.name IN ('{fpy_step_in}')
              AND w.caseId IN (SELECT caseId FROM case_effective)
        ),
        case_failed AS (
            SELECT
                caseId,
                {failed_pivot_sql}
            FROM reviewer_first
            WHERE rn = 1
            GROUP BY caseId
        )
        SELECT {",".join(step_metrics)}
        FROM case_effective ce
        LEFT JOIN case_failed ff ON ce.caseId = ff.caseId
        """
        df = client.query(query).to_dataframe()
        if df.empty:
            return jsonify({"steps": []})
        row = df.iloc[0]

        steps = []
        # Display in workflow order, including Scan Assessment as a placeholder (no FPY)
        for s in active_steps:
            if s == 'Scan Assessment':
                # No FPY for Scan Assessment — only include if config requires it
                continue
            safe = s.lower().replace(' ', '_').replace('/', '_')
            total = int(row.get(f"{safe}_total", 0))
            rework = int(row.get(f"{safe}_rework", 0))
            if total == 0:
                continue  # drop zero-pop bars
            passed = max(0, total - rework)
            fpy = round(passed / total * 100, 1)
            steps.append({"step": s, "fpy": fpy, "total": total, "rework": rework})
        return jsonify({"steps": steps})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/report/cases-by-step", methods=["GET"])
def report_cases_by_step():
    """Returns the case list for a specific step's population on the report.
    Used by Fetch Cases on the FPY and LT charts (click a bar → drill down).

    Population: same as process counts for that step — cases whose effective
    completion date (real reviewer accept, or implicit via later steps) falls
    in the date range.

    Query params:
      step           — required; canonical step name (e.g. 'Planning')
      date_from/to   — report date range
      product, product_group, case_type, surgeon — standard filters
    """
    try:
        args = request.args
        step = args.get('step', '').strip()
        if not step:
            return jsonify({"error": "step parameter required"}), 400

        date_from     = args.get('date_from', '')
        date_to       = args.get('date_to', '')
        product       = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()
        case_type     = args.get('case_type', '').strip()
        surgeon       = args.get('surgeon', '').strip()
        include_rework = args.get('include_rework') == 'true'

        active_steps = resolve_step_list(args)
        if step not in active_steps:
            # Step not in this product's workflow — return empty
            return jsonify({"cases": [], "count": 0, "step": step})

        case_conds = ["f.deleted = false", "f.canceled = false"]
        if product:
            prods = [p.strip() for p in product.split(',')]
            joined = "','".join(prods)
            case_conds.append(f"cc.name IN ('{joined}')")
        elif product_group:
            all_prods = []
            for g in [g.strip() for g in product_group.split(',')]:
                all_prods.extend(PRODUCT_GROUPS.get(g, []))
            if all_prods:
                joined = "','".join(all_prods)
                case_conds.append(f"cc.name IN ('{joined}')")
        if case_type:
            types = [t.strip() for t in case_type.split(',')]
            joined = "','".join(types)
            case_conds.append(f"ct.name IN ('{joined}')" if len(types) > 1 else f"ct.name = '{types[0]}'")
        if surgeon:
            case_conds.append(f"LOWER(f.alias) LIKE LOWER('%{sanitize(surgeon)}%')")
        case_where = " AND ".join(case_conds)

        if date_from: validate_date(date_from)
        if date_to:   validate_date(date_to)

        # Build per-step date columns (real reviewer accepts)
        step_cols = []
        for s in active_steps:
            safe = s.lower().replace(' ', '_').replace('/', '_')
            if s == 'Scan Assessment':
                step_cols.append(
                    f"MIN(CASE WHEN wm.name = 'Scan Assessment' AND w.workModuleUserType = 'WORKER' "
                    f"AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) AS {safe}_date"
                )
            else:
                # For PSP, use MAX (last accept) to match process counts rework logic
                agg = 'MAX' if s == 'Proposed Surgical Plan' else 'MIN'
                step_cols.append(
                    f"{agg}(CASE WHEN wm.name = '{s}' AND w.workModuleUserType = 'REVIEWER' "
                    f"AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) AS {safe}_date"
                )
        # Effective completion = COALESCE(this step's date, later step dates)
        step_idx = active_steps.index(step)
        target_safe = step.lower().replace(' ', '_').replace('/', '_')
        chain = [f"{active_steps[j].lower().replace(' ', '_').replace('/', '_')}_date"
                 for j in range(step_idx, len(active_steps))]
        target_eff_expr = f"COALESCE({', '.join(chain)})"

        step_cols_sql = ",\n            ".join(step_cols)
        step_in_list = "','".join(active_steps)

        date_filter_parts = [f"{target_eff_expr} IS NOT NULL"]
        if date_from: date_filter_parts.append(f"{target_eff_expr} >= '{date_from}'")
        if date_to:   date_filter_parts.append(f"{target_eff_expr} <= '{date_to}'")
        date_filter_sql = " AND ".join(date_filter_parts)

        # Build case list using existing helpers so the case table renders cleanly
        # Optional: rework CTE to flag cases where reviewer's first signoff = REJECT
        rework_cte = ""
        rework_join = ""
        rework_field = ""
        if include_rework and step != 'Scan Assessment':
            rework_cte = f""",
        reviewer_first_for_step AS (
            SELECT
                w.caseId,
                w.workModuleSignatureType AS first_result,
                ROW_NUMBER() OVER (
                    PARTITION BY w.caseId
                    ORDER BY wi.createdAt ASC, w.createdAt ASC
                ) AS rn
            FROM {tbl('WorkModuleSignoff')} w
            JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
            JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
            WHERE w.deleted = false
              AND w.workModuleUserType = 'REVIEWER'
              AND w.workModuleSignatureType IN ('ACCEPT','REJECT')
              AND wm.name = '{step}'
              AND w.caseId IN (SELECT caseId FROM target_cases)
        ),
        rework_flags AS (
            SELECT DISTINCT caseId, TRUE AS reworked
            FROM reviewer_first_for_step
            WHERE rn = 1 AND first_result = 'REJECT'
        )"""
            rework_join = "LEFT JOIN rework_flags rf ON base.id = rf.caseId"
            rework_field = ", COALESCE(rf.reworked, FALSE) AS reworked"

        query = f"""
        WITH case_step_dates AS (
            SELECT
                w.caseId AS step_caseId,
                {step_cols_sql}
            FROM {tbl('WorkModuleSignoff')} w
            JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
            JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
            JOIN {tbl('Case')} cf ON w.caseId = cf.id
            JOIN {tbl('CaseCategory')} cc ON cf.caseCategoryId = cc.id
            LEFT JOIN {tbl('CaseType')} ct ON cf.caseTypeId = ct.id
            WHERE w.deleted = false
              AND w.workModuleSignatureType = 'ACCEPT'
              AND wm.name IN ('{step_in_list}')
              AND cf.deleted = false
              AND {case_where.replace("f.", "cf.")}
            GROUP BY w.caseId
        ),
        target_cases AS (
            SELECT step_caseId AS caseId
            FROM case_step_dates
            WHERE {date_filter_sql}
        ),
        {signoff_cte()},
        {on_hold_cte()}
        {rework_cte}
        SELECT
            COALESCE(f.id, base.id) AS id,
            COALESCE(
                CONCAT(f.count, '_', f.alias),
                CAST(base.id AS STRING)
            ) AS alias,
            COALESCE(f.alias, CAST(base.id AS STRING)) AS alias_short,
            f.count AS case_number,
            COALESCE(f.case_category_name, cc2.name) AS case_category_name,
            COALESCE(f.phase, base.phase) AS phase,
            f.phy_nameFirst, f.phy_nameLast, f.fac_name, f.fac_state,
            f.laterality, COALESCE(f.onHold, base.onHold) AS onHold,
            COALESCE(f.createdAt, base.createdAt) AS createdAt,
            s.first_scan_upload_date, s.ship_wrk_comp_date,
            sd.seg_review_date, sd.first_psp_review_date, sd.surgeon_approval_date,
            COALESCE(oh.total_hold_days, 0) AS onhold_days,
            CASE WHEN s.ship_wrk_comp_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
                THEN TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
                ELSE NULL END AS total_lt,
            CASE WHEN sd.seg_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
                THEN TIMESTAMP_DIFF(sd.seg_review_date, s.first_scan_upload_date, DAY)
                ELSE NULL END AS seg_lt,
            CASE WHEN sd.surgeon_approval_date IS NOT NULL AND sd.first_psp_review_date IS NOT NULL
                THEN TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY)
                ELSE NULL END AS surgeon_lt,
            CASE WHEN sd.peer_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
                THEN GREATEST(0, TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY)
                    - COALESCE(oh.total_hold_days, 0)
                    - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)))
                ELSE NULL END AS digital_lt
            {rework_field}
        FROM {tbl('Case')} base
        LEFT JOIN {tbl('vw_fact_case')} f ON base.id = f.id
        LEFT JOIN {tbl('CaseCategory')} cc2 ON base.caseCategoryId = cc2.id
        LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON base.id = s.caseId
        LEFT JOIN signoff_dates sd ON base.id = sd.caseId
        LEFT JOIN on_hold_time oh ON base.id = oh.caseId
        {rework_join}
        WHERE base.id IN (SELECT caseId FROM target_cases)
        ORDER BY base.createdAt DESC LIMIT 500
        """
        df = client.query(query).to_dataframe()
        cases = format_cases(df)
        # Attach reworked flag if present
        if include_rework and 'reworked' in df.columns:
            for i, row in df.iterrows():
                cases[i]['reworked'] = bool(row['reworked'])
        return jsonify({"cases": cases, "count": len(df), "step": step})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/report/volume", methods=["GET"])
def report_volume():
    """Volume for the report: same population logic as report_otd.
    Counts cases where effective_psp_date (earliest of PSP REVIEWER, Peer Review REVIEWER,
    ship_wrk_comp_date) falls in the date range. Implicit-PSP rule applied so cases
    that skipped PSP review but completed downstream still count.

    date_field=first_scan_upload_date switches to the Submitted milestone instead —
    same raw-Case-table population as /analytics/trends (metric=volume,
    date_field=first_scan_upload_date), so the chart total and this drill-down
    always match (both include COMPLETED-phase cases, unlike vw_fact_case).
    """
    try:
        args = request.args
        date_from = args.get('date_from', '')
        date_to   = args.get('date_to', '')
        product   = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()
        case_type = args.get('case_type', '').strip()
        surgeon   = args.get('surgeon', '').strip()
        fetch_cases = args.get('fetch_cases') == 'true'
        date_field  = args.get('date_field', '').strip()
        use_scan_upload = date_field == 'first_scan_upload_date'

        # Include cancelled cases — PSP completion is the milestone; post-PSP cancellation should not affect volume
        case_conds = ["f.deleted = false"]
        if product:
            prods = [p.strip() for p in product.split(',')]
            joined = "','".join(prods)
            case_conds.append(f"cc.name IN ('{joined}')")
        elif product_group:
            all_prods = []
            for g in [g.strip() for g in product_group.split(',')]:
                all_prods.extend(PRODUCT_GROUPS.get(g, []))
            if all_prods:
                joined = "','".join(all_prods)
                case_conds.append(f"cc.name IN ('{joined}')")
        if case_type:
            types = [t.strip() for t in case_type.split(',')]
            joined = "','".join(types)
            case_conds.append(f"ct.name IN ('{joined}')" if len(types) > 1 else f"ct.name = '{types[0]}'")
        if surgeon:
            case_conds.append(f"LOWER(f.alias) LIKE LOWER('%{sanitize(surgeon)}%')")
        case_where = " AND ".join(case_conds)

        if date_from: validate_date(date_from)
        if date_to:   validate_date(date_to)

        if use_scan_upload:
            # Submitted milestone — mirrors /analytics/trends exactly: no PSP-not-null
            # requirement, date field is s.first_scan_upload_date from vw_lkup_stage_log_dates.
            query = f"""
            WITH {signoff_cte()},
            case_dates AS (
                SELECT
                    f.id AS caseId,
                    s.first_scan_upload_date AS milestone_date
                FROM {tbl('Case')} f
                JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
                LEFT JOIN {tbl('CaseType')} ct ON f.caseTypeId = ct.id
                LEFT JOIN signoff_dates sd ON f.id = sd.caseId
                LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON f.id = s.caseId
                WHERE {case_where}
            )
            SELECT caseId
            FROM case_dates
            WHERE 1=1
              {f"AND DATE(milestone_date) >= '{date_from}'" if date_from else ""}
              {f"AND DATE(milestone_date) <= '{date_to}'" if date_to else ""}
            """
        else:
            query = f"""
            WITH {signoff_cte()},
            case_dates AS (
                SELECT
                    f.id AS caseId,
                    CAST(sd.first_psp_review_date AS DATETIME) AS effective_psp_date
                FROM {tbl('Case')} f
                JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
                LEFT JOIN {tbl('CaseType')} ct ON f.caseTypeId = ct.id
                LEFT JOIN signoff_dates sd ON f.id = sd.caseId
                WHERE {case_where}
                  AND sd.first_psp_review_date IS NOT NULL
            )
            SELECT caseId
            FROM case_dates
            WHERE 1=1
              {f"AND effective_psp_date >= CAST('{date_from}' AS DATETIME)" if date_from else ""}
              {f"AND effective_psp_date <= CAST('{date_to} 23:59:59' AS DATETIME)" if date_to else ""}
            """
        df = client.query(query).to_dataframe()
        case_ids = df['caseId'].tolist() if fetch_cases else []

        if fetch_cases and case_ids:
            case_query = fetch_cases_by_ids_query(case_ids)
            case_df = client.query(case_query).to_dataframe()
            return jsonify({"cases": format_cases(case_df), "count": len(case_df)})

        return jsonify({"case_count": len(df)})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/report/otd", methods=["GET"])
def report_otd():
    """On-Time Delivery for R3ID cases.

    Population: same case set as the volume KPI, with the implicit-PSP rule:
    a case's effective PSP completion date is the EARLIEST of
      (a) Proposed Surgical Plan REVIEWER ACCEPT
      (b) Peer Review REVIEWER ACCEPT
      (c) ship_wrk_comp_date
    Cases without (a) but with (b) or (c) are still counted (PSP implicit via downstream).

    On Time: Digital Production LT <= OTD_TARGET_DAYS (default 14).
    Date filter applied to effective_psp_date.

    Config: DataGenie_Report_Config.yaml -> otd
    """
    OTD_TARGET_DAYS = 14  # Config: DataGenie_Report_Config.yaml -> otd.target_days
    try:
        args = request.args
        date_from = args.get('date_from', '')
        date_to   = args.get('date_to', '')
        product   = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()
        case_type = args.get('case_type', '').strip()
        surgeon   = args.get('surgeon', '').strip()

        # Include cancelled cases — PSP completion is the milestone; post-PSP cancellation should not affect OTD
        case_conds = ["f.deleted = false"]
        if product:
            prods = [p.strip() for p in product.split(',')]
            joined = "','".join(prods)
            case_conds.append(f"cc.name IN ('{joined}')")
        elif product_group:
            all_prods = []
            for g in [g.strip() for g in product_group.split(',')]:
                all_prods.extend(PRODUCT_GROUPS.get(g, []))
            if all_prods:
                joined = "','".join(all_prods)
                case_conds.append(f"cc.name IN ('{joined}')")
        if case_type:
            types = [t.strip() for t in case_type.split(',')]
            joined = "','".join(types)
            case_conds.append(f"ct.name IN ('{joined}')" if len(types) > 1 else f"ct.name = '{types[0]}'")
        if surgeon:
            case_conds.append(f"LOWER(f.alias) LIKE LOWER('%{sanitize(surgeon)}%')")
        case_where = " AND ".join(case_conds)

        if date_from: validate_date(date_from)
        if date_to:   validate_date(date_to)
        date_from_clause = f"AND effective_psp_date >= '{date_from}'" if date_from else ""
        date_to_clause   = f"AND effective_psp_date <= '{date_to}'"   if date_to   else ""

        # ── Branch: fetch_cases=true → return case list for an otd_status segment ──
        fetch = args.get('fetch_cases') == 'true'
        otd_status = args.get('otd_status', '').strip()  # 'On Time', 'Delayed', 'No Data'
        if fetch:
            # Map UI label to SQL status
            status_map_in = {'On Time':'ON_TIME','Delayed':'DELAYED','No Data':'NO_DATA'}
            sql_status = status_map_in.get(otd_status)
            status_filter = f"AND status = '{sql_status}'" if sql_status else ""
            fetch_query = f"""
            WITH {signoff_cte()},
                 {on_hold_cte()},
            case_dates AS (
                SELECT
                    f.id AS caseId,
                    s.first_scan_upload_date,
                    s.ship_wrk_comp_date,
                    sd.first_psp_review_date,
                    sd.surgeon_approval_date,
                    sd.peer_review_date,
                    DATE(LEAST(
                        COALESCE(sd.first_psp_review_date, DATETIME '9999-12-31'),
                        COALESCE(sd.peer_review_date,      DATETIME '9999-12-31'),
                        COALESCE(CAST(s.ship_wrk_comp_date AS DATETIME), DATETIME '9999-12-31')
                    )) AS effective_psp_date,
                    COALESCE(oh.total_hold_days, 0) AS hold_days
                FROM {tbl('Case')} f
                JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
                LEFT JOIN {tbl('CaseType')} ct ON f.caseTypeId = ct.id
                LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON f.id = s.caseId
                LEFT JOIN signoff_dates sd ON f.id = sd.caseId
                LEFT JOIN on_hold_time oh ON f.id = oh.caseId
                WHERE {case_where}
                  AND (sd.first_psp_review_date IS NOT NULL
                       OR sd.peer_review_date   IS NOT NULL
                       OR s.ship_wrk_comp_date  IS NOT NULL)
            ),
            pop AS (
                SELECT * FROM case_dates
                WHERE first_scan_upload_date IS NOT NULL
                  {date_from_clause} {date_to_clause}
            ),
            classified AS (
                SELECT
                    caseId,
                    CASE
                        WHEN first_psp_review_date IS NOT NULL AND first_scan_upload_date IS NOT NULL THEN
                            GREATEST(0,
                                TIMESTAMP_DIFF(CAST(first_psp_review_date AS DATETIME), first_scan_upload_date, DAY)
                                - hold_days
                                - GREATEST(0, COALESCE(
                                    TIMESTAMP_DIFF(surgeon_approval_date, first_psp_review_date, DAY), 0)))
                        ELSE NULL
                    END AS digital_lt
                FROM pop
            ),
            target_cases AS (
                SELECT caseId FROM (
                    SELECT caseId,
                        CASE
                            WHEN digital_lt IS NULL THEN 'ON_TIME'
                            WHEN digital_lt <= {OTD_TARGET_DAYS} THEN 'ON_TIME'
                            ELSE 'DELAYED'
                        END AS status
                    FROM classified
                )
                WHERE 1=1 {status_filter}
            )
            SELECT {case_select_fields()}
            {case_joins()}
            WHERE f.id IN (SELECT caseId FROM target_cases)
            ORDER BY f.createdAt DESC LIMIT 500
            """
            df = client.query(fetch_query).to_dataframe()
            return jsonify({"cases": format_cases(df), "count": len(df), "otd_status": otd_status})

        query = f"""
        WITH {signoff_cte()},
             {on_hold_cte()},
        -- Effective PSP and Digital LT inputs per case
        case_dates AS (
            SELECT
                f.id AS caseId,
                s.first_scan_upload_date,
                s.ship_wrk_comp_date,
                sd.first_psp_review_date,
                sd.surgeon_approval_date,
                sd.peer_review_date,
                -- Implicit PSP completion: earliest of PSP reviewer, peer review reviewer, or ship_wrk_comp_date
                DATE(LEAST(
                    COALESCE(sd.first_psp_review_date, DATETIME '9999-12-31'),
                    COALESCE(sd.peer_review_date,      DATETIME '9999-12-31'),
                    COALESCE(CAST(s.ship_wrk_comp_date AS DATETIME), DATETIME '9999-12-31')
                )) AS effective_psp_date,
                COALESCE(oh.total_hold_days, 0) AS hold_days
            FROM {tbl('Case')} f
            JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
            LEFT JOIN {tbl('CaseType')} ct ON f.caseTypeId = ct.id
            LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON f.id = s.caseId
            LEFT JOIN signoff_dates sd ON f.id = sd.caseId
            LEFT JOIN on_hold_time oh ON f.id = oh.caseId
            WHERE {case_where}
              -- At least one of PSP, Peer Review, or Ship date must exist (population)
              AND (sd.first_psp_review_date IS NOT NULL
                   OR sd.peer_review_date   IS NOT NULL
                   OR s.ship_wrk_comp_date  IS NOT NULL)
        ),
        pop AS (
            SELECT * FROM case_dates
            WHERE first_scan_upload_date IS NOT NULL
              {date_from_clause} {date_to_clause}
        ),
        lt_calc AS (
            SELECT
                caseId,
                effective_psp_date,
                CASE
                    -- Only calculate digital LT for cases with real PSP reviewer accept
                    -- Cases with implicit PSP (peer review/ship only) get NULL → treated as ON_TIME
                    WHEN first_psp_review_date IS NOT NULL AND first_scan_upload_date IS NOT NULL THEN
                        GREATEST(0,
                            TIMESTAMP_DIFF(CAST(first_psp_review_date AS DATETIME), first_scan_upload_date, DAY)
                            - hold_days
                            - GREATEST(0, COALESCE(
                                TIMESTAMP_DIFF(surgeon_approval_date, first_psp_review_date, DAY), 0)))
                    ELSE NULL
                END AS digital_lt
            FROM pop
        )
        SELECT
            CASE
                WHEN digital_lt IS NULL THEN 'ON_TIME'
                WHEN digital_lt <= {OTD_TARGET_DAYS} THEN 'ON_TIME'
                ELSE 'DELAYED'
            END AS otd_status,
            COUNT(DISTINCT caseId) AS case_count
        FROM lt_calc
        GROUP BY otd_status
        """
        df = client.query(query).to_dataframe()
        status_map = {r['otd_status']: int(r['case_count']) for _, r in df.iterrows()}
        on_time = status_map.get('ON_TIME', 0)
        delayed = status_map.get('DELAYED', 0)
        no_data = status_map.get('NO_DATA', 0)
        total   = on_time + delayed
        otd_pct = round(on_time / total * 100, 1) if total > 0 else 0
        return jsonify({
            "on_time":     on_time,
            "delayed":     delayed,
            "no_data":     no_data,
            "total":       total,
            "otd_pct":     otd_pct,
            "target_days": OTD_TARGET_DAYS
        })
    except Exception as e:
        return handle_error(request.endpoint, e)

QT_STEPS = [
    "Scan Assessment", "Segmentation", "Design Call Prep", "Input Collection",
    "Planning", "Jigs Design", "Design", "Proposed Surgical Plan",
]

QT_LOOKBACK = 30  # days — max window to find a role assignment before signoff

QT_CAP_PLANNING_SEC = 14 * 86400   # 14 days for Planning

QT_CAP_DEFAULT_SEC  =  7 * 86400   # 7 days for all others

def _qt_case_where(args):
    """Build WHERE for queue-time case universe."""
    product       = args.get("product", "").strip()
    product_group = args.get("product_group", "").strip()
    conds = ["f.deleted = false", "f.canceled = false"]
    if product:
        prods  = [p.strip() for p in product.split(",")]
        joined = "','".join(prods)
        conds.append(f"cc.name IN ('{joined}')")
    elif product_group:
        all_prods = []
        for g in [g.strip() for g in product_group.split(",")]:
            all_prods.extend(PRODUCT_GROUPS.get(g, []))
        if all_prods:
            joined = "','".join(all_prods)
            conds.append(f"cc.name IN ('{joined}')")
    else:
        conds.append("cc.name IN ('Reverse Total Shoulder Arthroplasty','Total Ankle Replacement')")
    return " AND ".join(conds)

def _qt_date_filter(args, ts_expr):
    date_from = validate_date(args.get("date_from", ""))
    date_to   = validate_date(args.get("date_to",   ""))
    parts = []
    if date_from:
        parts.append(f"DATE({ts_expr}) >= '{date_from}'")
    if date_to:
        parts.append(f"DATE({ts_expr}) <= '{date_to}'")
    return ("AND " + " AND ".join(parts)) if parts else ""

def _build_queue_time_query(args):
    """Build BigQuery SQL for queue time per step (team-level)."""
    case_where = _qt_case_where(args)
    date_filter = _qt_date_filter(args, "TIMESTAMP_ADD(CAST(fs.signoff_ts AS TIMESTAMP), INTERVAL 330 MINUTE)")
    steps_sql = "','".join(QT_STEPS)

    return f"""
    WITH
    cases AS (
      SELECT f.id AS caseId, cc.name AS product
      FROM {tbl('vw_fact_case')} f
      JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
      WHERE {case_where}
    ),

    -- First worker signoff per case per step
    first_signoffs AS (
      SELECT
        sig.refId AS caseId, sig.userId, sig.createdAt AS signoff_ts,
        REGEXP_EXTRACT(sig.type, r'r3idWorkModuleSignoff-(.+)-WORKER-ACCEPT') AS step_name
      FROM {_LOG} sig
      INNER JOIN cases c ON sig.refId = c.caseId
      WHERE sig.type LIKE 'r3idWorkModuleSignoff-%-WORKER-ACCEPT'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sig.refId,
          REGEXP_EXTRACT(sig.type, r'r3idWorkModuleSignoff-(.+)-WORKER-ACCEPT')
        ORDER BY sig.createdAt ASC
      ) = 1
    ),

    -- Work-start: last role-assignment before the first signoff (within lookback)
    work_starts AS (
      SELECT
        fs.caseId, fs.userId, fs.step_name, fs.signoff_ts, c.product,
        MAX(asn.createdAt) AS work_start_ts
      FROM first_signoffs fs
      INNER JOIN cases c ON c.caseId = fs.caseId
      LEFT JOIN {_LOG} asn
        ON  asn.refId   = fs.caseId
        AND asn.userId  = fs.userId
        AND asn.type    = 'r3idCaseRoleAssignmentUpdate'
        AND asn.createdAt <= fs.signoff_ts
        AND asn.createdAt >= TIMESTAMP_SUB(fs.signoff_ts, INTERVAL {QT_LOOKBACK} DAY)
      WHERE fs.step_name IN ('{steps_sql}')
        {date_filter}
      GROUP BY fs.caseId, fs.userId, fs.step_name, fs.signoff_ts, c.product
    ),

    -- Previous-step completion timestamps per case
    -- Worker-only steps: Scan Assessment, Design Call Prep, Input Collection
    -- Reviewer steps: Segmentation, Planning, Jigs Design, Design
    step_completions AS (
      SELECT w.caseId, wm.name AS step_name,
        MIN(CAST(w.createdAt AS TIMESTAMP)) AS completed_at
      FROM {tbl('WorkModuleSignoff')} w
      JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
      JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
      WHERE w.deleted = false AND w.workModuleSignatureType = 'ACCEPT'
        AND (
          (wm.name IN ('Scan Assessment','Design Call Prep','Input Collection') AND w.workModuleUserType = 'WORKER')
          OR
          (wm.name IN ('Segmentation','Planning','Jigs Design','Design') AND w.workModuleUserType = 'REVIEWER')
        )
      GROUP BY w.caseId, wm.name
    ),

    -- Scan upload date (queue start for Scan Assessment)
    scan_dates AS (
      SELECT caseId, CAST(first_scan_upload_date AS TIMESTAMP) AS scan_upload_ts
      FROM {tbl('vw_lkup_stage_log_dates')}
      WHERE first_scan_upload_date IS NOT NULL
    ),

    -- Resolve queue-start per step (product-aware)
    queue_raw AS (
      SELECT
        ws.caseId, ws.step_name, ws.work_start_ts, ws.product,
        CASE ws.step_name
          WHEN 'Scan Assessment'        THEN sd.scan_upload_ts
          WHEN 'Segmentation'           THEN sc_scan.completed_at
          WHEN 'Design Call Prep'       THEN sc_seg.completed_at
          WHEN 'Input Collection'       THEN sc_seg.completed_at
          WHEN 'Planning'               THEN COALESCE(sc_dcp.completed_at, sc_ic.completed_at, sc_seg.completed_at)
          WHEN 'Design'                 THEN sc_plan.completed_at
          WHEN 'Jigs Design'            THEN sc_des.completed_at
          WHEN 'Proposed Surgical Plan' THEN COALESCE(sc_jigs.completed_at, sc_des.completed_at)
          ELSE NULL
        END AS queue_start_ts
      FROM work_starts ws
      LEFT JOIN scan_dates sd             ON sd.caseId    = ws.caseId
      LEFT JOIN step_completions sc_scan  ON sc_scan.caseId = ws.caseId AND sc_scan.step_name = 'Scan Assessment'
      LEFT JOIN step_completions sc_seg   ON sc_seg.caseId  = ws.caseId AND sc_seg.step_name  = 'Segmentation'
      LEFT JOIN step_completions sc_dcp   ON sc_dcp.caseId  = ws.caseId AND sc_dcp.step_name  = 'Design Call Prep'
      LEFT JOIN step_completions sc_ic    ON sc_ic.caseId   = ws.caseId AND sc_ic.step_name   = 'Input Collection'
      LEFT JOIN step_completions sc_plan  ON sc_plan.caseId = ws.caseId AND sc_plan.step_name = 'Planning'
      LEFT JOIN step_completions sc_des   ON sc_des.caseId  = ws.caseId AND sc_des.step_name  = 'Design'
      LEFT JOIN step_completions sc_jigs  ON sc_jigs.caseId = ws.caseId AND sc_jigs.step_name = 'Jigs Design'
      WHERE ws.work_start_ts IS NOT NULL
    ),

    -- Compute queue time, apply per-step outlier caps
    queue_valid AS (
      SELECT
        caseId, step_name, product,
        TIMESTAMP_DIFF(work_start_ts, queue_start_ts, SECOND) AS queue_sec
      FROM queue_raw
      WHERE queue_start_ts IS NOT NULL
        AND work_start_ts > queue_start_ts
        AND TIMESTAMP_DIFF(work_start_ts, queue_start_ts, SECOND) >= 1
        AND TIMESTAMP_DIFF(work_start_ts, queue_start_ts, SECOND) <=
            CASE step_name
              WHEN 'Planning' THEN {QT_CAP_PLANNING_SEC}
              ELSE {QT_CAP_DEFAULT_SEC}
            END
    )

    SELECT
      step_name,
      product,
      COUNT(*)                           AS case_count,
      ROUND(AVG(queue_sec / 3600.0), 2)  AS avg_queue_hours,
      ROUND(APPROX_QUANTILES(queue_sec / 3600.0, 100)[OFFSET(50)], 2) AS median_queue_hours
    FROM queue_valid
    GROUP BY step_name, product
    ORDER BY step_name
    """

@app.route("/analytics/report/queue-time", methods=["GET"])
def report_queue_time():
    """
    Returns per-step queue time averages (team-level, no user breakdown).
    Response: { steps: [{step, product, avg_queue_hours, median_queue_hours, case_count}] }
    """
    try:
        query = _build_queue_time_query(request.args)
        df = client.query(query).to_dataframe()

        if df.empty:
            return jsonify({"steps": []})

        # rTSA step order
        rtsa_order = ["Scan Assessment", "Segmentation", "Design Call Prep",
                      "Planning", "Design", "Jigs Design", "Proposed Surgical Plan"]
        # TAR step order
        tar_order = ["Scan Assessment", "Segmentation", "Input Collection",
                     "Planning", "Design", "Proposed Surgical Plan"]

        def safe(v):
            try:
                f = float(v)
                return None if (math.isnan(f) or math.isinf(f)) else round(f, 2)
            except Exception:
                return None

        steps = []
        for _, r in df.iterrows():
            steps.append({
                "step":               str(r["step_name"]),
                "product":            str(r["product"]),
                "avg_queue_hours":    safe(r["avg_queue_hours"]),
                "median_queue_hours": safe(r["median_queue_hours"]),
                "case_count":         int(r["case_count"]),
            })

        # Sort by product-specific workflow order
        def sort_key(s):
            order = rtsa_order if "Shoulder" in s["product"] else tar_order
            try:
                return (s["product"], order.index(s["step"]))
            except ValueError:
                return (s["product"], 99)

        steps.sort(key=sort_key)

        return jsonify({"steps": steps})

    except Exception as e:
        return handle_error(request.endpoint, e)


# ══════════════════════════════════════════════════════════════
# Ported from r3id_app.py — KPI cards, WIP pipeline, submission/completion trends
# These only use helpers already shared between the two apps (build_where_clause,
# case_joins, on_hold_cte, signoff_cte, volume_case_joins, volume_where_clause,
# METRIC_MAP), so no new dependencies were introduced.
# ══════════════════════════════════════════════════════════════

def kpi_or_cases(args, date_field, metric_sql, fetch_cases=False, include_hold_user=False, exclude_outliers=False, avg_digital_lt=None):
    where = build_where_clause(args)
    df_filter = date_range_filter(date_field, args.get('date_from',''), args.get('date_to',''))
    if exclude_outliers:
        where = f"{where} AND {outlier_exclusion_sql(avg_digital_lt)}"
    if fetch_cases:
        hold_cte_str = f", {on_hold_user_cte()}" if include_hold_user else ""
        query = f"""
        WITH {signoff_cte()}, {on_hold_cte()}{hold_cte_str}
        SELECT {case_select_fields(include_hold_user)}
        {case_joins(include_hold_user)}
        WHERE {where} AND {df_filter}
        ORDER BY f.createdAt DESC LIMIT 500
        """
        df = client.query(query).to_dataframe()
        return jsonify({"cases": format_cases(df), "count": len(df)})
    else:
        query = f"""
        WITH {signoff_cte()}, {on_hold_cte()}
        SELECT COUNT(DISTINCT f.id) as case_count, ROUND(AVG({metric_sql}), 1) as avg_lt
        {case_joins()}
        WHERE {where} AND {df_filter}
        """
        df = client.query(query).to_dataframe().fillna(0)
        row = df.iloc[0]
        return jsonify({"case_count": int(row.get('case_count', 0)), "avg_lt": round(float(row.get('avg_lt', 0)), 1)})

@app.route("/analytics/kpi/total", methods=["GET"])
def kpi_total():
    try:
        args = request.args
        return kpi_or_cases(args, 's.ship_wrk_comp_date', METRIC_MAP['total_lt'][0],
            args.get('fetch_cases')=='true', exclude_outliers=args.get('exclude_outliers')=='true',
            avg_digital_lt=float(args.get('avg_digital_lt',0)))
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/kpi/digital", methods=["GET"])
def kpi_digital():
    try:
        args = request.args
        return kpi_or_cases(args, 'sd.first_psp_review_date', METRIC_MAP['digital_lt'][0],
            args.get('fetch_cases')=='true', exclude_outliers=args.get('exclude_outliers')=='true',
            avg_digital_lt=float(args.get('avg_digital_lt',0)))
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/kpi/seg", methods=["GET"])
def kpi_seg():
    try:
        args = request.args
        return kpi_or_cases(args, 'sd.seg_review_date', METRIC_MAP['seg_lt'][0],
            args.get('fetch_cases')=='true', exclude_outliers=args.get('exclude_outliers')=='true',
            avg_digital_lt=float(args.get('avg_digital_lt',0)))
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/kpi/surgeon", methods=["GET"])
def kpi_surgeon():
    try:
        args = request.args
        return kpi_or_cases(args, 'sd.surgeon_approval_date', METRIC_MAP['surgeon_lt'][0],
            args.get('fetch_cases')=='true', exclude_outliers=args.get('exclude_outliers')=='true',
            avg_digital_lt=float(args.get('avg_digital_lt',0)))
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/kpi/onhold", methods=["GET"])
def kpi_onhold():
    try:
        args = request.args
        fetch = args.get('fetch_cases') == 'true'
        where = build_where_clause(args) + " AND f.onHold = true"
        if fetch:
            query = f"WITH {signoff_cte()}, {on_hold_cte()}, {on_hold_user_cte()} SELECT {case_select_fields(True)} {case_joins(True)} WHERE {where} ORDER BY oh.total_hold_days DESC LIMIT 500"
            df = client.query(query).to_dataframe()
            return jsonify({"cases": format_cases(df), "count": len(df)})
        else:
            query = f"WITH {signoff_cte()}, {on_hold_cte()} SELECT COUNT(DISTINCT f.id) as case_count {case_joins()} WHERE {where}"
            df = client.query(query).to_dataframe().fillna(0)
            return jsonify({"case_count": int(df.iloc[0].get('case_count', 0))})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/wip", methods=["GET"])
def analytics_wip():
    try:
        args = request.args
        where = build_where_clause(args)
        fetch_step = args.get('fetch_step')
        # Cases where Design Call Prep REVIEWER signoff exists AND case sits in PLANNING_REVIEW work queue
        # are awaiting surgeon — they should NOT appear in CAD Design WIP, but should appear in Surgeon Approval.
        dcp_done_cte = f"""
        design_call_prep_done AS (
            SELECT DISTINCT w.caseId
            FROM {tbl('WorkModuleSignoff')} w
            LEFT JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
            LEFT JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
            WHERE w.deleted = false
              AND wm.name = 'Design Call Prep'
              AND w.workModuleUserType = 'REVIEWER'
              AND w.workModuleSignatureType = 'ACCEPT'
        )"""
        # In the CAD Design (psp_design) bucket: exclude cases where Design Call Prep is done AND case is in PLANNING_REVIEW.
        # Those cases have left CAD's hands and are waiting on surgeon — they belong in surgeon_approval instead.
        awaiting_surgeon_predicate = "(f.id IN (SELECT caseId FROM design_call_prep_done) AND f.work_queue = 'PLANNING_REVIEW')"
        # Implicit-PSP rule: a case is considered "past PSP" if any of these exist:
        #   - first_psp_review_date (formal PSP REVIEWER ACCEPT)
        #   - peer_review_date (completed peer review without formal PSP signoff)
        #   - ship_wrk_comp_date (shipped without formal PSP signoff)
        past_psp = "(sd.first_psp_review_date IS NOT NULL OR sd.peer_review_date IS NOT NULL OR s.ship_wrk_comp_date IS NOT NULL)"
        # Marketing exclusion — applies to Peer Review and Manufacturing WIP.
        # A case is a marketing case if 'marketing' appears anywhere in the surgeon name (case-insensitive).
        not_marketing = "(LOWER(COALESCE(f.phy_nameFirst,'') || ' ' || COALESCE(f.phy_nameLast,'')) NOT LIKE '%marketing%')"
        STEP_FILTERS = {
            'segmentation':
                f"s.first_scan_upload_date IS NOT NULL AND sd.seg_review_date IS NULL AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY')",
            'psp_design':
                f"sd.seg_review_date IS NOT NULL AND NOT {past_psp} AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY') AND NOT {awaiting_surgeon_predicate}"
                f" OR (sd.first_psp_review_date IS NOT NULL AND sd.surgeon_approval_date IS NULL AND sd.surgeon_last_action_reject = true AND sd.peer_review_date IS NULL AND s.ship_wrk_comp_date IS NULL AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY'))",
            'surgeon_approval':
                f"(sd.first_psp_review_date IS NOT NULL AND sd.surgeon_approval_date IS NULL AND COALESCE(sd.surgeon_last_action_reject, false) = false AND sd.peer_review_date IS NULL AND s.ship_wrk_comp_date IS NULL AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY')) OR (sd.seg_review_date IS NOT NULL AND NOT {past_psp} AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY') AND {awaiting_surgeon_predicate})",
            'peer_review':
                # Exclude: awaiting surgeon approval (already in surgeon bucket)
                # Exclude: in MANUFACTURING phase (already in manufacturing bucket)
                f"{past_psp} AND sd.peer_review_date IS NULL AND s.ship_wrk_comp_date IS NULL"
                f" AND f.onHold = false AND f.canceled = false"
                f" AND f.phase NOT IN ('SHIPPING','SURGERY','MANUFACTURING')"
                f" AND NOT (sd.first_psp_review_date IS NOT NULL AND sd.surgeon_approval_date IS NULL AND COALESCE(sd.surgeon_last_action_reject, false) = false)"
                f" AND {not_marketing}",
            'manufacturing':
                f"f.phase = 'MANUFACTURING' AND f.onHold = false AND f.canceled = false AND s.ship_wrk_comp_date IS NULL AND {not_marketing}",
        }
        if fetch_step:
            step_filter = STEP_FILTERS.get(fetch_step, "1=1")
            query = f"""
            WITH {signoff_cte()}, {on_hold_cte()}, {dcp_done_cte}
            SELECT {case_select_fields()}
            {case_joins()}
            WHERE {where} AND ({step_filter})
            ORDER BY f.createdAt DESC LIMIT 500
            """
            df = client.query(query).to_dataframe()
            return jsonify({"cases": format_cases(df), "count": len(df), "step": fetch_step})
        query = f"""
        WITH {signoff_cte()}, {on_hold_cte()}, {dcp_done_cte}
        SELECT
            COUNTIF(s.first_scan_upload_date IS NOT NULL AND sd.seg_review_date IS NULL
                AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY')) as segmentation,
            COUNTIF(
                (sd.seg_review_date IS NOT NULL
                AND NOT (sd.first_psp_review_date IS NOT NULL OR sd.peer_review_date IS NOT NULL OR s.ship_wrk_comp_date IS NOT NULL)
                AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY')
                AND NOT {awaiting_surgeon_predicate})
                OR
                (sd.first_psp_review_date IS NOT NULL AND sd.surgeon_approval_date IS NULL
                AND COALESCE(sd.surgeon_last_action_reject, false) = true
                AND sd.peer_review_date IS NULL AND s.ship_wrk_comp_date IS NULL
                AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY'))
            ) as psp_design,
            COUNTIF(
                (sd.first_psp_review_date IS NOT NULL AND sd.surgeon_approval_date IS NULL
                    AND COALESCE(sd.surgeon_last_action_reject, false) = false
                    AND sd.peer_review_date IS NULL AND s.ship_wrk_comp_date IS NULL
                    AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY'))
                OR
                (sd.seg_review_date IS NOT NULL
                    AND NOT (sd.first_psp_review_date IS NOT NULL OR sd.peer_review_date IS NOT NULL OR s.ship_wrk_comp_date IS NOT NULL)
                    AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY')
                    AND {awaiting_surgeon_predicate})
            ) as surgeon_approval,
            COUNTIF((sd.first_psp_review_date IS NOT NULL OR sd.peer_review_date IS NOT NULL OR s.ship_wrk_comp_date IS NOT NULL)
                AND sd.peer_review_date IS NULL AND s.ship_wrk_comp_date IS NULL
                AND f.onHold = false AND f.canceled = false
                AND f.phase NOT IN ('SHIPPING','SURGERY','MANUFACTURING')
                AND NOT (sd.first_psp_review_date IS NOT NULL AND sd.surgeon_approval_date IS NULL AND COALESCE(sd.surgeon_last_action_reject, false) = false)
                AND (LOWER(COALESCE(f.phy_nameFirst,'') || ' ' || COALESCE(f.phy_nameLast,'')) NOT LIKE '%marketing%')) as peer_review,
            COUNTIF(f.phase = 'MANUFACTURING' AND f.onHold = false AND f.canceled = false AND s.ship_wrk_comp_date IS NULL
                AND (LOWER(COALESCE(f.phy_nameFirst,'') || ' ' || COALESCE(f.phy_nameLast,'')) NOT LIKE '%marketing%')) as manufacturing
        {case_joins()}
        WHERE {where}
        """
        df = client.query(query).to_dataframe().fillna(0)
        row = df.iloc[0]
        return jsonify({
            "segmentation": int(row.get('segmentation', 0)),
            "psp_design": int(row.get('psp_design', 0)),
            "surgeon_approval": int(row.get('surgeon_approval', 0)),
            "peer_review": int(row.get('peer_review', 0)),
            "manufacturing": int(row.get('manufacturing', 0)),
        })
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/trends", methods=["GET"])
def analytics_trends():
    try:
        args = request.args
        metric = args.get('metric', 'volume')
        granularity = args.get('granularity', 'monthly')
        exclude_outliers = args.get('exclude_outliers') == 'true'
        avg_digital_lt = float(args.get('avg_digital_lt', 0))
        trunc = 'WEEK' if granularity == 'weekly' else 'MONTH'
        # breakdown_by: 'product' (default) or 'case_type'.
        # When 'case_type', group by ct.name / f.case_type_name instead of product.
        breakdown_by = args.get('breakdown_by', 'product').strip().lower()
        use_case_type = breakdown_by == 'case_type'

        if metric == 'volume':
            # Use raw Case table to include COMPLETED cases
            where = volume_where_clause(args)
            # date_field can be overridden by caller:
            #   first_scan_upload_date → Submitted (scan upload)
            #   first_psp_review_date  → Completed (PSP review) [default, uses effective PSP = LEAST(psp, peer review, ship)]
            date_field_param = args.get('date_field', '').strip()
            if date_field_param == 'first_scan_upload_date':
                date_field = 's.first_scan_upload_date'
            else:
                # PSP reviewer accept date only — matches stat card and process counts definition
                date_field = "CAST(sd.first_psp_review_date AS DATETIME)"
            df_filter = date_range_filter(date_field, args.get('date_from',''), args.get('date_to',''))
            # Also require PSP date is not null for completed bar
            if date_field_param != 'first_scan_upload_date':
                df_filter += " AND sd.first_psp_review_date IS NOT NULL"
            # Group by case type or product
            group_field = "ct.name" if use_case_type else "cc.name"
            query = f"""
            WITH {signoff_cte()}
            SELECT DATE_TRUNC(DATE(CAST({date_field} AS DATETIME)), {trunc}) as period,
                {group_field} as product, COUNT(DISTINCT f.id) as value
            {volume_case_joins()}
            WHERE {where} AND {df_filter}
            GROUP BY period, product ORDER BY period ASC
            """
        else:
            where = build_where_clause(args)
            if exclude_outliers:
                where = f"{where} AND {outlier_exclusion_sql(avg_digital_lt)}"
            if metric in METRIC_MAP:
                date_field = METRIC_MAP[metric][2]
                agg_expr = f"ROUND(AVG({METRIC_MAP[metric][0]}), 1)"
            else:
                date_field = 'sd.first_psp_review_date'
                agg_expr = 'COUNT(DISTINCT f.id)'
            df_filter = date_range_filter(date_field, args.get('date_from',''), args.get('date_to',''))
            # Group by case type or product
            group_field = "f.case_type_name" if use_case_type else "f.case_category_name"
            query = f"""
            WITH {signoff_cte()}, {on_hold_cte()}
            SELECT DATE_TRUNC(DATE(CAST({date_field} AS DATETIME)), {trunc}) as period,
                {group_field} as product, {agg_expr} as value
            {case_joins()}
            WHERE {where} AND {df_filter}
            GROUP BY period, product ORDER BY period ASC
            """

        df = client.query(query).to_dataframe()
        if df.empty:
            return jsonify({'periods': [], 'datasets': [], 'metric': metric, 'granularity': granularity})
        df = df.astype(str)
        periods = sorted(df['period'].unique().tolist())
        products = sorted(df['product'].unique().tolist())
        colors = ['#0284c7','#16a34a','#7c3aed','#d97706','#0891b2','#dc2626','#059669','#9333ea']
        datasets = []
        for i, product in enumerate(products):
            pdf = df[df['product'] == product]
            period_map = dict(zip(pdf['period'].tolist(), pdf['value'].tolist()))
            data = []
            for p in periods:
                try: data.append(float(period_map.get(p, 0) or 0))
                except: data.append(0)
            datasets.append({'label': shorten_product(product), 'full_label': product, 'data': data, 'color': colors[i % len(colors)]})
        return jsonify({'periods': periods, 'datasets': datasets, 'metric': metric, 'granularity': granularity})
    except Exception as e:
        return handle_error(request.endpoint, e)



# ══════════════════════════════════════════════════════════════
# Ported from r3id_app.py — Process Efficiency by Step + Exclusion Breakdown by Engineer
# ══════════════════════════════════════════════════════════════

_EFF_STEPS   = ['Scan Assessment','Segmentation','Jigs Design','Planning','Design','Proposed Surgical Plan']
_EFF_MIN_SEC_SCAN = 120   # 2 min floor for Scan Assessment
_EFF_MIN_SEC      = 600   # 10 min floor for all other steps
_EFF_MAX_SEC = 28800
_EFF_LOOKBACK = 30

def _eff_case_where(args):
    product_filter = args.get('product','').strip()
    product_group  = args.get('product_group','').strip()
    conds = ["f.deleted = false","f.canceled = false"]
    if product_filter:
        prods = [p.strip() for p in product_filter.split(',')]
        joined = "','".join(prods)
        conds.append("cc.name IN ('" + joined + "')")
    elif product_group:
        all_prods = []
        for g in [g.strip() for g in product_group.split(',')]:
            all_prods.extend(PRODUCT_GROUPS.get(g,[]))
        if all_prods:
            joined = "','".join(all_prods)
            conds.append("cc.name IN ('" + joined + "')")
    else:
        conds.append("cc.name IN ('Reverse Total Shoulder Arthroplasty','Total Ankle Replacement')")
    return " AND ".join(conds)

def _eff_date_filter(args, ts_expr):
    date_from = args.get('date_from','').strip()
    date_to   = args.get('date_to','').strip()
    parts = []
    if date_from:
        validate_date(date_from)
        parts.append(f"DATE({ts_expr}) >= '{date_from}'")
    if date_to:
        validate_date(date_to)
        parts.append(f"DATE({ts_expr}) <= '{date_to}'")
    return ("AND " + " AND ".join(parts)) if parts else ""

def _eff_valid_steps(args):
    step_filter = args.get('step','').strip()
    if step_filter:
        steps = [s.strip() for s in step_filter.split(',')]
        return [s for s in steps if s in _EFF_STEPS] or _EFF_STEPS
    return _EFF_STEPS

def _eff_core_query(args):
    """Returns the core classified CTE as a SQL string."""
    case_where  = _eff_case_where(args)
    valid_steps = _eff_valid_steps(args)
    steps_sql   = "','".join(valid_steps)
    ist_sig     = "TIMESTAMP_ADD(sig.createdAt, INTERVAL 330 MINUTE)"
    date_filter = _eff_date_filter(args, ist_sig)

    # User filter — step_user contains full names matching User table nameFirst + nameLast
    user_filter = args.get('step_user','').strip()
    if user_filter:
        users = [u.strip() for u in user_filter.split(',')]
        joined_users = "','".join(users)
        user_having = "AND TRIM(CONCAT(COALESCE(u.nameFirst,''),' ',COALESCE(u.nameLast,''))) IN ('" + joined_users + "')"
    else:
        user_having = ""

    return f"""
    cases AS (
      SELECT f.id AS caseId, cc.name AS product,
             f.laterality AS laterality, c2.preoperativeState AS preoperativeState,
             f.proposedIndication AS proposedIndication, f.designNotes AS designNotes
      FROM {tbl('vw_fact_case')} f
      JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
      LEFT JOIN {tbl('Case')} c2 ON f.id = c2.id
      WHERE {case_where}
    ),
    -- All passes for Planning TAR with actual seconds pre-computed
    planning_tar_passes AS (
      SELECT
        sig.refId AS caseId, sig.userId, sig.createdAt AS end_time,
        'Planning' AS step_name, c_filter.product AS product,
        c_filter.laterality AS laterality, c_filter.preoperativeState AS preoperativeState,
        c_filter.proposedIndication AS proposedIndication, c_filter.designNotes AS designNotes,
        TIMESTAMP_DIFF(sig.createdAt,
          MAX(asn.createdAt) OVER (
            PARTITION BY sig.refId, sig.userId
            ORDER BY sig.createdAt
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ), SECOND
        ) AS actual_sec_raw
      FROM {_LOG} sig
      INNER JOIN cases c_filter ON sig.refId = c_filter.caseId
      LEFT JOIN {_LOG} asn
        ON asn.refId = sig.refId
        AND asn.userId = sig.userId
        AND asn.type = 'r3idCaseRoleAssignmentUpdate'
        AND asn.createdAt <= sig.createdAt
        AND asn.createdAt >= TIMESTAMP_SUB(sig.createdAt, INTERVAL {_EFF_LOOKBACK} DAY)
      WHERE sig.type = 'r3idWorkModuleSignoff-Planning-WORKER-ACCEPT'
        AND c_filter.product = 'Total Ankle Replacement'
        {date_filter}
    ),
    -- For each case+user: pick the pass closest to standard (150 min = 9000 sec)
    -- On tie, pick the slower one (higher actual_sec)
    planning_tar_best AS (
      SELECT *
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY caseId, userId
            ORDER BY
              ABS(actual_sec_raw - 9000) ASC,  -- closest to 150 min
              actual_sec_raw DESC               -- on tie, slower
          ) AS rn
        FROM planning_tar_passes
        WHERE actual_sec_raw IS NOT NULL
      )
      WHERE rn = 1
        -- Only include if within 50%-150% of standard (75-225 min = 4500-13500 sec)
        AND actual_sec_raw BETWEEN 4500 AND 18000
    ),
    signoffs_raw AS (
      SELECT
        sig.refId AS caseId, sig.userId, sig.createdAt AS end_time,
        REGEXP_EXTRACT(sig.type,
          r'r3idWorkModuleSignoff-(.+)-(?:WORKER|REVIEWER|APPROVER)-ACCEPT'
        ) AS step_name,
        ROW_NUMBER() OVER (
          PARTITION BY sig.refId,
            REGEXP_EXTRACT(sig.type,
              r'r3idWorkModuleSignoff-(.+)-(?:WORKER|REVIEWER|APPROVER)-ACCEPT'
            )
          ORDER BY sig.createdAt DESC
        ) AS rn
      FROM {_LOG} sig
      INNER JOIN cases c_filter ON sig.refId = c_filter.caseId
      WHERE sig.type LIKE 'r3idWorkModuleSignoff-%-WORKER-ACCEPT'
        AND sig.type != 'r3idWorkModuleSignoff-Planning-WORKER-ACCEPT'
        {date_filter}
    ),
    signoffs AS (
      SELECT signoffs_raw.caseId, signoffs_raw.userId, signoffs_raw.end_time,
             signoffs_raw.step_name, c_filter2.product,
             c_filter2.laterality, c_filter2.preoperativeState,
             c_filter2.proposedIndication, c_filter2.designNotes
      FROM signoffs_raw
      JOIN cases c_filter2 ON signoffs_raw.caseId = c_filter2.caseId
      WHERE signoffs_raw.rn = 1 AND signoffs_raw.step_name IN ('{steps_sql}')
      UNION ALL
      -- Add Planning TAR best passes (already filtered to qualified range)
      SELECT caseId, userId, end_time, step_name, product,
             laterality, preoperativeState, proposedIndication, designNotes
      FROM planning_tar_best
      WHERE 'Planning' IN ('{steps_sql}')
    ),
    paired AS (
      SELECT
        s.caseId, s.userId, s.step_name, s.end_time, s.product,
        s.laterality, s.preoperativeState, s.proposedIndication, s.designNotes,
        MAX(asn.createdAt) AS start_time
      FROM signoffs s
      LEFT JOIN {_LOG} asn
        ON asn.refId = s.caseId
        AND asn.userId = s.userId
        AND asn.type = 'r3idCaseRoleAssignmentUpdate'
        AND asn.createdAt <= s.end_time
        AND asn.createdAt >= TIMESTAMP_SUB(s.end_time, INTERVAL {_EFF_LOOKBACK} DAY)
      GROUP BY s.caseId, s.userId, s.step_name, s.end_time, s.product,
               s.laterality, s.preoperativeState, s.proposedIndication, s.designNotes
    ),
    classified AS (
      SELECT
        p.caseId, p.userId, p.step_name, p.end_time, p.product,
        p.laterality, p.preoperativeState, p.proposedIndication, p.designNotes,
        TRIM(CONCAT(COALESCE(u.nameFirst,''),' ',COALESCE(u.nameLast,''))) AS worker_name,
        CASE
          -- Planning TAR: already validated in planning_tar_best, always valid
          WHEN p.step_name = 'Planning' AND p.product = 'Total Ankle Replacement'
            THEN 'valid'
          WHEN p.start_time IS NULL THEN 'no_pair'
          WHEN p.step_name = 'Scan Assessment'
               AND TIMESTAMP_DIFF(p.end_time, p.start_time, SECOND) < {_EFF_MIN_SEC_SCAN} THEN 'click_through'
          WHEN p.step_name != 'Scan Assessment'
               AND TIMESTAMP_DIFF(p.end_time, p.start_time, SECOND) < {_EFF_MIN_SEC} THEN 'click_through'
          WHEN TIMESTAMP_DIFF(p.end_time, p.start_time, SECOND) > {_EFF_MAX_SEC} THEN 'over_standard'
          ELSE 'valid'
        END AS pair_status,
        CASE
          -- Planning TAR: use pre-computed actual_sec from planning_tar_best
          WHEN p.step_name = 'Planning' AND p.product = 'Total Ankle Replacement'
            THEN pt.actual_sec_raw
          ELSE TIMESTAMP_DIFF(p.end_time, p.start_time, SECOND)
        END AS actual_sec
      FROM paired p
      LEFT JOIN {tbl('User')} u ON p.userId = u.id
      LEFT JOIN planning_tar_best pt
        ON pt.caseId = p.caseId AND pt.userId = p.userId
      WHERE 1=1 {user_having}
    )"""

@app.route("/analytics/process-efficiency", methods=["GET"])
def process_efficiency():
    """Process efficiency: standard_time / actual_time * 100%.
    Workers only. Last signoff per (caseId, step). Excludes <2min and >8hr pairs.
    """
    try:
        args = request.args
        granularity = args.get('granularity','daily')
        ist_sig = "TIMESTAMP_ADD(end_time, INTERVAL 330 MINUTE)"
        if granularity == 'daily':
            period_expr = f"DATE({ist_sig})"
        elif granularity == 'monthly':
            period_expr = f"FORMAT_DATE('%Y-%m', {ist_sig})"
        else:
            period_expr = f"FORMAT_DATE('%G-W%V', {ist_sig})"

        core = _eff_core_query(args)
        query = f"""
        WITH {core},
        valid_only AS (
          SELECT *, {period_expr} AS period
          FROM classified WHERE pair_status = 'valid'
        )
        SELECT
          worker_name, step_name, product, period, caseId,
          laterality, preoperativeState, proposedIndication, designNotes,
          actual_sec / 60.0 AS actual_min
        FROM valid_only
        ORDER BY worker_name, step_name, period
        """

        df = client.query(query).to_dataframe()
        if df.empty:
            return jsonify({"users":[],"team_avg":0,"granularity":granularity,"periods":[]})

        # Bilateral/revision multiplier computed per case (not per aggregated group —
        # a worker/step/period bucket can contain a mix of bilateral and non-bilateral
        # cases, so the multiplier has to be applied before any averaging happens).
        df['time_multiplier'] = df.apply(
            lambda r: get_case_time_multiplier(
                r['laterality'], r['preoperativeState'], r['proposedIndication'], r['designNotes']
            ), axis=1
        )
        df['standard_min'] = df.apply(
            lambda r: get_design_time(r['step_name'], 'WORKER', r['product'], r['time_multiplier']) or 0, axis=1
        )
        df_valid = df[df['standard_min'] > 0].copy()
        df_valid['eff'] = df_valid['standard_min'] / df_valid['actual_min'] * 100
        # Exclude rows where efficiency > 200% of what's expected — the cap itself
        # scales with the same bilateral/revision multiplier as standard_min, so a
        # doubled-standard case isn't unfairly capped at the same flat threshold.
        df_valid = df_valid[df_valid['eff'] <= 200.0 * df_valid['time_multiplier']]

        # Per-user weighted efficiency (each row is now one case, so this is a
        # straight sum of standard vs actual minutes across all its cases)
        user_eff = df_valid.groupby('worker_name').apply(
            lambda g: round(g['standard_min'].sum() / g['actual_min'].sum() * 100, 1)
        ).reset_index()
        user_eff.columns = ['userId','efficiency']
        user_eff = user_eff.sort_values('efficiency', ascending=False)

        total_std = df_valid['standard_min'].sum()
        total_act = df_valid['actual_min'].sum()
        team_avg  = round(total_std / total_act * 100, 1) if total_act > 0 else 0

        periods_sorted = sorted(df_valid['period'].unique().tolist())
        period_data = []
        for p in periods_sorted:
            p_df = df_valid[df_valid['period'] == str(p)]
            p_std = p_df['standard_min'].sum()
            p_act = p_df['actual_min'].sum()
            period_data.append({'period': str(p), 'efficiency': round(p_std/p_act*100,1) if p_act>0 else 0})

        return jsonify({
            "users": [{'userId':r['userId'],'efficiency':float(r['efficiency'])} for _,r in user_eff.iterrows()],
            "team_avg": team_avg,
            "granularity": granularity,
            "periods": period_data
        })
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/process-efficiency/exclusions", methods=["GET"])
def process_efficiency_exclusions():
    """Counts of excluded signoffs by reason, grouped by user and step.
    All cases appear in either efficiency chart or here.
    Planning TAR cases with no qualifying pass shown as out_of_range.
    """
    try:
        args = request.args
        case_where  = _eff_case_where(args)
        valid_steps = _eff_valid_steps(args)
        steps_sql   = "\',\'".join(valid_steps)
        ist_sig     = "TIMESTAMP_ADD(sig.createdAt, INTERVAL 330 MINUTE)"
        date_filter = _eff_date_filter(args, ist_sig)
        user_filter = args.get('step_user','').strip()
        if user_filter:
            users = [u.strip() for u in user_filter.split(',')]
            joined_users = "\',\'".join(users)
            user_having = "AND TRIM(CONCAT(COALESCE(u.nameFirst,\'\'),' ',COALESCE(u.nameLast,\'\'))) IN (\'" + joined_users + "\')"
        else:
            user_having = ""

        # Part 1: standard exclusions from classified CTE
        core = _eff_core_query(args)
        std_query = f"""
        WITH {core}
        SELECT worker_name, step_name, pair_status AS excl_reason, COUNT(*) AS cnt
        FROM classified
        WHERE pair_status != 'valid'
        GROUP BY worker_name, step_name, pair_status
        """
        df_std = client.query(std_query).to_dataframe()

        # Part 2: Planning TAR cases excluded because no pass fell in 75-225 min window
        df_tar = pd.DataFrame()
        if 'Planning' in valid_steps:
            # User filter for this query uses worker_name (already resolved), not u.nameFirst
            if user_filter:
                users = [u.strip() for u in user_filter.split(',')]
                joined_users = "','".join(users)
                tar_user_having = "AND worker_name IN ('" + joined_users + "')"
            else:
                tar_user_having = ""

            planning_tar_excl_query = f"""
            WITH
            cases AS (
              SELECT f.id AS caseId
              FROM {tbl('vw_fact_case')} f
              JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
              WHERE {case_where} AND cc.name = 'Total Ankle Replacement'
            ),
            all_passes AS (
              SELECT
                sig.refId AS caseId,
                sig.userId,
                TRIM(CONCAT(COALESCE(u.nameFirst,''),' ',COALESCE(u.nameLast,''))) AS worker_name,
                TIMESTAMP_DIFF(sig.createdAt, MAX(asn.createdAt), SECOND) AS actual_sec
              FROM {_LOG} sig
              INNER JOIN cases c ON sig.refId = c.caseId
              LEFT JOIN {_LOG} asn
                ON asn.refId = sig.refId
                AND asn.userId = sig.userId
                AND asn.type = 'r3idCaseRoleAssignmentUpdate'
                AND asn.createdAt <= sig.createdAt
                AND asn.createdAt >= TIMESTAMP_SUB(sig.createdAt, INTERVAL {_EFF_LOOKBACK} DAY)
              LEFT JOIN {tbl('User')} u ON sig.userId = u.id
              WHERE sig.type = 'r3idWorkModuleSignoff-Planning-WORKER-ACCEPT'
                {date_filter}
              GROUP BY sig.refId, sig.userId, sig.createdAt, u.nameFirst, u.nameLast
            ),
            best_pass AS (
              SELECT caseId, userId, worker_name, actual_sec,
                ROW_NUMBER() OVER (
                  PARTITION BY caseId, userId
                  ORDER BY ABS(actual_sec - 9000) ASC, actual_sec DESC
                ) AS rn
              FROM all_passes
              WHERE actual_sec IS NOT NULL
            ),
            excluded_cases AS (
              SELECT caseId, userId, worker_name,
                CASE
                  WHEN actual_sec < {_EFF_MIN_SEC} THEN 'click_through'
                  ELSE 'over_standard'
                END AS excl_reason
              FROM best_pass
              WHERE rn = 1
                AND NOT (actual_sec BETWEEN 4500 AND 18000)
            )
            SELECT worker_name, 'Planning' AS step_name, excl_reason, COUNT(*) AS cnt
            FROM excluded_cases
            WHERE 1=1 {tar_user_having}
            GROUP BY worker_name, excl_reason
            """
            df_tar = client.query(planning_tar_excl_query).to_dataframe()

        df = pd.concat([df_std, df_tar], ignore_index=True) if not df_tar.empty else df_std

        # Total = classified rows + Planning TAR excluded cases
        core2 = _eff_core_query(args)
        tot_q = f"WITH {core2} SELECT COUNT(*) AS n FROM classified"
        tot_df = client.query(tot_q).to_dataframe()
        classified_count = int(tot_df['n'].iloc[0]) if not tot_df.empty else 0
        tar_excl_count = int(df_tar['cnt'].sum()) if not df_tar.empty else 0
        total_signoffs = classified_count + tar_excl_count

        by_user, by_step = {}, {}
        total_excluded = 0
        for _, row in df.iterrows():
            wn   = (row['worker_name'] or 'Unknown').strip()
            step = row['step_name']
            ps   = row['excl_reason']
            cnt  = int(row['cnt'])
            total_excluded += cnt
            if wn not in by_user:
                by_user[wn] = {'label':wn,'click_through':0,'over_standard':0,'no_pair':0}
            by_user[wn][ps] = by_user[wn].get(ps,0) + cnt
            if step not in by_step:
                by_step[step] = {'label':step,'click_through':0,'over_standard':0,'no_pair':0}
            by_step[step][ps] = by_step[step].get(ps,0) + cnt

        sort_key = lambda x: sum([x.get(k,0) for k in ['click_through','over_standard','no_pair']])
        return jsonify({
            "total_excluded": total_excluded,
            "total_signoffs": total_signoffs,
            "by_user":  sorted(by_user.values(),  key=sort_key, reverse=True),
            "by_step":  sorted(by_step.values(),  key=sort_key, reverse=True),
        })
    except Exception as e:
        return handle_error(request.endpoint, e)



# ══════════════════════════════════════════════════════════════
# Camstar cross-service proxy — forwards to the already-deployed camstar-app
# instead of connecting to Postgres directly, keeping this app's clean
# BigQuery-only dependency profile (no psycopg2). camstar_app.py also exposes
# debug/write routes (wip_v2/_debug*, _fix_hip_anchor) that must NOT be
# reachable through here, so this is an explicit whitelist, not a catch-all.
# ══════════════════════════════════════════════════════════════

CAMSTAR_SERVICE_URL = os.environ.get("CAMSTAR_SERVICE_URL", "").rstrip("/")

def _camstar_proxy(path):
    """Forwards the incoming request's query string to camstar-app and
    returns its JSON response verbatim (same route names/params, so no
    field-name translation needed — response shapes already match what
    report_generator.html's existing JS expects)."""
    if not CAMSTAR_SERVICE_URL:
        return jsonify({"error": "CAMSTAR_SERVICE_URL not configured"}), 500
    try:
        resp = requests.get(f"{CAMSTAR_SERVICE_URL}{path}", params=request.args, timeout=30)
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.error(f"Camstar proxy error ({path}): {e}")
        return jsonify({"error": f"Camstar service unreachable: {e}"}), 502
    except ValueError as e:
        logger.error(f"Camstar proxy non-JSON response ({path}): {e}")
        return jsonify({"error": "Camstar service returned an invalid response"}), 502

@app.route("/analytics/camstar/teams", methods=["GET"])
def camstar_teams_proxy():
    return _camstar_proxy("/analytics/camstar/teams")

@app.route("/analytics/camstar/products", methods=["GET"])
def camstar_products_proxy():
    return _camstar_proxy("/analytics/camstar/products")

@app.route("/analytics/camstar/steps", methods=["GET"])
def camstar_steps_proxy():
    return _camstar_proxy("/analytics/camstar/steps")

@app.route("/analytics/camstar/users", methods=["GET"])
def camstar_users_proxy():
    return _camstar_proxy("/analytics/camstar/users")

@app.route("/analytics/camstar/volume", methods=["GET"])
def camstar_volume_proxy():
    return _camstar_proxy("/analytics/camstar/volume")

@app.route("/analytics/camstar/utilization", methods=["GET"])
def camstar_utilization_proxy():
    return _camstar_proxy("/analytics/camstar/utilization")

@app.route("/analytics/camstar/fpy", methods=["GET"])
def camstar_fpy_proxy():
    return _camstar_proxy("/analytics/camstar/fpy")

@app.route("/analytics/camstar/otd", methods=["GET"])
def camstar_otd_proxy():
    return _camstar_proxy("/analytics/camstar/otd")

@app.route("/analytics/camstar/wip", methods=["GET"])
def camstar_wip_proxy():
    return _camstar_proxy("/analytics/camstar/wip")

@app.route("/analytics/camstar/process-counts", methods=["GET"])
def camstar_process_counts_proxy():
    return _camstar_proxy("/analytics/camstar/process-counts")

@app.route("/analytics/camstar/lt-by-step", methods=["GET"])
def camstar_lt_by_step_proxy():
    return _camstar_proxy("/analytics/camstar/lt-by-step")

@app.route("/analytics/camstar/volume/submitted", methods=["GET"])
def camstar_volume_submitted_proxy():
    return _camstar_proxy("/analytics/camstar/volume/submitted")

@app.route("/analytics/camstar/wip-trend", methods=["GET"])
def camstar_wip_trend_proxy():
    return _camstar_proxy("/analytics/camstar/wip-trend")


@app.route("/analytics/camstar/forecast", methods=["GET"])
def camstar_forecast_proxy():
    """Proxies to camstar-app's own /analytics/forecast (note: different path
    on that end). Deliberately namespaced under /analytics/camstar/ here
    rather than reusing the bare /analytics/forecast path, since that same
    URL is also used by the BQ side of Report Generator (still unbuilt) with
    a completely different product-group taxonomy — proxying the shared path
    directly would risk a future BQ-side forecast call silently resolving
    against Camstar's CAMSTAR_PRODUCT_GROUPS instead of BQ's own groups."""
    return _camstar_proxy("/analytics/forecast")


# ── Camstar Genie stat tiles / charts (Identity CR MA vs KA, Fixation) ──
# Added at the bottom, alongside the rest of the Camstar whitelist, so the
# existing proxy block above is untouched. Proxied through _camstar_proxy(),
# same as every other route in this whitelist — no separate Postgres
# connection, no new dependency, no new Railway variable needed.

@app.route("/analytics/camstar/breakdown/ka-ma", methods=["GET"])
def camstar_breakdown_ka_ma_proxy():
    return _camstar_proxy("/analytics/camstar/breakdown/ka-ma")

@app.route("/analytics/camstar/breakdown/fixation", methods=["GET"])
def camstar_breakdown_fixation_proxy():
    return _camstar_proxy("/analytics/camstar/breakdown/fixation")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)

