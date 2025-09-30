# app.py
# =============================================================================
# Crossref Preprints Explorer — Live Mode + Cancel + Checkpoints (FIXED)
# =============================================================================
# Adds missing sidebar toggles:
#   • Require ALL predicates
#   • Preprint only
# And threads them through counts/preview/live harvest/presets.
# =============================================================================

import os
import io
import json
import time
from datetime import datetime, date, timedelta
from typing import List, Dict

import pandas as pd
import requests
import streamlit as st

# -----------------------------------------------------------------------------
# App Config & Theme
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Crossref Preprints Explorer",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
section.main > div { padding-top: 1rem; }
.block-container { padding-top: 1rem; }
div[data-testid="stMetricValue"] { font-size: 1.4rem; }
.small-muted { color:#6b6b6b; font-size:0.9rem; }
footer {visibility: hidden;}
.stButton>button { border-radius: 8px; padding: 0.5rem 1rem; }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Globals
# -----------------------------------------------------------------------------
CROSSREF_WORKS = "https://api.crossref.org/works"
DEFAULT_MAILTO = os.getenv("CROSSREF_MAILTO", "your.email@example.com")
UA_TEMPLATE = "Crossref-PreprintHarvester/4.0 (mailto:{})"
UA = UA_TEMPLATE.format(DEFAULT_MAILTO)

# Cancel flag
if "stop_requested" not in st.session_state:
    st.session_state.stop_requested = False

def _request_stop():
    st.session_state.stop_requested = True

def _clear_stop():
    st.session_state.stop_requested = False

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _set_mailto(mailto: str):
    global UA
    UA = UA_TEMPLATE.format(mailto)

def _normalize_window(start_iso: str, end_iso: str):
    s = datetime.fromisoformat(start_iso).replace(hour=0, minute=0, second=0, microsecond=0)
    e = datetime.fromisoformat(end_iso).replace(hour=23, minute=59, second=59, microsecond=0)
    return s.strftime("%Y-%m-%dT%H:%M:%S"), e.strftime("%Y-%m-%dT%H:%M:%S")

def _date_from_parts(d):
    try:
        parts = (d or {}).get("date-parts", [[]])[0]
        if not parts: return None
        y = f"{parts[0]:04d}"
        m = f"{(parts[1] if len(parts) > 1 else 1):02d}"
        dd = f"{(parts[2] if len(parts) > 2 else 1):02d}"
        return f"{y}-{m}-{dd}"
    except Exception:
        return None

def _first(x, key=None):
    if not x: return None
    v = x[0]
    return v.get(key) if key and isinstance(v, dict) else v

def _json(obj):
    if obj is None: return None
    try:
        return json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception:
        return None

def _join_authors(a_list):
    if not a_list: return None
    names = []
    for a in a_list:
        nm = " ".join(filter(None, [a.get("given"), a.get("family")])).strip()
        if not nm: nm = a.get("name") or a.get("literal")
        if nm: names.append(nm)
    return "; ".join(names) if names else None

def _fetch_page(params, max_retries=6, base_sleep=0.5):
    headers = {"User-Agent": UA}
    last_exc = None
    for attempt in range(max_retries):
        try:
            r = requests.get(CROSSREF_WORKS, params=params, headers=headers, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(base_sleep * (2 ** attempt))
                continue
            try:
                st.write("Crossref error payload:", r.json())
            except Exception:
                st.write("Crossref error text:", r.text[:800])
            r.raise_for_status()
        except requests.RequestException as e:
            last_exc = e
            time.sleep(base_sleep * (2 ** attempt))
    if last_exc:
        raise last_exc

# -----------------------------------------------------------------------------
# Streaming & shaping
# -----------------------------------------------------------------------------
def _total_results(filters: List[str], sort_key="deposited") -> int:
    params = {
        "filter": ",".join(filters),
        "rows": 0,
        "cursor": "*",
        "mailto": st.session_state.mailto or DEFAULT_MAILTO,
        "sort": sort_key,
        "order": "asc",
    }
    try:
        data = _fetch_page(params)
        return int(data.get("message", {}).get("total-results", 0))
    except Exception:
        params["rows"] = 1
        data = _fetch_page(params)
        return int(data.get("message", {}).get("total-results", 0))

def _stream_with_filters(params_filters: List[str], rows=1000, cursor="*", sort_key="deposited"):
    """Yield full items (need subtype + institutions)."""
    assert rows <= 1000, "Crossref caps rows at 1000"
    params = {
        "filter": ",".join(params_filters),
        "rows": rows,
        "cursor": cursor,
        "mailto": st.session_state.mailto or DEFAULT_MAILTO,
        "sort": sort_key,
        "order": "asc",
    }
    last_cursors = set()
    yielded = 0
    total = None

    while True:
        data = _fetch_page(params)
        msg = data.get("message", {})
        if total is None:
            total = msg.get("total-results") or 0

        items = msg.get("items") or []
        n = len(items)
        if n == 0:
            cur = msg.get("next-cursor")
            if cur and cur not in last_cursors:
                params["cursor"] = cur
                last_cursors.add(cur)
                continue
            break

        for it in items:
            yielded += 1
            yield it

        if isinstance(total, int) and total > 0 and yielded >= total:
            break

        cur = msg.get("next-cursor")
        if not cur: break
        if cur in last_cursors:
            params["cursor"] = cur
            continue
        last_cursors.add(cur)
        params["cursor"] = cur

def _extract_relations(rel):
    if not rel or not isinstance(rel, dict):
        return (None, None, None)
    def pick(kind):
        items = rel.get(kind) or []
        dois = []
        for it in items:
            doi = it.get("id")
            if doi and doi.lower().startswith("https://doi.org/"):
                doi = doi.split("org/", 1)[1]
            if doi: dois.append(doi)
        return "; ".join(sorted(set(dois))) if dois else None
    return (pick("is-preprint-of"), pick("has-preprint"), pick("is-version-of"))

def _one_row_wide(m):
    title                 = _first(m.get("title"))
    original_title        = _first(m.get("original-title"))
    short_title           = _first(m.get("short-title"))
    subtitle              = _first(m.get("subtitle"))
    container_title       = _first(m.get("container-title"))
    short_container_title = _first(m.get("short-container-title"))

    created_date          = _date_from_parts(m.get("created"))
    posted_date           = _date_from_parts(m.get("posted"))
    deposited_date        = _date_from_parts(m.get("deposited"))
    indexed_date          = _date_from_parts(m.get("indexed"))
    issued_date           = _date_from_parts(m.get("issued"))
    published_online_date = _date_from_parts(m.get("published-online"))
    published_print_date  = _date_from_parts(m.get("published-print"))
    accepted_date         = _date_from_parts(m.get("accepted"))
    approved_date         = _date_from_parts(m.get("approved"))

    is_preprint_of, has_preprint, is_version_of = _extract_relations(m.get("relation"))
    relation_json          = _json(m.get("relation"))

    authors_pretty         = _join_authors(m.get("author"))
    authors_json           = _json(m.get("author"))

    license_url            = _first(m.get("license"), "URL")
    licenses_json          = _json(m.get("license"))
    links_json             = _json(m.get("link"))
    primary_url            = (m.get("resource", {}) or {}).get("primary", {}).get("URL")

    issn_json              = _json(m.get("ISSN"))
    issn_type_json         = _json(m.get("issn-type"))
    isbn_type_json         = _json(m.get("isbn-type"))
    alternative_id_json    = _json(m.get("alternative-id"))

    subjects               = "; ".join(m.get("subject") or []) if m.get("subject") else None
    subjects_json          = _json(m.get("subject"))
    language               = m.get("language")

    funders_json           = _json(m.get("funder"))

    reference_count        = m.get("reference-count")
    is_referenced_by_count = m.get("is-referenced-by-count")
    references_json        = _json(m.get("reference"))

    update_to_json         = _json(m.get("update-to"))
    update_policy          = m.get("update-policy")
    update_type            = _first(m.get("update-to"), "type")

    publisher              = m.get("publisher")
    member                 = m.get("member")
    prefix                 = m.get("prefix")
    doi                    = m.get("DOI")
    url                    = m.get("URL")
    type_                  = m.get("type")
    subtype                = m.get("subtype")

    archive_json           = _json(m.get("archive"))
    content_domain_json    = _json(m.get("content-domain"))
    assertion_json         = _json(m.get("assertion"))
    institution_name       = _first(m.get("institution"), "name")
    institution_json       = _json(m.get("institution"))
    group_title            = m.get("group-title")
    source                 = m.get("source")
    score                  = m.get("score")
    abstract_raw           = m.get("abstract")

    return {
        "doi": doi, "url": url, "primary_url": primary_url,
        "title": title, "original_title": original_title, "short_title": short_title, "subtitle": subtitle,
        "type": type_, "subtype": subtype, "prefix": prefix, "publisher": publisher,
        "container_title": container_title, "short_container_title": short_container_title,
        "institution_name": institution_name,
        "created_date": created_date, "posted_date": posted_date, "deposited_date": deposited_date,
        "indexed_date": indexed_date, "issued_date": issued_date,
        "published_online_date": published_online_date, "published_print_date": published_print_date,
        "accepted_date": accepted_date, "approved_date": approved_date,
        "authors": authors_pretty, "authors_json": authors_json,
        "license_url": license_url, "licenses_json": licenses_json, "links_json": links_json,
        "subjects": subjects, "subjects_json": subjects_json, "language": language,
        "issn_json": issn_json, "issn_type_json": issn_type_json, "isbn_type_json": isbn_type_json,
        "alternative_id_json": alternative_id_json,
        "funder_json": funders_json, "reference_count": reference_count,
        "is_referenced_by_count": is_referenced_by_count, "references_json": references_json,
        "is_preprint_of": is_preprint_of, "has_preprint": has_preprint,
        "is_version_of": is_version_of, "relation_json": relation_json,
        "update_type": update_type, "update_policy": update_policy, "update_to_json": update_to_json,
        "archive_json": archive_json, "content_domain_json": content_domain_json, "assertion_json": assertion_json,
        "institution_json": institution_json, "group_title": group_title,
        "member": member, "source": source, "score": score, "abstract_raw": abstract_raw,
    }

# -----------------------------------------------------------------------------
# Filters & Predicates
# -----------------------------------------------------------------------------
def _filters_base(from_iso, until_iso):
    return [f"from-posted-date:{from_iso}",
            f"until-posted-date:{until_iso}",
            "type:posted-content"]

def _fanout_api_filters(from_iso, until_iso, prefixes=None, members=None, group_titles_exact=None):
    base = _filters_base(from_iso, until_iso)
    sets = []
    prefixes = [str(p).strip() for p in (prefixes or []) if p]
    members  = [str(m).strip() for m in (members  or []) if m]
    gtitles  = [str(g).strip() for g in (group_titles_exact or []) if g]
    if prefixes or members or gtitles:
        for p in (prefixes or [None]):
            for m in (members or [None]):
                for g in (gtitles or [None]):
                    flt = list(base)
                    if p: flt.append(f"prefix:{p}")
                    if m: flt.append(f"member:{m}")
                    if g: flt.append(f"group-title:{g}")
                    sets.append(flt)
    else:
        sets.append(base)
    return sets

def _eval_predicate_on_item(item,
                            group_title_contains=None,
                            institution_contains=None,
                            institution_equals=None,   # exact, case-insensitive
                            url_contains=None,
                            doi_startswith=None,
                            doi_contains=None,
                            require_all=False):
    group_title_contains = [s.lower() for s in (group_title_contains or []) if s]
    institution_contains = [s.lower() for s in (institution_contains or []) if s]
    institution_equals   = {s.lower() for s in (institution_equals or []) if s}
    url_contains         = [s.lower() for s in (url_contains or []) if s]
    doi_startswith       = [s.lower() for s in (doi_startswith or []) if s]
    doi_contains         = [s.lower() for s in (doi_contains or []) if s]

    doi = (item.get("DOI") or "").lower()
    url = (item.get("URL") or "").lower()
    primary_url = (((item.get("resource") or {}).get("primary") or {}).get("URL") or "").lower()
    gt_l = (item.get("group-title") or "")
    gt_l = gt_l.lower() if isinstance(gt_l, str) else ""

    insts = item.get("institution") or []
    inst_names = []
    if isinstance(insts, list):
        for ins in insts:
            nm = (ins or {}).get("name")
            if nm: inst_names.append(str(nm))
    inst_blob_l = " | ".join(inst_names).lower() if inst_names else ""

    checks = []
    if group_title_contains:
        checks.append(any(s in gt_l for s in group_title_contains))
    if institution_contains:
        checks.append(any(s in inst_blob_l for s in institution_contains))
    if institution_equals:
        eq_any = any((nm or "").lower() in institution_equals for nm in inst_names)
        checks.append(eq_any)
    if url_contains:
        checks.append(any(s in url or s in primary_url for s in url_contains))
    if doi_startswith:
        checks.append(any(doi.startswith(s) for s in doi_startswith))
    if doi_contains:
        checks.append(any(s in doi for s in doi_contains))

    if not checks:
        return True
    return all(checks) if require_all else any(checks)

# -----------------------------------------------------------------------------
# Cached helpers: counts & harvest
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def count_posted_and_preprint(date_start: str,
                              date_end: str,
                              mailto: str,
                              prefixes: List[str],
                              members: List[str],
                              group_titles_exact: List[str],
                              sort_key="deposited",
                              rows_per_call=1000) -> Dict[str, int]:
    """Counts over API-side narrowing only (no predicates)."""
    _set_mailto(mailto)
    from_iso, until_iso = _normalize_window(date_start, date_end)
    filter_sets = _fanout_api_filters(from_iso, until_iso, prefixes, members, group_titles_exact)

    posted_total = 0
    for flt in filter_sets:
        posted_total += _total_results(flt, sort_key=sort_key)

    preprint_total = 0
    seen = set()
    for flt in filter_sets:
        for item in _stream_with_filters(flt, rows=rows_per_call, cursor="*", sort_key=sort_key):
            doi = (item.get("DOI") or "").lower()
            if not doi or doi in seen: continue
            if (item.get("type") == "posted-content") and (item.get("subtype") == "preprint"):
                preprint_total += 1
            seen.add(doi)

    return {"posted_content_total": posted_total, "preprint_total": preprint_total}

@st.cache_data(show_spinner=False)
def count_after_predicates(date_start: str,
                           date_end: str,
                           mailto: str,
                           prefixes: List[str],
                           members: List[str],
                           group_titles_exact: List[str],
                           group_title_contains: List[str],
                           institution_contains: List[str],
                           institution_equals: List[str],
                           url_contains: List[str],
                           doi_startswith: List[str],
                           doi_contains: List[str],
                           require_all: bool,
                           rows_per_call=1000,
                           sort_key="deposited") -> Dict[str, int]:
    """Counts AFTER applying API-side narrowing AND client predicates."""
    _set_mailto(mailto)
    from_iso, until_iso = _normalize_window(date_start, date_end)
    filter_sets = _fanout_api_filters(from_iso, until_iso, prefixes, members, group_titles_exact)

    seen = set()
    posted_matching = 0
    preprint_matching = 0

    for flt in filter_sets:
        for item in _stream_with_filters(flt, rows=rows_per_call, cursor="*", sort_key=sort_key):
            doi = (item.get("DOI") or "").lower()
            if not doi or doi in seen:
                continue
            ok = _eval_predicate_on_item(
                item,
                group_title_contains=group_title_contains,
                institution_contains=institution_contains,
                institution_equals=institution_equals,
                url_contains=url_contains,
                doi_startswith=doi_startswith,
                doi_contains=doi_contains,
                require_all=require_all
            )
            if ok and (item.get("type") == "posted-content"):
                posted_matching += 1
                if item.get("subtype") == "preprint":
                    preprint_matching += 1
            seen.add(doi)

    return {"posted_matching": posted_matching, "preprint_matching": preprint_matching}

@st.cache_data(show_spinner=False)
def harvest_data(date_start: str,
                 date_end: str,
                 mailto: str,
                 prefixes: List[str],
                 members: List[str],
                 group_titles_exact: List[str],
                 group_title_contains: List[str],
                 institution_contains: List[str],
                 institution_equals: List[str],
                 url_contains: List[str],
                 doi_startswith: List[str],
                 doi_contains: List[str],
                 require_all: bool,
                 preprint_only: bool,
                 rows_per_call=1000,
                 sort_key="deposited") -> pd.DataFrame:
    """Harvest rows after API-side narrowing and predicates (optionally preprint-only)."""
    _set_mailto(mailto)
    from_iso, until_iso = _normalize_window(date_start, date_end)
    filter_sets = _fanout_api_filters(from_iso, until_iso, prefixes, members, group_titles_exact)

    rows = []
    seen = set()
    for flt in filter_sets:
        for item in _stream_with_filters(flt, rows=rows_per_call, cursor="*", sort_key=sort_key):
            doi = (item.get("DOI") or "").lower()
            if not doi or doi in seen:
                continue
            ok = _eval_predicate_on_item(
                item,
                group_title_contains=group_title_contains,
                institution_contains=institution_contains,
                institution_equals=institution_equals,
                url_contains=url_contains,
                doi_startswith=doi_startswith,
                doi_contains=doi_contains,
                require_all=require_all
            )
            if ok:
                if (not preprint_only) or (
                    item.get("type") == "posted-content" and item.get("subtype") == "preprint"
                ):
                    rows.append(_one_row_wide(item))
            seen.add(doi)

    df = pd.DataFrame(rows)
    if not df.empty:
        df.drop_duplicates(subset=["doi"], inplace=True)
        if preprint_only:
            df = df[df["subtype"] == "preprint"].copy()
    return df

# -----------------------------------------------------------------------------
# Live mode helpers
# -----------------------------------------------------------------------------
def _month_slices(start_iso: str, end_iso: str):
    """Return [(YYYY-MM-DD, YYYY-MM-DD), ...] month windows clamped to [start,end]."""
    start = datetime.fromisoformat(start_iso).date()
    end = datetime.fromisoformat(end_iso).date()
    cur = start.replace(day=1)
    slices = []
    while cur <= end:
        if cur.month == 12:
            nxt = cur.replace(year=cur.year+1, month=1, day=1)
        else:
            nxt = cur.replace(month=cur.month+1, day=1)
        m_start = cur
        m_end = (nxt - timedelta(days=1))
        s = max(m_start, start)
        e = min(m_end, end)
        if s <= e:
            slices.append((s.isoformat(), e.isoformat()))
        cur = nxt
    return slices if slices else [(start_iso, end_iso)]

def live_harvest_stream(date_start: str,
                        date_end: str,
                        mailto: str,
                        prefixes: List[str],
                        members: List[str],
                        group_titles_exact: List[str],
                        group_title_contains: List[str],
                        institution_contains: List[str],
                        institution_equals: List[str],
                        url_contains: List[str],
                        doi_startswith: List[str],
                        doi_contains: List[str],
                        require_all: bool,
                        preprint_only: bool,
                        checkpoint_every: int = 0,
                        resume_path: str = "",
                        chunk_by_month: bool = False,
                        tick_every_rows: int = 200):
    """
    Generator yielding ('event', payload):
      - 'resume': str
      - 'checkpoint': str
      - 'slice_done': dict
      - 'tick': dict {matched, preprints, seen, last_row}
      - 'stopped': str
      - 'done': pd.DataFrame
    """
    _clear_stop()
    _set_mailto(mailto)

    rows = []
    seen = set()
    if resume_path and os.path.exists(resume_path):
        try:
            prev = pd.read_parquet(resume_path)
            if not prev.empty:
                rows.extend(prev.to_dict(orient="records"))
                seen.update(prev["doi"].str.lower().dropna().tolist())
                yield ("resume", f"Resumed from {resume_path} with {len(prev):,} rows.")
        except Exception as e:
            yield ("resume", f"Resume failed: {e}")

    matched = len(rows)
    preprints = sum(1 for r in rows if (r.get("subtype") == "preprint"))
    seen_count = len(seen)
    since_last_tick = 0

    slices = _month_slices(date_start, date_end) if chunk_by_month else [(date_start, date_end)]

    for s_start, s_end in slices:
        if st.session_state.stop_requested:
            yield ("stopped", "Stopped by user.")
            return
        from_iso, until_iso = _normalize_window(s_start, s_end)
        for flt in _fanout_api_filters(from_iso, until_iso, prefixes, members, group_titles_exact):
            for item in _stream_with_filters(flt, rows=1000, cursor="*", sort_key="deposited"):
                if st.session_state.stop_requested:
                    yield ("stopped", "Stopped by user.")
                    return
                doi = (item.get("DOI") or "").lower()
                if not doi or doi in seen:
                    continue
                seen.add(doi)
                seen_count += 1
                ok = _eval_predicate_on_item(
                    item,
                    group_title_contains=group_title_contains,
                    institution_contains=institution_contains,
                    institution_equals=institution_equals,
                    url_contains=url_contains,
                    doi_startswith=doi_startswith,
                    doi_contains=doi_contains,
                    require_all=require_all
                )
                if ok and ((not preprint_only) or (item.get("type")=="posted-content" and item.get("subtype")=="preprint")):
                    row = _one_row_wide(item)
                    rows.append(row)
                    matched += 1
                    if row.get("subtype") == "preprint":
                        preprints += 1
                    since_last_tick += 1
                    if since_last_tick >= tick_every_rows:
                        since_last_tick = 0
                        yield ("tick", {"matched": matched, "preprints": preprints, "seen": seen_count, "last_row": row})
                    if checkpoint_every and (matched % checkpoint_every == 0):
                        try:
                            pd.DataFrame(rows).to_parquet("checkpoint.parquet", index=False)
                            yield ("checkpoint", f"Checkpoint saved: checkpoint.parquet ({matched:,} rows).")
                        except Exception as e:
                            yield ("checkpoint", f"Checkpoint failed: {e}")

        yield ("slice_done", {"slice": (s_start, s_end), "matched": matched, "preprints": preprints, "seen": seen_count})

    df = pd.DataFrame(rows)
    yield ("done", df)

# -----------------------------------------------------------------------------
# Sidebar – Controls
# -----------------------------------------------------------------------------
with st.sidebar:
    st.header("⚙️ Settings")
    mailto = st.text_input("Contact email for Crossref (mailto)", value=DEFAULT_MAILTO,
                           help="Crossref etiquette; helps avoid throttling.")
    st.session_state.mailto = mailto

    st.markdown("---")
    st.subheader("Date window")
    today = date.today()
    default_start = today.replace(month=1, day=1)
    colA, colB = st.columns(2)
    with colA:
        start_date = st.date_input("Start date", value=default_start, max_value=today)
    with colB:
        end_date = st.date_input("End date", value=today, max_value=today)

    st.markdown("---")
    st.subheader("API-side narrowing (fast)")
    prefixes = st.text_input("Prefixes (comma-separated)", placeholder="10.1101, 10.36227")
    members = st.text_input("Members (comma-separated)", placeholder="246, 301, 297")
    gtitles = st.text_input("Exact group-titles (comma-separated)", placeholder="PsyArXiv, Authorea Preprints")

    st.markdown("---")
    st.subheader("Client-side predicates")
    gt_contains = st.text_input("group-title contains (CSV)", placeholder="rXiv, medRxiv")
    inst_equals = st.text_input("Institution equals (CSV) — exact", placeholder="bioRxiv, medRxiv")
    inst_contains = st.text_input("institution contains (CSV)", placeholder="Center for Open Science")
    url_contains = st.text_input("URL contains (CSV)", placeholder="osf.io, scielopreprints")
    doi_starts = st.text_input("DOI startswith (CSV)", placeholder="10.36227/techrxiv.")
    doi_contains = st.text_input("DOI contains (CSV)", placeholder="scielopreprints")

    # >>> FIX: the two missing toggles <<<
    col_flags = st.columns(2)
    with col_flags[0]:
        require_all = st.toggle("Require ALL predicates", value=False)
    with col_flags[1]:
        preprint_only = st.toggle("Preprint only", value=True)

    st.markdown("---")
    st.subheader("Run options")
    live_mode = st.toggle("Live mode (stream updates)", value=True,
                          help="Show partial results, progress, and leaderboards while fetching.")
    quick_preview_k = st.number_input("Quick preview rows (0 = off)", value=0, min_value=0, step=500,
                                      help="Fetch the first K matching rows quickly.")
    checkpoint_every = st.number_input("Checkpoint every N rows (0 = off)", value=2000, min_value=0, step=500,
                                       help="Write a parquet checkpoint periodically.")
    resume_path = st.text_input("Resume from checkpoint (.parquet)", value="")
    chunk_by_month = st.toggle("Chunk by month (faster first results)", value=False)

    st.markdown("---")
    st.subheader("Presets")
    preset_name = st.text_input("Save current inputs as preset (name)")
    save_preset = st.button("💾 Save preset")
    load_choice = st.selectbox("Load preset", options=["(none)"] + st.session_state.get("presets_list", []))
    load_preset = st.button("📥 Load selected preset")

# -----------------------------------------------------------------------------
# Preset store
# -----------------------------------------------------------------------------
def _csv_to_list(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()] if s else []

def _list_to_csv(v: List[str]) -> str:
    return ", ".join(v) if v else ""

if "presets" not in st.session_state:
    st.session_state.presets = {}
if "presets_list" not in st.session_state:
    st.session_state.presets_list = []

if save_preset and preset_name.strip():
    preset = dict(
        mailto=mailto,
        start_date=str(start_date),
        end_date=str(end_date),
        prefixes=_csv_to_list(prefixes),
        members=_csv_to_list(members),
        gtitles=_csv_to_list(gtitles),
        gt_contains=_csv_to_list(gt_contains),
        inst_equals=_csv_to_list(inst_equals),
        inst_contains=_csv_to_list(inst_contains),
        url_contains=_csv_to_list(url_contains),
        doi_starts=_csv_to_list(doi_starts),
        doi_contains=_csv_to_list(doi_contains),
        require_all=require_all,          # save
        preprint_only=preprint_only,      # save
        live_mode=live_mode,
        quick_preview_k=int(quick_preview_k),
        checkpoint_every=int(checkpoint_every),
        resume_path=resume_path,
        chunk_by_month=chunk_by_month,
    )
    st.session_state.presets[preset_name] = preset
    if preset_name not in st.session_state.presets_list:
        st.session_state.presets_list.append(preset_name)
    st.success(f"Preset '{preset_name}' saved.")

if load_preset and load_choice and load_choice != "(none)":
    p = st.session_state.presets.get(load_choice, {})
    if p:
        st.session_state.mailto = p.get("mailto", mailto)
        st.sidebar.text_input("Contact email for Crossref (mailto)", value=p.get("mailto", mailto), key="mailto_rebind")
        st.sidebar.date_input("Start date", value=datetime.fromisoformat(p["start_date"]).date(), key="start_rebind")
        st.sidebar.date_input("End date", value=datetime.fromisoformat(p["end_date"]).date(), key="end_rebind")
        st.sidebar.text_input("Prefixes (comma-separated)", value=_list_to_csv(p["prefixes"]), key="pref_rebind")
        st.sidebar.text_input("Members (comma-separated)", value=_list_to_csv(p["members"]), key="mem_rebind")
        st.sidebar.text_input("Exact group-titles (comma-separated)", value=_list_to_csv(p["gtitles"]), key="gt_rebind")
        st.sidebar.text_input("group-title contains (CSV)", value=_list_to_csv(p["gt_contains"]), key="gtc_rebind")
        st.sidebar.text_input("Institution equals (CSV) — exact", value=_list_to_csv(p.get("inst_equals", [])), key="inst_eq_rebind")
        st.sidebar.text_input("institution contains (CSV)", value=_list_to_csv(p["inst_contains"]), key="instc_rebind")
        st.sidebar.text_input("URL contains (CSV)", value=_list_to_csv(p["url_contains"]), key="urlc_rebind")
        st.sidebar.text_input("DOI startswith (CSV)", value=_list_to_csv(p["doi_starts"]), key="dois_rebind")
        st.sidebar.text_input("DOI contains (CSV)", value=_list_to_csv(p["doi_contains"]), key="doic_rebind")
        st.sidebar.toggle("Require ALL predicates", value=p.get("require_all", False), key="reqall_rebind")
        st.sidebar.toggle("Preprint only", value=p.get("preprint_only", True), key="ppo_rebind")
        st.sidebar.toggle("Live mode (stream updates)", value=p.get("live_mode", True), key="live_rebind")
        st.sidebar.number_input("Quick preview rows (0 = off)", value=int(p.get("quick_preview_k", 0)), key="qp_rebind")
        st.sidebar.number_input("Checkpoint every N rows (0 = off)", value=int(p.get("checkpoint_every", 0)), key="cp_rebind")
        st.sidebar.text_input("Resume from checkpoint (.parquet)", value=p.get("resume_path", ""), key="rp_rebind")
        st.sidebar.toggle("Chunk by month (faster first results)", value=p.get("chunk_by_month", False), key="cbm_rebind")
        st.info(f"Preset '{load_choice}' loaded. (If fields didn’t update, re-open the sidebar.)")

# -----------------------------------------------------------------------------
# Header
# -----------------------------------------------------------------------------
st.title("🧪 Crossref Preprints Explorer")
st.caption("Live counts & harvesting for posted-content (incl. preprints), with API-side narrowing and precise client predicates.")

# -----------------------------------------------------------------------------
# Validation
# -----------------------------------------------------------------------------
problems = []
if not mailto or "@" not in mailto:
    problems.append("Please provide a valid **mailto** (e.g., you@org).")
if start_date > end_date:
    problems.append("Start date must be earlier than or equal to end date.")
if problems:
    st.error(" • " + "\n • ".join(problems))

# -----------------------------------------------------------------------------
# Main controls
# -----------------------------------------------------------------------------
col1, col2, col3 = st.columns([1,1,1])
with col1:
    run_counts = st.button("📊 Get Counts", use_container_width=True, disabled=bool(problems))
with col2:
    run_harvest = st.button("⬇️ Harvest Rows", type="primary", use_container_width=True, disabled=bool(problems))
with col3:
    cancel_btn = st.button("🛑 Cancel", use_container_width=True, on_click=_request_stop)
st.markdown("<div class='small-muted'>Tip: prefer API-side narrowing (prefix/member/group-title) for speed.</div>", unsafe_allow_html=True)

# Parse inputs
def _csv_to_list(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()] if s else []

prefixes_list = _csv_to_list(prefixes)
members_list = _csv_to_list(members)
gtitles_list = _csv_to_list(gtitles)
gt_contains_list = _csv_to_list(gt_contains)
inst_equals_list = _csv_to_list(inst_equals)
inst_contains_list = _csv_to_list(inst_contains)
url_contains_list = _csv_to_list(url_contains)
doi_starts_list = _csv_to_list(doi_starts)
doi_contains_list = _csv_to_list(doi_contains)

start_iso = str(start_date)
end_iso = str(end_date)

# -----------------------------------------------------------------------------
# Counts
# -----------------------------------------------------------------------------
if run_counts and not problems:
    with st.spinner("Querying Crossref and counting..."):
        try:
            totals = count_posted_and_preprint(
                date_start=start_iso,
                date_end=end_iso,
                mailto=mailto,
                prefixes=prefixes_list,
                members=members_list,
                group_titles_exact=gtitles_list,
            )
            filtered = count_after_predicates(
                date_start=start_iso,
                date_end=end_iso,
                mailto=mailto,
                prefixes=prefixes_list,
                members=members_list,
                group_titles_exact=gtitles_list,
                group_title_contains=gt_contains_list,
                institution_contains=inst_contains_list,
                institution_equals=inst_equals_list,
                url_contains=url_contains_list,
                doi_startswith=doi_starts_list,
                doi_contains=doi_contains_list,
                require_all=require_all,  # now wired
            )
        except Exception as e:
            st.exception(e)
            totals, filtered = None, None

    if totals is not None and filtered is not None:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Posted-content (window/slice)", f"{totals['posted_content_total']:,}")
        with c2:
            st.metric("Preprint (window/slice)", f"{totals['preprint_total']:,}")
        with c3:
            st.metric("Posted-content (after predicates)", f"{filtered['posted_matching']:,}")
        with c4:
            st.metric("Preprint (after predicates)", f"{filtered['preprint_matching']:,}")
        st.caption("Counts are deduped by DOI across API slices.")

# -----------------------------------------------------------------------------
# Harvest
# -----------------------------------------------------------------------------
if run_harvest and not problems:
    # Quick preview (fast exit)
    if quick_preview_k > 0 and not live_mode:
        with st.spinner(f"Fetching first {quick_preview_k} rows..."):
            from_iso, until_iso = _normalize_window(start_iso, end_iso)
            picked, seen_preview = [], set()
            for flt in _fanout_api_filters(from_iso, until_iso, prefixes_list, members_list, gtitles_list):
                for item in _stream_with_filters(flt, rows=min(1000, max(1000, quick_preview_k)), cursor="*", sort_key="deposited"):
                    if len(picked) >= quick_preview_k:
                        break
                    doi = (item.get("DOI") or "").lower()
                    if not doi or doi in seen_preview:
                        continue
                    seen_preview.add(doi)
                    ok = _eval_predicate_on_item(
                        item,
                        group_title_contains=gt_contains_list,
                        institution_contains=inst_contains_list,
                        institution_equals=inst_equals_list,
                        url_contains=url_contains_list,
                        doi_startswith=doi_starts_list,
                        doi_contains=doi_contains_list,
                        require_all=require_all,
                    )
                    if ok and ((not preprint_only) or (item.get("type")=="posted-content" and item.get("subtype")=="preprint")):
                        picked.append(_one_row_wide(item))
                if len(picked) >= quick_preview_k:
                    break
        st.subheader("Quick preview")
        df_prev = pd.DataFrame(picked)
        if df_prev.empty:
            st.warning("No rows in the quick preview.")
        else:
            st.write(f"Previewed **{len(df_prev):,}** rows.")
            st.dataframe(df_prev.head(50), use_container_width=True)
        st.stop()

    if live_mode:
        status = st.status("Streaming from Crossref...", expanded=True)
        m1, m2, m3 = st.columns(3)
        ph_table = st.empty()
        ph_left, ph_mid, ph_right = st.columns(3)
        t_start = time.time()

        buffer_rows = []

        for phase, payload in live_harvest_stream(
            date_start=start_iso,
            date_end=end_iso,
            mailto=mailto,
            prefixes=prefixes_list,
            members=members_list,
            group_titles_exact=gtitles_list,
            group_title_contains=gt_contains_list,
            institution_contains=inst_contains_list,
            institution_equals=inst_equals_list,
            url_contains=url_contains_list,
            doi_startswith=doi_starts_list,
            doi_contains=doi_contains_list,
            require_all=require_all,   # now wired
            preprint_only=preprint_only,  # now wired
            checkpoint_every=int(checkpoint_every),
            resume_path=resume_path,
            chunk_by_month=chunk_by_month,
            tick_every_rows=200,
        ):
            if phase == "resume":
                status.write(payload)
            elif phase == "checkpoint":
                status.write(payload)
            elif phase == "slice_done":
                status.write(f"Finished slice {payload['slice'][0]} → {payload['slice'][1]}")
                m1.metric("Rows matched", f"{payload['matched']:,}")
                m2.metric("Preprints matched", f"{payload['preprints']:,}")
                elapsed = int(time.time() - t_start)
                m3.metric("Elapsed", f"{elapsed//60}m {elapsed%60}s")
            elif phase == "tick":
                lr = payload.get("last_row")
                if lr:
                    buffer_rows.append(lr)
                    if len(buffer_rows) > 5000:
                        buffer_rows = buffer_rows[-3000:]
                m1.metric("Rows matched", f"{payload['matched']:,}")
                m2.metric("Preprints matched", f"{payload['preprints']:,}")
                elapsed = int(time.time() - t_start)
                m3.metric("Elapsed", f"{elapsed//60}m {elapsed%60}s")

                if buffer_rows:
                    df_live = pd.DataFrame(buffer_rows[-200:])
                    cols = ["doi","subtype","group_title","publisher","member","prefix","posted_date","title","institution_name","primary_url"]
                    ph_table.dataframe(df_live[[c for c in cols if c in df_live.columns]].tail(30), use_container_width=True)
                    with ph_left:
                        st.markdown("**Top group_title (live)**")
                        st.dataframe(df_live.groupby("group_title").size().sort_values(ascending=False).head(10))
                    with ph_mid:
                        st.markdown("**Top publisher (live)**")
                        st.dataframe(df_live.groupby("publisher").size().sort_values(ascending=False).head(10))
                    with ph_right:
                        st.markdown("**By year (posted_date)**")
                        if "posted_date" in df_live.columns:
                            yrs = pd.to_datetime(df_live["posted_date"], errors="coerce").dt.year
                            st.dataframe(yrs.value_counts().sort_index())

            elif phase == "stopped":
                status.update(label=payload, state="error")
                break
            elif phase == "done":
                df_final = payload
                status.update(label=f"Done. Rows: {len(df_final):,}", state="complete")
                st.subheader("Results")
                if df_final.empty:
                    st.warning("No rows returned for the given filters.")
                else:
                    st.write(f"Returned **{len(df_final):,}** rows.")
                    cols = ["doi","subtype","group_title","publisher","member","prefix","posted_date","title","institution_name","primary_url"]
                    st.dataframe(df_final[[c for c in cols if c in df_final.columns]].head(50), use_container_width=True)

                    csv_buf = io.StringIO(); df_final.to_csv(csv_buf, index=False)
                    st.download_button("Download CSV", data=csv_buf.getvalue(),
                                       file_name=f"crossref_{'preprints_' if preprint_only else ''}{start_iso}_{end_iso}.csv",
                                       mime="text/csv", use_container_width=True)
                    pq_buf = io.BytesIO(); df_final.to_parquet(pq_buf, index=False)
                    st.download_button("Download Parquet", data=pq_buf.getvalue(),
                                       file_name=f"crossref_{'preprints_' if preprint_only else ''}{start_iso}_{end_iso}.parquet",
                                       mime="application/octet-stream", use_container_width=True)
                break
    else:
        # Classic (cached) harvest
        with st.spinner("Streaming records from Crossref..."):
            df = harvest_data(
                date_start=start_iso,
                date_end=end_iso,
                mailto=mailto,
                prefixes=prefixes_list,
                members=members_list,
                group_titles_exact=gtitles_list,
                group_title_contains=gt_contains_list,
                institution_contains=inst_contains_list,
                institution_equals=inst_equals_list,
                url_contains=url_contains_list,
                doi_startswith=doi_starts_list,
                doi_contains=doi_contains_list,
                require_all=require_all,   # now wired
                preprint_only=preprint_only,  # now wired
            )
        st.subheader("Results")
        if df.empty:
            st.warning("No rows returned for the given filters.")
        else:
            st.write(f"Returned **{len(df):,}** rows.")
            preview_cols = ["doi","subtype","group_title","publisher","member","prefix","posted_date","title","institution_name","primary_url"]
            st.dataframe(df[[c for c in preview_cols if c in df.columns]].head(50), use_container_width=True)

            csv_buf = io.StringIO(); df.to_csv(csv_buf, index=False)
            st.download_button("Download CSV", data=csv_buf.getvalue(),
                               file_name=f"crossref_{'preprints_' if preprint_only else ''}{start_iso}_{end_iso}.csv",
                               mime="text/csv", use_container_width=True)
            pq_buf = io.BytesIO(); df.to_parquet(pq_buf, index=False)
            st.download_button("Download Parquet", data=pq_buf.getvalue(),
                               file_name=f"crossref_{'preprints_' if preprint_only else ''}{start_iso}_{end_iso}.parquet",
                               mime="application/octet-stream", use_container_width=True)

# -----------------------------------------------------------------------------
# Help
# -----------------------------------------------------------------------------
with st.expander("ℹ️ How it works"):
    st.markdown("""
- **Counts (window/slice)**: Posted-content uses Crossref `message.total-results`; preprint requires streaming because `/works` doesn't allow `select=subtype`.
- **Counts (after predicates)**: streams items and applies your predicates; this aligns with Harvest when "Preprint only" is on.
- **Live mode**: streams rows, updates metrics, supports Cancel, and can checkpoint to `checkpoint.parquet` so you can resume.
- **Performance**: Prefer API-side narrowing (prefix/member/group-title). Use `institution_equals` for precise server targeting.
- **Etiquette**: Keep a real **mailto** — improves reliability and supports Crossref.
""")

st.markdown("<div class='small-muted'>Made with ❤️ for long crawls without the boredom.</div>", unsafe_allow_html=True)
