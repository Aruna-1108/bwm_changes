import frappe
from frappe.integrations.utils import make_get_request
from frappe.utils import now_datetime, get_datetime, add_to_date

from urllib.parse import quote
from datetime import datetime


SETTINGS_DOCTYPE = "India Mart API Settings"
LOG_DOCTYPE = "India MART Sync Log"
ENQUIRY_DOCTYPE = "IndiaMART Enquiry Details"
ENQUIRY_UNIQUE_FIELD = "im_enquiry_id"
MAX_DAYS_WINDOW = 7


def _safe_str(v):
    return (v or "").strip()

def _now():
    return now_datetime()

def _dt(v):
    try:
        return get_datetime(v) if v else None
    except Exception:
        return None

def _throw(msg):
    frappe.throw(msg)

def _format_indiamart_dt(dt_obj):
    if not dt_obj:
        return ""
    d = _dt(dt_obj)
    if not d:
        return ""

    dd = str(d.day).zfill(2)
    mm = str(d.month).zfill(2)
    yyyy = str(d.year)

    HH = str(d.hour).zfill(2)
    MM = str(d.minute).zfill(2)
    SS = str(d.second).zfill(2)

    return dd + "-" + mm + "-" + yyyy + " " + HH + ":" + MM + ":" + SS

def _encode_qs_value(val):
    s = _safe_str(val)
    return quote(s, safe=":-_.")

def _build_full_url(base_url, params):
    base = _safe_str(base_url).strip()
    if not base:
        _throw("API Base URL is missing.")
    if "?" in base:
        base = base.split("?", 1)[0]

    parts = []
    for k, v in (params or {}).items():
        if v is None:
            continue
        k_enc = quote(str(k), safe="")
        v_enc = _encode_qs_value(str(v))
        parts.append(k_enc + "=" + v_enc)

    return base.rstrip("/") + "/?" + "&".join(parts)


def _parse_enquiry_datetime(row):
    raw = (
        row.get("QUERY_TIME")
        or row.get("DATE")
        or row.get("ENQ_DATE")
        or row.get("EnquiryDateTime")
        or row.get("ENQUIRY_TIME")
        or ""
    )
    raw = _safe_str(raw)
    if not raw:
        return None

    d = _dt(raw)
    if d:
        return d

    fmts = [
        "%d-%m-%Y %H:%M:%S",
        "%d-%b-%Y %H:%M:%S",
        "%d-%b-%Y",
        "%d-%m-%Y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(raw, f)
        except Exception:
            pass
    return None


def _pick_im_enquiry_id(row):
    return _safe_str(
        row.get("UNIQUE_QUERY_ID")
        or row.get("QUERY_ID")
        or row.get("QueryId")
        or row.get("query_id")
    )


def _guess_full_name(row):
    return _safe_str(row.get("SENDER_NAME") or row.get("SenderName") or row.get("NAME") or "")

def _guess_mobile(row):
    return _safe_str(row.get("SENDER_MOBILE") or row.get("SenderMobile") or row.get("MOBILE") or "")

def _guess_email(row):
    return _safe_str(row.get("SENDER_EMAIL") or row.get("SenderEmail") or row.get("EMAIL") or "")

def _guess_company(row):
    return _safe_str(row.get("SENDER_COMPANY") or row.get("COMPANY") or "")

def _guess_city(row):
    return _safe_str(row.get("SENDER_CITY") or row.get("CITY") or "")

def _guess_state(row):
    return _safe_str(row.get("SENDER_STATE") or row.get("STATE") or "")

def _guess_country(row):
    return _safe_str(row.get("SENDER_COUNTRY_ISO") or row.get("SENDER_COUNTRY") or row.get("COUNTRY") or "")

def _guess_pincode(row):
    return _safe_str(row.get("SENDER_PINCODE") or row.get("PINCODE") or row.get("PIN_CODE") or "")

def _guess_address(row):
    return _safe_str(row.get("SENDER_ADDRESS") or row.get("ADDRESS") or "")

def _guess_subject(row):
    return _safe_str(row.get("SUBJECT") or "")

def _guess_product_name(row):
    return _safe_str(row.get("QUERY_PRODUCT_NAME") or row.get("PRODUCT_NAME") or "")

def _guess_message(row):
    return _safe_str(row.get("QUERY_MESSAGE") or row.get("ENQ_MESSAGE") or row.get("MESSAGE") or "")


# ✅ FIXED MAPPING HERE
def _guess_query_type(row):
    """
    Your DocType allows: Call, Buy Lead, Direct
    IndiaMART may send: B/C/D
    """
    v = _safe_str(row.get("QUERY_TYPE") or row.get("QueryType") or row.get("query_type") or "")
    if not v:
        return ""

    u = v.upper()

    if u == "B":
        return "Buy Lead"
    if u == "C":
        return "Call"
    if u == "D":
        return "Direct"

    if "BUY" in u:
        return "Buy Lead"
    if "CALL" in u:
        return "Call"
    if "DIRECT" in u:
        return "Direct"

    return "Direct"


def _create_sync_log(settings_name):
    doc = frappe.get_doc({
        "doctype": LOG_DOCTYPE,
        "indiamart_setting_api_id": settings_name,
        "started_on": _now(),
        "status": "",
        "fetched_count": 0,
        "created_count": 0,
        "duplicate_count": 0,
        "error_count": 0,
        "error_log": ""
    })
    doc.insert(ignore_permissions=True)
    return doc.name


def _finish_sync_log(log_name, status, counts, error_log=None):
    update = {
        "status": status,
        "ended_on": _now(),
        "fetched_count": counts.get("fetched", 0),
        "created_count": counts.get("created", 0),
        "duplicate_count": counts.get("duplicate", 0),
        "error_count": counts.get("errors", 0),
        "error_log": error_log or ""
    }
    frappe.db.set_value(LOG_DOCTYPE, log_name, update)


def _extract_records(payload):
    if not isinstance(payload, dict):
        return [], "Unexpected response format (not dict)."

    status = _safe_str(payload.get("STATUS"))
    msg = _safe_str(payload.get("MESSAGE"))
    code = payload.get("CODE", "")

    if status and status.upper() != "SUCCESS":
        return [], (msg or ("IndiaMART API failed. CODE=" + str(code)))

    records = payload.get("RESPONSE") or []
    if not isinstance(records, list):
        records = []

    return records, (msg or "")


def _upsert_enquiry(row, settings_doc):
    im_enquiry_id = _pick_im_enquiry_id(row)
    if not im_enquiry_id:
        return "skipped"

    existing = frappe.db.get_value(
        ENQUIRY_DOCTYPE,
        {ENQUIRY_UNIQUE_FIELD: im_enquiry_id},
        "name"
    )

    enquiry_dt = _parse_enquiry_datetime(row)

    data = {
        "im_enquiry_id": im_enquiry_id,
        "indiamart_api_setting_id": settings_doc.name,
        "lead_user":settings_doc.lead_user,

        "full_name": _guess_full_name(row),
        "company": _guess_company(row),

        "query_type": _guess_query_type(row),
        "status": "Open",
        "sync_status": "New",

        "mobile": _guess_mobile(row),
        "email": _guess_email(row),

        "product_name": _guess_product_name(row),
        "subject": _guess_subject(row),

        "city": _guess_city(row),
        "state": _guess_state(row),
        "country": _guess_country(row),

        "pin_code": _guess_pincode(row),
        "address": _guess_address(row),

        "message": _guess_message(row),

        "receiver_mobile": _safe_str(row.get("RECEIVER_MOBILE") or ""),
        "sender_id": _safe_str(row.get("SENDER_ID") or ""),
        "call_duration": _safe_str(row.get("CALL_DURATION") or ""),
    }

    if enquiry_dt:
        data["enquiry_datetime"] = enquiry_dt
        try:
            data["date"] = enquiry_dt.date()
            data["time"] = enquiry_dt.time()
        except Exception:
            pass

    if existing:
        doc = frappe.get_doc(ENQUIRY_DOCTYPE, existing)
        for k, v in data.items():
            if v is not None and v != "":
                doc.set(k, v)
        doc.save(ignore_permissions=True)
        return "duplicate"

    doc = frappe.get_doc({"doctype": ENQUIRY_DOCTYPE})
    for k, v in data.items():
        if v is not None and v != "":
            doc.set(k, v)
    doc.insert(ignore_permissions=True)
    return "created"


def _get_next_chunk(from_dt, to_dt):
    chunk_start = from_dt
    chunk_end = add_to_date(chunk_start, days=MAX_DAYS_WINDOW)
    if chunk_end > to_dt:
        chunk_end = to_dt
    return chunk_start, chunk_end


@frappe.whitelist()
def run_sync(setting_name):
    settings_doc = frappe.get_doc(SETTINGS_DOCTYPE, setting_name)

    if not int(getattr(settings_doc, "enabled", 0) or 0):
        _throw("India Mart API Settings is disabled.")

    api_base_url = _safe_str(getattr(settings_doc, "api_base_url", ""))
    if not api_base_url:
        _throw("API Base URL is missing in India Mart API Settings.")

    # api_key is Password field -> get_password
    api_key = _safe_str(settings_doc.get_password("api_key"))
    if not api_key:
        _throw("API Key is missing or not saved properly in India Mart API Settings.")

    from_dt = _dt(getattr(settings_doc, "from_date_time", None))
    to_dt = _dt(getattr(settings_doc, "to_date_time", None))

    if not from_dt or not to_dt:
        _throw("Please set From Date Time and To Date Time in India Mart API Settings.")
    if to_dt <= from_dt:
        _throw("To Date Time must be greater than From Date Time.")

    chunk_start, chunk_end = _get_next_chunk(from_dt, to_dt)

    log_name = _create_sync_log(setting_name)
    counts = {"fetched": 0, "created": 0, "duplicate": 0, "errors": 0}
    error_lines = []

    try:
        params = {
            "glusr_crm_key": api_key,
            "start_time": _format_indiamart_dt(chunk_start),
            "end_time": _format_indiamart_dt(chunk_end),
        }

        full_url = _build_full_url(api_base_url, params)
        payload = make_get_request(full_url)

        records, info = _extract_records(payload)
        if info:
            error_lines.append(info)

        counts["fetched"] = len(records)

        for row in records:
            try:
                res = _upsert_enquiry(row, settings_doc)
                if res == "created":
                    counts["created"] += 1
                elif res == "duplicate":
                    counts["duplicate"] += 1
            except Exception:
                counts["errors"] += 1
                error_lines.append(frappe.get_traceback())

        if chunk_end < to_dt:
            settings_doc.from_date_time = chunk_end

        settings_doc.last_success_sync_on = _now()
        settings_doc.save(ignore_permissions=True)

        status = "Success"
        if counts["errors"] > 0:
            status = "Partial"

        error_lines.append("Chunk Start=" + _format_indiamart_dt(chunk_start) + " | Chunk End=" + _format_indiamart_dt(chunk_end))
        error_lines.append("URL=" + full_url)

        _finish_sync_log(log_name, status, counts, "\n".join([x for x in error_lines if x]))

        return {
            "status": status,
            "log": log_name,
            "chunk_start": _format_indiamart_dt(chunk_start),
            "chunk_end": _format_indiamart_dt(chunk_end),
            "fetched": counts["fetched"],
            "created": counts["created"],
            "duplicate": counts["duplicate"],
            "errors": counts["errors"],
        }

    except Exception:
        error_lines.append(frappe.get_traceback())
        _finish_sync_log(log_name, "Failed", counts, "\n".join(error_lines))
        raise