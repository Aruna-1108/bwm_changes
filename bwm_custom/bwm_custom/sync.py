import frappe
from frappe.integrations.utils import make_get_request
from frappe.utils import now_datetime, get_datetime, add_to_date

from urllib.parse import quote
from datetime import datetime
from frappe.utils import strip_html


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

def _guess_product_category(row):
    return _safe_str(
        row.get("QUERY_MCAT_NAME") or
        row.get("QUERY_PRODUCT_CATEGORY") or
        row.get("PRODUCT_CATEGORY") or
        ""
    )


def _guess_message(row):
    return _safe_str(row.get("QUERY_MESSAGE") or row.get("ENQ_MESSAGE") or row.get("MESSAGE") or "")


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


# =========================
# Territory Mapping
# =========================
def _norm_txt(v):
    return " ".join((v or "").strip().lower().split())


def _get_table_multiselect_link_field():
    meta = frappe.get_meta(ENQUIRY_DOCTYPE)
    table_field = meta.get_field("territory")
    if not table_field or not table_field.options:
        return ""

    child_dt = table_field.options
    child_meta = frappe.get_meta(child_dt)

    for df in child_meta.fields:
        if df.fieldtype == "Link" and df.options == "Territory":
            return df.fieldname

    for df in child_meta.fields:
        if df.fieldtype == "Link":
            return df.fieldname

    return ""


def _get_mapping_doc(settings_name, city_name=None, state_name=None):
    city_name_norm = _norm_txt(city_name)
    state_name_norm = _norm_txt(state_name)

    if not settings_name:
        return None

    meta = frappe.get_meta("IndiaMART Mapping")

    has_city = meta.has_field("city")
    has_state = meta.has_field("state")

    if not has_city and not has_state:
        return None

    fields = ["name"]
    if has_city:
        fields.append("city")
    if has_state:
        fields.append("state")

    rows = frappe.get_all(
        "IndiaMART Mapping",
        filters={"india_mart_api_settings": settings_name},
        fields=fields
    )

    # 1. Exact city field match
    if has_city and city_name_norm:
        for row in rows:
            if _norm_txt(row.get("city")) == city_name_norm:
                return frappe.get_doc("IndiaMART Mapping", row.get("name"))

    # 2. Backward compatibility:
    # if city value was wrongly stored inside state field
    if has_state and city_name_norm:
        for row in rows:
            if _norm_txt(row.get("state")) == city_name_norm:
                return frappe.get_doc("IndiaMART Mapping", row.get("name"))

    # 3. Real state match
    if has_state and state_name_norm:
        for row in rows:
            if _norm_txt(row.get("state")) == state_name_norm:
                return frappe.get_doc("IndiaMART Mapping", row.get("name"))

    return None


def _set_territory_rows(doc, settings_name, city_name=None, state_name=None):
    mapping_doc = _get_mapping_doc(settings_name, city_name, state_name)
    if not mapping_doc:
        return

    link_field = _get_table_multiselect_link_field()
    if not link_field:
        return

    existing_values = []
    current_rows = doc.get("territory") or []
    for row in current_rows:
        existing_values.append(_safe_str(row.get(link_field)))

    for map_row in (mapping_doc.get("territory") or []):
        territory_name = _safe_str(map_row.get("territory"))
        if not territory_name:
            continue

        if territory_name in existing_values:
            continue

        doc.append("territory", {
            link_field: territory_name
        })
        existing_values.append(territory_name)


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
    enquiry_state = _guess_state(row)
    enquiry_city = _guess_city(row)

    data = {
        "im_enquiry_id": im_enquiry_id,
        "indiamart_api_setting_id": settings_doc.name,
        "lead_user": settings_doc.lead_user,

        "full_name": _guess_full_name(row),
        "company": _guess_company(row),

        "query_type": _guess_query_type(row),
        "status": "Open",
        "sync_status": "New",

        "mobile": _guess_mobile(row),
        "email": _guess_email(row),

        "product_name": _guess_product_name(row),
        "product_category":_guess_product_category(row),
        "subject": _guess_subject(row),

        "city": enquiry_city,
        "state": enquiry_state,
        "country": _guess_country(row),

        "pin_code": _guess_pincode(row),
        "address": _guess_address(row),

        "message": strip_html(_guess_message(row)),

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

        _set_territory_rows(doc, settings_doc.name, enquiry_city, enquiry_state)

        doc.save(ignore_permissions=True)
        return "duplicate"

    doc = frappe.get_doc({"doctype": ENQUIRY_DOCTYPE})
    for k, v in data.items():
        if v is not None and v != "":
            doc.set(k, v)

    _set_territory_rows(doc, settings_doc.name, enquiry_city, enquiry_state)

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

        error_lines.append(
            "Chunk Start=" + _format_indiamart_dt(chunk_start) +
            " | Chunk End=" + _format_indiamart_dt(chunk_end)
        )
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