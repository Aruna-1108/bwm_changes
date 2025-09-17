# bwm_custom/bwm_custom/essl_sync.py
# eSSL (webapiservice.asmx) → ERPNext Employee Checkin via GetTransactionsLog
# Option B: near real-time polling (every minute) using cursor + overlap
#
# Enhancements:
# - Auto IN/OUT (log_type) inference per employee per day (alternating)
# - Backfill utility to fill log_type for already-inserted blank rows
# - Settings toggle: essl_infer_log_type (defaults to ON)

import html
import re
from datetime import datetime, timedelta

import frappe
import requests
from frappe.utils import get_datetime, cint

SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
TEMPURI = "http://tempuri.org/"

# ---------------------------------------------------------------------------
# SETTINGS (support Single and non-Single ESSL doctypes, plus field aliases)
# ---------------------------------------------------------------------------
SETTINGS_DOTYPE = "ESSL"       # Your settings doctype label
SETTINGS_NAME = "ESSL"         # If Single, its "name" equals the doctype

# Aliases: read any of these fieldnames for the given logical key
_CONF_ALIASES = {
    "essl_api_url":          ["essl_api_url"],
    "essl_username":         ["essl_username", "essl_user_name"],
    "essl_password":         ["essl_password"],  # plain Data field
    "essl_serial_number":    ["essl_serial_number", "custom_essl_serial_number", "serial_number"],
    "essl_days_back":        ["essl_days_back"],
    "essl_allow_duplicates": ["essl_allow_duplicates"],
    "essl_site_url":         ["custom_site_url", "site_url"],  # not required; debug only
    "essl_last_cursor":      ["essl_last_cursor"],             # Datetime
    "essl_infer_log_type":   ["essl_infer_log_type"],          # Check (0/1); optional; defaults to ON
}

def _get_settings_doc():
    """Return the ESSL settings doc (Single or most recently modified non-Single row)."""
    try:
        meta = frappe.get_meta(SETTINGS_DOTYPE)
        if getattr(meta, "is_single", 0):
            return frappe.get_cached_doc(SETTINGS_DOTYPE)
    except Exception:
        pass

    name = frappe.db.get_value(SETTINGS_DOTYPE, {}, "name", order_by="modified desc")
    return frappe.get_doc(SETTINGS_DOTYPE, name) if name else None

def _conf_from_settings(key):
    """Read a logical key from the ESSL settings record (password is plain)."""
    doc = _get_settings_doc()
    if not doc:
        return None

    if key == "essl_password":
        val = doc.get("essl_password")
        if isinstance(val, str):
            val = val.strip()
        return val or None

    for fname in _CONF_ALIASES.get(key, [key]):
        val = doc.get(fname)
        if isinstance(val, str):
            val = val.strip()
        if val not in (None, "", " "):
            return val
    return None

def _conf(key, default=None):
    """Resolve value with precedence: ESSL settings → site_config → default."""
    # 1) ESSL settings
    val = _conf_from_settings(key)
    if val not in (None, "", " "):
        return val

    # 2) site_config (respect aliases)
    for fname in _CONF_ALIASES.get(key, [key]):
        v = frappe.local.conf.get(fname)
        if isinstance(v, str):
            v = v.strip()
        if v not in (None, "", " "):
            return v

    # 3) default
    return default

def _check_required_conf_transactions():
    """Ensure minimum config exists before calling the SOAP API."""
    required = ("essl_api_url", "essl_username", "essl_password")
    missing = [k for k in required if not _conf(k)]
    if missing:
        raise frappe.ValidationError(f"Missing ESSL settings: {', '.join(missing)}")

# ---------------------------------------------------------------------------
# SOAP helpers
# ---------------------------------------------------------------------------
def _soap_envelope(inner_xml: str) -> str:
    return f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="{SOAP_NS}">
  <soap:Body>
    {inner_xml}
  </soap:Body>
</soap:Envelope>""".strip()

def _soap_call_raw(method: str, params: dict) -> str:
    """POST a SOAP 1.1 call and return the FULL envelope text (we parse manually)."""
    _check_required_conf_transactions()
    url = _conf("essl_api_url")
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": f"{TEMPURI}{method}",
    }

    # Preserve parameter order (server can be picky)
    params_xml = "".join(
        f"<{k}>{html.escape('' if params[k] is None else str(params[k]))}</{k}>"
        for k in params.keys()
    )
    inner = f'<{method} xmlns="{TEMPURI}">{params_xml}</{method}>'
    xml = _soap_envelope(inner)

    resp = requests.post(url, data=xml.encode("utf-8"), headers=headers, timeout=40)
    resp.raise_for_status()
    return resp.text

# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------
def _parse_transactions_xml(resp_xml: str):
    """Extracts (GetTransactionsLogResult, strDataList) as (result_text, data_text)."""
    m_res = re.search(
        r"<GetTransactionsLogResult>(.*?)</GetTransactionsLogResult>",
        resp_xml, flags=re.S | re.I
    )
    result_text = (m_res.group(1).strip() if m_res else "")

    m_data = re.search(r"<strDataList>(.*?)</strDataList>", resp_xml, flags=re.S | re.I)
    data_text = (m_data.group(1) if m_data else "")
    data_text = html.unescape(data_text)
    return result_text, data_text

def _parse_strdatalist(data_text: str):
    """Parse TAB-separated lines → [{'emp_code': str, 'ts': datetime}, ...]."""
    rows = []
    for raw in (data_text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        emp_code = parts[0].strip()
        ts_s = parts[1].strip()
        try:
            ts = get_datetime(ts_s)   # "YYYY-MM-DD HH:MM:SS"
        except Exception:
            continue
        rows.append({"emp_code": emp_code, "ts": ts})
    return rows

# ---------------------------------------------------------------------------
# Mapping & insertion (WITH log_type logic)
# ---------------------------------------------------------------------------
def _map_employee(emp_code: str) -> str | None:
    """Map device emp_code → Employee.name via:
       1) attendance_device_id → 2) Employee.name → 3) employee_number
    """
    if not emp_code:
        return None
    name = frappe.db.get_value("Employee", {"attendance_device_id": emp_code}, "name")
    if name:
        return name
    if frappe.db.exists("Employee", emp_code):
        return emp_code
    name = frappe.db.get_value("Employee", {"employee_number": emp_code}, "name")
    return name  # could be None

def _infer_log_type(emp: str, ts) -> str:
    """
    Decide IN/OUT by alternating punches for an employee per calendar day.
    1st punch of the day → IN, 2nd → OUT, 3rd → IN, ...
    Uses rows strictly BEFORE current ts to choose the next state deterministically.
    """
    r = frappe.db.sql(
        """
        SELECT COUNT(*) AS c
        FROM `tabEmployee Checkin`
        WHERE employee = %s
          AND DATE(time) = DATE(%s)
          AND time < %s
        """,
        (emp, ts, ts),
        as_dict=True,
    )
    c = (r[0]["c"] if r else 0) or 0
    return "IN" if c % 2 == 0 else "OUT"

def _insert_checkin(emp: str, ts, device_id="Biometrics"):
    """Insert Employee Checkin with de-dup and auto log_type (IN/OUT)."""
    if not emp or not ts:
        return "skipped_invalid", None

    # De-dup by (employee, time)
    allow_dups = cint(_conf("essl_allow_duplicates", 0))
    if not allow_dups and frappe.db.exists("Employee Checkin", {"employee": emp, "time": ts}):
        return "skipped_existing", None

    log_type = None
    if cint(_conf("essl_infer_log_type", 1)):  # default ON
        log_type = _infer_log_type(emp, ts)

    doc = frappe.get_doc({
        "doctype": "Employee Checkin",
        "employee": emp,
        "time": ts,
        "device_id": device_id,
        **({"log_type": log_type} if log_type else {}),
    })
    doc.flags.ignore_permissions = True
    doc.insert()
    return "inserted", doc.name

# ---------------------------------------------------------------------------
# PUBLIC APIs
# ---------------------------------------------------------------------------
@frappe.whitelist()
def essl_sync_transactions(from_datetime: str,
                           to_datetime: str,
                           serial_number: str = "",
                           preview: int | str = 0):
    """Fetch logs via GetTransactionsLog and (optionally) insert Employee Checkins (with IN/OUT)."""
    _check_required_conf_transactions()
    preview = cint(preview)
    serial = (serial_number or _conf("essl_serial_number") or "")

    payload = {
        "FromDateTime": from_datetime,
        "ToDateTime": to_datetime,
        "SerialNumber": serial,
        "UserName": _conf("essl_username"),
        "UserPassword": _conf("essl_password"),
    }

    xml = _soap_call_raw("GetTransactionsLog", payload)
    result_text, data_text = _parse_transactions_xml(xml)
    raw_rows = _parse_strdatalist(data_text)

    out = {
        "result": result_text,
        "from": from_datetime,
        "to": to_datetime,
        "serial_number": serial,
        "preview": bool(preview),
        "counts": {"fetched": len(raw_rows), "inserted": 0, "skipped_existing": 0, "skipped_invalid": 0, "unmatched": 0},
        "unmatched": [],   # up to 50 examples of unmapped employees
        "examples": [],    # up to 10 inserted (or preview) rows
    }

    # Map device codes → Employee
    mapped = []
    for r in raw_rows:
        emp = _map_employee(r["emp_code"])
        if not emp:
            out["counts"]["unmatched"] += 1
            if len(out["unmatched"]) < 50:
                out["unmatched"].append({"emp_code": r["emp_code"], "time": str(r["ts"])})
            continue
        mapped.append({"employee": emp, "ts": r["ts"]})

    # Insert or preview
    for m in mapped:
        emp = m["employee"]
        ts = m["ts"]

        if preview:
            if len(out["examples"]) < 10:
                out["examples"].append({"employee": emp, "time": str(ts)})
            continue

        status, name = _insert_checkin(emp, ts, device_id="Biometrics")
        if status not in out["counts"]:
            out["counts"][status] = 0
        out["counts"][status] += 1
        if status == "inserted" and len(out["examples"]) < 10:
            out["examples"].append({"name": name, "employee": emp, "time": str(ts)})

    return out

@frappe.whitelist()
def essl_sync(from_datetime: str, to_datetime: str, preview: int | str = 0, serial_number: str = ""):
    """Thin wrapper around essl_sync_transactions (keeps server-script call short)."""
    return essl_sync_transactions(
        from_datetime=from_datetime,
        to_datetime=to_datetime,
        serial_number=serial_number or "",
        preview=preview,
    )

# ---------------------------------------------------------------------------
# Cursor helpers + 1-minute tick (Option B)
# ---------------------------------------------------------------------------
def _get_last_cursor():
    try:
        return _conf_from_settings("essl_last_cursor")
    except Exception:
        return None

def _set_last_cursor(dt_str: str):
    try:
        if frappe.get_meta(SETTINGS_DOTYPE).is_single:
            frappe.db.set_value(SETTINGS_DOTYPE, SETTINGS_NAME, "essl_last_cursor", dt_str)
        else:
            name = frappe.db.get_value(SETTINGS_DOTYPE, {}, "name", order_by="modified desc")
            if name:
                frappe.db.set_value(SETTINGS_DOTYPE, name, "essl_last_cursor", dt_str)
        frappe.db.commit()
    except Exception:
        pass

@frappe.whitelist()
def sync_realtime_tick(overlap_seconds: int = 90, backfill_minutes_if_empty: int = 10, preview: int | str = 0):
    """
    Run every minute:
      - to   = now
      - from = (last_cursor - overlap) OR (now - backfill) if cursor empty
    De-dup makes it idempotent.
    """
    now = datetime.now()
    to_dt = now.strftime("%Y-%m-%d %H:%M:%S")

    last = _get_last_cursor()
    if last:
        start = get_datetime(last) - timedelta(seconds=max(0, int(overlap_seconds)))
    else:
        start = now - timedelta(minutes=max(1, int(backfill_minutes_if_empty)))
    from_dt = start.strftime("%Y-%m-%d %H:%M:%S")

    # Prevent overlapping runs
    with frappe.cache().lock("essl_realtime_sync_lock", timeout=55):
        out = essl_sync_transactions(from_dt, to_dt, preview=preview)
        _set_last_cursor(to_dt)
        return out

# ---------------------------------------------------------------------------
# (Optional) legacy n-days pull
# ---------------------------------------------------------------------------
def sync_last_n_days_transactions():
    """Pull last n days (essl_days_back, default 7) up to now; safe due to de-dup."""
    try:
        n = cint(_conf("essl_days_back") or 7)
        now = datetime.now()
        to_dt = now.strftime("%Y-%m-%d %H:%M:%S")
        from_dt = (now - timedelta(days=max(1, n) - 1)).strftime("%Y-%m-%d 00:00:00")
        essl_sync_transactions(from_dt, to_dt, preview=0)
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), f"eSSL GetTransactionsLog sync failed: {e}")

# ---------------------------------------------------------------------------
# Backfill (one-time utility) — fill blank log_type by alternating per employee/day
# ---------------------------------------------------------------------------
@frappe.whitelist()
def essl_backfill_log_type(employee: str = None, date_from: str = None, date_to: str = None, dry_run: int | str = 1):
    """
    Fill missing log_type by alternating IN/OUT per employee per day, ordered by time.
    - Optional filters: employee, date_from, date_to (YYYY-MM-DD).
    - dry_run=1 -> only returns what would change.
    """
    dry = cint(dry_run)
    params = []
    where = ["(ec.log_type IS NULL OR ec.log_type = '')"]

    if employee:
        where.append("ec.employee = %s")
        params.append(employee)
    if date_from:
        where.append("DATE(ec.time) >= %s")
        params.append(date_from)
    if date_to:
        where.append("DATE(ec.time) <= %s")
        params.append(date_to)

    rows = frappe.db.sql(
        f"""
        SELECT ec.name, ec.employee, ec.time
        FROM `tabEmployee Checkin` ec
        WHERE {" AND ".join(where)}
        ORDER BY ec.employee, DATE(ec.time), ec.time
        """,
        tuple(params),
        as_dict=True,
    )

    out = {"to_update": 0, "updated": 0, "samples": []}
    last_emp = None
    last_date = None
    flip = 0   # 0=>IN, 1=>OUT

    for r in rows:
        emp = r["employee"]
        d = r["time"].date()
        if emp != last_emp or d != last_date:
            flip = 0
            last_emp, last_date = emp, d

        log_type = "IN" if flip == 0 else "OUT"
        flip = 1 - flip

        if dry:
            if len(out["samples"]) < 20:
                out["samples"].append({"name": r["name"], "employee": emp, "time": str(r["time"]), "log_type": log_type})
            out["to_update"] += 1
        else:
            frappe.db.set_value("Employee Checkin", r["name"], "log_type", log_type, update_modified=False)
            out["updated"] += 1

    if not dry and rows:
        frappe.db.commit()
    return out

# ---------------------------------------------------------------------------
# Debug endpoint (does NOT reveal password)
# ---------------------------------------------------------------------------
@frappe.whitelist()
def essl_conf_debug():
    """Quick check that settings are read correctly (password not returned)."""
    doc = _get_settings_doc()
    return {
        "doc_found": bool(doc),
        "doc_name": getattr(doc, "name", None),
        "essl_api_url": _conf("essl_api_url"),
        "essl_username": _conf("essl_username"),
        "essl_serial_number": _conf("essl_serial_number"),
        "essl_days_back": _conf("essl_days_back"),
        "essl_allow_duplicates": _conf("essl_allow_duplicates"),
        "essl_site_url": _conf("essl_site_url"),
        "essl_last_cursor": _conf("essl_last_cursor"),
        "essl_infer_log_type": cint(_conf("essl_infer_log_type", 1)),
        "password_present": bool(_conf("essl_password")),
    }
