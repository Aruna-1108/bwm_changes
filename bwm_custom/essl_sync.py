# bwm_custom/bwm_custom/essl_sync.py
# eSSL (webapiservice.asmx) → ERPNext Employee Checkin via GetTransactionsLog
#
# Reads config from an ESSL settings DocType (can be Single or non-Single):
#   - essl_api_url (Data)
#   - essl_user_name (Data)  ← alias for essl_username
#   - essl_password (Data)   ← plain text (no decryption)
#   - custom_essl_serial_number (Data)  ← alias for essl_serial_number (optional)
#   - essl_days_back (Int) (optional, default 7)
#   - essl_allow_duplicates (Check) (optional)
#
# Public whitelisted endpoints:
#   1) essl_sync_transactions(from_datetime, to_datetime, serial_number="", preview=0)
#   2) essl_sync(from_datetime, to_datetime, preview=0, serial_number="")
#   3) essl_conf_debug()  ← quick config sanity check (does NOT return password)
#
# Typical Server Script (Scheduler Event) call:
#   frappe.call("bwm_custom.essl_sync.essl_sync",
#               from_datetime=from_dt, to_datetime=to_dt, preview=0)

import html
import re
from datetime import datetime, timedelta
from collections import defaultdict

import frappe
import requests
from frappe.utils import getdate, get_datetime, cint

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
}

def _get_settings_doc():
    """Return the ESSL settings doc (Single or most recently modified non-Single row)."""
    try:
        meta = frappe.get_meta(SETTINGS_DOTYPE)
        if getattr(meta, "is_single", 0):
            # Single DocType: only pass doctype
            return frappe.get_cached_doc(SETTINGS_DOTYPE)
    except Exception:
        pass

    # Non-Single: pick latest modified row (or any)
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
# Mapping & insertion (NO log_type logic)
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

def _has_field(doctype: str, fieldname: str) -> bool:
    try:
        return fieldname in frappe.get_meta(doctype).get_fieldnames()
    except Exception:
        return False

def _insert_checkin(emp: str, ts, device_name=None, device_location=None):
    """Insert Employee Checkin with de-dup (NO log_type used)."""
    if not emp or not ts:
        return "skipped_invalid", None

    allow_dups = cint(_conf("essl_allow_duplicates", 0))
    if not allow_dups and frappe.db.exists("Employee Checkin", {"employee": emp, "time": ts}):
        return "skipped_existing", None

    doc_dict = {
        "doctype": "Employee Checkin",
        "employee": emp,
        "time": ts,
        "device_id": "Biometrics"
    }
 

    doc = frappe.get_doc(doc_dict)
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
    """Fetch logs via GetTransactionsLog and (optionally) insert Employee Checkins (no log_type)."""
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
        "counts": {"fetched": len(raw_rows), "inserted": 0, "skipped_existing": 0, "unmatched": 0},
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

    # Insert or preview (NO IN/OUT alternation)
    for m in mapped:
        emp = m["employee"]
        ts = m["ts"]

        if preview:
            if len(out["examples"]) < 10:
                out["examples"].append({"employee": emp, "time": str(ts)})
            continue

        status, name = _insert_checkin(emp, ts, device_name="Biometrics")
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
# Scheduler helper (optional; if you wire via hooks.py instead of Server Script)
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
# Debug endpoint (does NOT reveal password)(For Checking)
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
        "password_present": bool(_conf("essl_password")),
    }
