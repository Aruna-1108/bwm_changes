# bwm_custom/bwm_custom/essl_sync.py
# eSSL (webapiservice.asmx) → ERPNext Employee Checkin via GetTransactionsLog
# Option B: near real-time polling (every minute) using cursor + overlap
#
# Multi-Device Enhancements:
# - Supports multiple Serial Numbers (CSV in ESSL.essl_serial_number, or ESSL child table "essl_serials")
# - Per-device cursor (last_cursor) when child table exists; falls back to global essl_last_cursor
# - Single tick iterates all enabled devices, applying overlap and dedup per device
# - Employee Checkin.device_id is set to "ESSL:<serial>"
#
# IN/OUT Enhancements:
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
    "essl_serial_number":    ["essl_serial_number", "custom_essl_serial_number", "serial_number"],  # may be CSV
    "essl_days_back":        ["essl_days_back"],
    "essl_allow_duplicates": ["essl_allow_duplicates"],
    "essl_site_url":         ["custom_site_url", "site_url"],  # not required; debug only
    "essl_last_cursor":      ["essl_last_cursor"],             # Datetime (global fallback)
    "essl_infer_log_type":   ["essl_infer_log_type"],          # Check (0/1); optional; defaults to ON
}

# Optional child table on ESSL settings:
# Fieldname on ESSL: essl_serials (Table)
# Child DocType: "ESSL Serial" with fields:
#   - serial_number (Data, req)
#   - enabled (Check, default 1)
#   - last_cursor (Datetime)
#   - device_label (Data, optional)


# ---------------------------------------------------------------------------
# Config getters
# ---------------------------------------------------------------------------
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
# Serial number management (CSV or Child Table)
# ---------------------------------------------------------------------------
def _parse_csv_serials(csv_val: str) -> list[str]:
    out = []
    if not csv_val:
        return out
    for tok in re.split(r"[,\s]+", str(csv_val)):
        s = tok.strip()
        if s and s.upper() != "ALL":
            out.append(s)
    # dedupe keep order
    seen = set()
    uniq = []
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def _get_child_rows_or_none(doc):
    """Return list of child rows if ESSL has a Table field 'essl_serials'; else None."""
    try:
        rows = getattr(doc, "essl_serials", None)
        if isinstance(rows, list):
            return rows
    except Exception:
        pass
    return None


def _get_all_serials() -> list[str]:
    """
    Return the list of serial numbers to poll (enabled only if child table present).
    Priority:
      1) Child table 'essl_serials' (enabled == 1)
      2) CSV in ESSL.essl_serial_number (comma/whitespace separated)
    """
    doc = _get_settings_doc()
    if not doc:
        return []

    rows = _get_child_rows_or_none(doc)
    if rows:
        vals = [ (r.get("serial_number") or "").strip() for r in rows if cint(r.get("enabled", 1)) ]
        return [v for v in vals if v]

    # fallback: CSV
    return _parse_csv_serials(_conf("essl_serial_number") or "")


def _get_last_cursor_global() -> str | None:
    try:
        return _conf_from_settings("essl_last_cursor")
    except Exception:
        return None


def _set_last_cursor_global(dt_str: str):
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


def _get_last_cursor_for_serial(serial: str) -> str | None:
    """
    If child table exists, return per-device last_cursor.
    Else return global last_cursor.
    """
    doc = _get_settings_doc()
    if not doc:
        return None

    rows = _get_child_rows_or_none(doc)
    if rows:
        for r in rows:
            if (r.get("serial_number") or "").strip() == serial:
                return r.get("last_cursor")
        return None
    # fallback: global
    return _get_last_cursor_global()


def _set_last_cursor_for_serial(serial: str, dt_str: str):
    """
    If child table exists, update per-device last_cursor.
    Else update global last_cursor.
    """
    doc = _get_settings_doc()
    if not doc:
        return

    rows = _get_child_rows_or_none(doc)
    if rows:
        changed = False
        for r in rows:
            if (r.get("serial_number") or "").strip() == serial:
                r.last_cursor = dt_str
                changed = True
                break
        if changed:
            # Save Single/non-Single settings doc safely
            doc.save(ignore_permissions=True)
            frappe.db.commit()
        return

    # fallback: global
    _set_last_cursor_global(dt_str)


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
# PUBLIC APIs: single-device sync (internal use by the multi-device tick)
# ---------------------------------------------------------------------------
def _sync_one_device(from_datetime: str, to_datetime: str, serial_number: str, preview: int | str = 0):
    """Fetch logs via GetTransactionsLog for ONE device and insert Employee Checkins."""
    _check_required_conf_transactions()
    preview = cint(preview)
    serial = (serial_number or "").strip()

    payload = {
        "FromDateTime": from_datetime,
        "ToDateTime": to_datetime,
        "SerialNumber": serial,  # device serial; required by server to scope
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

        status, name = _insert_checkin(emp, ts, device_id=f"ESSL:{serial or 'UNKNOWN'}")
        if status not in out["counts"]:
            out["counts"][status] = 0
        out["counts"][status] += 1
        if status == "inserted" and len(out["examples"]) < 10:
            out["examples"].append({"name": name, "employee": emp, "time": str(ts)})

    return out


@frappe.whitelist()
def essl_sync(from_datetime: str, to_datetime: str, preview: int | str = 0, serial_number: str = ""):
    """
    Thin wrapper for one device (kept for backward compatibility / ad-hoc runs).
    Prefer sync_realtime_tick() which loops all configured serials.
    """
    return _sync_one_device(from_datetime, to_datetime, serial_number=serial_number, preview=preview)


# ---------------------------------------------------------------------------
# Cursor helpers + 1-minute tick (Option B) — MULTI DEVICE
# ---------------------------------------------------------------------------
@frappe.whitelist()
def sync_realtime_tick(overlap_seconds: int = 90, backfill_minutes_if_empty: int = 10, preview: int | str = 0):
    """
    Run every minute for ALL configured devices:
      - to   = now
      - from = (per-device last_cursor - overlap) OR (now - backfill) if cursor empty
    De-dup makes it idempotent.
    """
    now = datetime.now()
    to_dt = now.strftime("%Y-%m-%d %H:%M:%S")
    serials = _get_all_serials()

    out = {
        "to": to_dt,
        "preview": bool(cint(preview)),
        "devices": [],
        "totals": {"fetched": 0, "inserted": 0, "skipped_existing": 0, "skipped_invalid": 0, "unmatched": 0},
    }

    # Prevent overlapping runs (single lock for the whole batch)
    with frappe.cache().lock("essl_realtime_sync_lock", timeout=55):
        for sn in serials or [""]:  # allow empty to support legacy single-device misconfig
            try:
                last = _get_last_cursor_for_serial(sn)
                if last:
                    start = get_datetime(last) - timedelta(seconds=max(0, int(overlap_seconds)))
                else:
                    start = now - timedelta(minutes=max(1, int(backfill_minutes_if_empty)))
                from_dt = start.strftime("%Y-%m-%d %H:%M:%S")

                dev_res = _sync_one_device(from_dt, to_dt, serial_number=sn, preview=preview)

                # accumulate totals
                for k in out["totals"].keys():
                    out["totals"][k] += dev_res["counts"].get(k, 0)

                out["devices"].append(dev_res)

                # Update per-device cursor only on success
                _set_last_cursor_for_serial(sn, to_dt)

            except Exception as e:
                # do not advance cursor on failure; log error
                frappe.log_error(frappe.get_traceback(), f"eSSL realtime device [{sn}] failed: {e}")
                out["devices"].append({
                    "serial_number": sn,
                    "error": str(e),
                })

    return out


# ---------------------------------------------------------------------------
# (Optional) legacy n-days pull (Multi-device)
# ---------------------------------------------------------------------------
def sync_last_n_days_transactions():
    """Pull last n days (essl_days_back, default 7) up to now for ALL devices; safe due to de-dup."""
    try:
        n = cint(_conf("essl_days_back") or 7)
        now = datetime.now()
        to_dt = now.strftime("%Y-%m-%d %H:%M:%S")
        from_dt = (now - timedelta(days=max(1, n) - 1)).strftime("%Y-%m-%d 00:00:00")

        for sn in _get_all_serials() or [""]:
            try:
                _sync_one_device(from_dt, to_dt, serial_number=sn, preview=0)
                _set_last_cursor_for_serial(sn, to_dt)
            except Exception as e:
                frappe.log_error(frappe.get_traceback(), f"eSSL legacy pull device [{sn}] failed: {e}")
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), f"eSSL GetTransactionsLog multi-device sync failed: {e}")


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
    serials = _get_all_serials()
    per_device_cursors = []
    for sn in serials or []:
        per_device_cursors.append({"serial_number": sn, "last_cursor": _get_last_cursor_for_serial(sn)})

    return {
        "doc_found": bool(doc),
        "doc_name": getattr(doc, "name", None),
        "essl_api_url": _conf("essl_api_url"),
        "essl_username": _conf("essl_username"),
        "serials": serials,
        "essl_days_back": _conf("essl_days_back"),
        "essl_allow_duplicates": _conf("essl_allow_duplicates"),
        "essl_site_url": _conf("essl_site_url"),
        "global_last_cursor": _get_last_cursor_global(),
        "per_device_cursors": per_device_cursors,
        "essl_infer_log_type": cint(_conf("essl_infer_log_type", 1)),
        "password_present": bool(_conf("essl_password")),
        "child_table_present": bool(_get_child_rows_or_none(doc)),
    }
