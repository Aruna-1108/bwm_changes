# bwm_custom/bwm_custom/doctype/visit_doctype/visit_doctype.py
import json
import frappe
from frappe.model.document import Document
from frappe.utils import now_datetime, getdate

ROUND_PLACES = 6

# -----------------------------
# Helpers
# -----------------------------
def _fmt(v):
    """Format numeric to fixed decimals (as float)."""
    try:
        return round(float(v), ROUND_PLACES)
    except Exception:
        return None


def _to_float(v):
    """Treat None / '', 'null', 'undefined' as no value and coerce to float."""
    if v in (None, "", " ", "null", "undefined"):
        return None
    try:
        f = float(v)
        # NaN check
        if f != f:
            return None
        return f
    except (TypeError, ValueError):
        return None


def _write_coords(doc: Document, prefix: str, lat, lng, accuracy):
    """
    Writes:
      - {prefix}_geo_location (JSON/Geolocation → dict; Data/Text → JSON string)
      - {prefix}_latitude (Float/Data)
      - {prefix}_longitude (Float/Data)
    Ensure your DocType has those fieldnames.
    """
    lat = _to_float(lat)
    lng = _to_float(lng)
    acc = _to_float(accuracy)

    if lat is None or lng is None:
        return

    if not (-90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0):
        frappe.throw("Invalid GPS coordinates.")

    payload = {"lat": lat, "lng": lng, "accuracy": float(acc or 0.0)}
    fld = f"{prefix}_geo_location"

    # Decide how to store based on fieldtype
    df = doc.meta.get_field(fld)
    if df and df.fieldtype in ("JSON", "Geolocation"):
        doc.set(fld, payload)  # framework serializes dict properly
    else:
        # Store as JSON string for Data/Small Text/etc to avoid pymysql dict error
        doc.set(fld, json.dumps(payload, separators=(",", ":")))

    # Optional separate fields
    doc.set(f"{prefix}_latitude", _fmt(lat))
    doc.set(f"{prefix}_longitude", _fmt(lng))


# -----------------------------
# Party → Address/Contact helpers
# -----------------------------
def _get_party_contact_address(party_type: str, party: str) -> dict:
    """
    Return best Address display + Contact info for given party.
    Prefers default Address/Contact; falls back to most recently linked.
    Works for Customer, Lead, Prospect, Supplier, Employee, etc.
    """
    out = {
        "address_name": None,
        "address_display": None,
        "contact_name": None,
        "contact_mobile": None,
        "contact_phone": None,
        "contact_email": None,
    }

    if not party_type or not party:
        return out

    # ---------- Address ----------
    try:
        from frappe.contacts.doctype.address.address import get_default_address, get_address_display

        address_name = get_default_address(party_type, party)
        if not address_name:
            # fallback: most recently linked address
            addr_names = frappe.get_all(
                "Dynamic Link",
                filters={
                    "link_doctype": party_type,
                    "link_name": party,
                    "parenttype": "Address",
                },
                order_by="creation desc",
                pluck="parent",
                limit=1,
            )
            address_name = addr_names[0] if addr_names else None

        if address_name:
            out["address_name"] = address_name
            out["address_display"] = get_address_display(address_name)
    except Exception:
        pass  # graceful fallback

    # ---------- Contact ----------
    contact_name = None
    try:
        from frappe.contacts.doctype.contact.contact import get_default_contact
        contact_name = get_default_contact(party_type, party)
    except Exception:
        pass

    if not contact_name:
        # fallback: most recently linked contact
        cnt_names = frappe.get_all(
            "Dynamic Link",
            filters={
                "link_doctype": party_type,
                "link_name": party,
                "parenttype": "Contact",
            },
            order_by="creation desc",
            pluck="parent",
            limit=1,
        )
        contact_name = cnt_names[0] if cnt_names else None

    if contact_name:
        contact = frappe.get_doc("Contact", contact_name)
        out["contact_name"] = contact.name
        # Prefer mobile; fall back to phone or first phone row
        mobile = (contact.mobile_no or "").strip() or None
        phone = (contact.phone or "").strip() or None
        if not (mobile or phone) and getattr(contact, "phone_nos", None):
            try:
                phone = (contact.phone_nos[0].get("phone") or "").strip() or None
            except Exception:
                phone = phone or None
        out["contact_mobile"] = mobile
        out["contact_phone"] = phone
        out["contact_email"] = (
            (contact.email_id or "").strip()
            or (
                contact.email_ids[0].get("email_id").strip()
                if getattr(contact, "email_ids", None)
                and contact.email_ids
                and contact.email_ids[0].get("email_id")
                else None
            )
        )

    return out


def _maybe_fill_party_fields(doc: Document):
    """
    If party is present, populate party_address (Small Text) and mobile_number (Data)
    when empty (or when party changed).
    Optional hidden fields you may add later:
      - party_address_name (Link → Address)
      - contact (Link → Contact)
      - contact_email (Data)
    """
    party_type = (doc.get("party_type") or "").strip()
    party = (doc.get("party") or "").strip()
    if not (party_type and party):
        return

    # Detect change if framework supports has_value_changed (v14+); else populate when blank.
    try:
        changed = doc.has_value_changed("party") or doc.has_value_changed("party_type")
    except Exception:
        changed = False

    needs_address = changed or not (doc.get("party_address") or "").strip()
    needs_phone = changed or not (doc.get("mobile_number") or "").strip()

    if not (needs_address or needs_phone):
        return

    info = _get_party_contact_address(party_type, party)

    if needs_address:
        doc.set("party_address", info.get("address_display") or "")

        # If you have a hidden Address link field, uncomment:
        # if "party_address_name" in [df.fieldname for df in doc.meta.get("fields", [])]:
        #     doc.set("party_address_name", info.get("address_name") or "")

    if needs_phone:
        phone_to_use = info.get("contact_mobile") or info.get("contact_phone") or ""
        doc.set("mobile_number", phone_to_use)

    # If you have hidden Contact/Email fields, set them too (optional):
    # if "contact" in [df.fieldname for df in doc.meta.get("fields", [])]:
    #     doc.set("contact", info.get("contact_name") or "")
    # if "contact_email" in [df.fieldname for df in doc.meta.get("fields", [])]:
    #     doc.set("contact_email", info.get("contact_email") or "")


# -----------------------------
# Whitelisted button handlers
# -----------------------------
@frappe.whitelist()
def visit_check_in(visit_name: str, lat=None, lng=None, accuracy=None):
    """Set check-in time + GPS."""
    if not visit_name or str(visit_name).startswith("new-"):
        frappe.throw("Please save the Visit before Check-In.")

    doc = frappe.get_doc("Visit Doctype", visit_name)

    if doc.check_in_time:
        frappe.throw("Already Checked In.")

    doc.check_in_time = now_datetime()
    _write_coords(doc, "in", lat, lng, accuracy)

    # Ensure party details are present too (useful if using button-first flow)
    _maybe_fill_party_fields(doc)

    doc.save(ignore_permissions=True)
    frappe.db.commit()
    return {
        "ok": True,
        "check_in_time": doc.check_in_time,
        "in_geo_location": doc.in_geo_location,
        "in_latitude": doc.in_latitude,
        "in_longitude": doc.in_longitude,
    }


@frappe.whitelist()
def visit_check_out(visit_name: str, lat=None, lng=None, accuracy=None):
    """Set check-out time + GPS."""
    if not visit_name or str(visit_name).startswith("new-"):
        frappe.throw("Please save the Visit before Check-Out.")

    doc = frappe.get_doc("Visit Doctype", visit_name)

    if not doc.check_in_time:
        frappe.throw("Please Check In first.")
    if doc.check_out_time:
        frappe.throw("Already Checked Out.")

    doc.check_out_time = now_datetime()
    _write_coords(doc, "out", lat, lng, accuracy)

    # Ensure party details are present too (in case they were blank)
    _maybe_fill_party_fields(doc)

    doc.save(ignore_permissions=True)
    frappe.db.commit()
    return {
        "ok": True,
        "check_out_time": doc.check_out_time,
        "out_geo_location": doc.out_geo_location,
        "out_latitude": doc.out_latitude,
        "out_longitude": doc.out_longitude,
    }


# -----------------------------
# Document controller
# -----------------------------
class VisitDoctype(Document):
    def validate(self):
        if not self.visit_date:
            self.visit_date = getdate()

        # Populate Party Address / Mobile when party is set and fields are blank
        _maybe_fill_party_fields(self)

    def on_submit(self):
        """
        Append ONLY this visit's ID (self.name) to the linked Run Sheet's child table.
        Field assumptions:
          - Visit Doctype:     run_sheet (Link → Run Sheet)
          - Run Sheet:         visit_history (Table → Run Sheet Logs)
          - Run Sheet Logs:    visit_id (Data)
        """
        if not getattr(self, "run_sheet", None):
            return  # nothing to append to

        rs = frappe.get_doc("Run Sheet", self.run_sheet)

        # Avoid duplicate append
        if any(getattr(r, "visit_id", None) == self.name for r in (rs.get("visit_history") or [])):
            return

        row = rs.append("visit_history", {})
        row.visit_id = self.name

        # Save parent
        rs.save(ignore_permissions=True)

        # Realtime ping to refresh the Run Sheet form (optional)
        try:
            frappe.publish_realtime(
                event="visit_appended",
                message={"run_sheet": rs.name, "visit_id": self.name},
            )
        except Exception:
            # Realtime is best-effort; don't block submit if not configured
            pass
