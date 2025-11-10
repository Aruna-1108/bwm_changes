# apps/bwm_custom/bwm_custom/bwm_custom/doctype/visit_doctype/visit_doctype.py

import frappe
from frappe.model.document import Document

# Correct imports in ERPNext v15
from frappe.contacts.doctype.address.address import (
    get_default_address,
    get_address_display,
)
from frappe.contacts.doctype.contact.contact import get_default_contact



# ------------------------------- Config ---------------------------------------

RUN_SHEET_TABLE_FIELD = "visit_history"          # Table field on Run Sheet
RUN_SHEET_CHILD_DTYPE = "Run Sheet Logs"         # Child doctype used in Run Sheet

CUSTOMER_TABLE_FIELD  = "custom_logs"            # Table field on Customer
CUSTOMER_CHILD_DTYPE  = "Customer Visit Reference"

LEAD_TABLE_FIELD      = "custom_runsheet_logs"   # Table field on Lead
LEAD_CHILD_DTYPE      = "Customer Visit Reference"

CRMDEAL_TABLE_FIELD   = "custom_logs"            # Table field on CRM Deal
CRMDEAL_CHILD_DTYPE   = "Customer Visit Reference"


def _assert_child_table(parent_dt: str, table_field: str, expected_child_dt: str) -> None:
    """Ensure the parent has a Table field pointing to the expected child doctype."""
    meta = frappe.get_meta(parent_dt)
    df = meta.get_field(table_field)
    if not df:
        frappe.throw(
            f"[{parent_dt}] missing field '{table_field}'. "
            f"Create a Table field with Options = '{expected_child_dt}'."
        )
    if df.fieldtype != "Table":
        frappe.throw(f"[{parent_dt}.{table_field}] must be a Table field (got {df.fieldtype}).")
    if (df.options or "").strip() != expected_child_dt:
        frappe.throw(
            f"[{parent_dt}.{table_field}] Options must be '{expected_child_dt}', "
            f"found '{df.options}'."
        )


def _compact(d: dict) -> dict:
    """Return a shallow copy of d with empty/None values removed."""
    return {k: v for k, v in (d or {}).items() if v not in (None, "", [], {})}

def _auto_set_nesta(doc):
    """Set custom_nesta from party_type.
    Non Existing Customer -> Pre-Nesta, others -> Post-Nesta.
    """
    if not hasattr(doc, "custom_nesta"):
        return

    pt = (getattr(doc, "party_type", "") or "").strip()
    if not pt:
        # no party type; clear or leave as-is—choose one:
        # doc.custom_nesta = ""
        return

    target = "Pre-Nesta" if pt == "Non Existing Customer" else "Post-Nesta"

    # Only write if different to avoid needless DB writes
    if (doc.custom_nesta or "").strip() != target:
        doc.custom_nesta = target



def _fill_visit_party_fields(doc: Document) -> None:
    """
    Server-side enrichment on Visit Doctype:
    - party_address (HTML/text)
    - mobile_number (from Contact; Lead fallback)
    - lead_name (for Lead)
    Safe: writes only if the field exists on doc.
    """
    party_type = getattr(doc, "party_type", None)
    party      = getattr(doc, "party", None)
    if not (party_type and party):
        # Clear stale
        if hasattr(doc, "party_address"):
            doc.party_address = ""
        if hasattr(doc, "mobile_number"):
            doc.mobile_number = ""
        if hasattr(doc, "lead_name"):
            doc.lead_name = ""
        return

    # --- Address
    addr_html = ""
    addr_name = get_default_address(party_type, party)
    if addr_name:
        addr_html = get_address_display(addr_name) or ""

    # --- Contact phone
    phone = ""
    contact_name = get_default_contact(party_type, party)
    if contact_name:
        cont = frappe.get_doc("Contact", contact_name)
        phone = cont.mobile_no or cont.phone or ""
        # check child table if needed
        if not phone and getattr(cont, "phone_nos", None):
            for ph in cont.phone_nos:
                if getattr(ph, "phone", None):
                    phone = ph.phone
                    break

    # --- Lead fallback
    if party_type == "Lead" and not phone:
        lead_vals = frappe.db.get_value("Lead", party, ["mobile_no", "phone"], as_dict=True)
        if lead_vals:
            phone = lead_vals.mobile_no or lead_vals.phone or ""

    # --- lead_name
    lead_name = ""
    if party_type == "Lead":
        lead_name = frappe.db.get_value("Lead", party, "lead_name") or ""

    # --- write back if fields exist
    if hasattr(doc, "party_address"):
        doc.party_address = addr_html or ""
    if hasattr(doc, "mobile_number"):
        doc.mobile_number = phone or ""
    if hasattr(doc, "lead_name"):
        doc.lead_name = lead_name or ""


def _runsheet_row_payload(doc: Document) -> dict:
    """
    Build the payload for Run Sheet Logs. Include many useful columns,
    while tolerating missing fields on the child doctype (Frappe ignores unknown keys).
    Adjust keys to match your child doctype fieldnames if needed.
    """
    return _compact({
        "visit_id":            getattr(doc, "name", None),
        "visit_date":          getattr(doc, "visit_date", None),
        "status":              getattr(doc,"custom_status",None),

        "party_type":          getattr(doc, "party_type", None),
        "party":               getattr(doc, "party", None),
        "party_name":          getattr(doc, "party", None),  # if child uses party_name

        # Quick readouts for the manager
        "purpose":             getattr(doc, "purpose", None),
        "outcome":             getattr(doc, "outcome", None),

        # Your custom “copy”/labels from Customize Form
        "purpose_of_visit":        getattr(doc, "purpose__of_visit", None),
        "outcome_of_visit":        getattr(doc, "outcome_of_visit", None),
        "outcome_of_visit_copy":   getattr(doc, "custom_nesta", None),
        "follow_up_date":          getattr(doc,"follow_up_date",None),
        "non_existing_customer":   getattr(doc,"non_existing_customer",None),

        # Party quick info (if you created fields on child)
        "party_address":       getattr(doc, "party_address", None),
        "phone_number":        getattr(doc, "mobile_number", None),

        # Ownership context
        "employee_id":         getattr(doc, "employee", None),
        "runsheet_id":         getattr(doc, "run_sheet", None),

        # Optional geo/timestamps if your child has them
        "check_in_time":       getattr(doc, "check_in_time", None),
        "check_out_time":      getattr(doc, "check_out_time", None),
        "meeting_start_time":  getattr(doc, "meeting_start_time", None),
        "meeting_end_time":    getattr(doc, "meeting_end_time", None),
        "waiting_start_time":  getattr(doc, "waiting_start_time", None),
        "waiting_end_time":    getattr(doc, "waiting_end_time", None),

        "in_latitude":         getattr(doc, "in_latitude", None),
        "in_longitude":        getattr(doc, "in_longitude", None),
        "out_latitude":        getattr(doc, "out_latitude", None),
        "out_longitude":       getattr(doc, "out_longitude", None),

        # If you want to store your custom_runsheet_party_id (from Visit) on RS row:
        "runsheet_party_id":   getattr(doc, "custom_runsheet_party_id", None),
    })


def _account_row_payload(doc: Document) -> dict:
    """
    Payload for Customer/Lead/CRM Deal child logs (“Customer Visit Reference”).
    Keep keys aligned to that child doctype for best UX; unknown keys are ignored.
    """
    employee_name = (
        frappe.db.get_value("Employee", doc.employee, "employee_name")
        if getattr(doc, "employee", None) else None
    )
    return _compact({
        "visit_id":       getattr(doc, "name", None),
        "visit_date":     getattr(doc, "visit_date", None),
        "purpose":        getattr(doc, "purpose", None),
        "outcome":        getattr(doc, "outcome", None),

        # Mirror the custom labels too, if your child has them
        "purpose_of_visit":      getattr(doc, "purpose__of_visit", None),
        "outcome_of_visit":      getattr(doc, "outcome_of_visit", None),
        "nesta_stage":           getattr(doc, "custom_nesta", None),

        # Owner context
        "employee_id":    getattr(doc, "employee", None),
        "employee_name":  employee_name,
        "runsheet_id":    getattr(doc, "run_sheet", None),
    })


# ------------------------------- Docclass -------------------------------------

class VisitDoctype(Document):
    def validate(self):
        # Auto-fill address/phone/lead_name on the Visit itself
        _fill_visit_party_fields(self)
        _auto_set_nesta(self)
    
    

    def on_submit(self):
        """On submit, append references to Run Sheet and (conditionally) Customer/Lead/CRM Deal."""
        # 1) Run Sheet
        if getattr(self, "run_sheet", None):
            _assert_child_table("Run Sheet", RUN_SHEET_TABLE_FIELD, RUN_SHEET_CHILD_DTYPE)
            rs = frappe.get_doc("Run Sheet", self.run_sheet)
            rs.append(RUN_SHEET_TABLE_FIELD, _runsheet_row_payload(self))
            rs.save(ignore_permissions=True)

        # 2) Account objects
        payload = _account_row_payload(self)

        # Customer
        if getattr(self, "party_type", None) == "Customer" and getattr(self, "party", None):
            _assert_child_table("Customer", CUSTOMER_TABLE_FIELD, CUSTOMER_CHILD_DTYPE)
            cust = frappe.get_doc("Customer", self.party)
            cust.append(CUSTOMER_TABLE_FIELD, payload)
            cust.save(ignore_permissions=True)

        # Lead
        if getattr(self, "party_type", None) == "Lead" and getattr(self, "party", None):
            _assert_child_table("Lead", LEAD_TABLE_FIELD, LEAD_CHILD_DTYPE)
            lead = frappe.get_doc("Lead", self.party)
            lead.append(LEAD_TABLE_FIELD, payload)
            lead.save(ignore_permissions=True)

        # CRM Deal
        if getattr(self, "party_type", None) == "CRM Deal" and getattr(self, "party", None):
            _assert_child_table("CRM Deal", CRMDEAL_TABLE_FIELD, CRMDEAL_CHILD_DTYPE)
            deal = frappe.get_doc("CRM Deal", self.party)
            deal.append(CRMDEAL_TABLE_FIELD, payload)
            deal.save(ignore_permissions=True)

        # (Frappe auto-commits per request)

    def on_cancel(self):
        """On cancel, remove the references we added on submit."""
        # Remove RS rows by visit_id (simple, robust)
        if getattr(self, "run_sheet", None):
            rs = frappe.get_doc("Run Sheet", self.run_sheet)
            rows = rs.get(RUN_SHEET_TABLE_FIELD) or []
            rs.set(
                RUN_SHEET_TABLE_FIELD,
                [r for r in rows if getattr(r, "visit_id", None) != self.name],
            )
            rs.save(ignore_permissions=True)

        # Match for account rows (child doctype may not store visit_id only)
        def _same_row(r):
            return (
                getattr(r, "visit_id", None) == getattr(self, "name", None)
                or (
                    getattr(r, "visit_date", None) == getattr(self, "visit_date", None)
                    and getattr(r, "runsheet_id", None) == getattr(self, "run_sheet", None)
                    and getattr(r, "employee_id", None) == getattr(self, "employee", None)
                )
            )

        # Customer
        if getattr(self, "party_type", None) == "Customer" and getattr(self, "party", None):
            cust = frappe.get_doc("Customer", self.party)
            rows = cust.get(CUSTOMER_TABLE_FIELD) or []
            cust.set(CUSTOMER_TABLE_FIELD, [r for r in rows if not _same_row(r)])
            cust.save(ignore_permissions=True)

        # Lead
        if getattr(self, "party_type", None) == "Lead" and getattr(self, "party", None):
            lead = frappe.get_doc("Lead", self.party)
            rows = lead.get(LEAD_TABLE_FIELD) or []
            lead.set(LEAD_TABLE_FIELD, [r for r in rows if not _same_row(r)])
            lead.save(ignore_permissions=True)

        # CRM Deal
        if getattr(self, "party_type", None) == "CRM Deal" and getattr(self, "party", None):
            deal = frappe.get_doc("CRM Deal", self.party)
            rows = deal.get(CRMDEAL_TABLE_FIELD) or []
            deal.set(CRMDEAL_TABLE_FIELD, [r for r in rows if not _same_row(r)])
            deal.save(ignore_permissions=True)
