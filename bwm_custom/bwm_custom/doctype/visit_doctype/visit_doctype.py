# apps/bwm_custom/bwm_custom/bwm_custom/doctype/visit_doctype/visit_doctype.py

import frappe
from frappe.model.document import Document


RUN_SHEET_TABLE_FIELD = "visit_history"                 # Table field on Run Sheet
RUN_SHEET_CHILD_DTYPE = "Run Sheet Logs"                # Child doctype used in Run Sheet

CUSTOMER_TABLE_FIELD  = "custom_logs"            # Table field on Customer
CUSTOMER_CHILD_DTYPE  = "Customer Visit Reference"      # Child doctype used in Customer

LEAD_TABLE_FIELD      = "custom_runsheet_logs"            # Table field on Lead (if used)
LEAD_CHILD_DTYPE      = "Customer Visit Reference"      # same child doctype used on Lead

CRMDEAL_TABLE_FIELD   = "custom_logs"                 # Table field on CRM Deal
CRMDEAL_CHILD_DTYPE   = "Customer Visit Reference"      # child doctype used on CRM Deal
# ------------------------------------------------------------------


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


class VisitDoctype(Document):
    def on_submit(self):
        """On submit, append references to Run Sheet and (conditionally) Customer/Lead/CRM Deal."""
        # 1) Run Sheet
        if getattr(self, "run_sheet", None):
            _assert_child_table("Run Sheet", RUN_SHEET_TABLE_FIELD, RUN_SHEET_CHILD_DTYPE)
            rs = frappe.get_doc("Run Sheet", self.run_sheet)
            rs.append(RUN_SHEET_TABLE_FIELD, {"visit_id": self.name})
            rs.save(ignore_permissions=True)

        # Common values
        employee_name = (
            frappe.db.get_value("Employee", self.employee, "employee_name")
            if getattr(self, "employee", None)
            else None
        )
        payload = {
            "employee_id": self.employee,
            "employee_name": employee_name,
            "purpose_of_visit": getattr(self, "purpose", None),
            "visit_date": getattr(self, "visit_date", None),
            "outcome_of_visit": getattr(self, "outcome", None),
            "runsheet_id": getattr(self, "run_sheet", None),
            "visit_id": getattr(self, "name", None),
        }

        # 2) Customer
        if getattr(self, "party_type", None) == "Customer" and getattr(self, "party", None):
            _assert_child_table("Customer", CUSTOMER_TABLE_FIELD, CUSTOMER_CHILD_DTYPE)
            cust = frappe.get_doc("Customer", self.party)
            cust.append(CUSTOMER_TABLE_FIELD, payload)
            cust.save(ignore_permissions=True)

        # 3) Lead
        if getattr(self, "party_type", None) == "Lead" and getattr(self, "party", None):
            _assert_child_table("Lead", LEAD_TABLE_FIELD, LEAD_CHILD_DTYPE)
            lead = frappe.get_doc("Lead", self.party)
            lead.append(LEAD_TABLE_FIELD, payload)
            lead.save(ignore_permissions=True)

        # 4) CRM Deal
        if getattr(self, "party_type", None) == "CRM Deal" and getattr(self, "party", None):
            _assert_child_table("CRM Deal", CRMDEAL_TABLE_FIELD, CRMDEAL_CHILD_DTYPE)
            deal = frappe.get_doc("CRM Deal", self.party)
            deal.append(CRMDEAL_TABLE_FIELD, payload)
            deal.save(ignore_permissions=True)

        # (Frappe auto-commits per request)

    def on_cancel(self):
        """On cancel, remove the references we added on submit."""
        # Run Sheet
        if getattr(self, "run_sheet", None):
            rs = frappe.get_doc("Run Sheet", self.run_sheet)
            rows = rs.get(RUN_SHEET_TABLE_FIELD) or []
            rs.set(
                RUN_SHEET_TABLE_FIELD,
                [r for r in rows if getattr(r, "visit_id", None) != self.name],
            )
            rs.save(ignore_permissions=True)

        # Match function for child rows in Customer/Lead/CRM Deal
        def _same_row(r):
            return (
                getattr(r, "visit_date", None) == getattr(self, "visit_date", None)
                and getattr(r, "runsheet_id", None) == getattr(self, "run_sheet", None)
                and getattr(r, "employee_id", None) == getattr(self, "employee", None)
                and getattr(r, "visit_id", None) == getattr(self, "name", None)
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
