from __future__ import annotations
import frappe
from typing import Optional

# ---------------- Constants ----------------
STATUS_FIELD = "custom_status"  

# EXACT option labels from your Select field
OPTIONS = {
    "IC_REVIEW": "IC To be reviewed",
    "MR_RELEASE": "Material Request to be released",
    "PO": "Waiting for Purchase",
    "PICK_LIST": "Pick List to be released",
    "PO_SUBMITTED":"PO Submitted",
}

# ------------- Sales Order: on_submit -------------
def handle_so_submit(doc, method):
    company = doc.company
    for it in (doc.items or []):
        # 1) initialize
        _set_child_status(it.name, OPTIONS["IC_REVIEW"])

        # 2) availability only if warehouse is specified on SO item
        wh = it.get("warehouse")  # may be None
        available = _available_qty(it.item_code, warehouse=wh, company=company)

        # 3) decide & set
        target = OPTIONS["PICK_LIST"] if float(available or 0) >= float(it.qty or 0) else OPTIONS["MR_RELEASE"]
        _set_child_status(it.name, target)

# -------- Material Request: on_submit (STRICT) --------
def handle_mr_submitted(doc, method):
    """
    STRICT: update only when MR Item has a direct link to Sales Order Item (sales_order_item).
    No fallback by Sales Order + Item Code.
    """
    for row in (doc.items or []):
        soi = getattr(row, "sales_order_item", None)
        if not soi:
            continue  # skip rows not linked to a specific SO Item

        try:
            frappe.db.set_value(
                "Sales Order Item",
                soi,
                STATUS_FIELD,
                OPTIONS["PO"],  # "Waiting for Purchase"
                update_modified=False,
            )
        except Exception:
            frappe.clear_messages()


def handle_po_submitted(doc, method):
    """
    STRICT: On Purchase Order submit, advance only those Sales Order Item rows
    that are *directly linked* in PO Items (via sales_order_item / so_detail).
    No fallback by (sales_order, item_code).
    """
    for row in (doc.items or []):
        # try the common link field first
        soi = (
            getattr(row, "sales_order_item", None)
            or getattr(row, "so_detail", None)      # sometimes used by maps
        )
        if not soi:
            continue  # strict mode: skip if there is no direct SOI link

        try:
            # (optional) guard: only move forward from "Waiting for Purchase"
            current = frappe.db.get_value("Sales Order Item", soi, STATUS_FIELD)
            if current and current != OPTIONS["PO"]:
                # If you want to always set regardless of current, delete this 'if' block
                pass

            frappe.db.set_value(
                "Sales Order Item",
                soi,
                STATUS_FIELD,
                OPTIONS["PO_SUBMITTED"],             # "PO submitted"
                update_modified=False,
            )
        except Exception:
            frappe.clear_messages()


# ----------------- Helpers -----------------
def _set_child_status(soi_name: str, status: str) -> None:
    if not soi_name or not status:
        return
    try:
        frappe.db.set_value("Sales Order Item", soi_name, STATUS_FIELD, status, update_modified=False)
    except Exception:
        frappe.clear_messages()

def _available_qty(item_code: str, warehouse: Optional[str], company: Optional[str]) -> float:
    """
    Available = actual_qty - reserved_qty
    Only checks the given warehouse.
    If no warehouse provided, returns 0.0 (no company-wide sum).
    """
    if not item_code or not warehouse:
        return 0.0

    row = frappe.db.get_value(
        "Bin",
        {"item_code": item_code, "warehouse": warehouse},
        ["actual_qty", "reserved_qty"],
        as_dict=True,
    )
    if not row:
        return 0.0

    return float(row.actual_qty or 0) - float(row.reserved_qty or 0)
