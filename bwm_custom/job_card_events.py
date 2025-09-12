import frappe

# ---------- helpers ----------
def _get_wip_from_work_order(wo_name: str) -> str | None:
    if not wo_name:
        return None
    return frappe.db.get_value("Work Order", wo_name, "wip_warehouse")

def _get_default_wip() -> str | None:
    return frappe.db.get_single_value("Manufacturing Settings", "default_wip_warehouse")

def _ensure_wip_on_doc_from_wo(doc):
    """Set doc.wip_warehouse from Work Order (then default), no DB writes here."""
    if doc.get("wip_warehouse"):
        return
    wip = _get_wip_from_work_order(doc.get("work_order")) or _get_default_wip()
    if not wip:
        frappe.throw("WIP Warehouse is required. Set it on the Work Order or define a Default WIP Warehouse in Manufacturing Settings.")
    doc.wip_warehouse = wip  # set on the in-memory doc (validate stage)

# ---------- VALIDATE ----------
def validate_job_card(doc, method=None):
    # Always zero out process loss
    if float(doc.get("process_loss_qty") or 0) != 0:
        doc.process_loss_qty = 0

    # Ensure WIP comes from Work Order (or default) before any core checks
    _ensure_wip_on_doc_from_wo(doc)

    # Compute this card's completed qty from time logs
    this_completed = 0.0
    for tl in (doc.get("time_logs") or []):
        try:
            this_completed += float(tl.get("completed_qty") or 0)
        except Exception:
            pass

    # Fallback to roll-up if no time logs
    if this_completed == 0 and float(doc.get("total_completed_qty") or 0) > 0:
        this_completed = float(doc.get("total_completed_qty") or 0)

    # Align for_quantity with what was actually completed
    if this_completed > 0:
        doc.for_quantity = this_completed

# ---------- ON SUBMIT ----------
def on_submit_job_card(doc, method=None):
    # Belt-and-suspenders: loss = 0 in DB
    if float(doc.get("process_loss_qty") or 0) != 0:
        frappe.db.set_value("Job Card", doc.name, "process_loss_qty", 0, update_modified=False)

    # Ensure WIP on this submitted doc (persist if we filled it during validate)
    if not doc.get("wip_warehouse"):
        wip = _get_wip_from_work_order(doc.get("work_order")) or _get_default_wip()
        if not wip:
            frappe.throw("WIP Warehouse is required. Set it on the Work Order or in Manufacturing Settings.")
        frappe.db.set_value("Job Card", doc.name, "wip_warehouse", wip, update_modified=False)
        doc.wip_warehouse = wip

    wo_name = doc.get("work_order")
    op_row  = doc.get("operation_id")
    if not (wo_name and op_row):
        frappe.msgprint("Missing Work Order / operation row; skipping rollup.")
        return

    # Aggregate totals from all submitted JCs (same WO + operation_id)
    total_qty = 0.0
    total_mins = 0.0
    cards = frappe.get_all("Job Card",
        filters={"docstatus": 1, "work_order": wo_name, "operation_id": op_row},
        fields=["name"]
    )
    for c in cards:
        logs = frappe.get_all("Job Card Time Log",
            filters={"parenttype": "Job Card", "parent": c["name"]},
            fields=["completed_qty", "time_in_mins"]
        )
        for l in logs:
            try:
                total_qty  += float(l.get("completed_qty") or 0)
                total_mins += float(l.get("time_in_mins") or 0)
            except Exception:
                pass

    # Update Work Order Operation child row
    updates = {"completed_qty": total_qty}
   
    elif frappe.db.has_column("Work Order Operation", "actual_operation_time"):
        updates["actual_operation_time"] = total_mins
    frappe.db.set_value("Work Order Operation", op_row, updates, update_modified=False)

    # Compute remaining and create next draft JC if needed
    wo_qty = float(frappe.db.get_value("Work Order", wo_name, "qty") or 0)
    remaining = max(wo_qty - total_qty, 0.0)

    # This card's completed qty (from its own time logs / roll-up)
    this_completed = 0.0
    for tl in (doc.get("time_logs") or []):
        try:
            this_completed += float(tl.get("completed_qty") or 0)
        except Exception:
            pass
    if this_completed == 0 and float(doc.get("total_completed_qty") or 0) > 0:
        this_completed = float(doc.get("total_completed_qty") or 0)

    if this_completed > 0 and remaining > 0:
        newjc = frappe.new_doc("Job Card")
        newjc.company = doc.company
        newjc.work_order = doc.work_order
        newjc.operation = doc.operation
        newjc.operation_id = op_row
        newjc.workstation = doc.get("workstation")
        newjc.operation_row_number = doc.get("operation_row_number")

       
        newjc.wip_warehouse = _get_wip_from_work_order(wo_name) or _get_default_wip()
        if not newjc.wip_warehouse:
            frappe.throw("WIP Warehouse is required for the next Job Card. Set it on the Work Order or in Manufacturing Settings.")

        newjc.for_quantity = remaining
        newjc.process_loss_qty = 0
        newjc.completed_qty = 0  # harmless if field doesn't exist in v15

        for f in ("batch_no", "bom_no", "project", "posting_date", "posting_time"):
            if doc.get(f):
                newjc.set(f, doc.get(f))

        newjc.insert(ignore_permissions=True)
        frappe.msgprint(f"Created Job Card {newjc.name} for remaining qty: {remaining}")
