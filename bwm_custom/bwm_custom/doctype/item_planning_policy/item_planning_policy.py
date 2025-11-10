# Copyright (c) 2025
# For license information, please see license.txt

from typing import Optional
import frappe
from frappe.model.document import Document
from frappe.utils import today, add_months, getdate

ROUND_PLACES = 3  # centralize rounding precision


class ItemPlanningPolicy(Document):
    def validate(self):
        """
        Auto-calc on Save (no blocking validation here):
          1) Last-3-Months Sales Qty
          2) Monthly & Daily Requirements
          3) Lead Days (avg PO->PR)  ← must run before ROL
          4) Minimum Inventory Qty
          5) ROL (uses calculated lead_days)
          6) ROQ
        """
        last3 = self.compute_last3_sales_qty()
        daily = self.compute_requirements(last3)   # returns daily
        self.compute_lead_days()                   # sets self.lead_days
        self.compute_minimum_inventory_qty(daily)
        self.compute_rol(daily)                    # uses self.lead_days
        self.compute_roq(daily)

    # ---------- 1) SALES (last 3 months) ----------
    def compute_last3_sales_qty(self) -> float:
        """Sum Sales Invoice Item.qty for last 3 months (submitted SIs only)."""
        item_code: Optional[str] = getattr(self, "item", None)
        if not item_code:
            self.last_3_month_sales_qty = 0.0
            return 0.0

        company: Optional[str] = getattr(self, "company", None)
        cost_center: Optional[str] = getattr(self, "cost_center", None)
        warehouse: Optional[str] = getattr(self, "request_for_warehouse", None)

        from_date = add_months(getdate(today()), -3)
        to_date = getdate(today())

        sql = """
            SELECT COALESCE(SUM(sii.qty), 0)
            FROM `tabSales Invoice Item` sii
            JOIN `tabSales Invoice` si ON si.name = sii.parent
            WHERE
                si.docstatus = 1
                AND sii.item_code = %s
                AND si.posting_date BETWEEN %s AND %s
        """
        params = [item_code, from_date, to_date]

        if company:
            sql += " AND si.company = %s"
            params.append(company)
        if cost_center:
            sql += " AND COALESCE(si.cost_center, sii.cost_center) = %s"
            params.append(cost_center)
        if warehouse:
            sql += " AND sii.warehouse = %s"
            params.append(warehouse)

        qty = frappe.db.sql(sql, params)
        last3 = float(qty[0][0]) if qty else 0.0
        self.last_3_month_sales_qty = round(last3, ROUND_PLACES)
        return last3

    # ---------- 2) REQUIREMENTS (monthly / daily) ----------
    def compute_requirements(self, last3: float) -> float:
        """Monthly = last3 / 3 ; Daily = monthly / 30. Returns daily."""
        monthly = (last3 or 0.0) / 3.0
        daily = monthly / 30.0
        self.monthly_requirement = round(monthly, ROUND_PLACES)
        self.daily_requirement = round(daily, ROUND_PLACES)
        return daily

    # ---------- 3) LEAD DAYS (avg PO -> PR) ----------
    def compute_lead_days(self) -> float:
        """
        Average lead days = AVG(DATEDIFF(PR.posting_date, PO.transaction_date))
        Filters:
          - company (PR header)
          - warehouse (PR Item)  [optional]
          - cost_center (prefer PO Item row, else PR Item row) [optional]
        Writes to self.lead_days (2 decimals).
        Includes fallback if PR Item lacks purchase_order_item.
        """
        item_code = (getattr(self, "item", None) or "").strip()
        company = (getattr(self, "company", None) or "").strip()
        warehouse = (getattr(self, "request_for_warehouse", None) or "").strip() or None
        cost_center = (getattr(self, "cost_center", None) or "").strip() or None

        if not item_code or not company:
            self.lead_days = 0.0
            return 0.0

        params = {
            "item_code": item_code,
            "company": company,
            "warehouse": warehouse,
            "cost_center": cost_center,
        }

        sql_main = """
            SELECT
                ROUND(AVG(DATEDIFF(pr.posting_date, po.transaction_date)), 2) AS lead_days
            FROM `tabPurchase Receipt Item` pri
            JOIN `tabPurchase Receipt` pr
                 ON pr.name = pri.parent AND pr.docstatus = 1
            JOIN `tabPurchase Order Item` poi
                 ON poi.name = pri.purchase_order_item
            JOIN `tabPurchase Order` po
                 ON po.name = poi.parent AND po.docstatus = 1
            WHERE
                pri.item_code = %(item_code)s
                AND pr.company = %(company)s
                AND (%(warehouse)s   IS NULL OR pri.warehouse = %(warehouse)s)
                AND (%(cost_center)s IS NULL OR COALESCE(poi.cost_center, pri.cost_center) = %(cost_center)s)
                AND pr.posting_date IS NOT NULL
                AND po.transaction_date IS NOT NULL
                AND DATEDIFF(pr.posting_date, po.transaction_date) >= 0
        """
        val = frappe.db.sql(sql_main, params)
        lead_days = float(val[0][0]) if val and val[0][0] is not None else None

        if lead_days is None:
            sql_fallback = """
                SELECT
                    ROUND(AVG(DATEDIFF(pr.posting_date, po.transaction_date)), 2) AS lead_days
                FROM `tabPurchase Receipt Item` pri
                JOIN `tabPurchase Receipt` pr
                     ON pr.name = pri.parent AND pr.docstatus = 1
                JOIN `tabPurchase Order` po
                     ON po.name = pri.purchase_order AND po.docstatus = 1
                LEFT JOIN `tabPurchase Order Item` poi
                     ON poi.parent = po.name AND poi.item_code = pri.item_code
                WHERE
                    pri.item_code = %(item_code)s
                    AND pr.company = %(company)s
                    AND (%(warehouse)s   IS NULL OR pri.warehouse = %(warehouse)s)
                    AND (%(cost_center)s IS NULL OR COALESCE(poi.cost_center, pri.cost_center) = %(cost_center)s)
                    AND pr.posting_date IS NOT NULL
                    AND po.transaction_date IS NOT NULL
                    AND DATEDIFF(pr.posting_date, po.transaction_date) >= 0
            """
            val2 = frappe.db.sql(sql_fallback, params)
            lead_days = float(val2[0][0]) if val2 and val2[0][0] is not None else 0.0

        self.lead_days = lead_days or 0.0
        if not self.lead_days:
            frappe.logger().info({
                "msg": "Lead days computed 0 — check filters/mapping",
                "item_code": item_code,
                "company": company,
                "warehouse": warehouse,
                "cost_center": cost_center
            })
        return self.lead_days

    # ---------- 4) MINIMUM INVENTORY QTY ----------
    def compute_minimum_inventory_qty(self, daily: float):
        """Minimum Inventory Qty = Safety Days × Daily Requirement"""
        safety_days = getattr(self, "safety_days", 0) or 0
        daily = daily or 0
        minimum_qty = safety_days * daily
        self.minimum_inventory_qty = round(minimum_qty, ROUND_PLACES)

    # ---------- 5) ROL ----------
    def compute_rol(self, daily: float):
        """
        ROL = (Safety Days + Lead Days) × Daily Requirement
        """
        safety_days = getattr(self, "safety_days", 0) or 0
        lead_days = getattr(self, "lead_days", 0) or 0
        daily = daily or 0
        rol_qty = (safety_days + lead_days) * daily
        self.rol = round(rol_qty, ROUND_PLACES)

    # ---------- 6) ROQ ----------
    def compute_roq(self, daily: float):
        """
        ROQ = Coverage Days × Daily Requirement
        """
        coverage_days = getattr(self, "coverage_days", 0) or 0
        daily = daily or 0
        roq = coverage_days * daily
        self.roq = round(roq, ROUND_PLACES)


# --------------- Update or Add New Row to Item Re-order ---------------

@frappe.whitelist()
def apply_item_reorder_from_policy(policy_name: str):
    """
    Upsert Item Reorder on Item based on Item Planning Policy.

    UPDATE only if existing row has:
      - SAME warehouse AND
      - SAME warehouse_group AND
      - ROL or ROQ changed

    Else INSERT a new row.

    Returns: {"item": <item>, "op": "updated"|"inserted"|"no_change"}
    """
    if not policy_name:
        frappe.throw("Policy name is required")

    policy = frappe.get_doc("Item Planning Policy", policy_name)

    # --- button-click validations (server-side safety) ---
    missing = []
    if not (policy.item or "").strip():                     missing.append("Item")
    if not (policy.request_for_warehouse or "").strip():    missing.append("Request for Warehouse")
    if not (policy.check_in_groups or "").strip():          missing.append("Check in (group)")
    if not (policy.material_request_type or "").strip():    missing.append("Material Request Type")
    if policy.rol in (None, ""):                            missing.append("Re-order Level (ROL)")
    if not (policy.roq or policy.minimum_inventory_qty):    missing.append("Re-order Qty (ROQ) or Minimum Inventory Qty")
    if missing:
        frappe.throw("Please enter: <b>{}</b>".format(", ".join(missing)))

    # --- mapping ---
    item_code = (policy.item or "").strip()
    wh        = (policy.request_for_warehouse or "").strip()
    wh_group  = (policy.check_in_groups or "").strip() or None
    mr_type   = (policy.material_request_type or "").strip() or "Purchase"
    rol       = float(policy.rol or 0)

    roq_val = policy.roq
    if roq_val is None or roq_val == "":
        roq = float(policy.minimum_inventory_qty or 0)
    else:
        roq = float(roq_val or 0)

    # --- load item & permissions ---
    item = frappe.get_doc("Item", item_code)
    if not frappe.has_permission("Item", ptype="write", doc=item):
        frappe.throw(f"You do not have permission to update Item {item_code}")

    child_fieldname = "reorder_levels"

    # --- robust matching helpers ---
    def norm(s: Optional[str]) -> str:
        return (s or "").strip().casefold()

    wh_norm       = norm(wh)
    wh_group_norm = norm(wh_group)

    # ---- find existing row with SAME warehouse AND SAME warehouse_group ----
    existing = None
    for row in (item.get(child_fieldname) or []):
        if norm(row.warehouse) == wh_norm and norm(getattr(row, "warehouse_group", None)) == wh_group_norm:
            existing = row
            break

    # helper: compare floats (avoid tiny rounding diffs)
    EPS = 1e-9
    def changed(a, b) -> bool:
        a = float(a or 0)
        b = float(b or 0)
        return abs(a - b) > EPS

    if existing:
        rol_changed = changed(existing.warehouse_reorder_level, rol)
        roq_changed = changed(existing.warehouse_reorder_qty,   roq)

        if rol_changed or roq_changed:
            existing.warehouse_reorder_level = rol
            existing.warehouse_reorder_qty   = roq
            existing.material_request_type   = mr_type
            # keys already match → do not change warehouse/group here
            op = "updated"
        else:
            op = "no_change"
    else:
        # No matching row → INSERT
        new_row = item.append(child_fieldname, {})
        new_row.warehouse               = wh
        new_row.warehouse_group         = wh_group or None
        new_row.warehouse_reorder_level = rol
        new_row.warehouse_reorder_qty   = roq
        new_row.material_request_type   = mr_type
        op = "inserted"

    item.save(ignore_permissions=False)
    frappe.db.commit()

    return {"item": item_code, "op": op}
