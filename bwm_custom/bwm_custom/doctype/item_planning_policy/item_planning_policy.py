# Copyright (c) 2025
# For license information, please see license.txt

from typing import Optional, Tuple
import math
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
          3) Lead Days (MEDIAN PO->PR)  ← must run before ROL timing
          4) Safety Days (statistical)
          5) Minimum Inventory Qty
          6) ROL (uses lead_days + safety_days)
          7) ROQ
        """
        last3 = self.compute_last3_sales_qty()
        daily = self.compute_requirements(last3)         # returns daily
        self.compute_lead_days()                         # sets self.lead_days (median)
        self.compute_safety_days()                       # sets self.safety_days
        self.compute_minimum_inventory_qty(daily)
        self.compute_rol(daily)                          # uses self.lead_days + self.safety_days
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

    # ---------- 3) LEAD DAYS (MEDIAN PO -> PR) ----------
    def compute_lead_days(self) -> float:
        """
        Lead Days = MEDIAN of (PR.posting_date - PO.transaction_date) in days.
        Filters:
          - company (PR header)
          - optional warehouse (PR Item)
          - optional cost_center (prefer PO Item row, else PR Item row)
        Writes to self.lead_days (2 decimals).
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

        # Pull raw lead-times as a vector (PO -> first PR for that PO item)
        sql = """
            SELECT
                GREATEST(0, DATEDIFF(fr.first_receipt_date, po.transaction_date)) AS lt_days
            FROM `tabPurchase Order Item` poi
            JOIN `tabPurchase Order` po
                 ON po.name = poi.parent AND po.docstatus = 1
            JOIN (
                SELECT
                    pri.purchase_order_item,
                    MIN(pr.posting_date) AS first_receipt_date
                FROM `tabPurchase Receipt Item` pri
                JOIN `tabPurchase Receipt` pr
                     ON pr.name = pri.parent AND pr.docstatus = 1
                WHERE pr.posting_date IS NOT NULL
                GROUP BY pri.purchase_order_item
            ) fr ON fr.purchase_order_item = poi.name
            JOIN `tabPurchase Receipt Item` pri2
                 ON pri2.purchase_order_item = poi.name
            JOIN `tabPurchase Receipt` pr2
                 ON pr2.name = pri2.parent AND pr2.docstatus = 1
            WHERE
                poi.item_code = %(item_code)s
                AND pr2.company = %(company)s
                AND (%(warehouse)s   IS NULL OR pri2.warehouse = %(warehouse)s)
                AND (%(cost_center)s IS NULL OR COALESCE(poi.cost_center, pri2.cost_center) = %(cost_center)s)
                AND po.transaction_date IS NOT NULL
                AND fr.first_receipt_date IS NOT NULL
                AND DATEDIFF(fr.first_receipt_date, po.transaction_date) >= 0
        """
        rows = frappe.db.sql(sql, params) or []
        vec = sorted(float(r[0]) for r in rows if r and r[0] is not None)

        if not vec:
            self.lead_days = 0.0
            frappe.logger().info({
                "msg": "Median lead days: no observations",
                "item_code": item_code,
                "company": company,
                "warehouse": warehouse,
                "cost_center": cost_center
            })
            return 0.0

        # Median
        n = len(vec)
        mid = n // 2
        if n % 2:
            median_val = vec[mid]
        else:
            median_val = (vec[mid - 1] + vec[mid]) / 2.0

        self.lead_days = round(median_val, 2)
        return self.lead_days

    # ---------- 4) SAFETY DAYS (statistical) ----------
    def compute_safety_days(self) -> float:
        """
        Safety Days = ( Z × sqrt( E[L]*σD^2 + E[D]^2*σL^2 ) ) / E[D]
        Where:
          - E[D], σD from weekly issues (SI/DN Items) over `weeks_back` weeks (default 26)
          - E[L], σL from PO -> PR samples over `lt_window_days` (default 365 days)
          - Z from Item Classification × Bucket Branch Detail; fallback 1.95996
        Uses: self.company, self.cost_center, self.request_for_warehouse, self.item
        Writes: self.safety_days (2 decimals)
        """
        item_code = (getattr(self, "item", None) or "").strip()
        company = (getattr(self, "company", None) or "").strip()
        cost_center = (getattr(self, "cost_center", None) or "").strip()
        warehouse = (getattr(self, "request_for_warehouse", None) or "").strip() or None

        if not item_code or not company or not cost_center:
            return float(getattr(self, "safety_days", 0) or 0)

        weeks_back = int(getattr(self, "weeks_back", 26) or 26)
        lt_window_days = int(getattr(self, "lt_window_days", 365) or 365)

        # Demand stats (E[D], σD)
        add_per_day, sigma_daily = self._get_demand_stats(
            item_code=item_code,
            cost_center=cost_center,
            weeks_back=weeks_back,
            warehouse=warehouse
        )

        # Lead time stats (E[L], σL)
        lt_avg_days, lt_sd_days = self._get_leadtime_stats(
            item_code=item_code,
            company=company,
            cost_center=cost_center,
            lt_window_days=lt_window_days,
            warehouse=warehouse
        )

        # Z
        z_value = self._get_z_value(item_code, cost_center)

        if add_per_day <= 0:
            self.safety_days = 0.0
            return self.safety_days

        # σDLT  = sqrt( E[L]*σD^2 + E[D]^2*σL^2 )
        sigma_dlt_units = math.sqrt(
            max(0.0, (lt_avg_days * (sigma_daily ** 2)) + ((add_per_day ** 2) * (lt_sd_days ** 2)))
        )

        # Safety Stock (units) = Z * σDLT
        safety_stock_units = z_value * sigma_dlt_units

        # Safety Days = SS / E[D]
        safety_days = safety_stock_units / add_per_day if add_per_day > 0 else 0.0
        self.safety_days = round(safety_days, 2)

        # Optional debug fields on DocType (uncomment if you add fields):
        # self.safety_stock_units = round(safety_stock_units, ROUND_PLACES)
        # self.sigma_during_lt_units = round(sigma_dlt_units, ROUND_PLACES)

        return self.safety_days

    def _get_demand_stats(
        self,
        item_code: str,
        cost_center: str,
        weeks_back: int,
        warehouse: Optional[str] = None
    ) -> Tuple[float, float]:
        """
        Returns (E[D] add_per_day, σD sigma_daily) from weekly customer issues.
        Aggregates Sales Invoice Item and Delivery Note Item (submitted),
        filtering by cost_center via (header/item) and optional warehouse.
        """
        params = {
            "item_code": item_code,
            "cc": cost_center,
            "weeks": weeks_back,
            "warehouse": warehouse,
        }

        # Weekly issues from Sales Invoice Items
        si_sql = """
            SELECT
              DATE_SUB(DATE(si.posting_date), INTERVAL WEEKDAY(si.posting_date) DAY) AS week_start,
              SUM(CASE WHEN sii.qty > 0 THEN sii.qty ELSE 0 END) AS week_qty
            FROM `tabSales Invoice` si
            JOIN `tabSales Invoice Item` sii ON sii.parent = si.name
            WHERE si.docstatus = 1
              AND sii.item_code = %(item_code)s
              AND si.posting_date >= DATE_SUB(CURDATE(), INTERVAL %(weeks)s WEEK)
              AND si.posting_date <  DATE_ADD(CURDATE(), INTERVAL 1 DAY)
              AND (COALESCE(si.cost_center, sii.cost_center) = %(cc)s)
              AND (%(warehouse)s IS NULL OR sii.warehouse = %(warehouse)s)
            GROUP BY week_start
        """

        # Weekly issues from Delivery Note Items (for non-invoiced or direct DN flows)
        dn_sql = """
            SELECT
              DATE_SUB(DATE(dn.posting_date), INTERVAL WEEKDAY(dn.posting_date) DAY) AS week_start,
              SUM(CASE WHEN dni.qty > 0 THEN dni.qty ELSE 0 END) AS week_qty
            FROM `tabDelivery Note` dn
            JOIN `tabDelivery Note Item` dni ON dni.parent = dn.name
            WHERE dn.docstatus = 1
              AND dni.item_code = %(item_code)s
              AND dn.posting_date >= DATE_SUB(CURDATE(), INTERVAL %(weeks)s WEEK)
              AND dn.posting_date <  DATE_ADD(CURDATE(), INTERVAL 1 DAY)
              AND (COALESCE(dn.cost_center, dni.cost_center) = %(cc)s)
              AND (%(warehouse)s IS NULL OR dni.warehouse = %(warehouse)s)
            GROUP BY week_start
        """

        # Union the weekly buckets and compute ADD & sigma
        wrapper = f"""
            SELECT
              AVG(week_qty)/7.0          AS add_per_day,
              STDDEV(week_qty)/SQRT(7.0) AS sigma_daily
            FROM (
                {si_sql}
                UNION ALL
                {dn_sql}
            ) q
            GROUP BY 1=1
        """
        val = frappe.db.sql(wrapper, params)
        if not val:
            return (0.0, 0.0)
        add_per_day = float(val[0][0] or 0.0)
        sigma_daily = float(val[0][1] or 0.0)
        return (add_per_day, sigma_daily)

    def _get_leadtime_stats(
        self,
        item_code: str,
        company: str,
        cost_center: str,
        lt_window_days: int,
        warehouse: Optional[str] = None
    ) -> Tuple[float, float]:
        """
        Returns (E[L], σL) from PO -> PR samples.
        Filters:
          - pr.company
          - cost_center via COALESCE(poi.cost_center, pri.cost_center)
          - optional exact warehouse via pri.warehouse
        """
        params = {
            "item_code": item_code,
            "company": company,
            "cc": cost_center,
            "days": lt_window_days,
            "warehouse": warehouse,
        }
        sql = """
            SELECT
              AVG(lt_days)    AS avg_lt_days,
              STDDEV(lt_days) AS sd_lt_days
            FROM (
              SELECT
                GREATEST(0, DATEDIFF(pr.posting_date, po.transaction_date)) AS lt_days
              FROM `tabPurchase Receipt Item` pri
              JOIN `tabPurchase Receipt` pr ON pr.name = pri.parent AND pr.docstatus = 1
              JOIN `tabPurchase Order` po   ON po.name = pri.purchase_order AND po.docstatus = 1
              LEFT JOIN `tabPurchase Order Item` poi
                     ON poi.parent = po.name AND poi.item_code = pri.item_code
              WHERE pri.item_code = %(item_code)s
                AND pr.company = %(company)s
                AND pr.posting_date >= DATE_SUB(CURDATE(), INTERVAL %(days)s DAY)
                AND pr.posting_date <  DATE_ADD(CURDATE(), INTERVAL 1 DAY)
                AND DATEDIFF(pr.posting_date, po.transaction_date) IS NOT NULL
                AND DATEDIFF(pr.posting_date, po.transaction_date) >= 0
                AND ( %(warehouse)s IS NULL OR pri.warehouse = %(warehouse)s )
                AND COALESCE(poi.cost_center, pri.cost_center) = %(cc)s
            ) t
        """
        val = frappe.db.sql(sql, params)
        if not val:
            return (0.0, 0.0)
        avg_lt = float(val[0][0] or 0.0)
        sd_lt = float(val[0][1] or 0.0)
        return (avg_lt, sd_lt)

    def _get_z_value(self, item_code: str, cost_center: str) -> float:
        """
        Maps Z via Item Classification × Bucket Branch Detail for the cost center.
        Fallback: 1.95996398454005 (~97.5% service level)
        """
        params = {"item_code": item_code, "cc": cost_center}
        sql = """
            SELECT
              CASE
                WHEN bbd.`_tier` IN ('A1','A+','A-Prime') THEN 2.32634787404084
                WHEN bbd.`_class` = 'A' OR bbd.`_tier` LIKE 'A%%' THEN 2.05374891063182
                ELSE 1.95996398454005
              END AS z_value
            FROM `tabItem Classification` ic
            JOIN `tabBucket Branch Detail` bbd ON bbd.parent = ic.name
            WHERE ic.item = %(item_code)s
              AND bbd.cost_center_name = %(cc)s
            LIMIT 1
        """
        val = frappe.db.sql(sql, params)
        if val and val[0] and val[0][0] is not None:
            return float(val[0][0])
        return 1.95996398454005

    # ---------- 5) MINIMUM INVENTORY QTY ----------
    def compute_minimum_inventory_qty(self, daily: float):
        """Minimum Inventory Qty = Safety Days × Daily Requirement"""
        safety_days = getattr(self, "safety_days", 0) or 0
        daily = daily or 0
        minimum_qty = safety_days * daily
        self.minimum_inventory_qty = round(minimum_qty, ROUND_PLACES)

    # ---------- 6) ROL ----------
    def compute_rol(self, daily: float):
        """
        ROL = (Safety Days + Lead Days) × Daily Requirement
        (Lead Days here is the MEDIAN computed above, for robust timing)
        """
        safety_days = getattr(self, "safety_days", 0) or 0
        lead_days = getattr(self, "lead_days", 0) or 0
        daily = daily or 0
        rol_qty = (safety_days + lead_days) * daily
        self.rol = round(rol_qty, ROUND_PLACES)

    # ---------- 7) ROQ ----------
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
    Upsert Item → Auto-reorder row using ONLY policy.final_rol and policy.final_roq.

    Uniqueness rule (ERPNext core): ONE row per (warehouse, material_request_type)

    Behavior:
      - Match EXISTING row by (warehouse, MR type)  ← ignore group for matching
      - If none, but there is EXACTLY ONE row for the same warehouse (legacy blank/diff MR),
        treat that as the target row
      - Update warehouse_reorder_level = final_rol, warehouse_reorder_qty = final_roq
      - Also update material_request_type and warehouse_group (if field exists)
      - If multiple duplicates exist for same (WH, MR), keep first, delete extras
    """
    if not policy_name:
        frappe.throw("Policy name is required")

    policy = frappe.get_doc("Item Planning Policy", policy_name)

    # --- validations (STRICT: require final_* fields) ---
    missing = []
    if not (policy.item or "").strip():
        missing.append("Item")
    if not (policy.request_for_warehouse or "").strip():
        missing.append("Request for Warehouse")
    if not (policy.material_request_type or "").strip():
        missing.append("Material Request Type")
    if (getattr(policy, "final_rol", None) in (None, "")):
        missing.append("Final ROL (final_rol)")
    if (getattr(policy, "final_roq", None) in (None, "")):
        missing.append("Final ROQ (final_roq)")
    if missing:
        frappe.throw("Please enter: <b>{}</b>".format(", ".join(missing)))

    # --- map core fields ---
    item_code = (policy.item or "").strip()
    wh        = (policy.request_for_warehouse or "").strip()
    wh_group  = (getattr(policy, "check_in_groups", None) or "").strip() or None  # optional/custom
    mr_type   = (policy.material_request_type or "Purchase").strip()

    # --- numeric from final_* (strict) ---
    try:
        rol = float(policy.final_rol)
    except Exception:
        frappe.throw("final_rol must be a number")
    try:
        roq = float(policy.final_roq)
    except Exception:
        frappe.throw("final_roq must be a number")

    # --- load item & permission ---
    item = frappe.get_doc("Item", item_code)
    if not frappe.has_permission("Item", ptype="write", doc=item):
        frappe.throw(f"You do not have permission to update Item {item_code}")

    rows = list(item.get("reorder_levels") or [])

    def norm(s: Optional[str]) -> str:
        return (s or "").strip().casefold()

    wh_n, mr_n = norm(wh), norm(mr_type)

    # 1) strict match by (warehouse, MR type)
    matches = [
        r for r in rows
        if norm(getattr(r, "warehouse", None)) == wh_n
        and norm(getattr(r, "material_request_type", "Purchase")) == mr_n
    ]

    # 2) fallback: warehouse-only, when it identifies exactly one legacy row
    if not matches:
        by_wh = [r for r in rows if norm(getattr(r, "warehouse", None)) == wh_n]
        if len(by_wh) == 1:
            matches = by_wh

    EPS = 1e-9
    def changed(a, b) -> bool:
        return abs(float(a or 0) - float(b or 0)) > EPS

    op = "no_change"

    if matches:
        keep = matches[0]
        extras = matches[1:]  # duplicates to clean

        rol_changed = changed(getattr(keep, "warehouse_reorder_level", 0), rol)
        roq_changed = changed(getattr(keep, "warehouse_reorder_qty", 0), roq)
        mr_changed  = (norm(getattr(keep, "material_request_type", "")) != mr_n)
        grp_changed = (norm(getattr(keep, "warehouse_group", None)) != norm(wh_group))

        if rol_changed or roq_changed or mr_changed or grp_changed:
            keep.warehouse_reorder_level = rol
            keep.warehouse_reorder_qty   = roq
            keep.material_request_type   = mr_type
            if hasattr(keep, "warehouse_group"):
                keep.warehouse_group = wh_group
            op = "updated"

        # remove duplicates violating uniqueness (WH, MR)
        for extra in extras:
            if norm(getattr(extra, "warehouse", None)) == wh_n and \
               norm(getattr(extra, "material_request_type", "")) == mr_n:
                item.get("reorder_levels").remove(extra)
                if op == "no_change":
                    op = "cleaned_duplicates"
    else:
        # no match → insert new row
        new_row = item.append("reorder_levels", {})
        new_row.warehouse               = wh
        if hasattr(new_row, "warehouse_group"):
            new_row.warehouse_group     = wh_group or None
        new_row.warehouse_reorder_level = rol
        new_row.warehouse_reorder_qty   = roq
        new_row.material_request_type   = mr_type
        op = "inserted"

    item.save(ignore_permissions=False)
    frappe.db.commit()
    return {"item": item_code, "op": op}
	