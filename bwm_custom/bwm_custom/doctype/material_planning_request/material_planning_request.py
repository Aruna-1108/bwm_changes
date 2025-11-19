# -*- coding: utf-8 -*-
# Copyright (c) 2025
# For license information, please see license.txt

from typing import Dict, Optional, Tuple
import math
import statistics
import json

import frappe
from frappe.model.document import Document
from frappe.utils import today, add_months, getdate

ROUND_PLACES = 3


class MaterialPlanningRequest(Document):
    """
    Parent DocType fields:
      - cost_center  (Link Cost Center)
      - posting_date (Date)
      - company      (Link Company)
      - warehouse    (Link Warehouse)
      - create_wo    (Button)
      - material_planning_request_item (Table → Material Planning Request Item)
    """

    def validate(self):
        """
        On every Save:
          - For each child row with Item + Cost Center:
              * recompute ABC / XYZ / FSN
              * demand stats
              * lead time (P50/P80/lt_used_days)
              * safety stock, safety days
              * recommended ROL, coverage_days, ROQ
              * stock snapshot + coverage from stock
              * projected_minus_rol (projected vs target ROL)
          - If no cost center or no item → clear those fields.
        """
        self._recompute_child_rows()

    # ------------------------------------------------------------------
    # Internal helper: recompute all children using backend logic
    # ------------------------------------------------------------------
    def _recompute_child_rows(self):
        cc = (self.cost_center or "").strip()

        # If no cost center, clear all calc fields
        if not cc:
            for row in (self.material_planning_request_item or []):
                self._clear_child_row(row)
            return

        for row in (self.material_planning_request_item or []):
            item_code = (row.item or "").strip()

            # No item → clear row
            if not item_code:
                self._clear_child_row(row)
                continue

            # Use backend function
            data = get_item_classification_for_mpr(item=item_code, cost_center=cc)
            if not data:
                self._clear_child_row(row)
                continue

            # ---- ABC / XYZ / FSN ----
            row.item_classification = (
                data.get("item_classification")
                or data.get("_class")
                or ""
            )
            row.xyz_classification = data.get("xyz_classification") or ""
            row.fsn = data.get("fsn") or ""

            # ---- Customer & variability stats ----
            if hasattr(row, "customer_count"):
                row.customer_count = data.get("customer_count") or 0
            if hasattr(row, "avg_monthly_qty"):
                row.avg_monthly_qty = data.get("avg_monthly_qty") or 0
            if hasattr(row, "sd"):
                row.sd = data.get("sd") or 0
            if hasattr(row, "total_units"):
                row.total_units = data.get("total_units") or 0
            if hasattr(row, "cv"):
                row.cv = data.get("cv") or 0

            # ---- Lead time ----
            if hasattr(row, "p50"):
                row.p50 = data.get("p50") or 0
            if hasattr(row, "p80"):
                row.p80 = data.get("p80") or 0
            if hasattr(row, "lt_used_days"):
                row.lt_used_days = data.get("lt_used_days") or 0

            # ---- Demand & safety ----
            if hasattr(row, "add_per_day"):
                row.add_per_day = data.get("add_per_day") or 0
            if hasattr(row, "sigma_daily"):
                row.sigma_daily = data.get("sigma_daily") or 0
            if hasattr(row, "safety_stock_unit"):
                row.safety_stock_unit = data.get("safety_stock_unit") or 0
            if hasattr(row, "safety_day"):
                row.safety_day = data.get("safety_day") or 0

            # ---- ROL, coverage, ROQ ----
            if hasattr(row, "recommended_rol"):
                row.recommended_rol = data.get("recommended_rol") or 0
            if hasattr(row, "coverage_days"):
                row.coverage_days = data.get("coverage_days") or 0
            if hasattr(row, "recommended_roq"):
                row.recommended_roq = (
                    data.get("recommended_roq")
                    or data.get("roq_suggested")
                    or 0
                )

            # ---- Stock & coverage from stock ----
            if hasattr(row, "on_hand_qty"):
                row.on_hand_qty = data.get("on_hand_qty") or 0
            if hasattr(row, "projected_qty"):
                row.projected_qty = data.get("projected_qty") or 0
            if hasattr(row, "reserved_qty"):
                row.reserved_qty = data.get("reserved_qty") or 0
            if hasattr(row, "ordered_qty"):
                row.ordered_qty = data.get("ordered_qty") or 0
            if hasattr(row, "on_hand_coverage_days"):
                row.on_hand_coverage_days = data.get("on_hand_coverage_days") or 0
            if hasattr(row, "projected_coverage_days"):
                row.projected_coverage_days = data.get("projected_coverage_days") or 0
            if hasattr(row, "projected_minus_rol"):
                row.projected_minus_rol = data.get("projected_minus_rol") or 0

    # ------------------------------------------------------------------
    # Helper to wipe calculated fields on a child row
    # ------------------------------------------------------------------
    def _clear_child_row(self, row):
        if hasattr(row, "item_classification"):
            row.item_classification = ""
        if hasattr(row, "xyz_classification"):
            row.xyz_classification = ""
        if hasattr(row, "fsn"):
            row.fsn = ""

        for f in (
            "customer_count",
            "avg_monthly_qty",
            "sd",
            "total_units",
            "cv",
            "p50",
            "p80",
            "lt_used_days",
            "add_per_day",
            "sigma_daily",
            "safety_stock_unit",
            "safety_day",
            "recommended_rol",
            "coverage_days",
            "recommended_roq",
            "on_hand_qty",
            "projected_qty",
            "reserved_qty",
            "ordered_qty",
            "on_hand_coverage_days",
            "projected_coverage_days",
            "projected_minus_rol",
        ):
            if hasattr(row, f):
                setattr(row, f, 0)


# ------------------------- Helper functions ------------------------- #

def _z_from_abc(abc_fine: Optional[str], tier: Optional[str]) -> float:
    """
    Z logic as in policy table:

      CASE
        WHEN abc_fine = 'A1' THEN 2.3263
        WHEN abc_coarse = 'A' THEN 2.0537
        ELSE 1.9600
      END
    """
    fine = (abc_fine or "").strip().upper()
    t = (tier or "").strip().upper()

    if fine == "A1":
        return 2.32634787404084
    if fine in {"A2", "A"} or t == "A":
        return 2.05374891063182
    return 1.95996398454005


def _weekly_demand_from_sle(item_code: str, cost_center: str) -> Tuple[float, float]:
    """
    Weekly demand from SLE (demand_weekly_365):

      - Window: last 365 days
      - Only issues (DN/SI, actual_qty < 0)
      - Bucket: YEARWEEK(posting_date, 3)
      - CC filter from Warehouse (cost_center or custom_cost__center)

    Returns:
      (add_per_day, sigma_daily)
    """
    if not item_code:
        return (0.0, 0.0)

    # dynamic warehouse CC expr
    wh_cc_expr = None
    has_cc = frappe.db.has_column("Warehouse", "cost_center")
    has_custom = frappe.db.has_column("Warehouse", "custom_cost__center")

    if has_cc and has_custom:
        wh_cc_expr = "COALESCE(w.cost_center, w.custom_cost__center)"
    elif has_cc:
        wh_cc_expr = "w.cost_center"
    elif has_custom:
        wh_cc_expr = "w.custom_cost__center"

    params = {"item_code": item_code}
    if cost_center and wh_cc_expr:
        params["cc"] = cost_center
        cc_filter = f"AND {wh_cc_expr} = %(cc)s"
    else:
        cc_filter = ""

    sql = f"""
        SELECT
          COUNT(*) AS weeks_sampled_365,
          SUM(dw.issued_qty_week) / 365.0             AS add_per_day,
          STDDEV_SAMP(dw.issued_qty_week) / SQRT(7.0) AS sigma_daily
        FROM (
          SELECT
            YEARWEEK(sle.posting_date, 3) AS yw,
            SUM(-sle.actual_qty) AS issued_qty_week
          FROM `tabStock Ledger Entry` sle
          JOIN `tabWarehouse` w ON w.name = sle.warehouse
          WHERE sle.is_cancelled = 0
            AND sle.voucher_type IN ('Delivery Note','Sales Invoice')
            AND sle.actual_qty < 0
            AND sle.item_code = %(item_code)s
            {cc_filter}
            AND sle.posting_date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)
          GROUP BY YEARWEEK(sle.posting_date, 3)
        ) dw
    """
    val = frappe.db.sql(sql, params)
    if not val or val[0] is None:
        return (0.0, 0.0)

    weeks_sampled = float(val[0][0] or 0.0)
    add_per_day = float(val[0][1] or 0.0)
    sigma_daily = float(val[0][2] or 0.0)

    if weeks_sampled <= 0:
        return (0.0, 0.0)

    return (add_per_day, sigma_daily)


def _choose_lead_days_for_mpr(
    abc_fine: Optional[str],
    p50_val: Optional[float],
    p80_val: Optional[float],
    lt_avg: Optional[float],
) -> Tuple[float, str]:
    """
    Same as SQL:

      A1/A2  → COALESCE(P80, AVG, 45)
      Others → COALESCE(P50, AVG, 45)
    """
    p50 = p50_val if (p50_val and p50_val > 0) else None
    p80 = p80_val if (p80_val and p80_val > 0) else None
    avg = lt_avg if (lt_avg and lt_avg > 0) else None

    if (abc_fine or "").upper() in ("A1", "A2"):
        if p80 is not None:
            return float(p80), "P80"
        if avg is not None:
            return float(avg), "AVG"
        return 45.0, "FALLBACK45"
    else:
        if p50 is not None:
            return float(p50), "P50"
        if avg is not None:
            return float(avg), "AVG"
        return 45.0, "FALLBACK45"


def _pick_policy(
    abc_fine: Optional[str],
    xyz: Optional[str],
    fsn: Optional[str],
    customers: int,
    cv: Optional[float],
    active_months_12: int,
) -> str:
    """
    Policy logic, same as your SQL:

      - MTS:
          A1/A2 + X + FS (F/S) + customers >= 2
      - MTS-Lite:
          A1/A2 + X/Y + FS + customers >= 2
        OR
          A1 + cv <= 1.75 + customers >= 5 + active_months_12 >= 6
      - else: MTO
    """
    a = (abc_fine or "").strip().upper()
    x = (xyz or "").strip().upper()
    f = (fsn or "").strip().upper()

    is_A12 = a in {"A1", "A2"}
    fsn_good = f in {"F", "S"}

    # MTS
    if is_A12 and x == "X" and fsn_good and customers >= 2:
        return "MTS"

    # MTS-Lite
    cond_main = (is_A12 and (x in {"X", "Y"}) and fsn_good and customers >= 2)
    cond_override = (
        a == "A1"
        and cv is not None and cv <= 1.75
        and customers >= 5
        and active_months_12 >= 6
    )
    if cond_main or cond_override:
        return "MTS-Lite"

    return "MTO"


# ------------------------- Whitelisted API for child row ------------------------- #

@frappe.whitelist()
def get_item_classification_for_mpr(item: str, cost_center: str) -> Dict:
    """
    Main backend for Material Planning Request Item.

    Input:
      - item
      - cost_center

    Returns a dict with keys that map directly to child fields.
    """

    item_code = (item or "").strip()
    cc_name = (cost_center or "").strip()
    if not item_code or not cc_name:
        return {}

    out: Dict = {}

    # -------------------------------------------------
    # 1) ABC fine & tier from Item Classification / Bucket Branch Detail
    # -------------------------------------------------
    abc_rows = frappe.db.sql(
        """
        SELECT
            b._class,
            b._tier
        FROM `tabItem Classification` ic
        JOIN `tabBucket Branch Detail` b
          ON b.parent = ic.name
         AND b.parenttype = 'Item Classification'
         AND b.parentfield = 'branch_buckets'
        WHERE (ic.name = %(item)s OR ic.item = %(item)s)
          AND b.cost_center_name = %(cc)s
        LIMIT 1
        """,
        {"item": item_code, "cc": cc_name},
        as_dict=True,
    )

    abc_fine = None
    tier = None
    if abc_rows:
        abc_fine = (abc_rows[0].get("_class") or "").strip().upper()
        tier = (abc_rows[0].get("_tier") or "").strip().upper()
        out["item_classification"] = abc_fine
        out["_class"] = abc_fine
        out["_tier"] = tier

    # -------------------------------------------------
    # 2) XYZ classification + monthly stats from T12 Sales Invoice
    # -------------------------------------------------
    from_date = add_months(getdate(today()), -12)
    to_date = getdate(today())

    xyz_sql = """
        SELECT DATE_FORMAT(si.posting_date, '%%Y-%%m') AS ym,
               COALESCE(SUM(sii.qty), 0) AS m_qty
        FROM `tabSales Invoice Item` sii
        JOIN `tabSales Invoice` si ON si.name = sii.parent
        WHERE si.docstatus = 1
          AND sii.item_code = %s
          AND COALESCE(si.cost_center, sii.cost_center) = %s
          AND si.posting_date BETWEEN %s AND %s
        GROUP BY ym
        ORDER BY ym
    """
    rows = frappe.db.sql(xyz_sql, (item_code, cc_name, from_date, to_date))
    monthly_vals = [float(r[1] or 0.0) for r in rows] if rows else []

    xyz = "Z"
    avg_m = 0.0
    sd_m = 0.0
    units_365d = 0.0
    cv: Optional[float] = None

    if monthly_vals and sum(monthly_vals) != 0:
        n = len(monthly_vals)
        avg_m = sum(monthly_vals) / n
        sd_m = float(statistics.stdev(monthly_vals)) if n >= 2 else 0.0
        units_365d = sum(monthly_vals)
        cv = (sd_m / avg_m) if avg_m > 0 else None

        if avg_m == 0 or cv is None:
            xyz = "Z"
        elif cv <= 0.75:
            xyz = "X"
        elif cv <= 1.25:
            xyz = "Y"
        else:
            xyz = "Z"
    else:
        xyz = "Z"
        avg_m = 0.0
        sd_m = 0.0
        units_365d = 0.0
        cv = None

    out["xyz_classification"] = xyz
    out["avg_monthly_qty"] = round(avg_m, ROUND_PLACES)
    out["sd"] = round(sd_m, ROUND_PLACES)
    out["total_units"] = round(units_365d, ROUND_PLACES)
    out["cv"] = round(cv, 3) if cv is not None else None

    # -------------------------------------------------
    # 3) FSN + customers (T12) from invoices
    # -------------------------------------------------
    sql_cust = """
        SELECT COUNT(DISTINCT si.customer)
        FROM `tabSales Invoice Item` sii
        JOIN `tabSales Invoice` si ON si.name = sii.parent
        WHERE si.docstatus = 1
          AND sii.item_code = %s
          AND COALESCE(si.cost_center, sii.cost_center) = %s
          AND si.posting_date BETWEEN %s AND %s
          AND si.customer IS NOT NULL AND si.customer <> ''
    """
    cust_res = frappe.db.sql(sql_cust, (item_code, cc_name, from_date, to_date))
    customers_t12 = int((cust_res[0][0] if cust_res else 0) or 0)

    # active months from monthly_vals
    active_months_12 = sum(1 for q in monthly_vals if q >= 1) if monthly_vals else 0

    if active_months_12 >= 9:
        fsn = "F"
    elif 4 <= active_months_12 <= 8:
        fsn = "S"
    else:
        fsn = "N"

    out["fsn"] = fsn
    out["customer_count"] = customers_t12
    out["active_months_12"] = active_months_12

    # -------------------------------------------------
    # 4) Lead time P50 / P80 / AVG (PO→PR, last 730 days)
    # -------------------------------------------------
    wh_cc_expr = None
    has_cc = frappe.db.has_column("Warehouse", "cost_center")
    has_custom = frappe.db.has_column("Warehouse", "custom_cost__center")

    if has_cc and has_custom:
        wh_cc_expr = "COALESCE(w.cost_center, w.custom_cost__center)"
    elif has_cc:
        wh_cc_expr = "w.cost_center"
    elif has_custom:
        wh_cc_expr = "w.custom_cost__center"

    lt_params = {"item_code": item_code}
    cc_filter = ""
    if wh_cc_expr and cc_name:
        lt_params["cc"] = cc_name
        cc_filter = f"AND {wh_cc_expr} = %(cc)s"

    lt_sql = f"""
        SELECT GREATEST(0, DATEDIFF(pr.posting_date, po.transaction_date)) AS lt_days
        FROM `tabPurchase Receipt Item` pri
        JOIN `tabPurchase Receipt` pr ON pr.name = pri.parent AND pr.docstatus = 1
        JOIN `tabPurchase Order` po   ON po.name = pri.purchase_order AND po.docstatus = 1
        LEFT JOIN `tabWarehouse` w    ON w.name = COALESCE(pri.warehouse, po.set_warehouse)
        WHERE pri.item_code = %(item_code)s
          AND pr.posting_date >= DATE_SUB(CURDATE(), INTERVAL 730 DAY)
          {cc_filter}
    """
    lt_rows = frappe.db.sql(lt_sql, lt_params)
    lt_values = [float(r[0]) for r in lt_rows if r[0] is not None]

    p50_val: Optional[float] = None
    p80_val: Optional[float] = None
    lt_avg: Optional[float] = None

    if lt_values:
        lt_values.sort()
        n = len(lt_values)
        lt_avg = sum(lt_values) / n

        # median (P50)
        mid = n // 2
        if n % 2 == 1:
            p50_val = lt_values[mid]
        else:
            p50_val = (lt_values[mid - 1] + lt_values[mid]) / 2.0

        # P80 (nearest-rank)
        pos80 = int(math.ceil(0.80 * n)) - 1
        pos80 = max(0, min(pos80, n - 1))
        p80_val = lt_values[pos80]

    out["p50"] = round(p50_val, 2) if p50_val is not None else 0
    out["p80"] = round(p80_val, 2) if p80_val is not None else 0

    lt_used, lt_basis = _choose_lead_days_for_mpr(abc_fine, p50_val, p80_val, lt_avg)
    out["lt_used_days"] = round(lt_used, 2)
    out["lt_basis"] = lt_basis

    # -------------------------------------------------
    # 5) Effective demand stats (ADD, σ_daily)
    # -------------------------------------------------
    add_per_day = 0.0
    sigma_daily = 0.0

    # X items → monthly stats (if valid), otherwise weekly SLE
    if xyz == "X" and units_365d > 0 and sd_m > 0:
        add_per_day = units_365d / 365.0
        sigma_daily = sd_m / math.sqrt(365.0 / 12.0)
    else:
        add_per_day, sigma_daily = _weekly_demand_from_sle(item_code, cc_name)

    out["add_per_day"] = round(add_per_day, 6)
    out["sigma_daily"] = round(sigma_daily, 6)

    # -------------------------------------------------
    # 6) Safety stock + Safety days (B2 rule)
    #
    #   SafetyDays = (Z * sigma_daily * sqrt(L)) / ADD
    #   SS_units   = Z * sigma_daily * sqrt(L)
    # -------------------------------------------------
    Z = _z_from_abc(abc_fine, tier)
    L = float(lt_used or 0)

    if add_per_day > 0 and sigma_daily > 0 and L > 0:
        sigma_during_lt_units = sigma_daily * math.sqrt(L)
        safety_stock_units = Z * sigma_during_lt_units
        safety_days = safety_stock_units / add_per_day
    else:
        sigma_during_lt_units = 0.0
        safety_stock_units = 0.0
        safety_days = 0.0

    out["safety_stock_unit"] = round(max(0.0, safety_stock_units), 2)
    out["safety_day"] = round(max(0.0, safety_days), 2)

    # -------------------------------------------------
    # 7) Recommended ROL = ADD * L + safety_stock_units
    # -------------------------------------------------
    if add_per_day > 0 and L > 0:
        rol_suggested = add_per_day * L + safety_stock_units
    else:
        rol_suggested = 0.0

    out["recommended_rol"] = round(max(0.0, rol_suggested), 2)

    # -------------------------------------------------
    # 8) Policy → coverage_days (MTS / MTS-Lite / MTO) + ROQ
    # -------------------------------------------------
    policy = _pick_policy(abc_fine, xyz, fsn, customers_t12, cv, active_months_12)
    if policy == "MTS":
        coverage = 40
    elif policy == "MTS-Lite":
        coverage = 20
    else:
        coverage = 0

    out["coverage_days"] = coverage
    out["policy_recommendation"] = policy

    roq_suggested = coverage * add_per_day if coverage and add_per_day > 0 else 0.0
    out["roq_suggested"] = round(roq_suggested, 2)
    out["recommended_roq"] = out["roq_suggested"]

    # -------------------------------------------------
    # 9) Stock snapshot (Bin) + coverage from stock
    # -------------------------------------------------
    on_hand_qty = 0.0
    projected_qty = 0.0
    reserved_qty = 0.0
    ordered_qty = 0.0

    if wh_cc_expr and cc_name:
        stock_sql = f"""
            SELECT
                SUM(b.actual_qty)    AS on_hand_qty,
                SUM(b.projected_qty) AS projected_qty,
                SUM(b.reserved_qty)  AS reserved_qty,
                SUM(b.ordered_qty)   AS ordered_qty
            FROM `tabBin` b
            JOIN `tabWarehouse` w ON w.name = b.warehouse
            WHERE b.item_code = %(item_code)s
              AND {wh_cc_expr} = %(cc)s
        """
        stock_rows = frappe.db.sql(
            stock_sql,
            {"item_code": item_code, "cc": cc_name},
            as_dict=True,
        )
        if stock_rows:
            s = stock_rows[0] or {}
            on_hand_qty = float(s.get("on_hand_qty") or 0.0)
            projected_qty = float(s.get("projected_qty") or 0.0)
            reserved_qty = float(s.get("reserved_qty") or 0.0)
            ordered_qty = float(s.get("ordered_qty") or 0.0)

    out["on_hand_qty"] = round(on_hand_qty, 3)
    out["projected_qty"] = round(projected_qty, 3)
    out["reserved_qty"] = round(reserved_qty, 3)
    out["ordered_qty"] = round(ordered_qty, 3)

    if add_per_day > 0:
        on_hand_cov = on_hand_qty / add_per_day
        proj_cov = projected_qty / add_per_day
    else:
        on_hand_cov = 0.0
        proj_cov = 0.0

    out["on_hand_coverage_days"] = round(on_hand_cov, 2)
    out["projected_coverage_days"] = round(proj_cov, 2)

    # -------------------------------------------------
    # 10) Projected – ROL units (surplus / shortage vs target ROL)
    # -------------------------------------------------
    recommended_rol_val = float(out.get("recommended_rol") or 0.0)
    projected_val = float(out.get("projected_qty") or 0.0)
    projected_minus_rol_units = projected_val - recommended_rol_val
    out["projected_minus_rol"] = round(projected_minus_rol_units, 2)

    return out


# ------------------------- Material Request creation from MPR ------------------------- #

@frappe.whitelist()
def create_material_request_for_mpr(
    mpr_name: str,
    rows: Optional[object] = None,
) -> Dict:
    """
    Called from the 'Create Material Request' button on Material Planning Request.

    Logic:
      - Create ONE Material Request (Draft)
      - Only for selected child rows (rows arg)
      - For each selected row with:
          * item
          * recommended_rol > 0
        append a Material Request Item with:
          - item_code     = row.item
          - qty           = row.recommended_rol (rounded for whole-number UOMs)
          - schedule_date = MPR.posting_date
          - company       = MPR.company
          - cost_center   = MPR.cost_center
          - warehouse     = MPR.warehouse
    """
    if not mpr_name:
        frappe.throw("Material Planning Request name is required")

    # normalise 'rows' -> Python set of child row names
    selected_names = set()

    if isinstance(rows, str):
        try:
            parsed = json.loads(rows)
        except Exception:
            parsed = [rows]
        if isinstance(parsed, (list, tuple, set)):
            selected_names = {str(x) for x in parsed}
    elif isinstance(rows, (list, tuple, set)):
        selected_names = {str(x) for x in rows}

    if not selected_names:
        frappe.throw("Please select at least one row in Material Planning Request Item.")

    mpr = frappe.get_doc("Material Planning Request", mpr_name)

    if not mpr.company:
        frappe.throw("Please set Company on the Material Planning Request")
    if not mpr.cost_center:
        frappe.throw("Please set Cost Center on the Material Planning Request")
    if not getattr(mpr, "warehouse", None):
        frappe.throw("Please set Warehouse on the Material Planning Request")

    mr = frappe.new_doc("Material Request")
    mr.company = mpr.company
    mr.material_request_type = "Purchase"  # change if needed
    mr.transaction_date = mpr.posting_date or today()
    mr.schedule_date = mpr.posting_date or today()

    for row in (mpr.material_planning_request_item or []):
        # only selected rows
        if row.name not in selected_names:
            continue

        item_code = (row.item or "").strip()
        qty = float(row.recommended_rol or 0)

        if not item_code or qty <= 0:
            continue

        # --- handle whole-number UOMs (like Nos) ---
        uom = (getattr(row, "uom", None) or "").strip()
        if uom:
            must_be_whole = frappe.db.get_value("UOM", uom, "must_be_whole_number")
            if must_be_whole:
                qty = math.ceil(qty)  # round UP to nearest whole number

        if qty <= 0:
            continue

        child = mr.append("items", {})
        child.item_code = item_code
        child.qty = qty
        if uom:
            child.uom = uom
        child.schedule_date = mpr.posting_date or today()
        child.company = mpr.company
        child.cost_center = mpr.cost_center
        child.warehouse = mpr.warehouse  # from parent MPR

    if not mr.items:
        frappe.throw("No selected items with Recommended ROL > 0 to create Material Request.")

    mr.insert(ignore_permissions=False)
    frappe.db.commit()

    return {"mpr": mpr_name, "material_request": mr.name}
