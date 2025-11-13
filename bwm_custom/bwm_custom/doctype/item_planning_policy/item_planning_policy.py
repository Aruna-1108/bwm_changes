# Copyright (c) 2025
# For license information, please see license.txt

from typing import Optional, Tuple
import math
import statistics
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
          3) Lead Days selection (A1/A2 -> P-80, else P-50)
          4) Safety Days (statistical; B2 rule, ignore σL)
          5) Minimum Inventory Qty
          6) ROL (uses selected lead_days + safety_days)
          7) ROQ
          8) XYZ Classification (T12 variability)
          9) FSN logic (F/S/N from active months in T12)
         10) Policy recommendation (MTS / MTS-Lite / MTO) → Coverage Days
        """
        # Demand base
        last3 = self.compute_last3_sales_qty()
        daily = self.compute_requirements(last3)  # returns daily

        # Lead-time stats & item class cache
        self.compute_p50_lead_days()
        self._fetch_and_assign_item_classification()

        # XYZ & FSN metrics (T12-based)
        self.compute_xyz_classification()
        self._t12_customer_invoice_fsn()

        # Downstream calcs using lead-days + demand stats
        self.select_lead_days()
        self.compute_safety_days()
        self.compute_minimum_inventory_qty(daily)
        self.compute_rol(daily)

        # Policy → Coverage → ROQ
        self.compute_policy_recommendation()
        self.apply_coverage_from_policy()
        self.compute_roq(daily)

    # -------------------- Helpers to fetch classification from Bucket Branch Detail --------------------
    def _fetch_item_classification_row(self, item_code: str, cost_center: str):
        if not item_code or not cost_center:
            return None
        sql = """
            SELECT
              COALESCE(bbd._class, '') AS _class,
              COALESCE(bbd._tier, '')  AS _tier
            FROM `tabItem Classification` ic
            JOIN `tabBucket Branch Detail` bbd ON bbd.parent = ic.name
            WHERE ic.item = %s
              AND bbd.cost_center_name = %s
            LIMIT 1
        """
        val = frappe.db.sql(sql, (item_code, cost_center), as_dict=True)
        if not val:
            return None
        return val[0]

    def _fetch_and_assign_item_classification(self):
        item_code = (getattr(self, "item", None) or "").strip()
        cost_center = (getattr(self, "cost_center", None) or "").strip()
        if not item_code or not cost_center:
            return ""
        row = self._fetch_item_classification_row(item_code, cost_center)
        if not row:
            self._tier_value = ""
            self._class_value = ""
            return ""
        cls = (row.get("_class") or "").strip()
        tier = (row.get("_tier") or "").strip()
        if hasattr(self, "item_classification") and cls:
            self.item_classification = cls
        self._tier_value = tier.upper() if tier else ""
        self._class_value = cls.upper() if cls else ""
        return cls

    # -------------------- XYZ Classification (T12) --------------------
    def compute_xyz_classification(self) -> Tuple[str, Optional[float]]:
        item_code = (getattr(self, "item", "") or "").strip()
        cost_center = (getattr(self, "cost_center", "") or "").strip()
        if not item_code or not cost_center:
            if hasattr(self, "xyz_classifications"):
                self.xyz_classifications = ""
            if hasattr(self, "cv_t12"):
                self.cv_t12 = None
            if hasattr(self, "avg_m_qty"):
                self.avg_m_qty = 0.0
            if hasattr(self, "sd_m_qty"):
                self.sd_m_qty = 0.0
            if hasattr(self, "units_365d"):
                self.units_365d = 0.0
            return ("", None)

        from_date = add_months(getdate(today()), -12)
        to_date = getdate(today())

        sql = """
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
        rows = frappe.db.sql(sql, (item_code, cost_center, from_date, to_date))
        monthly_vals = [float(r[1] or 0.0) for r in rows] if rows else []

        if not monthly_vals or sum(monthly_vals) == 0:
            xyz, avg_m, sd_m, cv, units_365d = "Z", 0.0, 0.0, None, 0.0
        else:
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

        if hasattr(self, "avg_m_qty"):
            self.avg_m_qty = round(avg_m, ROUND_PLACES)
        if hasattr(self, "sd_m_qty"):
            self.sd_m_qty = round(sd_m, ROUND_PLACES)
        if hasattr(self, "cv_t12"):
            self.cv_t12 = round(cv, 3) if cv is not None else None
        if hasattr(self, "xyz_classifications"):
            self.xyz_classifications = xyz
        if hasattr(self, "units_365d"):
            self.units_365d = round(units_365d, ROUND_PLACES)

        return (xyz, cv)

    # --------------------- FSN Logic + CUSTOMERS (T12) ----------------------
    def _t12_customer_invoice_fsn(self) -> Tuple[int, int, int, str]:
        """
        Last 12 months KPIs (T12 window):
          - customers_t12   = distinct customers in last 12 months
          - invoices_t12    = distinct invoices in last 12 months
          - active_months_12= months in T12 where qty >= 1
          - fsn_logic       → F (>=9) , S (4–8) , N (<=3)
        """
        item_code = (getattr(self, "item", "") or "").strip()
        cost_center = (getattr(self, "cost_center", "") or "").strip()
        if not item_code or not cost_center:
            if hasattr(self, "fsn_logic"):
                self.fsn_logic = "N"
            return (0, 0, 0, "N")

        from_date = add_months(getdate(today()), -12)
        to_date = getdate(today())

        # Distinct customers in T12
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
        customers_t12 = int(frappe.db.sql(sql_cust, (item_code, cost_center, from_date, to_date))[0][0] or 0)

        # Distinct invoices in T12
        sql_inv = """
            SELECT COUNT(DISTINCT si.name)
            FROM `tabSales Invoice Item` sii
            JOIN `tabSales Invoice` si ON si.name = sii.parent
            WHERE si.docstatus = 1
              AND sii.item_code = %s
              AND COALESCE(si.cost_center, sii.cost_center) = %s
              AND si.posting_date BETWEEN %s AND %s
        """
        invoices_t12 = int(frappe.db.sql(sql_inv, (item_code, cost_center, from_date, to_date))[0][0] or 0)

        # Active months in T12
        sql_months = """
            SELECT COUNT(*) FROM (
              SELECT DATE_FORMAT(si.posting_date, '%%Y-%%m') AS ym, SUM(sii.qty) AS m_qty
              FROM `tabSales Invoice Item` sii
              JOIN `tabSales Invoice` si ON si.name = sii.parent
              WHERE si.docstatus = 1
                AND sii.item_code = %s
                AND COALESCE(si.cost_center, sii.cost_center) = %s
                AND si.posting_date BETWEEN %s AND %s
              GROUP BY ym
              HAVING SUM(sii.qty) >= 1
            ) t
        """
        res = frappe.db.sql(sql_months, (item_code, cost_center, from_date, to_date))
        active_months_12 = int((res[0][0] if res else 0) or 0)

        # FSN class based on active months in T12
        if active_months_12 >= 9:
            fsn_logic = "F"
        elif 4 <= active_months_12 <= 8:
            fsn_logic = "S"
        else:
            fsn_logic = "N"

        if hasattr(self, "customers_t12"):
            self.customers_t12 = customers_t12
        if hasattr(self, "invoices_t12"):
            self.invoices_t12 = invoices_t12
        if hasattr(self, "active_months_12"):
            self.active_months_12 = active_months_12
        if hasattr(self, "fsn_logic"):
            self.fsn_logic = fsn_logic

        return (customers_t12, invoices_t12, active_months_12, fsn_logic)

    # -------------------- Policy Recommendation (MTS / MTS-Lite / MTO) --------------------
    def compute_policy_recommendation(self) -> str:
        """
        Exactly aligned with SQL policy_pick:

        MTS:
          abc_fine ∈ {A1, A2}
          AND xyz_class = 'X'
          AND fsn_class ∈ {F, S}
          AND customers_t12 ≥ 2

        MTS-Lite:
          abc_fine ∈ {A1, A2} AND (
              (xyz_class ∈ {X, Y} AND fsn_class ∈ {F, S} AND customers_t12 ≥ 2)
           OR (abc_fine='A1' AND cv_t12 ≤ 1.75 AND customers_t12 ≥ 5 AND active_months_12 ≥ 6)
          )

        Else: MTO
        """
        # abc_fine: try dedicated field; fallback to item_classification
        abc_fine = (getattr(self, "abc_fine", None) or getattr(self, "item_classification", "") or "").strip().upper()
        xyz = (getattr(self, "xyz_classifications", "") or "").strip().upper()
        fsn = (getattr(self, "fsn_logic", "") or "").strip().upper()
        customers = int(getattr(self, "customers_t12", 0) or 0)      # T12-based
        active_m = int(getattr(self, "active_months_12", 0) or 0)    # T12-based
        cv = getattr(self, "cv_t12", None)
        try:
            cv = float(cv) if cv is not None else None
        except Exception:
            cv = None

        is_A12 = abc_fine in {"A1", "A2"}
        fsn_good = fsn in {"F", "S"}

        # MTS gate (same as query)
        if is_A12 and xyz == "X" and fsn_good and customers >= 2:
            rec = "MTS"
        else:
            # MTS-Lite gates (same as query)
            cond_main = (is_A12 and (xyz in {"X", "Y"}) and fsn_good and customers >= 2)
            cond_override = (
                abc_fine == "A1"
                and (cv is not None and cv <= 1.75)
                and customers >= 5
                and active_m >= 6
            )
            rec = "MTS-Lite" if (cond_main or cond_override) else "MTO"

        if hasattr(self, "policy_recommendation"):
            self.policy_recommendation = rec
        return rec

    # ---------------- Coverage Days from Policy ----------------
    def apply_coverage_from_policy(self) -> int:
        """
        Map policy_recommendation → coverage_days.
          - MTS       : 40
          - MTS-Lite  : 20
          - else (MTO): 0
        """
        rec = (getattr(self, "policy_recommendation", "") or "").strip().upper()
        if rec == "MTS":
            days = 40
        elif rec in {"MTS-LITE", "MTS LITE", "MTSLITE"}:
            days = 20
        else:
            days = 0

        if hasattr(self, "coverage_days"):
            self.coverage_days = days
        return days

    # ---------- 1) SALES (last 3 months) ----------
    def compute_last3_sales_qty(self) -> float:
        """Sum Sales Invoice Item.qty for last 3 months (submitted SIs only)."""
        item_code: Optional[str] = getattr(self, "item", None)
        if not item_code:
            self.last_3_month_sales_qty = 0.0
            return 0.0

        company: Optional[str] = getattr(self, "company", None)
        cost_center: Optional[str] = getattr(self, "cost_center", None)

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

    # ---------- 3) LEAD DAYS (legacy median PO -> PR) ----------
    def compute_lead_days(self) -> float:
        """
        Median of (PR.posting_date - PO.transaction_date) in days.
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

        n = len(vec)
        mid = n // 2
        median_val = vec[mid] if n % 2 else (vec[mid - 1] + vec[mid]) / 2.0
        self.lead_days = round(median_val, 2)
        return self.lead_days

    # ---------- 3.1 P-50, P-80, Average lead day ----------
    def compute_p50_lead_days(self) -> float:
        def percentile_nearest_rank(sorted_vec, q: float) -> float:
            n = len(sorted_vec)
            if n == 0:
                return 0.0
            if n == 1:
                return float(sorted_vec[0])
            rank = max(1, math.ceil((q / 100.0) * n))
            return float(sorted_vec[rank - 1])

        item_code = (getattr(self, "item", None) or "").strip()
        company = (getattr(self, "company", None) or "").strip()
        warehouse = (getattr(self, "request_for_warehouse", None) or "").strip() or None
        cost_center = (getattr(self, "cost_center", None) or "").strip() or None

        def _reset_targets():
            for f in ("lead_days", "p50_lead_days", "p50", "p80", "lead_time_average"):
                if hasattr(self, f):
                    setattr(self, f, 0.0)

        if not item_code or not company:
            _reset_targets()
            return 0.0

        params = {
            "item_code": item_code,
            "company": company,
            "warehouse": warehouse,
            "cost_center": cost_center,
        }

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
            _reset_targets()
            return 0.0

        p50 = round(percentile_nearest_rank(vec, 50.0), 2)
        p80 = round(percentile_nearest_rank(vec, 80.0), 2)
        avg = round(sum(vec) / len(vec), 2)

        self.lead_days = p50
        if hasattr(self, "p50_lead_days"):
            self.p50_lead_days = p50
        if hasattr(self, "p50"):
            self.p50 = p50
        if hasattr(self, "p80"):
            self.p80 = p80
        if hasattr(self, "lead_time_average"):
            self.lead_time_average = avg

        return p50

    def _selected_lead_days(self) -> float:
        """
        - A1/A2  -> P-80 (if available), else P-50, else current lead_days, else 0
        - Others -> P-50 (if available), else current lead_days, else 0
        """
        ic = (getattr(self, "item_classification", "") or "").strip().upper()
        p50 = float(getattr(self, "p50", getattr(self, "p50_lead_days", 0)) or 0)
        p80 = float(getattr(self, "p80", 0) or 0)
        current = float(getattr(self, "lead_days", 0) or 0)
        if ic in {"A1", "A2"}:
            return p80 if p80 > 0 else (p50 if p50 > 0 else current)
        else:
            return p50 if p50 > 0 else (current if current > 0 else 0.0)

    def select_lead_days(self):
        """Write the chosen lead_days to the doc (A1/A2 -> P-80, else P-50)."""
        self.lead_days = self._selected_lead_days()

    # ---------- Safety Days (B2 rule, ignore σL) ----------
    def compute_safety_days(self) -> float:
        """
        B2 rule (ignore lead-time variability):
          SafetyDays = ( Z * sigma_daily * sqrt(L) ) / ADD

        Where:
          ADD         = average daily demand from weekly issues (Sales Invoice + Delivery Note)
          sigma_daily = stdev of weekly issues / sqrt(7)
          L           = selected lead days:
                          A1/A2 → P80; others → P50; if 0 then fallback lead_time_average; else 45
          Z           = service factor by classification (_tier/_class)
        """
        item_code = (getattr(self, "item", None) or "").strip()
        company = (getattr(self, "company", None) or "").strip()
        cost_center = (getattr(self, "cost_center", None) or "").strip()
        warehouse = (getattr(self, "request_for_warehouse", None) or "").strip() or None

        if not item_code or not company or not cost_center:
            return float(getattr(self, "safety_days", 0) or 0)

        # Demand stats from weekly issues
        weeks_back = int(getattr(self, "weeks_back", 26) or 26)
        add_per_day, sigma_daily = self._get_demand_stats(
            item_code=item_code,
            cost_center=cost_center,
            weeks_back=weeks_back,
            warehouse=warehouse,
        )

        # Lead days: classification-based percentile with fallbacks
        L = float(self._selected_lead_days() or 0.0)
        if L <= 0:
            # use average lead time computed in compute_p50_lead_days(), then hard fallback 45
            L = float(getattr(self, "lead_time_average", 0) or 45.0)

        Z = self._get_z_value(item_code, cost_center)

        if add_per_day <= 0 or L <= 0 or sigma_daily <= 0:
            self.safety_days = 0.0
            return self.safety_days

        safety_days = (Z * sigma_daily * math.sqrt(L)) / add_per_day
        self.safety_days = round(max(0.0, safety_days), 2)
        return self.safety_days

    def _get_demand_stats(
        self,
        item_code: str,
        cost_center: str,
        weeks_back: int,
        warehouse: Optional[str] = None
    ) -> Tuple[float, float]:
        """
        Returns (E[D] add_per_day, σD sigma_daily) from weekly issues
        (Sales Invoice Items + Delivery Note Items).
        """
        params = {"item_code": item_code, "cc": cost_center, "weeks": weeks_back, "warehouse": warehouse}

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
        """

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
        """

        # Average & stdev across weekly buckets
        wrapper = f"""
            SELECT
              AVG(week_qty)/7.0          AS add_per_day,
              STDDEV(week_qty)/SQRT(7.0) AS sigma_daily
            FROM (
                {si_sql}
                UNION ALL
                {dn_sql}
            ) q
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
        """Returns (E[L], σL) from PO -> PR samples."""
        params = {"item_code": item_code, "company": company, "cc": cost_center, "days": lt_window_days, "warehouse": warehouse}
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
        """Map Z from cached _tier/_class; fallback to SQL CASE; default 1.95996."""
        tier = getattr(self, "_tier_value", None)
        cls = getattr(self, "_class_value", None)
        if tier:
            tier = tier.strip().upper()
        if cls:
            cls = cls.strip().upper()
        if tier in {"A1", "A+", "A-PRIME"}:
            return 2.32634787404084
        if cls == "A" or (tier and tier.startswith("A")):
            return 2.05374891063182

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
        """Minimum Inventory Qty = Safety Days × Daily Requirement."""
        safety_days = getattr(self, "safety_days", 0) or 0
        daily = daily or 0
        minimum_qty = safety_days * daily
        self.minimum_inventory_qty = round(minimum_qty, ROUND_PLACES)

    # ---------- 6) ROL ----------
    def compute_rol(self, daily: float):
        """
        ROL = (Safety Days + Lead Days) × Daily Requirement.
        Lead Days chosen dynamically by item_classification (A1/A2 -> P-80; else -> P-50).
        """
        safety_days = float(getattr(self, "safety_days", 0) or 0)
        lead_days = self._selected_lead_days()
        daily = float(daily or 0)
        rol_qty = (safety_days + lead_days) * daily
        self.rol = round(rol_qty, ROUND_PLACES)

    # ---------- 7) ROQ ----------
    def compute_roq(self, daily: float):
        """ROQ = Coverage Days × Daily Requirement."""
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
    """
    if not policy_name:
        frappe.throw("Policy name is required")

    policy = frappe.get_doc("Item Planning Policy", policy_name)

    # validations (STRICT: require final_* fields)
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

    # map core fields
    item_code = (policy.item or "").strip()
    wh = (policy.request_for_warehouse or "").strip()
    wh_group = (getattr(policy, "check_in_groups", None) or "").strip() or None  # optional/custom
    mr_type = (policy.material_request_type or "Purchase").strip()

    # numeric from final_* (strict)
    try:
        rol = float(policy.final_rol)
    except Exception:
        frappe.throw("final_rol must be a number")
    try:
        roq = float(policy.final_roq)
    except Exception:
        frappe.throw("final_roq must be a number")

    # load item & permission
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
        mr_changed = (norm(getattr(keep, "material_request_type", "")) != mr_n)
        grp_changed = (norm(getattr(keep, "warehouse_group", None)) != norm(wh_group))

        if rol_changed or roq_changed or mr_changed or grp_changed:
            keep.warehouse_reorder_level = rol
            keep.warehouse_reorder_qty = roq
            keep.material_request_type = mr_type
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
        new_row.warehouse = wh
        if hasattr(new_row, "warehouse_group"):
            new_row.warehouse_group = wh_group or None
        new_row.warehouse_reorder_level = rol
        new_row.warehouse_reorder_qty = roq
        new_row.material_request_type = mr_type
        op = "inserted"

    item.save(ignore_permissions=False)
    frappe.db.commit()
    return {"item": item_code, "op": op}
