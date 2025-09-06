# Date-range Attendance Report (Script Report)

from itertools import groupby

import frappe
from frappe import _
from frappe.query_builder.functions import Count, Sum
from frappe.utils import getdate, add_days, get_first_day, get_last_day, today
from frappe.utils.nestedset import get_descendants_of

Filters = frappe._dict

STATUS_MAP = {
    "Present": "P",
    "Absent": "A",
    "Half Day/Other Half Absent": "HD/A",
    "Half Day/Other Half Present": "HD/P",
    "Work From Home": "WFH",
    "On Leave": "L",
    "Holiday": "H",
    "Weekly Off": "WO",
}
DAY_ABBR = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def execute(filters: Filters | None = None):
    filters = frappe._dict(filters or {})

    # ---- default safe values to avoid popups / None issues
    if not filters.get("from_date"):
        filters.from_date = get_first_day(today())
    if not filters.get("to_date"):
        filters.to_date = get_last_day(today())

    if not filters.get("company"):
        filters.company = frappe.defaults.get_user_default("Company")
    if not filters.company:
        companies = frappe.db.get_all("Company", pluck="name", limit=1)
        filters.company = companies[0] if companies else "Test"

    filters.companies = [filters.company]
    if filters.get("include_company_descendants"):
        try:
            filters.companies.extend(get_descendants_of("Company", filters.company))
        except Exception:
            pass

    attendance_map = get_attendance_map(filters)
    if not attendance_map:
        frappe.msgprint(_("No attendance records found."), alert=True, indicator="orange")
        return [], [], None, None

    columns = get_columns(filters)
    data = get_data(filters, attendance_map)
    if not data:
        frappe.msgprint(_("No attendance records found for this criteria."), alert=True, indicator="orange")
        return columns, [], None, None

    message = get_message() if not filters.get("summarized_view") else ""
    chart = get_chart_data(attendance_map, filters)
    return columns, data, message, chart


def get_message() -> str:
    colors = ["green", "red", "orange", "#914EE3", "green", "#3187D8", "#878787", "#878787"]
    order = [
        "Present", "Absent", "Half Day/Other Half Absent", "Half Day/Other Half Present",
        "Work From Home", "On Leave", "Holiday", "Weekly Off",
    ]
    html = []
    for i, status in enumerate(order):
        html.append(
            f"<span style='border-left:2px solid {colors[i]};padding-right:12px;padding-left:5px;margin-right:3px;'>"
            f"{_(status)} - {STATUS_MAP[status]}</span>"
        )
    return "".join(html)


# ---------------- helpers: dates & columns ----------------

def iter_dates(filters: Filters):
    d = getdate(filters.from_date)
    end = getdate(filters.to_date)
    while d <= end:
        yield d
        d = add_days(d, 1)


def get_columns(filters: Filters) -> list[dict]:
    cols = []
    if filters.get("group_by"):
        mapping = {
            "Branch": "Branch",
            "Grade": "Employee Grade",
            "Department": "Department",
            "Designation": "Designation",
        }
        opts = mapping.get(filters.group_by)
        cols.append({
            "label": _(filters.group_by),
            "fieldname": frappe.scrub(filters.group_by),
            "fieldtype": "Link",
            "options": opts,
            "width": 120,
        })

    cols.extend([
        {"label": _("Employee"), "fieldname": "employee", "fieldtype": "Link", "options": "Employee", "width": 135},
        {"label": _("Employee Name"), "fieldname": "employee_name", "fieldtype": "Data", "width": 120},
    ])

    if filters.get("summarized_view"):
        cols.extend([
            {"label": _("Total Present"), "fieldname": "total_present", "fieldtype": "Float", "width": 110},
            {"label": _("Total Leaves"), "fieldname": "total_leaves", "fieldtype": "Float", "width": 110},
            {"label": _("Total Absent"), "fieldname": "total_absent", "fieldtype": "Float", "width": 110},
            {"label": _("Total Holidays"), "fieldname": "total_holidays", "fieldtype": "Float", "width": 120},
            {"label": _("Unmarked Days"), "fieldname": "unmarked_days", "fieldtype": "Float", "width": 130},
        ])
        cols.extend(get_columns_for_leave_types())
        cols.extend([
            {"label": _("Total Late Entries"), "fieldname": "total_late_entries", "fieldtype": "Float", "width": 140},
            {"label": _("Total Early Exits"), "fieldname": "total_early_exits", "fieldtype": "Float", "width": 140},
        ])
    else:
        cols.append({"label": _("Shift"), "fieldname": "shift", "fieldtype": "Data", "width": 120})
        for d in iter_dates(filters):
            cols.append({
                "label": f"{d.day} {DAY_ABBR[d.weekday()]}",
                "fieldtype": "Data",
                "fieldname": d.isoformat(),   # unique key per day
                "width": 65,
            })
    return cols


def get_columns_for_leave_types() -> list[dict]:
    names = frappe.db.get_all("Leave Type", pluck="name")
    return [{"label": n, "fieldname": frappe.scrub(n), "fieldtype": "Float", "width": 120} for n in names]


# ---------------- data build ----------------

def get_employee_related_details(filters: Filters):
    Employee = frappe.qb.DocType("Employee")
    q = (
        frappe.qb.from_(Employee)
        .select(
            Employee.name, Employee.employee_name, Employee.designation, Employee.grade,
            Employee.department, Employee.branch, Employee.company, Employee.holiday_list,
        )
        .where(Employee.company.isin(filters.companies))
    )
    if filters.get("employee"):
        q = q.where(Employee.name == filters.employee)

    group_by = filters.get("group_by")
    if group_by:
        q = q.orderby(group_by.lower())

    emps = q.run(as_dict=True)

    groups = []
    m = {}
    if group_by:
        keyer = lambda d: "" if d[group_by.lower()] is None else d[group_by.lower()]
        for param, items in groupby(sorted(emps, key=keyer), key=keyer):
            groups.append(param)
            m.setdefault(param, frappe._dict())
            for e in items:
                m[param][e.name] = e
    else:
        for e in emps:
            m[e.name] = e
    return m, groups


def get_holiday_map(filters: Filters):
    holiday_lists = frappe.db.get_all("Holiday List", pluck="name")
    default_hl = frappe.get_cached_value("Company", filters.company, "default_holiday_list")
    holiday_lists.append(default_hl)

    Holiday = frappe.qb.DocType("Holiday")
    out = frappe._dict()
    for hl in holiday_lists:
        if not hl:
            continue
        rows = (
            frappe.qb.from_(Holiday)
            .select(Holiday.holiday_date, Holiday.weekly_off)
            .where((Holiday.parent == hl) & (Holiday.holiday_date.between(filters.from_date, filters.to_date)))
        ).run(as_dict=True)
        out.setdefault(hl, [{"date": r.holiday_date.isoformat(), "weekly_off": r.weekly_off} for r in rows])
    return out


def get_attendance_records(filters: Filters):
    Attendance = frappe.qb.DocType("Attendance")
    status = (
        frappe.qb.terms.Case()
        .when(((Attendance.status == "Half Day") & (Attendance.half_day_status == "Present")), "Half Day/Other Half Present")
        .when(((Attendance.status == "Half Day") & (Attendance.half_day_status == "Absent")), "Half Day/Other Half Absent")
        .else_(Attendance.status)
    )
    q = (
        frappe.qb.from_(Attendance)
        .select(Attendance.employee, Attendance.attendance_date, status.as_("status"), Attendance.shift)
        .where(
            (Attendance.docstatus == 1)
            & (Attendance.company.isin(filters.companies))
            & (Attendance.attendance_date.between(filters.from_date, filters.to_date))
        )
        .orderby(Attendance.employee, Attendance.attendance_date)
    )
    if filters.get("employee"):
        q = q.where(Attendance.employee == filters.employee)
    return q.run(as_dict=True)


def get_attendance_map(filters: Filters):
    """employee -> shift -> { 'YYYY-MM-DD': 'Status' }"""
    rows = get_attendance_records(filters)
    m, leaves = {}, {}

    for row in rows:
        date_key = row.attendance_date.isoformat()
        shift_name = row.shift or ""
        if row.status == "On Leave":
            leaves.setdefault(row.employee, {}).setdefault(shift_name, []).append(date_key)
            continue
        m.setdefault(row.employee, {}).setdefault(shift_name, {})
        m[row.employee][shift_name][date_key] = row.status

    # propagate leave across all recorded shifts for that employee
    for emp, by_shift in leaves.items():
        if emp not in m:
            m.setdefault(emp, {}).setdefault("", {})
        for assigned, dates in by_shift.items():
            for dt in dates:
                for sh in list(m[emp].keys()):
                    m[emp][sh][dt] = "On Leave"
    return m


def get_rows(emp_details, filters, holiday_map, attendance_map):
    records = []
    default_hl = frappe.get_cached_value("Company", filters.company, "default_holiday_list")

    for emp, det in emp_details.items():
        hl = det.holiday_list or default_hl
        holidays = holiday_map.get(hl)

        if filters.get("summarized_view"):
            attendance = get_attendance_status_for_summarized_view(emp, filters, holidays)
            if not attendance:
                continue
            row = {"employee": emp, "employee_name": det.employee_name}
            set_defaults_for_summarized_view(filters, row)
            row.update(attendance)
            row.update(get_leave_summary(emp, filters))
            row.update(get_entry_exits_summary(emp, filters))
            records.append(row)
        else:
            emp_att = attendance_map.get(emp)
            if not emp_att:
                continue
            rows = get_attendance_status_for_detailed_view(emp, filters, emp_att, holidays)
            for r in rows:
                r.update({"employee": emp, "employee_name": det.employee_name})
            records.extend(rows)
    return records


def get_data(filters, attendance_map):
    emp_details, group_vals = get_employee_related_details(filters)
    holidays = get_holiday_map(filters)

    if filters.get("group_by"):
        out = []
        group_col = frappe.scrub(filters.group_by)
        for val in group_vals:
            if not val:
                continue
            rows = get_rows(emp_details[val], filters, holidays, attendance_map)
            if rows:
                out.append({group_col: val})
                out.extend(rows)
        return out
    else:
        return get_rows(emp_details, filters, holidays, attendance_map)


def set_defaults_for_summarized_view(filters, row):
    for col in get_columns(filters):
        if col.get("fieldtype") == "Float":
            row[col.get("fieldname")] = 0.0


# ---------------- summarized view helpers (NULL-safe) ----------------

def get_attendance_summary_and_days(employee, filters):
    Attendance = frappe.qb.DocType("Attendance")

    present_case = frappe.qb.terms.Case().when(((Attendance.status == "Present") | (Attendance.status == "Work From Home")), 1).else_(0)
    sum_present = Sum(present_case).as_("total_present")

    absent_case = frappe.qb.terms.Case().when(Attendance.status == "Absent", 1).else_(0)
    sum_absent = Sum(absent_case).as_("total_absent")

    leave_case = frappe.qb.terms.Case().when(Attendance.status == "On Leave", 1).else_(0)
    sum_leave = Sum(leave_case).as_("total_leaves")

    half_day_case = frappe.qb.terms.Case().when(Attendance.status == "Half Day", 0.5).else_(0)
    sum_half_day = Sum(half_day_case).as_("total_half_days")

    summary_rows = (
        frappe.qb.from_(Attendance)
        .select(sum_present, sum_absent, sum_leave, sum_half_day)
        .where(
            (Attendance.docstatus == 1)
            & (Attendance.employee == employee)
            & (Attendance.company.isin(filters.companies))
            & (Attendance.attendance_date.between(filters.from_date, filters.to_date))
        )
    ).run(as_dict=True)

    # Coerce NULL -> 0.0 to avoid TypeError when adding
    if summary_rows:
        summary = summary_rows[0]
        summary.total_present = float(summary.total_present or 0)
        summary.total_absent  = float(summary.total_absent  or 0)
        summary.total_leaves  = float(summary.total_leaves  or 0)
        summary.total_half_days = float(summary.total_half_days or 0)
    else:
        summary = frappe._dict(
            total_present=0.0, total_absent=0.0, total_leaves=0.0, total_half_days=0.0
        )

    days = (
        frappe.qb.from_(Attendance)
        .select(Attendance.attendance_date)
        .distinct()
        .where(
            (Attendance.docstatus == 1)
            & (Attendance.employee == employee)
            & (Attendance.company.isin(filters.companies))
            & (Attendance.attendance_date.between(filters.from_date, filters.to_date))
        )
    ).run(pluck=True)

    return summary, days


def get_attendance_status_for_summarized_view(employee, filters, holidays):
    summary, attendance_dates = get_attendance_summary_and_days(employee, filters)

    all_keys = [d.isoformat() for d in iter_dates(filters)]
    marked = set([d.isoformat() for d in attendance_dates])

    total_holidays = total_unmarked = 0
    for k in all_keys:
        if k in marked:
            continue
        s = get_holiday_status(k, holidays)
        if s in ["Weekly Off", "Holiday"]:
            total_holidays += 1
        elif not s:
            total_unmarked += 1

    return {
        "total_present": summary.total_present + summary.total_half_days,
        "total_leaves": summary.total_leaves + summary.total_half_days,
        "total_absent": summary.total_absent,
        "total_holidays": total_holidays,
        "unmarked_days": total_unmarked,
    }


# ---------------- detailed view helpers ----------------

def get_attendance_status_for_detailed_view(employee, filters, emp_attendance, holidays):
    rows = []
    for shift_name, by_date in emp_attendance.items():
        row = {"shift": shift_name}
        for d in iter_dates(filters):
            key = d.isoformat()
            status = by_date.get(key)
            if status is None and holidays:
                status = get_holiday_status(key, holidays)
            row[key] = STATUS_MAP.get(status, "")
        rows.append(row)
    return rows


def get_holiday_status(date_iso, holidays):
    if not holidays:
        return None
    for h in holidays:
        if date_iso == h.get("date"):
            return "Weekly Off" if h.get("weekly_off") else "Holiday"
    return None


def get_leave_summary(employee, filters):
    Attendance = frappe.qb.DocType("Attendance")
    day_case = frappe.qb.terms.Case().when(Attendance.status == "Half Day", 0.5).else_(1)
    sum_days = Sum(day_case).as_("leave_days")

    details = (
        frappe.qb.from_(Attendance)
        .select(Attendance.leave_type, sum_days)
        .where(
            (Attendance.employee == employee)
            & (Attendance.docstatus == 1)
            & (Attendance.company.isin(filters.companies))
            & ((Attendance.leave_type.isnotnull()) | (Attendance.leave_type != ""))
            & (Attendance.attendance_date.between(filters.from_date, filters.to_date))
        )
        .groupby(Attendance.leave_type)
    ).run(as_dict=True)

    out = {}
    for d in details:
        if not d.leave_type:
            continue
        out[frappe.scrub(d.leave_type)] = d.leave_days
    return out


def get_entry_exits_summary(employee, filters):
    Attendance = frappe.qb.DocType("Attendance")
    late_case = frappe.qb.terms.Case().when(Attendance.late_entry == "1", "1")
    early_case = frappe.qb.terms.Case().when(Attendance.early_exit == "1", "1")

    res = (
        frappe.qb.from_(Attendance)
        .select(Count(late_case).as_("total_late_entries"), Count(early_case).as_("total_early_exits"))
        .where(
            (Attendance.docstatus == 1)
            & (Attendance.employee == employee)
            & (Attendance.company.isin(filters.companies))
            & (Attendance.attendance_date.between(filters.from_date, filters.to_date))
        )
    ).run(as_dict=True)
    return res[0] if res else {"total_late_entries": 0, "total_early_exits": 0}


# ---------------- chart (same counting rules as reference) ----------------

def get_chart_data(attendance_map: dict, filters: Filters) -> dict:
    date_keys = [d.isoformat() for d in iter_dates(filters)]

    labels: list[str] = []
    absent: list[float] = []
    present: list[float] = []
    leave: list[float] = []

    for k in date_keys:
        dt = getdate(k)
        labels.append(f"{DAY_ABBR[dt.weekday()]} {dt.day:02d}")

        total_absent_on_day = 0.0
        total_present_on_day = 0.0
        total_leaves_on_day = 0.0

        for emp_id, shifts in attendance_map.items():
            leave_counted_for_emp = False
            for shift_name, att in shifts.items():
                status_on_day = att.get(k)
                if status_on_day == "On Leave":
                    if not leave_counted_for_emp:
                        total_leaves_on_day += 1
                        leave_counted_for_emp = True
                elif status_on_day == "Absent":
                    total_absent_on_day += 1
                elif status_on_day in ["Present", "Work From Home"]:
                    total_present_on_day += 1
                elif status_on_day == "Half Day":
                    total_present_on_day += 0.5
                    total_leaves_on_day += 0.5

        absent.append(total_absent_on_day)
        present.append(total_present_on_day)
        leave.append(total_leaves_on_day)

    return {
        "data": {
            "labels": labels,
            "datasets": [
                {"name": _("Absent"), "values": absent},
                {"name": _("Present"), "values": present},
                {"name": _("Leave"), "values": leave},
            ],
        },
        "type": "line",
        "colors": ["red", "green", "blue"],
    }
