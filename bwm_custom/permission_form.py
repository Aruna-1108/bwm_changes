import frappe

APPLICANT_EMAIL_FIELD = "employee_email_id"
DOC_APPROVER_FIELD = "leave_approver"
EMPLOYEE_FIELD = "employee_name"
EMPLOYEE_LEAVE_APPROVER_FIELD = "leave_approver"

HR_ROLES = {
    "HR Manager",
    "System Manager",
    "HR User",
    "Administrator"
}


def norm(value):
    return (value or "").strip().lower()


def sql_norm(expr):
    return "LOWER(TRIM(IFNULL({0}, '')))".format(expr)


# ================================
# QUERY LEVEL PERMISSION (LIST VIEW)
# ================================
def get_permission_query_conditions(user=None, doctype=None, **kwargs):
    user = norm(user or frappe.session.user)
    roles = set(frappe.get_roles(user))

    # HR can see all
    if HR_ROLES & roles:
        return ""

    table = "`tabPermission Form`"
    user_sql = frappe.db.escape(user)

    return """
        (
            {applicant_expr} = {user}
            OR {doc_approver_expr} = {user}
            OR {owner_expr} = {user}

            OR EXISTS (
                SELECT 1
                FROM `tabEmployee` emp
                WHERE emp.name = {table}.`{employee_field}`
                  AND {emp_user_expr} = {user}
            )

            OR EXISTS (
                SELECT 1
                FROM `tabEmployee` emp_rm
                LEFT JOIN `tabEmployee` mgr
                    ON mgr.name = emp_rm.reports_to
                WHERE emp_rm.name = {table}.`{employee_field}`
                  AND {mgr_user_expr} = {user}
            )

            OR EXISTS (
                SELECT 1
                FROM `tabEmployee` emp_la
                WHERE emp_la.name = {table}.`{employee_field}`
                  AND LOWER(TRIM(IFNULL(emp_la.`{emp_leave_approver_field}`, ''))) = {user}
            )
        )
    """.format(
        applicant_expr=sql_norm(f"{table}.`{APPLICANT_EMAIL_FIELD}`"),
        doc_approver_expr=sql_norm(f"{table}.`{DOC_APPROVER_FIELD}`"),
        owner_expr=sql_norm(f"{table}.`owner`"),
        emp_user_expr=sql_norm("emp.`user_id`"),
        mgr_user_expr=sql_norm("mgr.`user_id`"),
        table=table,
        employee_field=EMPLOYEE_FIELD,
        emp_leave_approver_field=EMPLOYEE_LEAVE_APPROVER_FIELD,
        user=user_sql
    )


# ================================
# DOC LEVEL PERMISSION (FORM VIEW)
# ================================
def has_permission(doc, ptype, user=None):
    user = norm(user or frappe.session.user)
    roles = set(frappe.get_roles(user))

    # HR can see all
    if HR_ROLES & roles:
        return True

    applicant = norm(getattr(doc, APPLICANT_EMAIL_FIELD, None))
    doc_approver = norm(getattr(doc, DOC_APPROVER_FIELD, None))
    owner = norm(getattr(doc, "owner", None))
    employee = getattr(doc, EMPLOYEE_FIELD, None)

    # Applicant
    if applicant == user:
        return True

    # Owner
    if owner == user:
        return True

    # Approver in document
    if doc_approver == user:
        return True

    if employee:
        # Employee himself
        employee_user = norm(
            frappe.db.get_value("Employee", employee, "user_id")
        )
        if employee_user == user:
            return True

        # Reporting Manager
        reports_to = frappe.db.get_value("Employee", employee, "reports_to")
        if reports_to:
            manager_user = norm(
                frappe.db.get_value("Employee", reports_to, "user_id")
            )
            if manager_user == user:
                return True

        # Employee Master Leave Approver
        employee_leave_approver = norm(
            frappe.db.get_value("Employee", employee, EMPLOYEE_LEAVE_APPROVER_FIELD)
        )
        if employee_leave_approver == user:
            return True

    return None