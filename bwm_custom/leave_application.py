import frappe

APPLICANT_EMAIL_FIELD = "custom_employee_email_id"
DOC_APPROVER_FIELD = "leave_approver"
EMPLOYEE_FIELD = "employee"
EMPLOYEE_LEAVE_APPROVER_FIELD = "leave_approver"

HR_ROLES = {"HR Manager", "System Manager", "HR User", "Administrator"}


def norm(v):
    return (v or "").strip().lower()


def sql_norm(expr):
    return "LOWER(TRIM(IFNULL({0}, '')))".format(expr)


def get_permission_query_conditions(user=None, doctype=None, **kwargs):
    user = norm(user or frappe.session.user)
    roles = set(frappe.get_roles(user))

    if HR_ROLES & roles:
        return ""

    user_sql = frappe.db.escape(user)
    table = "`tabLeave Application`"

    return """
        (
            {doc_approver_expr} = {user}
            OR {applicant_expr} = {user}
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
                  AND {emp_leave_approver_expr} = {user}
            )
        )
    """.format(
        doc_approver_expr=sql_norm("{0}.`{1}`".format(table, DOC_APPROVER_FIELD)),
        applicant_expr=sql_norm("{0}.`{1}`".format(table, APPLICANT_EMAIL_FIELD)),
        emp_user_expr=sql_norm("emp.`user_id`"),
        mgr_user_expr=sql_norm("mgr.`user_id`"),
        emp_leave_approver_expr=sql_norm("emp_la.`{0}`".format(EMPLOYEE_LEAVE_APPROVER_FIELD)),
        table=table,
        employee_field=EMPLOYEE_FIELD,
        user=user_sql
    )


def has_permission(doc, ptype, user=None):
    user = norm(user or frappe.session.user)
    roles = set(frappe.get_roles(user))

    if HR_ROLES & roles:
        return True

    applicant_email = norm(getattr(doc, APPLICANT_EMAIL_FIELD, None))
    doc_approver_email = norm(getattr(doc, DOC_APPROVER_FIELD, None))
    employee = getattr(doc, EMPLOYEE_FIELD, None)

    # Leave approver on Leave Application
    if doc_approver_email == user:
        return True

    # Applicant email on Leave Application
    if applicant_email == user:
        return True

    if employee:
        # Employee himself
        employee_user = norm(frappe.db.get_value("Employee", employee, "user_id"))
        if employee_user == user:
            return True

        # Reporting manager from Employee.reports_to
        reports_to = frappe.db.get_value("Employee", employee, "reports_to")
        if reports_to:
            manager_user = norm(frappe.db.get_value("Employee", reports_to, "user_id"))
            if manager_user == user:
                return True

        # Leave approver from Employee master
        employee_leave_approver = norm(
            frappe.db.get_value("Employee", employee, EMPLOYEE_LEAVE_APPROVER_FIELD)
        )
        if employee_leave_approver == user:
            return True

    return None