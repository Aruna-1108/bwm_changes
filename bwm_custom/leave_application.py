import frappe

APPLICANT_EMAIL_FIELD = "custom_employee_email_id"
APPROVER_FIELD = "leave_approver"
EMPLOYEE_FIELD = "employee"

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
            {approver_expr} = {user}
            OR {applicant_expr} = {user}
            OR EXISTS (
                SELECT 1
                FROM `tabEmployee` emp
                WHERE emp.name = {table}.`{employee_field}`
                  AND {emp_user_expr} = {user}
            )
        )
    """.format(
        approver_expr=sql_norm("{0}.`{1}`".format(table, APPROVER_FIELD)),
        applicant_expr=sql_norm("{0}.`{1}`".format(table, APPLICANT_EMAIL_FIELD)),
        emp_user_expr=sql_norm("emp.`user_id`"),
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
    approver_email = norm(getattr(doc, APPROVER_FIELD, None))
    employee = getattr(doc, EMPLOYEE_FIELD, None)

    if approver_email == user:
        return True

    if applicant_email == user:
        return True

    if employee:
        employee_user = norm(frappe.db.get_value("Employee", employee, "user_id"))
        if employee_user == user:
            return True

    return None