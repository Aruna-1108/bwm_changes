import frappe

APPLICANT_EMAIL_FIELD = "custom_employee_email_id"
APPROVER_FIELD = "leave_approver"
EMPLOYEE_FIELD = "employee"

def get_permission_query_conditions(user=None, doctype=None, **kwargs):
    user = user or frappe.session.user
    roles = set(frappe.get_roles(user))

    if {"HR Manager", "System Manager", "HR User"} & roles:
        return ""

    user_sql = frappe.db.escape(user)
    table = "`tabLeave Application`"

    return f"""
        (
            {table}.`{APPROVER_FIELD}` = {user_sql}
            OR {table}.`{APPLICANT_EMAIL_FIELD}` = {user_sql}
            OR EXISTS (
                SELECT 1
                FROM `tabEmployee` emp
                WHERE emp.name = {table}.`{EMPLOYEE_FIELD}`
                  AND emp.user_id = {user_sql}
            )
        )
    """


def has_permission(doc, ptype, user=None):
    user = user or frappe.session.user
    roles = set(frappe.get_roles(user))

    if {"HR Manager", "System Manager", "HR User"} & roles:
        return True

    approver_email = getattr(doc, APPROVER_FIELD, None)
    applicant_email = getattr(doc, APPLICANT_EMAIL_FIELD, None)
    employee = getattr(doc, EMPLOYEE_FIELD, None)

    if approver_email == user or applicant_email == user:
        return True

    if employee:
        employee_user = frappe.db.get_value("Employee", employee, "user_id")
        if employee_user == user:
            return True

    return False