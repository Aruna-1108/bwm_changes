import frappe

APPLICANT_EMAIL_FIELD = "employee_email_id"
APPROVER_FIELD = "leave_approver"

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


def get_permission_query_conditions(user=None, doctype=None, **kwargs):
    user = norm(user or frappe.session.user)
    roles = set(frappe.get_roles(user))

    if HR_ROLES & roles:
        return ""

    table = "`tabPermission Form`"
    user_sql = frappe.db.escape(user)

    return """
        (
            {applicant_expr} = {user}
            OR {approver_expr} = {user}
            OR {owner_expr} = {user}
        )
    """.format(
        applicant_expr=sql_norm("{0}.`{1}`".format(table, APPLICANT_EMAIL_FIELD)),
        approver_expr=sql_norm("{0}.`{1}`".format(table, APPROVER_FIELD)),
        owner_expr=sql_norm("{0}.`owner`".format(table)),
        user=user_sql
    )


def has_permission(doc, ptype, user=None):
    user = norm(user or frappe.session.user)
    roles = set(frappe.get_roles(user))

    if HR_ROLES & roles:
        return True

    applicant = norm(getattr(doc, APPLICANT_EMAIL_FIELD, None))
    approver = norm(getattr(doc, APPROVER_FIELD, None))
    owner = norm(getattr(doc, "owner", None))

    if applicant == user:
        return True

    if owner == user:
        return True

    if approver == user:
        return True

    return None