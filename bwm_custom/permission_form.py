import frappe

# =========================================================
# Permission Form - User Permission Script
# =========================================================
# Logic:
# 1. HR/Admin roles -> full access to all records
# 2. Applicant      -> full access to own record
# 3. Owner          -> full access to own created record
# 4. Approver       -> full access to assigned record
# 5. Others         -> fall back to standard Frappe permissions
# =========================================================

APPLICANT_EMAIL_FIELD = "employee_email_id"   # stores User ID / email
APPROVER_FIELD = "leave_approver"             # stores User ID / email

HR_ROLES = {
    "HR Manager",
    "System Manager",
    "HR User",
    "Administrator"
}


def norm(value):
    """Normalize Python value for safe comparison."""
    return (value or "").strip().lower()


def sql_norm(expr):
    """Normalize SQL expression for safe comparison."""
    return "LOWER(TRIM(IFNULL({0}, '')))".format(expr)


def get_permission_query_conditions(user=None, doctype=None, **kwargs):
    """
    Controls which records are visible in list view.
    HR/Admin -> all
    Others   -> applicant OR approver OR owner matches logged-in user
    """
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
    """
    Controls access to an individual document.
    """
    user = norm(user or frappe.session.user)
    roles = set(frappe.get_roles(user))

    if HR_ROLES & roles:
        return True

    applicant = norm(getattr(doc, APPLICANT_EMAIL_FIELD, None))
    approver = norm(getattr(doc, APPROVER_FIELD, None))
    owner = norm(getattr(doc, "owner", None))

    # Applicant -> full access
    if applicant == user:
        return True

    # Owner -> full access
    if owner == user:
        return True

    # Approver -> full access
    if approver == user:
        return True

    # Let standard Frappe permissions decide
    return None