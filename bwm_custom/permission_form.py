import frappe

APPLICANT_EMAIL_FIELD = "employee_email_id"   # stores User ID
APPROVER_FIELD        = "leave_approver"      # stores User ID

HR_ROLES = {"HR Manager", "System Manager", "HR User"}

def _sql_norm(expr: str) -> str:
    # MariaDB/MySQL: robust case/space-insensitive compare
    return f"LOWER(TRIM({expr}))"

def get_permission_query_conditions(user: str | None = None,
                                    doctype: str | None = None, **kwargs) -> str:
    """
    HR roles -> see all
    Others   -> where applicant or approver (or owner, optional) equals current user id
    """
    user = (user or frappe.session.user or "").strip()
    roles = set(frappe.get_roles(user))
    if HR_ROLES & roles:
        return ""

    table   = "`tabPermission Form`"
    user_sql = frappe.db.escape(user)

    return f"""
        {_sql_norm(f"{table}.`{APPLICANT_EMAIL_FIELD}`")} = LOWER(TRIM({user_sql}))
        OR {_sql_norm(f"{table}.`{APPROVER_FIELD}`")}    = LOWER(TRIM({user_sql}))
        OR {_sql_norm(f"{table}.`owner`")}               = LOWER(TRIM({user_sql}))  -- remove if you don't want owners
    """

def has_permission(doc, ptype, user) -> bool:
    roles = set(frappe.get_roles(user))
    if HR_ROLES & roles:
        return True

    norm = lambda v: (v or "").strip().lower()
    u = norm(user)
    return (
        norm(getattr(doc, APPLICANT_EMAIL_FIELD, None)) == u
        or norm(getattr(doc, APPROVER_FIELD, None)) == u
        or norm(getattr(doc, "owner", None)) == u      # remove if you don't want owners
    )
