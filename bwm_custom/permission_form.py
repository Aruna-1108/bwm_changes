import frappe

# --- CONFIG ---
APPLICANT_EMAIL_FIELD = "employee_email_id"   # stores User ID (email)
APPROVER_FIELD        = "leave_approver"      # stores User ID (email)
HR_ROLES = {"HR Manager", "System Manager", "HR User", "Administrator"}
# -------------

def _sql_norm(expr: str) -> str:
    """Case/space-insensitive compare for MariaDB/MySQL."""
    return f"LOWER(TRIM({expr}))"

def get_permission_query_conditions(user: str | None = None,
                                    doctype: str | None = None, **kwargs) -> str:
    """
    HR roles -> see all
    Others   -> rows where applicant OR approver (or owner) equals current user id
    """
    user = (user or frappe.session.user or "").strip()
    roles = set(frappe.get_roles(user))
    if HR_ROLES & roles:
        return ""

    table = "`tabPermission Form`"
    user_sql = frappe.db.escape(user)

    return f"""
        {_sql_norm(f"{table}.`{APPLICANT_EMAIL_FIELD}`")} = LOWER(TRIM({user_sql}))
        OR {_sql_norm(f"{table}.`{APPROVER_FIELD}`")}    = LOWER(TRIM({user_sql}))
        OR {_sql_norm(f"{table}.`owner`")}               = LOWER(TRIM({user_sql}))
    """

def has_permission(doc, ptype, user):
    """
    - HR/Admin always have access.
    - Applicant can access their own request.
    - Approver can ALWAYS open assigned forms (read/print/email/share).
    - For other cases, defer to Role Permission Manager by returning None.
    """
    roles = set(frappe.get_roles(user))
    if HR_ROLES & roles:
        return True

    def norm(v): return (v or "").strip().lower()
    u = norm(user)

    # Applicant access
    if norm(getattr(doc, APPLICANT_EMAIL_FIELD, None)) == u:
        return True

    # Approver access (always allowed to open)
    if norm(getattr(doc, APPROVER_FIELD, None)) == u:
        if ptype in {"read", "print", "email", "share"}:
            return True
        # If approvers should also edit/submit, uncomment below:
        # if ptype in {"read", "print", "email", "share", "write", "submit"}:
        #     return True
        return None

    # Not matched → let Role Permission Manager decide
    return None
