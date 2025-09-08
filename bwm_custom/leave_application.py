import frappe

APPLICANT_EMAIL_FIELD = "custom_employee_email_id"  # applicant email field
APPROVER_FIELD        = "leave_approver"            # approver email field

def get_permission_query_conditions(user: str | None = None, doctype: str | None = None, **kwargs) -> str:
    """
    Visibility rules:
      - HR Manager / System Manager / HR User -> see all
      - Everyone else (ESS, ESS Approver)     -> show if user is applicant OR approver
    """
    user = user or frappe.session.user
    roles = set(frappe.get_roles(user))

    table   = "`tabLeave Application`"
    user_sql = frappe.db.escape(user)

    # HR roles: unrestricted
    if {"HR Manager", "System Manager", "HR User"} & roles:
        return ""

    # For ESS + ESS Approver → show if either applicant or approver matches login email
    return f"""
        ({table}.`{APPLICANT_EMAIL_FIELD}` = {user_sql}
         OR {table}.`{APPROVER_FIELD}` = {user_sql})
    """


def has_permission(doc, ptype, user) -> bool:
    """
    Record-level check:
      - HR roles: always True
      - Others: only if applicant_email == user OR approver == user
    """
    roles = set(frappe.get_roles(user))

    if {"HR Manager", "System Manager", "HR User"} & roles:
        return True

    applicant_email = getattr(doc, APPLICANT_EMAIL_FIELD, None)
    approver_email  = getattr(doc, APPROVER_FIELD, None)

    return (applicant_email == user) or (approver_email == user)
