import frappe
from typing import Optional

APPLICANT_EMAIL_FIELD = "custom_employee_email_id"  # applicant email field
APPROVER_EMAIL_FIELD  = "custom_leave_approver"     # approver email field

HR_ROLES = {"System Manager", "HR Manager", "HR User", "Administrator"}


def get_permission_query_conditions(
    user: Optional[str] = None,
    doctype: Optional[str] = None,
    **kwargs
) -> str:
    """
    Visibility rules:
      - HR roles -> see all
      - Others   -> show if login email is applicant OR approver
    """
    user = user or frappe.session.user
    roles = set(frappe.get_roles(user))

    if HR_ROLES & roles:
        return ""

    dt = doctype or "Compensatory Leave Request"
    table = f"`tab{dt}`"
    user_sql = frappe.db.escape(user)

    # applicant == user OR approver == user
    return (
        f"{table}.`{APPLICANT_EMAIL_FIELD}` = {user_sql} "
        f"OR {table}.`{APPROVER_EMAIL_FIELD}` = {user_sql}"
    )


def has_permission(doc, ptype: Optional[str], user: str) -> bool:
    """
    Record-level check (covers direct URL access).
    """
    roles = set(frappe.get_roles(user))
    if HR_ROLES & roles:
        return True

    applicant_email = doc.get(APPLICANT_EMAIL_FIELD)
    approver_email  = doc.get(APPROVER_EMAIL_FIELD)

    return (applicant_email == user) or (approver_email == user)
