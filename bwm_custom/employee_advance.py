import frappe
from typing import Optional

APPLICANT_FIELD = "custom_employee_email_id"   # applicant email / User
APPROVER_FIELD  = "custom_expense_approver"    # approver email / User

HR_ROLES = {"System Manager", "HR Manager", "HR User"}


def get_permission_query_conditions(
    user: Optional[str] = None,
    doctype: Optional[str] = None,
    **kwargs
) -> str:
    """
    HR roles -> see all
    Others   -> visible if login user equals applicant OR approver
    """
    user = user or frappe.session.user
    if HR_ROLES & set(frappe.get_roles(user)):
        return ""

    dt = doctype or "Employee Advance"
    table = f"`tab{dt}`"
    user_sql = frappe.db.escape(user)

    return (
        f"{table}.`{APPLICANT_FIELD}` = {user_sql} "
        f"OR {table}.`{APPROVER_FIELD}` = {user_sql}"
    )


def has_permission(doc, ptype: Optional[str] = None, user: Optional[str] = None) -> bool:
    """
    Record-level check (blocks direct URL access).
    """
    user = user or frappe.session.user
    if HR_ROLES & set(frappe.get_roles(user)):
        return True

    u     = (user or "").strip().lower()
    appl  = (doc.get(APPLICANT_FIELD) or "").strip().lower()
    appr  = (doc.get(APPROVER_FIELD)  or "").strip().lower()
    return u == appl or u == appr
