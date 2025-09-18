import frappe
from typing import Optional

# Fieldnames on Attendance Request
APPLICANT_EMAIL_FIELD = "custom_employee_email_id"   # applicant's email
APPROVER_EMAIL_FIELD  = "custom_leave_approver_"     # approver's email  (confirm this fieldname)

HR_ROLES = {"HR Manager", "HR User", "System Manager", "Administrator"}


def get_permission_query_conditions(
    user: Optional[str] = None,
    doctype: Optional[str] = None,
    **kwargs
) -> str:
    """
    Visibility:
      - HR roles (HR Manager, HR User, System Manager, Administrator): see all
      - Others (ESS / ESS Approver): show if login email matches applicant OR approver
    """
    user = user or frappe.session.user
    roles = set(frappe.get_roles(user))

    # Unrestricted for HR/system roles
    if HR_ROLES & roles:
        return ""

    # Build table name safely (hooks will pass the correct doctype)
    table = f"`tab{doctype or 'Attendance Request'}`"

    # Escape the user string for SQL
    user_sql = frappe.db.escape(user)

    # Filter: applicant == user OR approver == user
    return (
        f"{table}.`{APPLICANT_EMAIL_FIELD}` = {user_sql} "
        f"OR {table}.`{APPROVER_EMAIL_FIELD}` = {user_sql}"
    )


def has_permission(doc, ptype: str, user: str) -> bool:
    """
    Record-level check:
      - HR roles: always True
      - Others: allowed if doc.applicant_email == user OR doc.approver_email == user
    """
    roles = set(frappe.get_roles(user))
    if HR_ROLES & roles:
        return True

    applicant_email = getattr(doc, APPLICANT_EMAIL_FIELD, None)
    approver_email  = getattr(doc, APPROVER_EMAIL_FIELD, None)

    return (applicant_email == user) or (approver_email == user)
