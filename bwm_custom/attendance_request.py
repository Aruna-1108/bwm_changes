import frappe
from typing import Optional

# Fieldnames on Attendance Request
APPLICANT_EMAIL_FIELD = "custom_employee_email_id"     # applicant's email (Data)
APPROVER_EMAIL_FIELD  = "custom_leave_approver_"       # approver's email (Data)

HR_ROLES = {"HR Manager", "HR User", "System Manager", "Administrator"}

def get_permission_query_conditions(user: Optional[str] = None,
                                    doctype: Optional[str] = None,
                                    **kwargs) -> str:
    """
    List / report filter:
      - HR roles: see all
      - Others: see only if applicant OR approver email matches the login user
    """
    user = user or frappe.session.user
    roles = set(frappe.get_roles(user))

    if HR_ROLES & roles:
        return ""

    dt = doctype or "Attendance Request"
    table = f"`tab{dt}`"
    user_sql = frappe.db.escape((user or "").strip().lower())

    # IMPORTANT: wrap OR inside parentheses
    return (
        f"("
        f"LOWER(TRIM({table}.`{APPLICANT_EMAIL_FIELD}`)) = {user_sql} "
        f"OR "
        f"LOWER(TRIM({table}.`{APPROVER_EMAIL_FIELD}`)) = {user_sql}"
        f")"
    )


def has_permission(doc, ptype: str = "read", user: Optional[str] = None) -> bool:
    """
    Record-level check:
      - HR roles: always True
      - Others: allowed if applicant OR approver email matches
    """
    user = user or frappe.session.user
    roles = set(frappe.get_roles(user))

    if HR_ROLES & roles:
        return True

    u = (user or "").strip().lower()
    applicant_email = (getattr(doc, APPLICANT_EMAIL_FIELD, "") or "").strip().lower()
    approver_email  = (getattr(doc, APPROVER_EMAIL_FIELD, "") or "").strip().lower()

    return (applicant_email == u) or (approver_email == u)
