import frappe

def salary_slip_permission(user):
    """
    Restrict Salary Slip visibility:
    - HR Manager, HR User, System Manager → see all
    - Others → only their own Salary Slip
    """
    roles = frappe.get_roles(user)
    if any(r in roles for r in ["System Manager", "HR Manager", "HR User"]):
        return ""  

    # Check employee linked to the user
    employee = frappe.db.get_value("Employee", {"user_id": user}, "name")
    if employee:
        return f"`tabSalary Slip`.`employee` = '{employee}'"

    return "1=0"  

def salary_slip_has_permission(doc, user):
    """
    Also check direct form access (bypasses list filter).
    - Check if the user has a valid role.
    - Verify the document's status (`docstatus` = 1).
    """
    roles = frappe.get_roles(user)
    if any(r in roles for r in ["System Manager", "HR Manager", "HR User"]):
        return True

    # Check if the user is the employee associated with the salary slip
    employee = frappe.db.get_value("Employee", {"user_id": user}, "name")
    if doc.employee == employee and doc.docstatus == 1:
        return True

    return False
