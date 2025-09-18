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

    
    employee = frappe.db.get_value("Employee", {"user_id": user}, "name")
    if employee:
        return f"`tabSalary Slip`.`employee` = '{employee}'"

    return "1=0"  


def salary_slip_has_permission(doc, user):
    """
    Also check direct form access (bypasses list filter).
    """
    roles = frappe.get_roles(user)
    if any(r in roles for r in ["System Manager", "HR Manager", "HR User"]):
        return True

    employee = frappe.db.get_value("Employee", {"user_id": user}, "name")
    return doc.employee == employee
