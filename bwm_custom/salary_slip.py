import frappe

def salary_slip_permission(user):
    """
    Restrict Salary Slip visibility:
    - HR Manager, HR User, System Manager → see all
    - Others → only their own Salary Slip
    """
    roles = frappe.get_roles(user)
    
    # If user is HR Manager, HR User, or System Manager, show all Salary Slips
    if any(r in roles for r in ["System Manager", "HR Manager", "HR User"]):
        return ""  # No restrictions, show all records

    # For other users, restrict to their own salary slip only
    employee = frappe.db.get_value("Employee", {"user_id": user}, "name")
    if employee:
        return f"`tabSalary Slip`.`employee` = '{employee}'"

    # If no employee found, restrict all records
    return "1=0"  # No records to show for non-authorized users


def salary_slip_has_permission(doc, user):
    """
    Check if the user has permission to access a specific Salary Slip.
    This is used for direct document access (bypasses list filter).
    - Check if the user has a valid role.
    - Verify the document's status (`docstatus` = 1).
    """
    roles = frappe.get_roles(user)
    
    # Allow access for HR Manager, HR User, or System Manager
    if any(r in roles for r in ["System Manager", "HR Manager", "HR User"]):
        return True  # No restriction for these roles
    
    # Check if the user is the employee associated with the salary slip
    employee = frappe.db.get_value("Employee", {"user_id": user}, "name")
    if doc.employee == employee and doc.docstatus == 1:
        return True  # Allow access to their own salary slip if docstatus = 1

    return False  # Deny access otherwise


def get_permission_query_conditions(user):
    """
    Restrict salary slip visibility in list view based on user roles and employee association.
    This will be called to generate the query conditions for listing Salary Slips.
    """
    roles = frappe.get_roles(user)
    
    # If the user is HR Manager, HR User, or System Manager, allow seeing all records
    if any(r in roles for r in ["System Manager", "HR Manager", "HR User"]):
        return ""  # No restrictions, show all records

    # Otherwise, only show Salary Slips linked to the user's employee record
    employee = frappe.db.get_value("Employee", {"user_id": user}, "name")
    if employee:
        return f"`tabSalary Slip`.`employee` = '{employee}'"

    # Deny access if no employee record is found
    return "1=0"  # No records to show for non-authorized users



