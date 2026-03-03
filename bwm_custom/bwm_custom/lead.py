import frappe


def _s(v):
    return (v or "").strip()


@frappe.whitelist()
def upsert_lead_from_indiamart(enquiry_name: str):
    """
    IndiaMART Enquiry -> Lead Upsert

    Rules:
    1) Find existing Lead by mobile_no first, else email_id
    2) If Lead exists: APPEND a child row to Lead.custom_enquiry_details
       - prevent duplicate by indiamart_enquiry_id
    3) If Lead not exists: CREATE Lead with 1 child row
    4) Update IndiaMART Enquiry Details:
       - status = "Lead Converted"
       - sync_status = "Converted"
       - lead = lead_name (if field exists)
    5) Clear, specific permission error messages
    """

    if not enquiry_name:
        frappe.throw("Missing enquiry_name")

    # Permission: must read enquiry
    if not frappe.has_permission("IndiaMART Enquiry Details", "read", enquiry_name):
        frappe.throw("Not permitted: need READ on IndiaMART Enquiry Details")

    enq = frappe.get_doc("IndiaMART Enquiry Details", enquiry_name)

    full_name = _s(getattr(enq, "full_name", ""))
    if not full_name:
        frappe.throw("Full Name is required")

    mobile = _s(getattr(enq, "mobile", ""))
    email = _s(getattr(enq, "email", ""))

    if not mobile and not email:
        frappe.throw("Mobile or Email is required to create/append Lead")

    country = _s(getattr(enq, "country", ""))
    if country == "IN":
        country = "India"

    enquiry_id = _s(getattr(enq, "im_enquiry_id", ""))

    # ---- Find existing lead (mobile preferred, then email) ----
    lead_name = ""
    if mobile:
        lead_name = frappe.db.get_value("Lead", {"mobile_no": mobile}, "name") or ""
    if not lead_name and email:
        lead_name = frappe.db.get_value("Lead", {"email_id": email}, "name") or ""

    # ---- Build enquiry row (child table fields) ----
    row_values = {
        "enquiry_date": getattr(enq, "date", "") or "",
        "product_name": getattr(enq, "product_name", "") or "",
        "product_category": getattr(enq, "product_category", "") or "",
        "remarks": getattr(enq, "subject", "") or "",
        "enquiry_owner": getattr(enq, "lead_user", "") or "",
        "indiamart_enquiry_id": enquiry_id,
    }

    created = 0
    appended = 0
    duplicate = 0

    # ---- Append to existing lead ----
    if lead_name:
        if not frappe.has_permission("Lead", "write", lead_name):
            frappe.throw("Not permitted: need WRITE on Lead " + lead_name)

        lead = frappe.get_doc("Lead", lead_name)

        # Ensure the custom child table exists
        if not hasattr(lead, "custom_enquiry_details"):
            frappe.throw("Lead is missing field: custom_enquiry_details")

        # Duplicate check by enquiry_id
        if enquiry_id:
            for r in (lead.custom_enquiry_details or []):
                if _s(getattr(r, "indiamart_enquiry_id", "")) == enquiry_id:
                    duplicate = 1
                    break

        if not duplicate:
            lead.append("custom_enquiry_details", row_values)
            lead.save(ignore_permissions=False)
            appended = 1

    # ---- Create new lead ----
    else:
        if not frappe.has_permission("Lead", "create"):
            frappe.throw("Not permitted: need CREATE on Lead")

        lead = frappe.get_doc({
            "doctype": "Lead",
            "naming_series": "CRM-LEAD-.YYYY.-",
            "first_name": full_name,
            "lead_name": full_name,
            "company_name": getattr(enq, "company", "") or "",
            "mobile_no": mobile or "",
            "phone": mobile or "",
            "email_id": email or "",
            "city": getattr(enq, "city", "") or "",
            "state": getattr(enq, "state", "") or "",
            "country": country or "",
            "status": "Lead",
            "custom_enquiry_details": [row_values],
        })
        lead.insert(ignore_permissions=False)
        lead_name = lead.name
        created = 1

    # ---- Update IndiaMART Enquiry Details ----
    if not frappe.has_permission("IndiaMART Enquiry Details", "write", enq.name):
        frappe.throw("Not permitted: need WRITE on IndiaMART Enquiry Details")

    meta = frappe.get_meta("IndiaMART Enquiry Details")
    update_map = {}

    if meta.has_field("status"):
        update_map["status"] = "Lead Converted"
    if meta.has_field("sync_status"):
        update_map["sync_status"] = "Converted"
    if meta.has_field("lead"):
        update_map["lead"] = lead_name

    if update_map:
        frappe.db.set_value("IndiaMART Enquiry Details", enq.name, update_map)

    frappe.db.commit()

    return {
        "lead": lead_name,
        "created": created,
        "appended": appended,
        "duplicate": duplicate
    }