import frappe


def _s(v):
    return str(v or "").strip()


@frappe.whitelist()
def upsert_lead_from_indiamart(enquiry_name):
    """
    IndiaMART Enquiry -> Lead Upsert

    Rules:
    1) Skip if enquiry status is already "Lead Converted"
    2) Find existing Lead by mobile_no first, else email_id
    3) If Lead exists: find child row by indiamart_enquiry_id
       - if found and nothing changed: skip save
       - if found and changed: update fields and save
       - if not found: append new row
    4) If Lead not exists: create Lead with 1 child row
    5) Update IndiaMART Enquiry Details:
       - status = "Lead Converted"
       - sync_status = "Converted"
       - lead = lead_name (if field exists)
    """

    if not enquiry_name:
        frappe.throw("Missing enquiry_name")

    if not frappe.has_permission("IndiaMART Enquiry Details", "read", enquiry_name):
        frappe.throw("Not permitted: need READ on IndiaMART Enquiry Details")

    enq = frappe.get_doc("IndiaMART Enquiry Details", enquiry_name)

    # Skip if already processed
    if _s(getattr(enq, "status", "")) == "Lead Converted":
        return {
            "lead": _s(getattr(enq, "lead", "")),
            "created": 0,
            "appended": 0,
            "updated": 0,
            "skipped": 1
        }

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

    lead_name = ""
    if mobile:
        lead_name = frappe.db.get_value("Lead", {"mobile_no": mobile}, "name") or ""
    if not lead_name and email:
        lead_name = frappe.db.get_value("Lead", {"email_id": email}, "name") or ""

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
    updated = 0
    skipped = 0

    if lead_name:
        if not frappe.has_permission("Lead", "write", lead_name):
            frappe.throw("Not permitted: need WRITE on Lead " + lead_name)

        lead = frappe.get_doc("Lead", lead_name)

        if not hasattr(lead, "custom_enquiry_details"):
            frappe.throw("Lead is missing field: custom_enquiry_details")

        existing_row = None
        if enquiry_id:
            for r in (lead.custom_enquiry_details or []):
                if _s(getattr(r, "indiamart_enquiry_id", "")) == enquiry_id:
                    existing_row = r
                    break

        if existing_row:
            # Check if anything actually changed
            changed = (
                _s(existing_row.enquiry_date)     != _s(row_values["enquiry_date"])     or
                _s(existing_row.product_name)     != _s(row_values["product_name"])     or
                _s(existing_row.product_category) != _s(row_values["product_category"]) or
                _s(existing_row.remarks)          != _s(row_values["remarks"])          or
                _s(existing_row.enquiry_owner)    != _s(row_values["enquiry_owner"])
            )

            if changed:
                existing_row.enquiry_date     = row_values["enquiry_date"]
                existing_row.product_name     = row_values["product_name"]
                existing_row.product_category = row_values["product_category"]
                existing_row.remarks          = row_values["remarks"]
                existing_row.enquiry_owner    = row_values["enquiry_owner"]
                lead.save(ignore_permissions=False)
                updated = 1
            else:
                skipped = 1  # Nothing changed, skip save

        else:
            # New enquiry_id — append fresh row
            lead.append("custom_enquiry_details", row_values)
            lead.save(ignore_permissions=False)
            appended = 1

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
            "source": "IndiaMart",
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

    return {
        "lead": lead_name,
        "created": created,
        "appended": appended,
        "updated": updated,
        "skipped": skipped
    }