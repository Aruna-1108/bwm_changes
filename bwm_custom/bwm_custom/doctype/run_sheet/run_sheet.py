# apps/bwm_custom/bwm_custom/bwm_custom/doctype/run_sheet/run_sheet.py
# Copyright (c) 2025
# Controller for Doctype: Run Sheet

import frappe
from frappe.model.document import Document
from frappe.contacts.doctype.address.address import (
    get_default_address,
    get_address_display,
)
from frappe.contacts.doctype.contact.contact import get_default_contact


class RunSheet(Document):
    def validate(self):
        _fill_rows(self)




def _fill_rows(doc):
    """Fill dependent fields for each row in the child table."""
    for row in (getattr(doc, "party_list", []) or []):
        _fill_party_common(row)     # address + phone for Customer/Lead
        _fill_lead_specifics(row)   # lead_name for Lead


def _fill_party_common(row):
    """
    Populate 'address' (HTML) and 'phone_number' on a row
    for both Customers and Leads using their default Address/Contact.
    Falls back to Lead.mobile_no/phone when party_type = Lead and no Contact exists.
    """
    party_type = getattr(row, "party_type", None)
    party      = getattr(row, "party", None)

    # Default values if nothing to fill
    addr_html = ""
    phone     = ""

    if party_type and party:
       
        addr_name = get_default_address(party_type, party)
        if addr_name:
            addr_html = get_address_display(addr_name) or ""

      
        contact_name = get_default_contact(party_type, party)
        if contact_name:
            cont = frappe.get_doc("Contact", contact_name)
            phone = cont.mobile_no or cont.phone or ""
            if not phone and getattr(cont, "phone_nos", None):
                for ph in cont.phone_nos:
                    if getattr(ph, "phone", None):
                        phone = ph.phone
                        break

      
        if party_type == "Lead" and not phone:
            lead_vals = frappe.db.get_value(
                "Lead", party, ["mobile_no", "phone"], as_dict=True
            )
            if lead_vals:
                phone = lead_vals.mobile_no or lead_vals.phone or ""

   
    if hasattr(row, "address"):
        row.address = addr_html or ""
    if hasattr(row, "phone_number"):
        row.phone_number = phone or ""


def _fill_lead_specifics(row):
    """Populate 'lead_name' when party_type = Lead."""
    if getattr(row, "party_type", None) == "Lead" and getattr(row, "party", None):
        lead_name = frappe.db.get_value("Lead", row.party, "lead_name") or ""
        if hasattr(row, "lead_name"):
            row.lead_name = lead_name
    else:
        # Clear if not a Lead row (prevents stale values)
        if hasattr(row, "lead_name"):
            row.lead_name = ""


#hooks
def validate(doc, method=None):
    _fill_rows(doc)
