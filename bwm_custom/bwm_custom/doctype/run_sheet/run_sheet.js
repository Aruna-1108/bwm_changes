frappe.ui.form.on("Runsheet Party", {
    visit: function(frm, cdt, cdn) {
        let row = locals[cdt][cdn];

        if (!row.party) {
            frappe.msgprint("Please select a Party before visiting.");
            return;
        }

        if (!frm.doc.employee) {
            frappe.msgprint("Please set Employee on the Run Sheet first.");
            return;
        }

        frappe.new_doc("Visit Doctype", {
            employee: frm.doc.employee,
            party_type: row.party_type,
            party: row.party,
            party_address: row.address,
            phone: row.phone_number,
            custom_runsheet_party_id: row.name,
            run_sheet: row.parent,
            custom_purpose_of_visit: row.purpose__of_visit
        });
    }
});
