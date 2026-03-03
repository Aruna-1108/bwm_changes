frappe.ui.form.on("IndiaMART Enquiry Details", {
  refresh(frm) {
    if (frm.is_new()) return;

    // If already linked, show Open Lead
    if (frm.doc.lead) {
      frm.add_custom_button("Open Lead", () => {
        frappe.set_route("Form", "Lead", frm.doc.lead);
      });
      return;
    }

    frm.add_custom_button("Create Lead", async () => {
      try {
        const r = await frappe.call({
          method:"bwm_custom.bwm_custom.lead.upsert_lead_from_indiamart",
          args: { enquiry_name: frm.doc.name }
        });

        const out = (r && r.message) ? r.message : null;
        if (!out || !out.lead) {
          frappe.msgprint("No Lead returned from server.");
          return;
        }

        if (out.created) {
          frappe.show_alert({ message: "Lead created: " + out.lead, indicator: "green" });
        } else if (out.duplicate) {
          frappe.show_alert({ message: "Lead found: " + out.lead + " (Enquiry already exists)", indicator: "orange" });
        } else {
          frappe.show_alert({ message: "Lead found: " + out.lead + " (Enquiry appended)", indicator: "green" });
        }

        await frm.reload_doc();
        frappe.set_route("Form", "Lead", out.lead);

      } catch (e) {
        console.error(e);

        // If server throws frappe.throw, message is often in e.message or e._server_messages
        frappe.msgprint(
          "Not permitted / server error.<br><br>" +
          "Click 'Copy error to clipboard' and share the full message."
        );
      }
    });
  }
});