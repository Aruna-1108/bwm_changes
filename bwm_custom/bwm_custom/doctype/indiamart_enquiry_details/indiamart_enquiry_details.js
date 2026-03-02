// frappe.ui.form.on("India Mart API Settings", {
//   refresh(frm) {
//     if (frm.is_new()) return;

//     frm.add_custom_button("Run IndiaMART Sync", async () => {
//       await frappe.call({
//         method: "bwm_custom.integrations.indiamart.sync.run_sync_for_setting",
//         args: { setting_name: frm.doc.name }
//       });
//       frappe.msgprint("Sync completed. Check India MART Sync Log.");
//     });
//   }
// });


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

    // Create Lead (no backend insert) -> open new Lead with prefilled values
    frm.add_custom_button("Create Lead", () => {
      const full_name = (frm.doc.full_name || "").trim();
      if (!full_name) {
        frappe.msgprint("Full Name is required.");
        return;
      }

      // IMPORTANT:
      // Lead.country in ERPNext usually expects "India" (Country master), not "IN"
      const country_val = (frm.doc.country || "").trim();
      const country = (country_val === "IN") ? "India" : country_val;

      // Prefill values using route_options
      frappe.route_options = {
        first_name: full_name,
        lead_name: full_name,
        company_name: frm.doc.company || "",
        mobile_no: frm.doc.mobile || "",
        email_id: frm.doc.email || "",
        city: frm.doc.city || "",
        state: frm.doc.state || "",
        country: country || "",
        notes: frm.doc.message || ""
      };

      // Open new Lead form
      frappe.new_doc("Lead");
    });
  }
});