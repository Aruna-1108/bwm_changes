frappe.ui.form.on("India Mart API Settings", {
  refresh(frm) {
    if (frm.is_new()) return;

    frm.add_custom_button("Sync Now", () => {
      frm.call({
        method: "bwm_custom.bwm_custom.sync.run_sync",
        args: {
          setting_name: frm.doc.name
        },
        freeze: true,
        freeze_message: "Syncing IndiaMART enquiries..."
      }).then((r) => {
        const d = (r && r.message) ? r.message : null;

        if (!d) {
          frappe.msgprint({
            title: "IndiaMART Sync Result",
            message: "No response returned from server.",
            indicator: "orange"
          });
          return;
        }

        frappe.msgprint({
          title: "IndiaMART Sync Result",
          message: `
            <b>Status:</b> ${d.status}<br>
            <b>Log:</b> ${d.log}<br>
            <b>Fetched:</b> ${d.fetched}<br>
            <b>Created:</b> ${d.created}<br>
            <b>Duplicate:</b> ${d.duplicate}<br>
            <b>Errors:</b> ${d.errors}<br>
            <b>Cursor Before:</b> ${d.cursor_before || ""}<br>
            <b>Cursor After:</b> ${d.cursor_after || ""}
          `,
          indicator: (d.status === "Success") ? "green" : (d.status === "Partial") ? "orange" : "red"
        });

        frm.reload_doc();
      }).catch((e) => {
        frappe.msgprint({
          title: "IndiaMART Sync Failed",
          message: (e && e.message) ? e.message : e,
          indicator: "red"
        });
      });
    });
  }
});