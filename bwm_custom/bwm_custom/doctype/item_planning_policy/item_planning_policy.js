frappe.ui.form.on("Item Planning Policy", {
  refresh(frm) {
    frm.add_custom_button("Apply to Item", async () => {
      const d = frm.doc;
      const missing = [];

      // Validate ONLY on click (as requested)
      if (!d.item)                      missing.push("Item");
      if (!d.request_for_warehouse)     missing.push("Request for Warehouse");
      if (!d.check_in_groups)           missing.push("Check in (group)");
      if (!d.material_request_type)     missing.push("Material Request Type");
      if (d.rol == null || d.rol === "") missing.push("Re-order Level (ROL)");
      if (!(d.roq || d.minimum_inventory_qty))
        missing.push("Re-order Qty (ROQ) or Minimum Inventory Qty");

      if (missing.length) {
        frappe.msgprint({
          title: "Missing fields",
          indicator: "red",
          message: `Please enter: <b>${missing.join(", ")}</b>`,
        });
        const map = {
          "Item": "item",
          "Request for Warehouse": "request_for_warehouse",
          "Check in (group)": "check_in_groups",
          "Material Request Type": "material_request_type",
          "Re-order Level (ROL)": "rol",
          "Re-order Qty (ROQ) or Minimum Inventory Qty": "roq",
        };
        const first = map[missing[0]];
        if (first) frm.scroll_to_field(first);
        return;
      }

      frappe.call({
        method:
          "bwm_custom.bwm_custom.doctype.item_planning_policy.item_planning_policy.apply_item_reorder_from_policy",
        args: { policy_name: d.name },
        freeze: true,
        freeze_message: "Updating Item Reorder…",
        callback: (r) => {
          const op = r.message && r.message.op;
          if (op === "updated") {
            frappe.show_alert({ message: "Item Reorder updated (same warehouse & group).", indicator: "green" });
          } else if (op === "no_change") {
            frappe.show_alert({ message: "No change — ROL/ROQ already same.", indicator: "blue" });
          } else if (op === "inserted") {
            frappe.show_alert({ message: "New Item Reorder row inserted.", indicator: "green" });
          }
        },
      });
    });
  },
});
