// Material Planning Request & Items
// Auto-fill classification, demand stats, safety, ROL, coverage, stock snapshot
// and create Material Request from the parent button.

function clear_mpr_item_row(cdt, cdn) {
    const zero_fields = [
        "customer_count",
        "avg_monthly_qty",
        "sd",
        "total_units",
        "cv",
        "p50",
        "p80",
        "lt_used_days",
        "add_per_day",
        "sigma_daily",
        "safety_stock_unit",
        "safety_day",
        "recommended_rol",
        "coverage_days",
        "recommended_roq",
        "on_hand_qty",
        "projected_qty",
        "reserved_qty",
        "ordered_qty",
        "on_hand_coverage_days",
        "projected_coverage_days",
        "projected_minus_rol"
    ];
    const blank_fields = [
        "item_classification",
        "xyz_classification",
        "fsn"
    ];

    zero_fields.forEach(f => frappe.model.set_value(cdt, cdn, f, 0));
    blank_fields.forEach(f => frappe.model.set_value(cdt, cdn, f, ""));
}

// ---------------- CHILD: when Item changes ----------------

frappe.ui.form.on("Material Planning Request Item", {
    item: function (frm, cdt, cdn) {
        const row = frappe.get_doc(cdt, cdn);
        const cost_center = frm.doc.cost_center;
        const item = row.item;

        if (!item || !cost_center) {
            clear_mpr_item_row(cdt, cdn);
            return;
        }

        frappe.call({
            method: "bwm_custom.bwm_custom.doctype.material_planning_request.material_planning_request.get_item_classification_for_mpr",
            args: {
                item: item,
                cost_center: cost_center
            },
            callback: function (r) {
                if (!r.message) {
                    clear_mpr_item_row(cdt, cdn);
                    return;
                }

                const d = r.message;

                // ABC / XYZ / FSN
                frappe.model.set_value(cdt, cdn, "item_classification", d.item_classification || d._class || "");
                frappe.model.set_value(cdt, cdn, "xyz_classification", d.xyz_classification || "");
                frappe.model.set_value(cdt, cdn, "fsn", d.fsn || "");
                frappe.model.set_value(cdt, cdn, "customer_count", d.customer_count || 0);

                // T12 stats
                frappe.model.set_value(cdt, cdn, "avg_monthly_qty", d.avg_monthly_qty || 0);
                frappe.model.set_value(cdt, cdn, "sd", d.sd || 0);
                frappe.model.set_value(cdt, cdn, "total_units", d.total_units || 0);
                frappe.model.set_value(cdt, cdn, "cv", d.cv || 0);

                // Lead time
                frappe.model.set_value(cdt, cdn, "p50", d.p50 || 0);
                frappe.model.set_value(cdt, cdn, "p80", d.p80 || 0);
                frappe.model.set_value(cdt, cdn, "lt_used_days", d.lt_used_days || 0);

                // Demand & safety
                frappe.model.set_value(cdt, cdn, "add_per_day", d.add_per_day || 0);
                frappe.model.set_value(cdt, cdn, "sigma_daily", d.sigma_daily || 0);
                frappe.model.set_value(cdt, cdn, "safety_stock_unit", d.safety_stock_unit || 0);
                frappe.model.set_value(cdt, cdn, "safety_day", d.safety_day || 0);

                // ROL, coverage, ROQ
                frappe.model.set_value(cdt, cdn, "recommended_rol", d.recommended_rol || 0);
                frappe.model.set_value(cdt, cdn, "coverage_days", d.coverage_days || 0);
                frappe.model.set_value(cdt, cdn, "recommended_roq", d.recommended_roq || d.roq_suggested || 0);

                // Stock snapshot & projected vs ROL
                frappe.model.set_value(cdt, cdn, "on_hand_qty", d.on_hand_qty || 0);
                frappe.model.set_value(cdt, cdn, "projected_qty", d.projected_qty || 0);
                frappe.model.set_value(cdt, cdn, "reserved_qty", d.reserved_qty || 0);
                frappe.model.set_value(cdt, cdn, "ordered_qty", d.ordered_qty || 0);
                frappe.model.set_value(cdt, cdn, "on_hand_coverage_days", d.on_hand_coverage_days || 0);
                frappe.model.set_value(cdt, cdn, "projected_coverage_days", d.projected_coverage_days || 0);
                frappe.model.set_value(cdt, cdn, "projected_minus_rol", d.projected_minus_rol || 0);
            }
        });
    }
});

// ---------------- PARENT: Cost Center & Create MR button ----------------

frappe.ui.form.on("Material Planning Request", {
    cost_center: function (frm) {
        const cc = frm.doc.cost_center;

        if (!cc) {
            (frm.doc.material_planning_request_item || []).forEach(row => {
                clear_mpr_item_row(row.doctype, row.name);
            });
            return;
        }

        (frm.doc.material_planning_request_item || []).forEach(row => {
            if (!row.item) {
                clear_mpr_item_row(row.doctype, row.name);
                return;
            }

            frappe.call({
                method: "bwm_custom.bwm_custom.doctype.material_planning_request.material_planning_request.get_item_classification_for_mpr",
                args: {
                    item: row.item,
                    cost_center: cc
                },
                callback: function (r) {
                    if (!r.message) {
                        clear_mpr_item_row(row.doctype, row.name);
                        return;
                    }

                    const d = r.message;

                    // ABC / XYZ / FSN
                    frappe.model.set_value(row.doctype, row.name, "item_classification", d.item_classification || d._class || "");
                    frappe.model.set_value(row.doctype, row.name, "xyz_classification", d.xyz_classification || "");
                    frappe.model.set_value(row.doctype, row.name, "fsn", d.fsn || "");
                    frappe.model.set_value(row.doctype, row.name, "customer_count", d.customer_count || 0);

                    // T12 stats
                    frappe.model.set_value(row.doctype, row.name, "avg_monthly_qty", d.avg_monthly_qty || 0);
                    frappe.model.set_value(row.doctype, row.name, "sd", d.sd || 0);
                    frappe.model.set_value(row.doctype, row.name, "total_units", d.total_units || 0);
                    frappe.model.set_value(row.doctype, row.name, "cv", d.cv || 0);

                    // Lead time
                    frappe.model.set_value(row.doctype, row.name, "p50", d.p50 || 0);
                    frappe.model.set_value(row.doctype, row.name, "p80", d.p80 || 0);
                    frappe.model.set_value(row.doctype, row.name, "lt_used_days", d.lt_used_days || 0);

                    // Demand & safety
                    frappe.model.set_value(row.doctype, row.name, "add_per_day", d.add_per_day || 0);
                    frappe.model.set_value(row.doctype, row.name, "sigma_daily", d.sigma_daily || 0);
                    frappe.model.set_value(row.doctype, row.name, "safety_stock_unit", d.safety_stock_unit || 0);
                    frappe.model.set_value(row.doctype, row.name, "safety_day", d.safety_day || 0);

                    // ROL, coverage, ROQ
                    frappe.model.set_value(row.doctype, row.name, "recommended_rol", d.recommended_rol || 0);
                    frappe.model.set_value(row.doctype, row.name, "coverage_days", d.coverage_days || 0);
                    frappe.model.set_value(row.doctype, row.name, "recommended_roq", d.recommended_roq || d.roq_suggested || 0);

                    // Stock snapshot & projected vs ROL
                    frappe.model.set_value(row.doctype, row.name, "on_hand_qty", d.on_hand_qty || 0);
                    frappe.model.set_value(row.doctype, row.name, "projected_qty", d.projected_qty || 0);
                    frappe.model.set_value(row.doctype, row.name, "reserved_qty", d.reserved_qty || 0);
                    frappe.model.set_value(row.doctype, row.name, "ordered_qty", d.ordered_qty || 0);
                    frappe.model.set_value(row.doctype, row.name, "on_hand_coverage_days", d.on_hand_coverage_days || 0);
                    frappe.model.set_value(row.doctype, row.name, "projected_coverage_days", d.projected_coverage_days || 0);
                    frappe.model.set_value(row.doctype, row.name, "projected_minus_rol", d.projected_minus_rol || 0);
                }
            });
        });
    },

    // Button fieldname: create_wo  (label can be "Create MR")
    create_wo: function (frm) {
        if (frm.is_new()) {
            frappe.msgprint(__("Please save the document before creating Material Request."));
            return;
        }

        if (!frm.doc.warehouse) {
            frappe.msgprint(__("Please set Warehouse before creating Material Request."));
            return;
        }

        // ✅ read checked rows in child table
        const selected = frm.get_selected().material_planning_request_item || [];

        if (!selected.length) {
            frappe.msgprint(__("Please select at least one row in Material Planning Request Item."));
            return;
        }

        frappe.call({
            method: "bwm_custom.bwm_custom.doctype.material_planning_request.material_planning_request.create_material_request_for_mpr",
            args: {
                mpr_name: frm.doc.name,
                rows: selected
            },
            freeze: true,
            freeze_message: __("Creating Material Request..."),
            callback: function (r) {
                if (!r.message || !r.message.material_request) {
                    frappe.msgprint(__("No Material Request was created."));
                    return;
                }

                const mr = r.message.material_request;
                const link = `<a href="/app/material-request/${mr}" target="_blank">${mr}</a>`;

                frappe.msgprint({
                    title: __("Material Request Created"),
                    message: link,
                    indicator: "green"
                });

                frm.reload_doc();
            }
        });
    }
});
