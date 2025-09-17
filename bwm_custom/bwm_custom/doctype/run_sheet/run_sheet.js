// Client Script for Doctype: Run Sheet
frappe.ui.form.on('Run Sheet', {
  onload(frm) {
    // Subscribe once to realtime event for live updates
    if (!frm._visit_realtime_bound) {
      frm._visit_realtime_bound = true;

      frappe.realtime.on('visit_appended', (data) => {
        // react only if it's THIS run sheet
        if (!data || data.run_sheet !== frm.doc.name) return;

        frappe.show_alert({
          message: __('New Visit appended: {0}', [data.visit_id]),
          indicator: 'green'
        });

        // reload to bring in the new child row (fast + safe)
        frm.reload_doc().then(() => frm.refresh_field('visit_history'));
      });
    }
  },

  create_visit(frm) {
    const VISIT_DOCTYPE = 'Visit Doctype'; // adjust if your name differs
    frappe.model.with_doctype(VISIT_DOCTYPE, () => {
      const meta = frappe.get_meta(VISIT_DOCTYPE);
      const has  = f => meta.fields.some(df => df.fieldname === f);

      const v = frappe.model.get_new_doc(VISIT_DOCTYPE);

      // Prefill from Run Sheet
      if (has('company'))      v.company     = frm.doc.company;
      if (has('cost_center'))  v.cost_center = frm.doc.cost_center;
      if (has('territory'))    v.territory   = frm.doc.territory;
      if (has('party_type'))   v.party_type  = frm.doc.party_type;
      if (has('party'))        v.party       = frm.doc.party;
      if (has('employee'))     v.employee    = frm.doc.employee;
      if (has('run_sheet'))    v.run_sheet   = frm.doc.name;   // IMPORTANT

      // optional odo copy if your Visit has these fields
      if (has('odo_start_km') && frm.doc.odameter_start_km) v.odo_start_km = frm.doc.odameter_start_km;
      if (has('odo_end_km')   && frm.doc.odameter_end_km)   v.odo_end_km   = frm.doc.odameter_end_km;

      // Open the new Visit
      frappe.set_route('Form', VISIT_DOCTYPE, v.name);
    });
  }
});
