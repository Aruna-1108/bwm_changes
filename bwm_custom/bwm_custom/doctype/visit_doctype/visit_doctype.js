// ---- Load guard: prevent double-execution if script is included twice ----
if (window.__BWM_VISIT_JS_LOADED__) {
  // console.debug('[Visit Doctype] script already loaded; skipping re-init');
} else {
  window.__BWM_VISIT_JS_LOADED__ = true;

  // Helper: number or null (never '' or NaN)
  const numOrNull = (v) => (typeof v === 'number' && Number.isFinite(v)) ? v : null;

  // Helper: HTML5 GPS as a Promise
  function getGPS() {
    return new Promise((resolve, reject) => {
      if (!navigator.geolocation) return reject(new Error('No geolocation API'));
      navigator.geolocation.getCurrentPosition(
        pos => resolve({
          latitude: pos.coords.latitude,
          longitude: pos.coords.longitude,
          accuracy: pos.coords.accuracy
        }),
        err => reject(err),
        { enableHighAccuracy: true, timeout: 12000, maximumAge: 0 }
      );
    });
  }

  frappe.ui.form.on('Visit Doctype', {
    refresh(frm) {
      // UI: make stamped fields read-only
      [
        'check_in_time','in_geo_location','in_latitude','in_longitude',
        'check_out_time','out_geo_location','out_latitude','out_longitude'
      ].forEach(f => frm.set_df_property(f, 'read_only', 1));

      // FYI: GPS requires HTTPS or localhost
      try {
        const ok = location.protocol === 'https:' || ['localhost','127.0.0.1'].includes(location.hostname);
        if (!ok) {
          frm.dashboard.add_comment(__('Location capture needs HTTPS (or localhost). Time will still be recorded.'), 'yellow');
        }
      } catch (e) {}
    },

    async check_in(frm) {
      if (frm.is_new()) await frm.save();        // avoid "new-..." names
      if (frm.doc.check_in_time) {
        frappe.msgprint(__('Already checked in on {0}', [
          frappe.datetime.str_to_user(frm.doc.check_in_time)
        ]));
        return;
      }

      frm.toggle_enable('check_in', false);
      try {
        const coords = await getGPS().catch(() => null);

        await frappe.call({
          method: 'bwm_custom.bwm_custom.doctype.visit_doctype.visit_doctype.visit_check_in',
          args: {
            visit_name: frm.doc.name,
            lat: numOrNull(coords?.latitude),
            lng: numOrNull(coords?.longitude),
            accuracy: numOrNull(coords?.accuracy),
          },
          freeze: true,
          freeze_message: __('Recording Check-In...')
        });

        await frm.reload_doc();
        frappe.show_alert({ message: __('Check-In saved.'), indicator: 'green' }, 6);
        if (!coords) frappe.show_alert({ message: __('Location not captured.'), indicator: 'orange' }, 7);
      } finally {
        frm.toggle_enable('check_in', true);
      }
    },

    async check_out(frm) {
      if (frm.is_new()) await frm.save();
      if (!frm.doc.check_in_time) {
        frappe.msgprint(__('Please Check In first.'));
        return;
      }
      if (frm.doc.check_out_time) {
        frappe.msgprint(__('Already checked out on {0}', [
          frappe.datetime.str_to_user(frm.doc.check_out_time)
        ]));
        return;
      }

      frm.toggle_enable('check_out', false);
      try {
        const coords = await getGPS().catch(() => null);

        await frappe.call({
          method: 'bwm_custom.bwm_custom.doctype.visit_doctype.visit_doctype.visit_check_out',
          args: {
            visit_name: frm.doc.name,
            lat: numOrNull(coords?.latitude),
            lng: numOrNull(coords?.longitude),
            accuracy: numOrNull(coords?.accuracy),
          },
          freeze: true,
          freeze_message: __('Recording Check-Out...')
        });

        await frm.reload_doc();
        frappe.show_alert({ message: __('Check-Out saved.'), indicator: 'green' }, 6);
        if (!coords) frappe.show_alert({ message: __('Location not captured.'), indicator: 'orange' }, 7);
      } finally {
        frm.toggle_enable('check_out', true);
      }
    },
  });
}
