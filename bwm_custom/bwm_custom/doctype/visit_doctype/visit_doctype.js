// Client Script for: Visit Doctype
// Path alternative (if file-based):
// apps/bwm_custom/bwm_custom/bwm_custom/doctype/visit_doctype/visit_doctype.js

frappe.ui.form.on("Visit Doctype", {
  refresh(frm) {
    // 6-step flow (match EXACT fieldnames in your doctype)
    const STEPS = [
      { btn: "check_in",      ts: "check_in_time",      lat: "in_latitude",                   lon: "in_longitude" },
      { btn: "waiting_start", ts: "waiting_start_time", lat: "custom_waiting_start_latitude", lon: "custom_waiting_start_longitude" },
      { btn: "wating_end",    ts: "waiting_end_time",   lat: "custom_waiting_end_latitude",   lon: "custom_waiting_end_longitude" },
      { btn: "meeting_start", ts: "meeting_start_time", lat: "custom_meeting_start_latitude", lon: "custom_meeting_start_longitude" },
      { btn: "meeting_end",   ts: "meeting_end_time",   lat: "custom_meeting_end_latitude",   lon: "custom_meeting_end_longitude" },
      { btn: "check_out",     ts: "check_out_time",     lat: "out_latitude",                  lon: "out_longitude" },
    ];

    // ---- helpers: visibility
    function hide_all() {
      STEPS.forEach(s => frm.set_df_property(s.btn, "hidden", 1));
    }
    function first_incomplete_index() {
      for (let i = 0; i < STEPS.length; i++) {
        if (!frm.doc[STEPS[i].ts]) return i;
      }
      return -1; // all done
    }
    function show_next_only() {
      hide_all();
      const i = first_incomplete_index();
      if (i !== -1) frm.set_df_property(STEPS[i].btn, "hidden", 0);
    }

    // ---- helpers: time & geo
    const nowDatetime = () => frappe.datetime.now_datetime(); // for Datetime fields
    const todayDate   = () => frappe.datetime.get_today();    // for Date fields (dd-mm-yyyy safe)

    function getGeo() {
      return new Promise(resolve => {
        if (!navigator.geolocation) return resolve(null);
        navigator.geolocation.getCurrentPosition(
          pos => resolve({
            lat: Number(pos.coords.latitude.toFixed(6)),
            lon: Number(pos.coords.longitude.toFixed(6)),
          }),
          () => resolve(null),
          { enableHighAccuracy: true, timeout: 8000, maximumAge: 0 }
        );
      });
    }

    async function stamp_and_save(step, $btn) {
      if (frm.__visit_saving__) return;
      frm.__visit_saving__ = true;

      try {
        $btn && $btn.prop("disabled", true);

        // 1) stamp this step's datetime
        const ts_val = nowDatetime();
        await frm.set_value(step.ts, ts_val);

        // 2) ensure visit_date is a proper Date (prevents invalid date error)
        if (!frm.doc.visit_date) {
          await frm.set_value("visit_date", todayDate());
        }

        // 3) geotag (best effort)
        const geo = await getGeo();
        if (geo) {
          if (step.lat) await frm.set_value(step.lat, geo.lat);
          if (step.lon) await frm.set_value(step.lon, geo.lon);
        }

        // 4) save
        frappe.dom.freeze("Saving…");
        await frm.save();

        // 5) reveal next button only
        show_next_only();

      } catch (e) {
        console.error(e);
        frappe.msgprint({
          title: "Save failed",
          message: "Error while saving the Visit.",
          indicator: "red",
        });
      } finally {
        frappe.dom.unfreeze();
        $btn && $btn.prop("disabled", false);
        frm.__visit_saving__ = false;
      }
    }

    // bind clicks
    function bind_clicks() {
      STEPS.forEach(step => {
        const fld = frm.fields_dict[step.btn];
        if (fld && fld.$input) {
          fld.$input.off("click.visitflow").on("click.visitflow", async () => {
            await stamp_and_save(step, fld.$input);
          });
        }
      });
    }

    bind_clicks();
    show_next_only();
  },
});
