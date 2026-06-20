// Copyright (c) 2025, Aruna Devi and contributors
// For license information, please see license.txt

frappe.query_reports["Attendance Report - Without Shift"] = {
	"filters": [
		{
			fieldname:"from_date",
			label: __("From Date"),
			fieldtype:"Date",
		

		},
			{
			fieldname:"to_date",
			label: __("To Date"),
			fieldtype:"Date",
		

		},
		
		{
			fieldname: "employee",
			label: __("Employee"),
			fieldtype: "Link",
			options: "Employee",
			get_query: () => {
				var company = frappe.query_report.get_filter_value("company");
				return {
					filters: {
						company: company,
					},
				};
			},
		},
		
		{
			fieldname: "company",
			label: __("Company"),
			fieldtype: "Link",
			options: "Company",
			default: frappe.defaults.get_user_default("Company"),
			reqd: 1,
		},
		{
			fieldname: "group_by",
			label: __("Group By"),
			fieldtype: "Select",
			options: ["", "Branch", "Grade", "Department", "Designation"],
		},
		{
			fieldname: "include_company_descendants",
			label: __("Include Company Descendants"),
			fieldtype: "Check",
			default: 1,
		},
		{
			fieldname: "summarized_view",
			label: __("Summarized View"),
			fieldtype: "Check",
			default: 0,
		},
	],
	formatter: function (value, row, column, data, default_formatter) {
		value = default_formatter(value, row, column, data);
		const summarized_view = frappe.query_report.get_filter_value("summarized_view");
		const group_by = frappe.query_report.get_filter_value("group_by");

		if (group_by && column.colIndex === 1) {
			value = "<strong>" + value + "</strong>";
		}

		// Day-status columns now start right after Employee / Employee Name
		// (no Shift column in between): colIndex > 2 when grouped, > 1 when not.
		if (!summarized_view) {
			if ((group_by && column.colIndex > 2) || (!group_by && column.colIndex > 1)) {
				if (value == "HD/P") value = "<span style='color:#914EE3'>" + value + "</span>";
				else if (value == "HD/A")
					value = "<span style='color:orange'>" + value + "</span>";
				else if (value == "P" || value == "WFH")
					value = "<span style='color:green'>" + value + "</span>";
				else if (value == "A") value = "<span style='color:red'>" + value + "</span>";
				else if (value == "L") value = "<span style='color:#318AD8'>" + value + "</span>";
				else value = "<span style='color:#878787'>" + value + "</span>";
			}
		}

		return value;
	},
};