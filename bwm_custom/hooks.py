app_name = "bwm_custom"
app_title = "Bwm Custom"
app_publisher = "Aruna Devi"
app_description = "BWM Custom"
app_email = "corporate.developer@banaraswala.com"
app_license = "mit"

# Apps
# ------------------

# required_apps = []

# Each item in the list will be shown as an app in the apps page
# add_to_apps_screen = [
# 	{
# 		"name": "bwm_custom",
# 		"logo": "/assets/bwm_custom/logo.png",
# 		"title": "Bwm Custom",
# 		"route": "/bwm_custom",
# 		"has_permission": "bwm_custom.api.permission.has_app_permission"
# 	}
# ]

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/bwm_custom/css/bwm_custom.css"
# app_include_js = "/assets/bwm_custom/js/bwm_custom.js"

# include js, css files in header of web template
# web_include_css = "/assets/bwm_custom/css/bwm_custom.css"
# web_include_js = "/assets/bwm_custom/js/bwm_custom.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "bwm_custom/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
doctype_js = {"Visit Doctype": "bwm_custom/doctype/visit_doctype/visit_doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Svg Icons
# ------------------
# include app icons in desk
# app_include_icons = "bwm_custom/public/icons.svg"

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "bwm_custom.utils.jinja_methods",
# 	"filters": "bwm_custom.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "bwm_custom.install.before_install"
# after_install = "bwm_custom.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "bwm_custom.uninstall.before_uninstall"
# after_uninstall = "bwm_custom.uninstall.after_uninstall"

# Integration Setup
# ------------------
# To set up dependencies/integrations with other apps
# Name of the app being installed is passed as an argument

# before_app_install = "bwm_custom.utils.before_app_install"
# after_app_install = "bwm_custom.utils.after_app_install"

# Integration Cleanup
# -------------------
# To clean up dependencies/integrations with other apps
# Name of the app being uninstalled is passed as an argument

# before_app_uninstall = "bwm_custom.utils.before_app_uninstall"
# after_app_uninstall = "bwm_custom.utils.after_app_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "bwm_custom.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# Permissions
permission_query_conditions = {
    "Leave Application": "bwm_custom.leave_application.get_permission_query_conditions",
    "Permission Form":"bwm_custom.permission_form.get_permission_query_conditions"
}

has_permission = {
    "Leave Application": "bwm_custom.leave_application.has_permission",
    "Permission Form":"bwm_custom.permission_form.has_permission"
}

# DocType Class
# ---------------
# Override standard doctype classes

# override_doctype_class = {
# 	"ToDo": "custom_app.overrides.CustomToDo"
# }

# Document Events
# ---------------
# Hook on document methods and events

doc_events = {
    # "*": {
    #     "on_update": "method",
    #     "on_cancel": "method",
    #     "on_trash": "method"
    # },

    "Job Card": {
        "validate": "bwm_custom.job_card_events.validate_job_card",
        "on_submit": "bwm_custom.job_card_events.on_submit_job_card"
    }
}




# Scheduled Tasks
# ---------------

# scheduler_events = {
# 	"all": [
# 		"bwm_custom.tasks.all"
# 	],
# 	"daily": [
# 		"bwm_custom.tasks.daily"
# 	],
# 	"hourly": [
# 		"bwm_custom.tasks.hourly"
# 	],
# 	"weekly": [
# 		"bwm_custom.tasks.weekly"
# 	],
# 	"monthly": [
# 		"bwm_custom.tasks.monthly"
# 	],
# }

# Testing
# -------

# before_tests = "bwm_custom.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "bwm_custom.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "bwm_custom.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["bwm_custom.utils.before_request"]
# after_request = ["bwm_custom.utils.after_request"]

# Job Events
# ----------
# before_job = ["bwm_custom.utils.before_job"]
# after_job = ["bwm_custom.utils.after_job"]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"bwm_custom.auth.validate"
# ]

# Automatically update python controller files with type annotations for this app.
# export_python_type_annotations = True

# default_log_clearing_doctypes = {
# 	"Logging DocType Name": 30  # days to retain logs
# }

