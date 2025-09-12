import requests
import xml.etree.ElementTree as ET
import json

# eSSL API URL (make sure to use the correct operation and parameters)
essl_api_url = "http://www.esslcloud.com/bwm/webapiservice.asmx?op=GetTransactionsLog"
params = {
    'FromDate': '2025-09-01',  # Start date
    'ToDate': '2025-09-10',    # End date
    'SerialNumber': 'HDP1240300476',  # Replace with your device's serial number
    'UserName': 'Harish',  # Username
    'UserPassword': 'Bwmhr@2025#'  # Password
}

# ERPNext API URL
erpnext_url = "https://ctest:8000/api/resource/Employee%20Checkin"
erpnext_headers = {
    'Authorization': 'token <your-api-token>',  # Replace with your actual ERPNext API token
    'Content-Type': 'application/json'
}

# Fetch eSSL attendance data
def get_essl_attendance():
    response = requests.get(essl_api_url, params=params)
    
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        attendance_data = []
        
        # Parse the XML and extract Employee ID and Check-in Time
        # Assuming your XML structure looks like <strDataList>EmployeeID CheckinTime</strDataList>
        for log in root.findall('.//strDataList'):
            # Split Employee ID and Check-in Time
            data = log.text.split()
            employee_id = data[0]
            check_in_time = data[1] + " " + data[2]  # Assuming the time has both date and time
            
            attendance_data.append({
                'employee_id': employee_id,
                'check_in_time': check_in_time
            })
        return attendance_data
    else:
        print(f"Failed to fetch data from eSSL API. Status code: {response.status_code}")
        return []

# Get employee details from ERPNext using employee_number (or employee_id)
def get_employee_by_id(employee_id):
    # ERPNext API to fetch employee by employee_number
    employee_url = f"https://ctest:8000/api/resource/Employee?filters={{'employee_number': '{employee_id}'}}"
    
    response = requests.get(employee_url, headers=erpnext_headers)
    if response.status_code == 200:
        employee = response.json()["data"][0]  # Assuming the employee exists
        return employee["name"]  # Return employee name or id
    else:
        print(f"Employee ID {employee_id} not found in ERPNext.")
        return None

# Create Employee Checkin record in ERPNext
def create_employee_checkin(employee_name, check_in_time):
    checkin_data = {
        "docstatus": 0,
        "doctype": "Employee Checkin",
        "name": f"checkin-{employee_name}-{check_in_time}",
        "__islocal": 1,
        "__unsaved": 1,
        "owner": "Administrator",
        "employee": employee_name,  # Employee Name from ERPNext
        "time": check_in_time,  # Check-in Time from eSSL
        "skip_auto_attendance": 0,
        "offshift": 0
    }

    response = requests.post(erpnext_url, headers=erpnext_headers, data=json.dumps(checkin_data))
    if response.status_code == 200:
        print(f"Employee Checkin successfully created for {employee_name} at {check_in_time}")
    else:
        print(f"Failed to create checkin for {employee_name}. Error: {response.text}")

# Main function to fetch attendance and create check-ins in ERPNext
def main():
    attendance_data = get_essl_attendance()
    for entry in attendance_data:
        employee_name = get_employee_by_id(entry['employee_id'])
        if employee_name:
            create_employee_checkin(employee_name, entry['check_in_time'])

# Run the script
if __name__ == "__main__":
    main()
