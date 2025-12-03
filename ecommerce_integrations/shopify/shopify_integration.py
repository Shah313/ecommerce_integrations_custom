# ecommerce_integration/shopify_integration/customer.py

import frappe
from frappe import _


@frappe.whitelist(allow_guest=True)
def customer_sync():
    import json

    data = frappe.local.form_dict
    if not data:
        return {"status": "error", "message": "No data received"}

    # Parse JSON payload
    customer_data = json.loads(frappe.request.data)

    email = customer_data.get("email")
    first_name = customer_data.get("first_name") or ""
    last_name = customer_data.get("last_name") or ""
    phone = customer_data.get("phone")

    # Check if customer already exists
    if frappe.db.exists("Customer", {"email_id": email}):
        return {"status": "exists"}

    # Create new ERPNext Customer
    customer = frappe.get_doc(
        {
            "doctype": "Customer",
            "customer_name": f"{first_name} {last_name}".strip(),
            "customer_group": "All Customer Groups",  # adjust as needed
            "territory": "All Territories",
            "email_id": email,
            "mobile_no": phone,
        }
    )
    customer.insert(ignore_permissions=True)

    # Handle default address if provided by Shopify
    if customer_data.get("default_address"):
        addr = customer_data["default_address"]
        address_doc = frappe.get_doc(
            {
                "doctype": "Address",
                "address_title": f"{first_name} {last_name}".strip(),
                "address_type": "Billing",
                "address_line1": addr.get("address1") or "",
                "address_line2": addr.get("address2") or "",
                "city": addr.get("city") or "",
                "state": addr.get("province") or "",
                "country": addr.get("country") or "",
                "pincode": addr.get("zip") or "",
                "phone": addr.get("phone") or phone,
                "links": [{"link_doctype": "Customer", "link_name": customer.name}],
            }
        )
        address_doc.insert(ignore_permissions=True)

    frappe.db.commit()
    return {"status": "success", "customer": customer.name}
