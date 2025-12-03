# import frappe
# from ecommerce_integrations.amazon.doctype.amazon_sp_api_settings.amazon_repository import AmazonRepository

# def sync_fulfillments(amz_setting_name):
#     """Convert shipped Amazon orders into Delivery Notes + Sales Invoices"""

#     settings = frappe.get_doc("Amazon SP API Settings", amz_setting_name)
#     repo = AmazonRepository(settings)

#     # Get all sales orders with FBA shipments (status = Shipped)
#     so_list = frappe.get_all(
#         "Sales Order",
#         filters={
#             "amazon_order_id": ["!=", ""],
#             "docstatus": 1,
#             "status": "To Deliver"
#         },
#         fields=["name", "amazon_order_id"]
#     )

#     count = 0

#     for so in so_list:
#         try:
#             # Fetch order details from Amazon
#             orders_api = repo.get_orders_instance()
#             order = orders_api.get_orders(amazon_order_ids=[so.amazon_order_id])

#             status = order.get("payload", {}).get("Orders", [])[0].get("OrderStatus")

#             if status != "Shipped":
#                 continue

#             # 1️⃣ Create Delivery Note
#             dn = frappe.new_doc("Delivery Note")
#             dn.sales_order = so.name
#             dn.company = settings.company
#             dn.customer = frappe.db.get_value("Sales Order", so.name, "customer")

#             # Fetch SO items
#             so_items = frappe.get_all(
#                 "Sales Order Item",
#                 filters={"parent": so.name},
#                 fields=["item_code", "qty", "rate", "warehouse"]
#             )

#             for item in so_items:
#                 dn.append("items", {
#                     "item_code": item.item_code,
#                     "qty": item.qty,
#                     "rate": item.rate,
#                     "warehouse": item.warehouse
#                 })

#             dn.insert(ignore_permissions=True)
#             dn.submit()

#             # 2️⃣ Create Sales Invoice
#             si = frappe.new_doc("Sales Invoice")
#             si.company = settings.company
#             si.customer = dn.customer
#             si.due_date = frappe.utils.now_date()
#             si.set_posting_time = 1
#             si.posting_date = frappe.utils.now_date()

#             for dn_item in dn.items:
#                 si.append("items", {
#                     "item_code": dn_item.item_code,
#                     "qty": dn_item.qty,
#                     "rate": dn_item.rate,
#                     "warehouse": dn_item.warehouse,
#                     "delivery_note": dn.name
#                 })

#             si.insert(ignore_permissions=True)
#             si.submit()

#             count += 1

#         except Exception as e:
#             frappe.log_error(f"Error syncing fulfillment for {so.name}: {str(e)}")

#     return count
