# import frappe
# from frappe import _
# from ecommerce_integrations.amazon.doctype.amazon_sp_api_settings.amazon_sp_api import Orders


# # -------------------------------------------------------------------
# # BUTTON TRIGGER
# # -------------------------------------------------------------------
# @frappe.whitelist()
# def sync_amazon_orders_now(settings_name: str):
#     frappe.enqueue(
#         "ecommerce_integrations.amazon.api.order_sync.pull_orders",
#         settings_name=settings_name,
#         queue="long",
#     )
#     return _("Order Sync Started in Background")


# # -------------------------------------------------------------------
# # PULL ORDERS
# # -------------------------------------------------------------------
# def pull_orders(settings_name: str):
#     settings = frappe.get_doc("Amazon SP API Settings", settings_name)

#     api = Orders(
#         client_id=settings.client_id,
#         client_secret=settings.get_password("client_secret"),
#         refresh_token=settings.refresh_token,
#         country_code=settings.country,
#     )

#     result = api.get_orders(
#         created_after=settings.custom_last_sync_time or "2025-01-01T00:00:00Z"
#     )
#     orders = result.get("payload", {}).get("Orders", [])

#     created_orders = []

#     for order in orders:
#         so_name = create_sales_order_from_api(order, settings)
#         if so_name:
#             created_orders.append(so_name)

#     settings.custom_last_sync_time = frappe.utils.now()
#     settings.save()
#     frappe.db.commit()
#     return created_orders


# # -------------------------------------------------------------------
# # CREATE SALES ORDER
# # -------------------------------------------------------------------
# def create_sales_order_from_api(order, settings):
#     amazon_order_id = order.get("AmazonOrderId")

#     # Prevent duplicate SO
#     existing = frappe.db.get_value("Sales Order", {"amazon_order_id": amazon_order_id})
#     if existing:
#         return existing

#     # -------------------------------------------------------
#     # CUSTOMER
#     # -------------------------------------------------------
#     buyer_email = (
#         order.get("BuyerInfo", {}).get("BuyerEmail")
#         or f"AmazonBuyer-{amazon_order_id}"
#     )

#     if not frappe.db.exists("Customer", buyer_email):
#         customer = frappe.new_doc("Customer")
#         customer.customer_name = buyer_email
#         customer.customer_type = settings.customer_type
#         customer.customer_group = settings.customer_group
#         customer.territory = settings.territory
#         customer.insert(ignore_permissions=True)
#     else:
#         customer = frappe.get_doc("Customer", buyer_email)

#     # -------------------------------------------------------
#     # SHIPPING ADDRESS
#     # -------------------------------------------------------
#     shipping = order.get("ShippingAddress")

#     if shipping:
#         addr = frappe.new_doc("Address")
#         addr.address_title = buyer_email
#         addr.address_type = "Shipping"
#         addr.address_line1 = shipping.get("AddressLine1", "Not Provided")
#         addr.city = shipping.get("City")
#         addr.state = shipping.get("StateOrRegion")
#         addr.pincode = shipping.get("PostalCode")

#         # Country Mapping
#         COUNTRY_MAP = {
#             "US": "United States",
#             "UK": "United Kingdom",
#             "GB": "United Kingdom",
#             "CA": "Canada",
#             "AU": "Australia",
#             "DE": "Germany",
#             "FR": "France",
#             "IT": "Italy",
#             "ES": "Spain",
#             "IN": "India",
#         }

#         country_code = shipping.get("CountryCode")
#         country_name = COUNTRY_MAP.get(country_code, "United States")

#         # Ensure country exists
#         if not frappe.db.exists("Country", country_name):
#             country_name = "United States"

#         addr.country = country_name

#         addr.append("links", {"link_doctype": "Customer", "link_name": customer.name})

#         try:
#             addr.insert(ignore_permissions=True)
#         except Exception:
#             pass

#     # -------------------------------------------------------
#     # SALES ORDER HEADER
#     # -------------------------------------------------------
#     so = frappe.new_doc("Sales Order")
#     so.amazon_order_id = amazon_order_id
#     so.marketplace_id = order.get("MarketplaceId")
#     so.company = settings.company
#     so.customer = customer.name
#     so.delivery_date = frappe.utils.nowdate()
#     so.transaction_date = frappe.utils.nowdate()

#     # -------------------------------------------------------
#     # ORDER ITEMS
#     # -------------------------------------------------------
#     order_items_api = Orders(
#         client_id=settings.client_id,
#         client_secret=settings.get_password("client_secret"),
#         refresh_token=settings.refresh_token,
#         country_code=settings.country,
#     )

#     items_payload = order_items_api.get_order_items(amazon_order_id)
#     items_list = items_payload.get("payload", {}).get("OrderItems", [])

#     for it in items_list:
#         sku = it.get("SellerSKU")
#         qty = it.get("QuantityOrdered", 1)
#         title = it.get("Title", sku)

#         # TRUNCATE item_name safely (max 140 chars)
#         safe_item_name = title[:140]

#         # CREATE ITEM IF NOT EXISTS
#         if not frappe.db.exists("Item", sku):
#             item = frappe.new_doc("Item")
#             item.item_code = sku
#             item.item_name = safe_item_name
#             item.item_group = settings.parent_item_group
#             item.stock_uom = "Nos"
#             item.insert(ignore_permissions=True)

#         # Add Item to Sales Order
#         so.append(
#             "items",
#             {
#                 "item_code": sku,
#                 "qty": qty,
#                 "rate": it.get("ItemPrice", {}).get("Amount", 0),
#                 "warehouse": settings.warehouse,
#             },
#         )

#     # Save + Submit SO
#     so.insert(ignore_permissions=True)
#     so.submit()
#     return so.name
