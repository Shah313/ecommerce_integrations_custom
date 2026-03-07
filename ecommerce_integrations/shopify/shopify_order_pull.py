# # ecommerce_integrations/shopify/shopify_order_pull.py

# import frappe
# from frappe import _
# from ecommerce_integrations.shopify.connection import temp_shopify_session
# from ecommerce_integrations.shopify.constants import ORDER_ID_FIELD, SETTING_DOCTYPE
# from ecommerce_integrations.shopify.order import sync_sales_order
# from ecommerce_integrations.shopify.utils import create_shopify_log
# from shopify.resources import Order
# from shopify.collection import PaginatedIterator

# @frappe.whitelist()
# def get_order_counts_by_status():
#     """Get count of orders in Shopify by financial_status"""
    
#     @temp_shopify_session
#     def _count():
#         status_counts = {}
        
#         # Count orders by financial status
#         for status in ["paid", "pending", "refunded", "voided", "partially_refunded"]:
#             try:
#                 count = Order.count(
#                     status="any",
#                     financial_status=status
#                 )
#                 status_counts[status] = count
#             except:
#                 status_counts[status] = "N/A"
        
#         # Count by fulfillment status
#         fulfillment_counts = {}
#         for status in ["fulfilled", "partial", "unfulfilled"]:
#             try:
#                 count = Order.count(
#                     status="any",
#                     fulfillment_status=status
#                 )
#                 fulfillment_counts[status] = count
#             except:
#                 fulfillment_counts[status] = "N/A"
        
#         return {
#             "financial_status": status_counts,
#             "fulfillment_status": fulfillment_counts
#         }
    
#     return _count()


# @frappe.whitelist()
# def bulk_pull_orders(filters):
#     """
#     Bulk pull orders with multiple filters.
    
#     filters = {
#         "financial_status": "paid",
#         "fulfillment_status": "unfulfilled",
#         "created_at_min": "2026-01-01",
#         "created_at_max": "2026-02-01",
#         "limit": 250
#     }
#     """
    
#     @temp_shopify_session
#     def _pull():
#         params = {
#             "status": "any",
#             "limit": filters.get("limit", 250),
#             "order": "created_at asc"
#         }
        
#         # Add optional filters
#         if filters.get("financial_status"):
#             params["financial_status"] = filters["financial_status"]
        
#         if filters.get("fulfillment_status"):
#             params["fulfillment_status"] = filters["fulfillment_status"]
        
#         if filters.get("created_at_min"):
#             params["created_at_min"] = filters["created_at_min"]
        
#         if filters.get("created_at_max"):
#             params["created_at_max"] = filters["created_at_max"]
        
#         pulled = []
#         skipped = []
#         failed = []
        
#         for page in PaginatedIterator(Order.find(**params)):
#             for order in page:
#                 order_dict = order.to_dict()
                
#                 # Check if already exists
#                 exists = frappe.db.exists("Sales Order", {
#                     ORDER_ID_FIELD: str(order_dict["id"])
#                 })
                
#                 if exists:
#                     skipped.append(order_dict["order_number"])
#                     continue
                
#                 try:
#                     log = create_shopify_log(
#                         method="bulk_pull_orders",
#                         request_data=order_dict,
#                         make_new=True,
#                     )
                    
#                     sync_sales_order(order_dict, request_id=log.name)
#                     pulled.append(order_dict["order_number"])
                    
#                 except Exception as e:
#                     failed.append({
#                         "order": order_dict["order_number"],
#                         "error": str(e)
#                     })
        
#         return {
#             "pulled": pulled,
#             "skipped": skipped,
#             "failed": failed,
#             "total_pulled": len(pulled),
#             "total_skipped": len(skipped),
#             "total_failed": len(failed)
#         }
    
#     return _pull()