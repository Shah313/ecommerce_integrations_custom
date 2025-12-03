import frappe
from ecommerce_integrations.shopify.utils import create_shopify_log
from . import refund, partial_refund


def shopify_refund_dispatcher(payload, request_id=None):
    """
    Shopify refunds/create webhook dispatcher
    Routes between full and partial refunds.
    """
    frappe.set_user("Administrator")
    frappe.flags.request_id = request_id

    try:
        # If refund includes line item quantities < original → partial
        refund_line_items = payload.get("refund_line_items", [])
        partial = False
        for li in refund_line_items:
            if li.get("quantity", 0) > 0:
                partial = True
                break

        if partial:
            partial_refund.handle_partial_refund(payload, request_id=request_id)
        else:
            refund.handle_refund(payload, request_id=request_id)

    except Exception as e:
        create_shopify_log(status="Error", exception=e, rollback=True)
        frappe.logger().error(f"[Shopify] Refund dispatcher failed: {e}")
