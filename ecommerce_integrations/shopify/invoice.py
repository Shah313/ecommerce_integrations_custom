
import time

import frappe
from erpnext.selling.doctype.sales_order.sales_order import make_sales_invoice
from frappe.utils import cint, cstr, getdate, nowdate

from ecommerce_integrations.shopify.constants import (
    ORDER_ID_FIELD,
    ORDER_NUMBER_FIELD,
    SETTING_DOCTYPE,
)
from ecommerce_integrations.shopify.utils import create_shopify_log

_SO_WAIT_RETRIES = 5
_SO_WAIT_SECONDS = 3


def prepare_sales_invoice(payload, request_id=None):
    from ecommerce_integrations.shopify.order import get_sales_order
    order = payload
    frappe.set_user("Administrator")
    setting = frappe.get_doc(SETTING_DOCTYPE)
    frappe.flags.request_id = request_id
    try:
        sales_order = None
        for attempt in range(1, _SO_WAIT_RETRIES + 1):
            sales_order = get_sales_order(cstr(order["id"]))
            if sales_order:
                break
            if attempt < _SO_WAIT_RETRIES:
                frappe.logger().info(f"[Shopify Invoice] SO not found for order {order['id']}, attempt {attempt}/{_SO_WAIT_RETRIES}. Waiting {_SO_WAIT_SECONDS}s...")
                time.sleep(_SO_WAIT_SECONDS)
        if sales_order:
            create_sales_invoice(order, setting, sales_order)
            create_shopify_log(status="Success")
        else:
            create_shopify_log(status="Invalid", message=f"Sales Order not found for Shopify order {order.get('id')} after {_SO_WAIT_RETRIES} retries.")
    except Exception as e:
        create_shopify_log(status="Error", exception=e, rollback=True)


def create_sales_invoice(shopify_order, setting, so):
    if not cint(setting.sync_sales_invoice):
        return
    if frappe.db.get_value("Sales Invoice", {ORDER_ID_FIELD: cstr(shopify_order.get("id"))}, "name"):
        return
    if so.docstatus != 1 or so.per_billed:
        return
    posting_date = getdate(shopify_order.get("created_at")) or nowdate()
    sales_invoice = make_sales_invoice(so.name, ignore_permissions=True)
    sales_invoice.set(ORDER_ID_FIELD, cstr(shopify_order.get("id")))
    sales_invoice.set(ORDER_NUMBER_FIELD, shopify_order.get("name"))
    sales_invoice.set_posting_time = 1
    sales_invoice.posting_date = posting_date
    sales_invoice.due_date = posting_date
    sales_invoice.naming_series = setting.sales_invoice_series or "SI-Shopify-"
    sales_invoice.flags.ignore_mandatory = True
    set_cost_center(sales_invoice.items, setting.cost_center)
    sales_invoice.insert(ignore_permissions=True)
    sales_invoice.submit()
    frappe.logger().info(f"[Shopify] Sales Invoice {sales_invoice.name} created for order {shopify_order.get('id')}")
    if sales_invoice.grand_total > 0:
        make_payment_entry_against_sales_invoice(sales_invoice, setting, posting_date)
    if shopify_order.get("note"):
        sales_invoice.add_comment(text=f"Order Note: {shopify_order.get('note')}")


def set_cost_center(items, cost_center):
    for item in items:
        item.cost_center = cost_center


def make_payment_entry_against_sales_invoice(doc, setting, posting_date=None):
    from erpnext.accounts.doctype.payment_entry.payment_entry import get_payment_entry
    existing = frappe.db.get_value("Payment Entry", {"reference_no": doc.name, "docstatus": ["!=", 2]}, "name")
    if existing:
        return
    payment_entry = get_payment_entry(doc.doctype, doc.name, bank_account=setting.cash_bank_account)
    payment_entry.flags.ignore_mandatory = True
    payment_entry.reference_no = doc.name
    payment_entry.posting_date = posting_date or nowdate()
    payment_entry.reference_date = posting_date or nowdate()
    payment_entry.insert(ignore_permissions=True)
    payment_entry.submit()
