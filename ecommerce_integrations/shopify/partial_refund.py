import frappe
from frappe.utils import flt, getdate

from ecommerce_integrations.shopify.constants import ORDER_ID_FIELD
from ecommerce_integrations.shopify.order import get_sales_order
from ecommerce_integrations.shopify.utils import create_shopify_log


def handle_partial_refund(payload, request_id=None):
    """
    Handle Shopify partial refunds.
    Instead of cancelling docs, create Return Sales Invoice / Delivery Note.
    """
    frappe.set_user("Administrator")
    frappe.flags.request_id = request_id

    refund = payload
    try:
        order_id = refund.get("order_id")
        if not order_id:
            create_shopify_log(status="Invalid", message="Refund missing order_id")
            return

        # ---- 1) Extract Reason
        reason_text = (
            refund.get("note")
            or refund.get("reason")
            or refund.get("refund_reason")
            or ""
        )

        # ---- 2) Sales Order
        so = get_sales_order(order_id)
        if not so:
            create_shopify_log(
                status="Invalid", message=f"No Sales Order found for order {order_id}"
            )
            return

        # ---- 3) Get Refund Qty from Shopify line items
        refund_items = {
            str(li.get("line_item_id")): li.get("quantity", 0)
            for li in refund.get("refund_line_items", [])
        }

        # ---- 4) Create Sales Invoice Return(s)
        si_names = frappe.get_all(
            "Sales Invoice",
            filters={"docstatus": 1, ORDER_ID_FIELD: str(order_id)},
            pluck="name",
        )
        for si_name in si_names:
            si_doc = frappe.get_doc("Sales Invoice", si_name)
            si_return = frappe.copy_doc(si_doc)
            si_return.is_return = 1
            si_return.return_against = si_doc.name
            for item in list(si_return.items):
                refunded_qty = (
                    refund_items.get(item.shopify_line_item_id, 0)
                    if hasattr(item, "shopify_line_item_id")
                    else 0
                )
                if refunded_qty:
                    item.qty = -refunded_qty
                    item.amount = -1 * refunded_qty * item.rate
                else:
                    si_return.items.remove(item)
            if si_return.items:
                if reason_text:
                    si_return.custom_reason_for_return_or_refund = reason_text
                si_return.save(ignore_permissions=True)
                si_return.submit()

        # ---- 5) Create Delivery Note Return(s) if restock
        restock_flag = True
        try:
            for li in refund.get("refund_line_items", []):
                if not li.get("restock_type") or li.get("restock_type") == "no_restock":
                    restock_flag = False
        except Exception:
            pass

        if restock_flag:
            dn_names = frappe.get_all(
                "Delivery Note",
                filters={"docstatus": 1, ORDER_ID_FIELD: str(order_id)},
                pluck="name",
            )
            for dn_name in dn_names:
                dn_doc = frappe.get_doc("Delivery Note", dn_name)
                dn_return = frappe.copy_doc(dn_doc)
                dn_return.is_return = 1
                dn_return.return_against = dn_doc.name
                for item in list(dn_return.items):
                    refunded_qty = (
                        refund_items.get(item.shopify_line_item_id, 0)
                        if hasattr(item, "shopify_line_item_id")
                        else 0
                    )
                    if refunded_qty:
                        item.qty = -refunded_qty
                    else:
                        dn_return.items.remove(item)
                if dn_return.items:
                    if reason_text:
                        dn_return.custom_reason_for_return_or_refund = reason_text
                    dn_return.save(ignore_permissions=True)
                    dn_return.submit()

        # ---- 6) Create Refund Payment Entry
        original_pe_name = frappe.db.get_value(
            "Payment Entry Reference",
            {"reference_doctype": "Sales Order", "reference_name": so.name},
            "parent",
        )
        if not original_pe_name:
            create_shopify_log(
                status="Invalid", message=f"No Payment Entry found for order {order_id}"
            )
            return

        original_pe = frappe.get_doc("Payment Entry", original_pe_name)
        refund_amount = flt(refund.get("transactions", [{}])[0].get("amount", 0))
        refund_posting_date = getdate(refund.get("created_at"))

        pe_return = frappe.new_doc("Payment Entry")
        pe_return.payment_type = "Pay"
        pe_return.party_type = original_pe.party_type
        pe_return.party = original_pe.party
        pe_return.paid_to = original_pe.paid_from
        pe_return.paid_from = original_pe.paid_to
        pe_return.paid_amount = refund_amount
        pe_return.received_amount = refund_amount
        pe_return.posting_date = refund_posting_date
        pe_return.reference_no = f"Shopify Partial Refund {refund.get('id')}"
        pe_return.reference_date = refund_posting_date
        if reason_text:
            pe_return.custom_reason_for_return_or_refund = reason_text
        if ORDER_ID_FIELD and ORDER_ID_FIELD in [
            f.fieldname for f in frappe.get_meta("Payment Entry").fields
        ]:
            setattr(pe_return, ORDER_ID_FIELD, str(order_id))
        pe_return.flags.ignore_mandatory = True
        pe_return.save(ignore_permissions=True)
        pe_return.submit()

        # ---- 7) Update SO status
        frappe.db.set_value(
            "Sales Order", so.name, "shopify_order_status", "partially refunded"
        )
        if reason_text:
            frappe.db.set_value(
                "Sales Order",
                so.name,
                "custom_reason_for_return_or_refund",
                reason_text,
            )

        frappe.db.commit()
        create_shopify_log(
            status="Success", message=f"Partial Refund → {pe_return.name}"
        )

    except Exception as e:
        create_shopify_log(status="Error", exception=e, rollback=True)
        frappe.logger().error(f"[Shopify] Partial Refund sync failed: {e}")
