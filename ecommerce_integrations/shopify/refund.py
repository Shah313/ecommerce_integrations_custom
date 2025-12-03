import frappe
from frappe.utils import flt, getdate

from ecommerce_integrations.shopify.constants import ORDER_ID_FIELD
from ecommerce_integrations.shopify.order import get_sales_order
from ecommerce_integrations.shopify.utils import create_shopify_log

from erpnext.controllers.sales_and_purchase_return import make_return_doc

ROUND_TOL = 0.01  # rounding tolerance in PKR


def _safe_cancel(doctype: str, name: str) -> bool:
    try:
        doc = frappe.get_doc(doctype, name)
        if doc.docstatus == 1:
            doc.cancel()
            frappe.logger().info(f"[Shopify Refund] Cancelled {doctype} {name}")
        return True
    except Exception as e:
        frappe.logger().warning(
            f"[Shopify Refund] Could not cancel {doctype} {name}: {e}"
        )
        return False


def _cumulative_refunded_amount(order_id: str, so_name: str) -> float:
    """Sum ALL prior refund Payment Entries (payment_type='Pay') for this order."""
    order_id_str = str(order_id)

    pe_names = frappe.db.sql(
        f"""
        (SELECT pe.name
           FROM `tabPayment Entry` pe
           JOIN `tabPayment Entry Reference` ref
             ON ref.parent = pe.name
          WHERE pe.docstatus = 1
            AND pe.payment_type = 'Pay'
            AND ref.reference_doctype = 'Sales Order'
            AND ref.reference_name = %(so)s)
        UNION
        (SELECT name
           FROM `tabPayment Entry`
          WHERE docstatus = 1
            AND payment_type = 'Pay'
            {"AND " + ORDER_ID_FIELD + " = %(oid)s" if ORDER_ID_FIELD else "AND 1=0"})
        """,
        {"so": so_name, "oid": order_id_str},
        as_dict=True,
    )

    if not pe_names:
        return 0.0

    names = [r["name"] for r in pe_names]
    total = (
        frappe.db.sql(
            """SELECT COALESCE(SUM(paid_amount), 0)
             FROM `tabPayment Entry`
            WHERE name IN %(names)s""",
            {"names": names},
        )[0][0]
        or 0.0
    )

    return flt(total)


def handle_refund(payload, request_id=None):
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

        # ---- 3) Original Payment Entry (Receive)
        pe_name = frappe.db.get_value(
            "Payment Entry Reference",
            {"reference_doctype": "Sales Order", "reference_name": so.name},
            "parent",
        )
        if not pe_name:
            create_shopify_log(
                status="Invalid", message=f"No Payment Entry found for order {order_id}"
            )
            return

        original_pe = frappe.get_doc("Payment Entry", pe_name)
        refund_amount = flt(
            refund.get("transactions", [{}])[0].get("amount", original_pe.paid_amount)
        )
        refund_posting_date = getdate(refund.get("created_at"))

        # ---- 4) Sales Invoice Returns (after delivery)
        si_names = frappe.get_all(
            "Sales Invoice",
            filters={"docstatus": 1, ORDER_ID_FIELD: str(order_id)},
            pluck="name",
        )
        for name in si_names:
            try:
                si_return = make_return_doc("Sales Invoice", name)
                si_return.posting_date = refund_posting_date

                refund_line_items = refund.get("refund_line_items", [])
                refund_map = {
                    str(li.get("line_item_id")): flt(li.get("quantity", 0))
                    for li in refund_line_items
                }

                for item in si_return.items:
                    line_id = str(
                        getattr(item, "custom_shopify_line_item_id", "") or ""
                    )
                    if line_id in refund_map:
                        refund_qty = refund_map[line_id]
                        item.qty = -refund_qty
                        item.amount = item.rate * item.qty

                if reason_text:
                    si_return.custom_reason_for_return_or_refund = reason_text

                si_return.save(ignore_permissions=True)
                si_return.submit()
                frappe.logger().info(
                    f"[Shopify Refund] Created Sales Invoice Return {si_return.name}"
                )

            except Exception as si_err:
                frappe.logger().error(
                    f"[Shopify Refund] Could not create SI Return for {name}: {si_err}"
                )

        # ---- 4b) Delivery Note Returns (respect restock flag)
        restock_flag = True
        try:
            line_items = refund.get("refund_line_items", [])
            for li in line_items:
                if not li.get("restock_type") or li.get("restock_type") == "no_restock":
                    restock_flag = False
        except Exception:
            pass

        dn_names = frappe.get_all(
            "Delivery Note",
            filters={"docstatus": 1, ORDER_ID_FIELD: str(order_id)},
            pluck="name",
        )
        if restock_flag:
            for name in dn_names:
                try:
                    dn_return = make_return_doc("Delivery Note", name)
                    dn_return.posting_date = refund_posting_date

                    refund_line_items = refund.get("refund_line_items", [])
                    refund_map = {
                        str(li.get("line_item_id")): flt(li.get("quantity", 0))
                        for li in refund_line_items
                    }

                    for item in dn_return.items:
                        line_id = str(
                            getattr(item, "custom_shopify_line_item_id", "") or ""
                        )
                        if line_id in refund_map:
                            refund_qty = refund_map[line_id]
                            item.qty = -refund_qty
                            item.amount = item.rate * item.qty

                    if reason_text:
                        dn_return.custom_reason_for_return_or_refund = reason_text

                    dn_return.save(ignore_permissions=True)
                    dn_return.submit()
                    frappe.logger().info(
                        f"[Shopify Refund] Created Delivery Note Return {dn_return.name}"
                    )

                except Exception as dn_err:
                    frappe.logger().error(
                        f"[Shopify Refund] Could not create DN Return for {name}: {dn_err}"
                    )
        else:
            frappe.logger().info(
                f"[Shopify Refund] Restock unchecked → skipping DN returns for order {order_id}"
            )

        # ---- 5) Handle Sales Order (full vs partial refund)
        if so.docstatus == 1:
            if abs(refund_amount - so.grand_total) <= ROUND_TOL:
                # 🔴 Full refund → cancel SO
                _safe_cancel("Sales Order", so.name)
                frappe.db.set_value(
                    "Sales Order", so.name, "shopify_order_status", "refunded"
                )
                target_so = so.name
            else:
                # 🟡 Partial refund → Amend SO and reduce qty
                so_amended = frappe.copy_doc(so)
                so_amended.amended_from = so.name

                refund_line_items = refund.get("refund_line_items", [])
                refund_map = {
                    str(li.get("line_item_id")): flt(li.get("quantity", 0))
                    for li in refund_line_items
                }

                for soi in so_amended.items:
                    line_id = str(getattr(soi, "custom_shopify_line_item_id", "") or "")
                    if line_id in refund_map:
                        refund_qty = refund_map[line_id]
                        soi.qty = max(0, soi.qty - refund_qty)
                        soi.amount = soi.rate * soi.qty

                so_amended.save(ignore_permissions=True)
                so_amended.submit()
                frappe.db.set_value(
                    "Sales Order",
                    so_amended.name,
                    "shopify_order_status",
                    "partially_refunded",
                )
                target_so = so_amended.name
        else:
            target_so = so.name

        # ---- Store reason on Sales Order
        if reason_text:
            frappe.db.set_value(
                "Sales Order",
                target_so,
                "custom_reason_for_return_or_refund",
                reason_text,
            )

        # ---- 6) Create refund Payment Entry
        pe_return = frappe.new_doc("Payment Entry")
        pe_return.payment_type = "Pay"
        pe_return.party_type = original_pe.party_type
        pe_return.party = original_pe.party
        pe_return.paid_to = original_pe.paid_from
        pe_return.paid_from = original_pe.paid_to
        pe_return.paid_amount = refund_amount
        pe_return.received_amount = refund_amount
        pe_return.posting_date = refund_posting_date
        pe_return.reference_no = f"Shopify Refund {refund.get('id')}"
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

        # ---- 7) Link refund PE to SO
        try:
            frappe.get_doc(
                {
                    "doctype": "Payment Entry Reference",
                    "parent": pe_return.name,
                    "parenttype": "Payment Entry",
                    "parentfield": "references",
                    "reference_doctype": "Sales Order",
                    "reference_name": so.name,
                    "allocated_amount": 0,
                }
            ).insert(ignore_permissions=True)
        except Exception as link_err:
            frappe.logger().warning(
                f"[Shopify Refund] Could not link refund PE to SO: {link_err}"
            )

        frappe.db.commit()
        create_shopify_log(status="Success", message=f"Refund → {pe_return.name}")
        frappe.logger().info(
            f"[Shopify Refund] Refund PE {pe_return.name}; reason={reason_text}; restock={restock_flag}"
        )

    except Exception as e:
        create_shopify_log(status="Error", exception=e, rollback=True)
        frappe.logger().error(f"[Shopify] Refund sync failed: {e}")
