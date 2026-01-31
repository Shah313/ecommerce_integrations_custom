from copy import deepcopy

import frappe
from erpnext.selling.doctype.sales_order.sales_order import make_delivery_note
from frappe.utils import cint, cstr, getdate

from ecommerce_integrations.shopify.constants import (
    FULLFILLMENT_ID_FIELD,
    ORDER_ID_FIELD,
    ORDER_NUMBER_FIELD,
    SETTING_DOCTYPE,
)
from ecommerce_integrations.shopify.order import get_sales_order
from ecommerce_integrations.shopify.utils import create_shopify_log


def prepare_delivery_note(payload, request_id=None):
    frappe.set_user("Administrator")
    setting = frappe.get_doc(SETTING_DOCTYPE)
    frappe.flags.request_id = request_id

    order = payload

    try:
        sales_order = get_sales_order(cstr(order["id"]))
        if sales_order:
            create_delivery_note(order, setting, sales_order)
            create_shopify_log(status="Success")
        else:
            create_shopify_log(
                status="Invalid",
                message="Sales Order not found for syncing delivery note.",
            )
    except Exception as e:
        create_shopify_log(status="Error", exception=e, rollback=True)


def _get_mapped_warehouse_for_location(setting, location_id):
    """Return ERPNext warehouse for Shopify location_id (fallback to default setting.warehouse)."""
    try:
        wh_map = setting.get_integration_to_erpnext_wh_mapping()
        mapped = wh_map.get(str(location_id))
        return mapped or setting.warehouse
    except Exception:
        return setting.warehouse


def _get_company_for_warehouse(warehouse):
    """Return company linked on Warehouse."""
    if not warehouse:
        return None
    try:
        return frappe.db.get_value("Warehouse", warehouse, "company")
    except Exception:
        return None


def _dn_company_debug_log(title, data: dict):
    """Log to Error Log + console logger for easy debugging."""
    try:
        frappe.log_error(message=frappe.as_json(data), title=title)
    except Exception:
        pass
def create_delivery_note(shopify_order, setting, so):
    if not cint(setting.sync_delivery_note):
        return

    USA_WAREHOUSES = {"DEL1 - USA - H"}
    DRAFT_ONLY_WAREHOUSES = {
        "PIFCO - HQ - H",
        "Reykjavikurvegur 64 - HQ - H",
    }

    for fulfillment in shopify_order.get("fulfillments"):
        fulfillment_id = fulfillment.get("id")
        location_id = fulfillment.get("location_id")

        if (
            not frappe.db.get_value(
                "Delivery Note", {FULLFILLMENT_ID_FIELD: fulfillment_id}, "name"
            )
            and so.docstatus == 1
        ):
            dn = make_delivery_note(so.name)

            setattr(dn, ORDER_ID_FIELD, fulfillment.get("order_id"))
            setattr(dn, ORDER_NUMBER_FIELD, shopify_order.get("name"))
            setattr(dn, FULLFILLMENT_ID_FIELD, fulfillment_id)

            dn.set_posting_time = 1
            dn.posting_date = getdate(fulfillment.get("created_at"))
            dn.naming_series = setting.delivery_note_series or "DN-Shopify-"

            # Map warehouse
            mapped_wh = _get_mapped_warehouse_for_location(setting, location_id)
            mapped_wh_company = _get_company_for_warehouse(mapped_wh)

            # Update items with warehouse
            dn.items = get_fulfillment_items(
                dn.items, fulfillment.get("line_items"), location_id
            )

            # Set company strictly from warehouse
            if mapped_wh_company:
                dn.company = mapped_wh_company

            # VERY IMPORTANT: clear taxes (avoid cross-company issues)
            dn.taxes = []
            dn.taxes_and_charges = None

            dn.flags.ignore_mandatory = True
            dn.save()

            # Determine final warehouse used
            item_warehouses = {x.warehouse for x in dn.items if x.warehouse}
            final_warehouse = list(item_warehouses)[0] if item_warehouses else None

            _dn_company_debug_log(
                "Shopify DN Warehouse Decision",
                {
                    "dn": dn.name,
                    "final_warehouse": final_warehouse,
                    "company": dn.company,
                    "submit_allowed": final_warehouse in USA_WAREHOUSES,
                },
            )

            # ✅ SUBMIT ONLY FOR USA
            if final_warehouse in USA_WAREHOUSES:
                dn.submit()
            else:
                # ❌ Keep draft for UK / Iceland
                frappe.logger("shopify_dn").info(
                    f"DN {dn.name} kept in DRAFT for warehouse {final_warehouse}"
                )

            if shopify_order.get("note"):
                dn.add_comment(text=f"Order Note: {shopify_order.get('note')}")

            # -------------------------------
            # Create Sales Invoice (unchanged)
            # -------------------------------
            try:
                from ecommerce_integrations.shopify.invoice import create_sales_invoice

                create_sales_invoice(shopify_order, setting, so)
            except Exception as e:
                frappe.logger().error(
                    f"[Shopify] Failed to create Sales Invoice: {e}"
                )


def get_fulfillment_items(dn_items, fulfillment_items, location_id=None):
    # local import to avoid circular imports
    from ecommerce_integrations.shopify.product import get_item_code

    fulfillment_items = deepcopy(fulfillment_items)

    setting = frappe.get_cached_doc(SETTING_DOCTYPE)
    wh_map = setting.get_integration_to_erpnext_wh_mapping()
    warehouse = wh_map.get(str(location_id)) or setting.warehouse

    final_items = []

    def find_matching_fullfilement_item(dn_item):
        nonlocal fulfillment_items

        for item in fulfillment_items:
            if get_item_code(item) == dn_item.item_code:
                fulfillment_items.remove(item)
                return item

    for dn_item in dn_items:
        if shopify_item := find_matching_fullfilement_item(dn_item):
            dn_item.update(
                {"qty": shopify_item.get("quantity"), "warehouse": warehouse}
            )
            final_items.append(dn_item)

    return final_items
