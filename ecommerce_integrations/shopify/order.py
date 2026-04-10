import json
from typing import Literal

import frappe
from frappe import _
from frappe.utils import cint, cstr, flt, get_datetime, getdate, nowdate
from shopify.collection import PaginatedIterator
from shopify.resources import Order

from ecommerce_integrations.shopify.connection import temp_shopify_session
from ecommerce_integrations.shopify.constants import (
    CUSTOMER_ID_FIELD,
    EVENT_MAPPER,
    ORDER_ID_FIELD,
    ORDER_ITEM_DISCOUNT_FIELD,
    ORDER_NUMBER_FIELD,
    ORDER_STATUS_FIELD,
    SETTING_DOCTYPE,
)
from ecommerce_integrations.shopify.customer import ShopifyCustomer
from ecommerce_integrations.shopify.product import (
    create_items_if_not_exist,
    get_item_code,
)
from ecommerce_integrations.shopify.utils import create_shopify_log
from ecommerce_integrations.utils.price_list import get_dummy_price_list
from ecommerce_integrations.utils.taxation import get_dummy_tax_category

DEFAULT_TAX_FIELDS = {
    "sales_tax": "default_sales_tax_account",
    "shipping": "default_shipping_charges_account",
}


def sync_sales_order(payload, request_id=None):
    order = payload
    frappe.set_user("Administrator")
    frappe.flags.request_id = request_id

    if frappe.db.get_value("Sales Order", filters={ORDER_ID_FIELD: cstr(order["id"])}):
        create_shopify_log(
            status="Invalid", message="Sales order already exists, not synced"
        )
        return

    try:
        shopify_customer = (
            order.get("customer") if order.get("customer") is not None else {}
        )
        shopify_customer["billing_address"] = order.get("billing_address", "")
        shopify_customer["shipping_address"] = order.get("shipping_address", "")
        customer_id = shopify_customer.get("id")

        if customer_id:
            customer = ShopifyCustomer(customer_id=customer_id)
            if not customer.is_synced():
                customer.sync_customer(customer=shopify_customer)
            else:
                customer.update_existing_addresses(shopify_customer)

        create_items_if_not_exist(order)

        setting = frappe.get_doc(SETTING_DOCTYPE)
        create_order(order, setting)

    except Exception as e:
        create_shopify_log(status="Error", exception=e, rollback=True)
    else:
        create_shopify_log(status="Success")


def create_order(order, setting, company=None):
    from ecommerce_integrations.shopify.fulfillment import create_delivery_note
    from ecommerce_integrations.shopify.invoice import create_sales_invoice

    so = create_sales_order(order, setting, company)
    if not so:
        return

    if order.get("fulfillments"):
        create_delivery_note(order, setting, so)

    create_sales_invoice(order, setting, so)


def create_sales_order(shopify_order, setting, company=None):
    customer = setting.default_customer
    if shopify_order.get("customer", {}):
        if customer_id := shopify_order.get("customer", {}).get("id"):
            customer = frappe.db.get_value(
                "Customer", {CUSTOMER_ID_FIELD: customer_id}, "name"
            )

    so = frappe.db.get_value(
        "Sales Order", {ORDER_ID_FIELD: shopify_order.get("id")}, "name"
    )

    if not so:
        items = get_order_items(
            shopify_order.get("line_items", []),
            setting,
            getdate(shopify_order.get("created_at")),
            taxes_inclusive=shopify_order.get("taxes_included"),
        )

        if not items:
            create_shopify_log(
                status="Error",
                exception=(
                    "No valid line items found in Shopify order "
                    f"{shopify_order.get('name')}. All items may have null "
                    "product_id or product_exists=False."
                ),
                rollback=True,
            )
            return ""

        taxes = get_order_taxes(shopify_order, setting, items)
        so = frappe.get_doc(
            {
                "doctype": "Sales Order",
                "naming_series": setting.sales_order_series or "SO-Shopify-",
                ORDER_ID_FIELD: str(shopify_order.get("id")),
                ORDER_NUMBER_FIELD: shopify_order.get("name"),
                "customer": customer,
                "transaction_date": getdate(shopify_order.get("created_at"))
                or nowdate(),
                "delivery_date": getdate(shopify_order.get("created_at")) or nowdate(),
                "company": setting.company,
                "selling_price_list": get_dummy_price_list(),
                "ignore_pricing_rule": 1,
                "items": items,
                "taxes": taxes,
                "tax_category": get_dummy_tax_category(),
            }
        )

        if company:
            so.update({"company": company, "status": "Draft"})

        so.flags.ignore_mandatory = True
        so.flags.shopify_order_json = json.dumps(shopify_order)
        so.save(ignore_permissions=True)
        so.submit()

        if shopify_order.get("note"):
            so.add_comment(text=f"Order Note: {shopify_order.get('note')}")

    else:
        so = frappe.get_doc("Sales Order", so)

    return so


def get_order_items(order_items, setting, delivery_date, taxes_inclusive):
    """Build ERPNext item rows from Shopify line items.

    Skips items where:
    - product_id is None (bundle add-ons, protection plans, gift cards)
    - product_exists is False (product deleted from Shopify)
    - item_code cannot be resolved in ERPNext

    These are logged but do not block order creation.
    """
    items = []

    for shopify_item in order_items:
        product_id = shopify_item.get("product_id")
        title = shopify_item.get("title", "Unknown item")

        # Skip null product_id items — protection plans, gift cards, bundles
        if not product_id:
            frappe.logger().info(
                f"[Shopify] Order item skipped — null product_id: '{title}'"
            )
            continue

        # Skip deleted/archived products
        if not shopify_item.get("product_exists"):
            frappe.logger().info(
                f"[Shopify] Order item skipped — product_exists=False: '{title}'"
            )
            continue

        item_code = get_item_code(shopify_item)
        if not item_code:
            frappe.logger().warning(
                f"[Shopify] Could not resolve ERPNext item for '{title}' "
                f"(product_id={product_id}). Skipping."
            )
            continue

        qty = cint(shopify_item.get("quantity")) or 1
        items.append(
            {
                "item_code": item_code,
                "item_name": shopify_item.get("name"),
                "rate": _get_item_price(shopify_item, taxes_inclusive),
                "delivery_date": delivery_date,
                "qty": qty,
                "stock_uom": shopify_item.get("uom") or "Nos",
                "warehouse": setting.warehouse,
                ORDER_ITEM_DISCOUNT_FIELD: (
                    _get_total_discount(shopify_item) / qty
                ),
                "custom_shopify_line_item_id": str(shopify_item.get("id")),
            }
        )

    return items


def _get_item_price(line_item, taxes_inclusive: bool) -> float:
    price = flt(line_item.get("price"))
    qty = cint(line_item.get("quantity")) or 1
    total_discount = _get_total_discount(line_item)

    if not taxes_inclusive:
        return price - (total_discount / qty)

    total_taxes = sum(flt(tax.get("price")) for tax in line_item.get("tax_lines", []))
    return price - (total_taxes + total_discount) / qty


def _get_total_discount(line_item) -> float:
    discount_allocations = line_item.get("discount_allocations") or []
    return sum(flt(d.get("amount")) for d in discount_allocations)


def get_order_taxes(shopify_order, setting, items):
    taxes = []
    line_items = shopify_order.get("line_items", [])

    for line_item in line_items:
        # Skip null product_id items — same guard as get_order_items
        if not line_item.get("product_id"):
            continue

        item_code = get_item_code(line_item)
        if not item_code:
            continue

        for tax in line_item.get("tax_lines", []):
            taxes.append(
                {
                    "charge_type": "Actual",
                    "account_head": get_tax_account_head(tax, charge_type="sales_tax"),
                    "description": (
                        get_tax_account_description(tax)
                        or f"{tax.get('title')} - {tax.get('rate') * 100.0:.2f}%"
                    ),
                    "tax_amount": tax.get("price"),
                    "included_in_print_rate": 0,
                    "cost_center": setting.cost_center,
                    "item_wise_tax_detail": {
                        item_code: [
                            flt(tax.get("rate")) * 100,
                            flt(tax.get("price")),
                        ]
                    },
                    "dont_recompute_tax": 1,
                }
            )

    update_taxes_with_shipping_lines(
        taxes,
        shopify_order.get("shipping_lines", []),
        setting,
        items,
        taxes_inclusive=shopify_order.get("taxes_included"),
    )

    if cint(setting.consolidate_taxes):
        taxes = consolidate_order_taxes(taxes)

    for row in taxes:
        tax_detail = row.get("item_wise_tax_detail")
        if isinstance(tax_detail, dict):
            row["item_wise_tax_detail"] = json.dumps(tax_detail)

    return taxes


def consolidate_order_taxes(taxes):
    tax_account_wise_data = {}
    for tax in taxes:
        account_head = tax["account_head"]
        tax_account_wise_data.setdefault(
            account_head,
            {
                "charge_type": "Actual",
                "account_head": account_head,
                "description": tax.get("description"),
                "cost_center": tax.get("cost_center"),
                "included_in_print_rate": 0,
                "dont_recompute_tax": 1,
                "tax_amount": 0,
                "item_wise_tax_detail": {},
            },
        )
        tax_account_wise_data[account_head]["tax_amount"] += flt(tax.get("tax_amount"))
        if tax.get("item_wise_tax_detail"):
            tax_account_wise_data[account_head]["item_wise_tax_detail"].update(
                tax["item_wise_tax_detail"]
            )

    return tax_account_wise_data.values()


def get_tax_account_head(
    tax, charge_type: Literal["shipping", "sales_tax"] | None = None
):
    tax_title = str(tax.get("title", ""))

    tax_account = frappe.db.get_value(
        "Shopify Tax Account",
        {"parent": SETTING_DOCTYPE, "shopify_tax": tax_title},
        "tax_account",
    )

    if not tax_account and charge_type:
        tax_account = frappe.db.get_single_value(
            SETTING_DOCTYPE, DEFAULT_TAX_FIELDS[charge_type]
        )

    if not tax_account:
        frappe.throw(
            _("Tax Account not specified for Shopify Tax {0}").format(tax_title)
        )

    return tax_account


def get_tax_account_description(tax):
    return frappe.db.get_value(
        "Shopify Tax Account",
        {"parent": SETTING_DOCTYPE, "shopify_tax": tax.get("title")},
        "tax_description",
    )


def update_taxes_with_shipping_lines(
    taxes, shipping_lines, setting, items, taxes_inclusive=False
):
    shipping_as_item = cint(setting.add_shipping_as_item) and setting.shipping_item

    for shipping_charge in shipping_lines:
        if shipping_charge.get("price"):
            shipping_discounts = shipping_charge.get("discount_allocations") or []
            total_discount = sum(flt(d.get("amount")) for d in shipping_discounts)

            shipping_taxes = shipping_charge.get("tax_lines") or []
            total_tax = sum(flt(t.get("price")) for t in shipping_taxes)

            shipping_charge_amount = flt(shipping_charge["price"]) - flt(total_discount)
            if bool(taxes_inclusive):
                shipping_charge_amount -= total_tax

            if shipping_as_item:
                items.append(
                    {
                        "item_code": setting.shipping_item,
                        "rate": shipping_charge_amount,
                        "delivery_date": (
                            items[-1]["delivery_date"] if items else nowdate()
                        ),
                        "qty": 1,
                        "stock_uom": "Nos",
                        "warehouse": setting.warehouse,
                    }
                )
            else:
                taxes.append(
                    {
                        "charge_type": "Actual",
                        "account_head": get_tax_account_head(
                            shipping_charge, charge_type="shipping"
                        ),
                        "description": get_tax_account_description(shipping_charge)
                        or shipping_charge.get("title", "Shipping"),
                        "tax_amount": shipping_charge_amount,
                        "cost_center": setting.cost_center,
                    }
                )

        for tax in shipping_charge.get("tax_lines", []):
            taxes.append(
                {
                    "charge_type": "Actual",
                    "account_head": get_tax_account_head(tax, charge_type="sales_tax"),
                    "description": (
                        get_tax_account_description(tax)
                        or f"{tax.get('title')} - {tax.get('rate') * 100.0:.2f}%"
                    ),
                    "tax_amount": tax["price"],
                    "cost_center": setting.cost_center,
                    "item_wise_tax_detail": (
                        {
                            setting.shipping_item: [
                                flt(tax.get("rate")) * 100,
                                flt(tax.get("price")),
                            ]
                        }
                        if shipping_as_item
                        else {}
                    ),
                    "dont_recompute_tax": 1,
                }
            )


def get_sales_order(order_id):
    """Get ERPNext sales order using shopify order id."""
    sales_order = frappe.db.get_value(
        "Sales Order", filters={ORDER_ID_FIELD: cstr(order_id)}
    )
    if sales_order:
        return frappe.get_doc("Sales Order", sales_order)


def cancel_order(payload, request_id=None):
    frappe.set_user("Administrator")
    frappe.flags.request_id = request_id
    order = payload

    try:
        order_id = order["id"]
        order_status = order["financial_status"]
        sales_order = get_sales_order(order_id)

        if not sales_order:
            create_shopify_log(status="Invalid", message="Sales Order does not exist")
            return

        sales_invoice = frappe.db.get_value(
            "Sales Invoice", filters={ORDER_ID_FIELD: cstr(order_id)}
        )
        delivery_notes = frappe.db.get_list(
            "Delivery Note", filters={ORDER_ID_FIELD: cstr(order_id)}
        )

        if sales_invoice:
            frappe.db.set_value(
                "Sales Invoice", sales_invoice, ORDER_STATUS_FIELD, order_status
            )
        for dn in delivery_notes:
            frappe.db.set_value(
                "Delivery Note", dn.name, ORDER_STATUS_FIELD, order_status
            )

        if not sales_invoice and not delivery_notes and sales_order.docstatus == 1:
            sales_order.cancel()
        else:
            frappe.db.set_value(
                "Sales Order", sales_order.name, ORDER_STATUS_FIELD, order_status
            )

    except Exception as e:
        create_shopify_log(status="Error", exception=e)
    else:
        create_shopify_log(status="Success")


@temp_shopify_session
def sync_old_orders():
    shopify_setting = frappe.get_cached_doc(SETTING_DOCTYPE)
    if not cint(shopify_setting.sync_old_orders):
        return

    orders = _fetch_old_orders(
        shopify_setting.old_orders_from, shopify_setting.old_orders_to
    )

    for order in orders:
        if frappe.db.get_value(
            "Sales Order", {ORDER_ID_FIELD: cstr(order.get("id"))}
        ):
            continue

        log = create_shopify_log(
            method=EVENT_MAPPER["orders/create"],
            request_data=json.dumps(order),
            make_new=True,
        )
        sync_sales_order(order, request_id=log.name)

    shopify_setting = frappe.get_doc(SETTING_DOCTYPE)
    shopify_setting.sync_old_orders = 0
    shopify_setting.save()


def _fetch_old_orders(from_time, to_time):
    from_time = get_datetime(from_time).astimezone().isoformat()
    to_time = get_datetime(to_time).astimezone().isoformat()

    orders_iterator = PaginatedIterator(
        Order.find(created_at_min=from_time, created_at_max=to_time, limit=250)
    )

    for orders in orders_iterator:
        for order in orders:
            yield order.to_dict()