# Copyright (c) 2022, Frappe and contributors
# For license information, please see license.txt

import time
import dateutil
import frappe
from frappe import _

from ecommerce_integrations.amazon.doctype.amazon_sp_api_settings.amazon_sp_api import (
    SPAPI,
    CatalogItems,
    Finances,
    Orders,
    SPAPIError,
)
from ecommerce_integrations.amazon.doctype.amazon_sp_api_settings.amazon_sp_api_settings import (
    AmazonSPAPISettings,
)


class AmazonRepository:
    def __init__(self, amz_setting):
        if isinstance(amz_setting, str):
            amz_setting = frappe.get_doc("Amazon SP API Settings", amz_setting)

        self.amz_setting: AmazonSPAPISettings = amz_setting
        self.instance_params = dict(
            iam_arn=amz_setting.iam_arn,
            client_id=amz_setting.client_id,
            client_secret=amz_setting.get_password("client_secret"),
            refresh_token=amz_setting.refresh_token,
            aws_access_key=amz_setting.aws_access_key,
            aws_secret_key=amz_setting.get_password("aws_secret_key"),
            country_code=amz_setting.country,
        )

    # -------------------------------------------------------------------------
    # CORE SP-API CALL WRAPPER
    # -------------------------------------------------------------------------

    def call_sp_api_method(self, sp_api_method, **kwargs):
        """Generic wrapper with retry + disable sync on repeated failure."""
        errors = {}
        max_retries = self.amz_setting.max_retry_limit or 1

        for _ in range(max_retries):
            try:
                result = sp_api_method(**kwargs)
                return result.get("payload")
            except SPAPIError as e:
                errors[e.error] = e.error_description
                time.sleep(1)

        # Disable sync on repeated failures
        self.amz_setting.enable_sync = 0
        self.amz_setting.save()

        msg = "<br>".join([f"{k}: {v}" for k, v in errors.items()])
        frappe.throw(
            _("Amazon Sync disabled: Maximum retries exceeded.<br>{0}").format(msg)
        )

    # -------------------------------------------------------------------------
    # SP-API INSTANCES
    # -------------------------------------------------------------------------

    def get_finances_instance(self) -> Finances:
        return Finances(**self.instance_params)

    def get_orders_instance(self) -> Orders:
        return Orders(**self.instance_params)

    def get_catalog_items_instance(self) -> CatalogItems:
        return CatalogItems(**self.instance_params)

    # -------------------------------------------------------------------------
    # ACCOUNT / COUNTRY HELPERS
    # -------------------------------------------------------------------------

    def get_account(self, name: str) -> str:
        """Return (or auto-create) an 'Amazon XYZ' account under configured group."""
        acct = frappe.db.get_value("Account", {"account_name": f"Amazon {name}"})
        if acct:
            return acct

        new = frappe.new_doc("Account")
        new.account_name = f"Amazon {name}"
        new.company = self.amz_setting.company
        new.parent_account = self.amz_setting.market_place_account_group
        new.insert(ignore_permissions=True)
        return new.name

    def map_country_code(self, code: str) -> str:
        """Convert Amazon country codes to ERPNext Country names."""
        if not code:
            return ""

        mapping = {
            "US": "United States",
            "CA": "Canada",
            "MX": "Mexico",
            "GB": "United Kingdom",
            "UK": "United Kingdom",
            "DE": "Germany",
            "FR": "France",
            "IT": "Italy",
            "ES": "Spain",
            "IN": "India",
            "JP": "Japan",
            "AU": "Australia",
            "AE": "United Arab Emirates",
            "SA": "Saudi Arabia",
        }
        return mapping.get(code, code)

    # -------------------------------------------------------------------------
    # FINANCIALS (Charges + Fees for TAXES table)
    # -------------------------------------------------------------------------

    def get_charges_and_fees(self, order_id: str) -> dict:
        """
        Use Finances API to fetch charges + fees.
        Returned format:
            {
                "charges": [ {charge_type, account_head, tax_amount, description}, ... ],
                "fees":    [ {charge_type, account_head, tax_amount, description}, ... ]
            }
        """
        finances = self.get_finances_instance()
        payload = self.call_sp_api_method(
            finances.list_financial_events_by_order_id,
            order_id=order_id,
        )

        if not payload:
            frappe.log_error(f"No financial events payload for order {order_id}", "Amazon SP API")
            return {"charges": [], "fees": []}

        results = {"charges": [], "fees": []}

        while True:
            events = payload.get("FinancialEvents", {}).get("ShipmentEventList", [])
            next_token = payload.get("NextToken")

            for evt in events:
                for item in evt.get("ShipmentItemList", []):
                    seller_sku = item.get("SellerSKU")

                    # Charges (excluding Principal because it is item price)
                    for charge in item.get("ItemChargeList", []):
                        amount = float(
                            charge.get("ChargeAmount", {}).get("CurrencyAmount", 0) or 0
                        )
                        charge_type = charge.get("ChargeType")

                        if amount != 0 and charge_type != "Principal":
                            results["charges"].append(
                                {
                                    "charge_type": "Actual",
                                    "account_head": self.get_account(charge_type),
                                    "tax_amount": amount,
                                    "description": f"{charge_type} for {seller_sku}",
                                }
                            )

                    # Fees
                    for fee in item.get("ItemFeeList", []):
                        amount = float(
                            fee.get("FeeAmount", {}).get("CurrencyAmount", 0) or 0
                        )
                        fee_type = fee.get("FeeType")

                        if amount != 0:
                            results["fees"].append(
                                {
                                    "charge_type": "Actual",
                                    "account_head": self.get_account(fee_type),
                                    "tax_amount": amount,
                                    "description": f"{fee_type} for {seller_sku}",
                                }
                            )

            if not next_token:
                break

            payload = self.call_sp_api_method(
                finances.list_financial_events_by_order_id,
                order_id=order_id,
                next_token=next_token,
            )

        return results

    # -------------------------------------------------------------------------
    # ITEM CREATION / LOOKUP
    # -------------------------------------------------------------------------

    def create_item(self, order_item: dict) -> str:
        """Create ERPNext Item from Amazon Catalog + Fields Map."""
        catalog = self.get_catalog_items_instance()
        amazon_item_payload = catalog.get_catalog_item(order_item["ASIN"])
        amazon_item = amazon_item_payload.get("payload") or amazon_item_payload

        item = frappe.new_doc("Item")

        # Map fields from amazon_fields_map
        for m in self.amz_setting.amazon_fields_map:
            if m.use_to_find_item_code:
                item.item_code = order_item.get(m.amazon_field)
            if m.item_field:
                item.set(m.item_field, order_item.get(m.amazon_field))

        attrs = (amazon_item.get("AttributeSets") or [{}])[0]

        group = attrs.get("ProductGroup")
        brand = attrs.get("Brand")
        mfr = attrs.get("Manufacturer")

        if group:
            item.item_group = group
        if brand:
            item.brand = brand
        if mfr:
            item.manufacturer = mfr

        item.insert(ignore_permissions=True)

        # Link to Ecommerce Item
        eci = frappe.new_doc("Ecommerce Item")
        eci.integration = frappe.get_meta("Amazon SP API Settings").module
        eci.erpnext_item_code = item.item_code
        eci.integration_item_code = order_item["ASIN"]
        eci.sku = order_item["SellerSKU"]
        eci.insert(ignore_permissions=True)

        return item.item_code

    def get_item_code(self, order_item: dict) -> str:
        """Find Item via mapped field, or create if allowed."""
        for m in self.amz_setting.amazon_fields_map:
            if not m.use_to_find_item_code:
                continue

            value = order_item.get(m.amazon_field)
            if value is None:
                continue

            item_code = frappe.db.get_value(
                "Item",
                {m.item_field: value},
                "item_code",
            )
            if item_code:
                return item_code

            if not self.amz_setting.create_item_if_not_exists:
                frappe.throw(
                    _("Item not found using field {0} = {1}")
                    .format(m.item_field, value)
                )
            # else: allowed to create → break and create
            break

        # Create new item
        return self.create_item(order_item)

    # -------------------------------------------------------------------------
    # ORDER ITEMS WITH TAX + SHIPPING
    # -------------------------------------------------------------------------

    def get_order_items(self, order_id: str) -> list:
        """Get order items including per-item tax & shipping breakdown."""
        orders_api = self.get_orders_instance()
        payload = self.call_sp_api_method(
            orders_api.get_order_items,
            order_id=order_id,
        )

        final_items = []
        warehouse = self.amz_setting.warehouse

        while True:
            items = payload.get("OrderItems", []) if payload else []
            next_token = payload.get("NextToken") if payload else None

            for oi in items:
                if oi.get("QuantityOrdered", 0) <= 0:
                    continue

                qty = oi.get("QuantityOrdered", 1)

                item_price = float(oi.get("ItemPrice", {}).get("Amount", 0) or 0)
                item_tax = float(oi.get("ItemTax", {}).get("Amount", 0) or 0)
                ship_price = float(oi.get("ShippingPrice", {}).get("Amount", 0) or 0)
                ship_tax = float(oi.get("ShippingTax", {}).get("Amount", 0) or 0)

                per_item_shipping = ship_price / qty if qty else 0
                rate = item_price + per_item_shipping

                final_items.append(
                    {
                        "item_code": self.get_item_code(oi),
                        "item_name": oi.get("SellerSKU"),
                        "description": oi.get("Title"),
                        "rate": rate,
                        "qty": qty,
                        "warehouse": warehouse,
                        "stock_uom": "Nos",
                        "conversion_factor": 1,
                        "amazon_item_price": item_price,
                        "amazon_item_tax": item_tax,
                        "amazon_shipping": ship_price,
                        "amazon_shipping_tax": ship_tax,
                    }
                )

            if not next_token:
                break

            payload = self.call_sp_api_method(
                orders_api.get_order_items,
                order_id=order_id,
                next_token=next_token,
            )

        return final_items

    # -------------------------------------------------------------------------
    # SALES ORDER CREATION
    # -------------------------------------------------------------------------

    def create_sales_order(self, order: dict) -> str | None:
        """
        Create Sales Order from Amazon order:
        - Customer + Address
        - Items + Taxes (from Finances if available, else fallback)
        - Auto run DN/SI/Payment logic based on status.
        """
        order_id = order.get("AmazonOrderId")
        if not order_id:
            return None

        existing = frappe.db.get_value(
            "Sales Order",
            {"amazon_order_id": order_id},
            "name",
        )
        if existing:
            return existing

        items = self.get_order_items(order_id)
        if not items:
            return None

        # ---------------- CUSTOMER ----------------
        buyer = order.get("BuyerInfo") or {}
        buyer_email = buyer.get("BuyerEmail") or f"Buyer-{order_id}"

        customer = frappe.db.get_value("Customer", buyer_email)
        if not customer:
            c = frappe.new_doc("Customer")
            c.customer_name = buyer_email
            c.customer_group = self.amz_setting.customer_group
            c.territory = self.amz_setting.territory
            c.customer_type = self.amz_setting.customer_type
            c.insert(ignore_permissions=True)
            customer = c.name

        # ---------------- ADDRESS ----------------
        shipping = order.get("ShippingAddress")
        if shipping:
            addr = frappe.new_doc("Address")
            addr.address_line1 = shipping.get("AddressLine1", "Not Provided")
            addr.city = shipping.get("City")
            addr.pincode = shipping.get("PostalCode")
            addr.state = shipping.get("StateOrRegion")
            addr.country = self.map_country_code(shipping.get("CountryCode"))
            addr.address_type = "Shipping"
            addr.append("links", {"link_doctype": "Customer", "link_name": customer})
            try:
                addr.insert(ignore_permissions=True)
            except Exception as e:
                # If Country mapping fails, just log and continue
                frappe.log_error(
                    f"Address insert failed for Amazon Order {order_id}: {str(e)}",
                    "Amazon Address Error",
                )

        # ---------------- SALES ORDER HEADER ----------------
        so = frappe.new_doc("Sales Order")
        so.amazon_order_id = order_id
        so.marketplace_id = order.get("MarketplaceId")
        so.customer = customer
        so.company = self.amz_setting.company

        purchase_date = order.get("PurchaseDate")
        latest_ship_date = order.get("LatestShipDate")

        if purchase_date:
            so.transaction_date = dateutil.parser.parse(purchase_date).date()
        if latest_ship_date:
            so.delivery_date = dateutil.parser.parse(latest_ship_date).date()

        # ---------------- ITEMS + TOTALS ----------------
        total_item_tax = 0.0
        total_ship = 0.0
        total_ship_tax = 0.0
        total_item_amount = 0.0

        for it in items:
            so.append(
                "items",
                {
                    "item_code": it["item_code"],
                    "item_name": it["item_name"],
                    "description": it["description"],
                    "qty": it["qty"],
                    "rate": it["rate"],
                    "stock_uom": "Nos",
                    "warehouse": it["warehouse"],
                },
            )

            total_item_tax += it["amazon_item_tax"]
            total_ship += it["amazon_shipping"]
            total_ship_tax += it["amazon_shipping_tax"]
            total_item_amount += it["amazon_item_price"] * it["qty"]

        # ---------------- TAXES & CHARGES ----------------
        taxes_charges_account = self.amz_setting.taxes_charges

        if taxes_charges_account:
            # First try detailed Finances charges + fees
            charges_and_fees = self.get_charges_and_fees(order_id)

            if charges_and_fees.get("charges") or charges_and_fees.get("fees"):
                # Use Amazon Finances breakdown
                for charge in charges_and_fees.get("charges", []):
                    so.append("taxes", charge)
                for fee in charges_and_fees.get("fees", []):
                    so.append("taxes", fee)
            else:
                # Fallback: Use per-item tax/shipping totals
                if total_item_tax > 0:
                    so.append(
                        "taxes",
                        {
                            "charge_type": "Actual",
                            "account_head": taxes_charges_account,
                            "tax_amount": total_item_tax,
                            "description": "Sales Tax from Amazon",
                        },
                    )

                if total_ship > 0:
                    shipping_account = frappe.db.get_value(
                        "Account",
                        {"account_name": "Freight and Forwarding Charges"},
                        "name",
                    ) or taxes_charges_account

                    so.append(
                        "taxes",
                        {
                            "charge_type": "Actual",
                            "account_head": shipping_account,
                            "tax_amount": total_ship,
                            "description": "Shipping Charge from Amazon",
                        },
                    )

                if total_ship_tax > 0:
                    so.append(
                        "taxes",
                        {
                            "charge_type": "Actual",
                            "account_head": taxes_charges_account,
                            "tax_amount": total_ship_tax,
                            "description": "Shipping Tax from Amazon",
                        },
                    )

        # Debug log for reconciliation
        frappe.log_error(
            f"Amazon Order {order_id} Breakdown: "
            f"Items={total_item_amount:.2f}, "
            f"Shipping={total_ship:.2f}, "
            f"Item Tax={total_item_tax:.2f}, "
            f"Shipping Tax={total_ship_tax:.2f}, "
            f"Total={total_item_amount + total_ship + total_item_tax + total_ship_tax:.2f}",
            "Amazon Order Breakdown",
        )

        so.insert(ignore_permissions=True)
        so.submit()

        # After SO creation, process DN / SI / Payment Entry based on status
        self.process_order_documents_based_on_status(order, so.name)

        return so.name

    # -------------------------------------------------------------------------
    # DELIVERY NOTE
    # -------------------------------------------------------------------------

    def create_delivery_note_from_so(self, sales_order_name: str) -> str | None:
        try:
            existing = frappe.db.get_value(
                "Delivery Note",
                {"custom_against_sales_order": sales_order_name, "docstatus": ["<", 2]},
                "name",
            )
            if existing:
                return existing

            so = frappe.get_doc("Sales Order", sales_order_name)

            dn = frappe.new_doc("Delivery Note")
            dn.company = so.company
            dn.customer = so.customer
            dn.custom_against_sales_order = so.name
            dn.custom_amazon_order_id = so.amazon_order_id

            for it in so.items:
                dn.append(
                    "items",
                    {
                        "item_code": it.item_code,
                        "item_name": it.item_name,
                        "description": it.description,
                        "qty": it.qty,
                        "rate": it.rate,
                        "warehouse": it.warehouse,
                        "uom": it.uom,
                        "conversion_factor": it.conversion_factor,
                        "against_sales_order": so.name,
                        "so_detail": it.name,
                        "allow_zero_valuation_rate": 1,
                    },
                )

            dn.insert(ignore_permissions=True)
            dn.submit()

            return dn.name

        except Exception as e:
            frappe.log_error(
                f"DN Error for SO {sales_order_name}: {str(e)}",
                "Amazon DN Error",
            )
            return None

    # -------------------------------------------------------------------------
    # SALES INVOICE
    # -------------------------------------------------------------------------

    def create_sales_invoice_from_so(self, sales_order_name: str) -> str | None:
        try:
            existing = frappe.db.get_value(
                "Sales Invoice",
                {"custom_against_sales_order": sales_order_name, "docstatus": ["<", 2]},
                "name",
            )
            if existing:
                return existing

            so = frappe.get_doc("Sales Order", sales_order_name)

            si = frappe.new_doc("Sales Invoice")
            si.company = so.company
            si.customer = so.customer
            si.custom_against_sales_order = so.name
            si.custom_amazon_order_id = so.amazon_order_id
            si.due_date = frappe.utils.add_days(frappe.utils.today(), 7)

            for it in so.items:
                si.append(
                    "items",
                    {
                        "item_code": it.item_code,
                        "qty": it.qty,
                        "rate": it.rate,
                        "description": it.description,
                        "warehouse": it.warehouse,
                        "uom": it.uom,
                        "conversion_factor": it.conversion_factor,
                        "sales_order": so.name,
                        "so_detail": it.name,
                    },
                )

            for t in so.taxes:
                si.append(
                    "taxes",
                    {
                        "charge_type": t.charge_type,
                        "account_head": t.account_head,
                        "description": t.description,
                        "tax_amount": t.tax_amount,
                    },
                )

            si.insert(ignore_permissions=True)
            si.submit()

            return si.name

        except Exception as e:
            frappe.log_error(
                f"SI Error for SO {sales_order_name}: {str(e)}",
                "Amazon SI Error",
            )
            return None

    # -------------------------------------------------------------------------
    # SETTLEMENT CHECK (NET PAYOUT) — PRODUCTION
    # -------------------------------------------------------------------------

    def check_amazon_settlement_available(self, order_id: str) -> float:
        """
        Returns actual NET PAYOUT received from Amazon (ItemPrice + Shipping - Fees)
        based on Finances → list_financial_events_by_order_id.

        If payout is NOT available yet → returns 0
        → Payment Entry will NOT be created for this order yet.
        """
        finances = self.get_finances_instance()

        try:
            payload = self.call_sp_api_method(
                finances.list_financial_events_by_order_id,
                order_id=order_id,
            )
        except Exception as e:
            frappe.log_error(
                f"Finances call failed for {order_id}: {str(e)}",
                "Amazon Settlement Debug",
            )
            return 0.0

        frappe.log_error(
            f"FINANCE DEBUG for {order_id}: {payload}",
            "Amazon Settlement Debug",
        )

        if not payload:
            return 0.0

        events = payload.get("FinancialEvents", {}).get("ShipmentEventList", [])
        if not events:
            return 0.0

        total_payout = 0.0

        for event in events:
            for item in event.get("ShipmentItemList", []):
                # ITEM PRICE (Principal)
                item_price = 0.0
                shipping = 0.0
                fees = 0.0

                for c in item.get("ItemChargeList", []):
                    charge_type = c.get("ChargeType")
                    amount = float(
                        c.get("ChargeAmount", {}).get("CurrencyAmount", 0) or 0
                    )
                    if charge_type == "Principal":
                        item_price += amount
                    elif charge_type == "Shipping":
                        shipping += amount

                for f in item.get("ItemFeeList", []):
                    fees += float(
                        f.get("FeeAmount", {}).get("CurrencyAmount", 0) or 0
                    )

                total_payout += (item_price + shipping - fees)

        return total_payout if total_payout > 0 else 0.0

    # -------------------------------------------------------------------------
    # PAYMENT ENTRY FROM REAL AMAZON PAYOUT
    # -------------------------------------------------------------------------

    def create_payment_entry_from_si(self, sales_invoice_name: str, settlement_amount: float) -> str | None:
        try:
            existing = frappe.db.get_value(
                "Payment Entry",
                {"reference_no": sales_invoice_name, "docstatus": ["<", 2]},
                "name",
            )
            if existing:
                return existing

            si = frappe.get_doc("Sales Invoice", sales_invoice_name)

            pe = frappe.new_doc("Payment Entry")
            pe.payment_type = "Receive"
            pe.company = si.company
            pe.party_type = "Customer"
            pe.party = si.customer

            pe.paid_amount = settlement_amount
            pe.received_amount = settlement_amount

            pe.reference_no = sales_invoice_name
            pe.reference_date = frappe.utils.today()
            pe.custom_amazon_order_id = si.custom_amazon_order_id

            payout_acct = getattr(self.amz_setting, "amazon_payout_account", None)
            if not payout_acct:
                frappe.throw("Please set Amazon Payout Account in Amazon SP API Settings.")

            pe.paid_to = payout_acct

            pe.append(
                "references",
                {
                    "reference_doctype": "Sales Invoice",
                    "reference_name": sales_invoice_name,
                    "allocated_amount": settlement_amount,
                },
            )

            pe.insert(ignore_permissions=True)
            pe.submit()

            frappe.log_error(
                f"Payment Entry {pe.name} created with settlement: {settlement_amount}",
                "Amazon PE Created",
            )

            return pe.name

        except Exception as e:
            frappe.log_error(
                f"Payment Entry Error for SI {sales_invoice_name}: {str(e)}",
                "Amazon PE Error",
            )
            return None

    # -------------------------------------------------------------------------
    # PROCESS ORDER STATUS → DN + SI + PAYMENT ENTRY
    # -------------------------------------------------------------------------

    def process_order_documents_based_on_status(self, amazon_order: dict, sales_order_name: str):
        status = amazon_order.get("OrderStatus", "")

        # DELIVERY NOTE on shipped / partially shipped
        if status in ["Shipped", "PartiallyShipped"]:
            self.create_delivery_note_from_so(sales_order_name)

        # SALES INVOICE + PAYMENT ENTRY
        if status in ["Shipped", "InvoiceUnconfirmed"]:
            si_name = self.create_sales_invoice_from_so(sales_order_name)
            if not si_name:
                return

            settlement = self.check_amazon_settlement_available(
                amazon_order.get("AmazonOrderId")
            )

            if settlement > 0:
                self.create_payment_entry_from_si(si_name, settlement)
            else:
                frappe.log_error(
                    f"Settlement NOT available for Order {amazon_order.get('AmazonOrderId')} — skipping Payment Entry",
                    "Amazon Settlement",
                )

    # -------------------------------------------------------------------------
    # MAIN GET ORDERS LOOP
    # -------------------------------------------------------------------------

    def get_orders(self, created_after: str) -> list:
        """
        Fetch orders from Amazon → create SOs → auto-create DN, SI, Payment Entry
        when appropriate based on status & settlement.
        """
        orders_api = self.get_orders_instance()

        order_statuses = [
            "PendingAvailability",
            "Pending",
            "Unshipped",
            "PartiallyShipped",
            "Shipped",
            "InvoiceUnconfirmed",
            "Canceled",
            "Unfulfillable",
        ]
        fulfillment_channels = ["FBA", "SellerFulfilled"]

        payload = self.call_sp_api_method(
            orders_api.get_orders,
            created_after=created_after,
            order_statuses=order_statuses,
            fulfillment_channels=fulfillment_channels,
            max_results=50,
        )

        if not payload:
            return []

        created_so_list: list[str] = []

        while True:
            orders_list = payload.get("Orders") or []
            next_token = payload.get("NextToken")

            for order in orders_list:
                so_name = self.create_sales_order(order)
                if so_name:
                    created_so_list.append(so_name)

            if not next_token:
                break

            payload = self.call_sp_api_method(
                orders_api.get_orders,
                created_after=created_after,
                next_token=next_token,
            )

        return created_so_list


# -------------------------------------------------------------------------
# CREDENTIAL VALIDATION
# -------------------------------------------------------------------------

def validate_amazon_sp_api_credentials(**args) -> None:
    api = SPAPI(
        iam_arn=args.get("iam_arn"),
        client_id=args.get("client_id"),
        client_secret=args.get("client_secret"),
        refresh_token=args.get("refresh_token"),
        aws_access_key=args.get("aws_access_key"),
        aws_secret_key=args.get("aws_secret_key"),
        country_code=args.get("country"),
    )

    try:
        api.get_access_token()
        api.get_auth()
    except SPAPIError as e:
        msg = f"<b>Error:</b> {e.error}<br/><b>Description:</b> {e.error_description}"
        frappe.throw(msg)


# -------------------------------------------------------------------------
# HOOK ENTRYPOINT FOR BACKGROUND JOB
# -------------------------------------------------------------------------

def get_orders(amz_setting_name: str, created_after: str) -> list:
    """Called by background job 'Get Amazon Orders - {setting}'."""
    ar = AmazonRepository(amz_setting_name)
    return ar.get_orders(created_after)


# -------------------------------------------------------------------------
# STATE NAME FROM PINCODE (unchanged from original)
# -------------------------------------------------------------------------

def get_state_name_from_pincode(country_code=None, postal_code=None, state=None):
    if not all((country_code, postal_code)):
        return state

    def get_first_three(value):
        if isinstance(value, str) and value.isdigit() and len(value) == 6:
            return int(value[:3])
        if isinstance(value, int) and len(str(value)) == 6:
            return int(str(value)[:3])

    if (
        "india_compliance" in frappe.get_installed_apps()
        and country_code.lower() == "in"
    ):
        from india_compliance.gst_indica.constants import STATE_PINCODE_MAPPING

        first_three = get_first_three(postal_code)
        if not first_three:
            return state

        for _state, ranges in STATE_PINCODE_MAPPING.items():
            if isinstance(ranges[0], tuple):
                for r in ranges:
                    if r[0] <= first_three <= r[1]:
                        return _state
            else:
                if ranges[0] <= first_three <= ranges[1]:
                    return _state

    return state
