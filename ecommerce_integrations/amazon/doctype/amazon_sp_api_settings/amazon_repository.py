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
                # Get company currency
        company_currency = frappe.db.get_value("Company", so.company, "default_currency")
        so.currency = company_currency  # Explicitly set

        # Also set conversion rate if needed
        so.conversion_rate = 1.0
        so.plc_conversion_rate = 1.0


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
            dn.currency = so.currency
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
            si.currency = so.currency  # Inherit from Sales Order
            si.conversion_rate = so.conversion_rate
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

    def check_amazon_settlement_available(self, order_id: str):
        """
        FULL Amazon US Settlement Logic
        --------------------------------
        1. Fetch ALL settlement groups (biweekly payout groups)
        2. For each group, fetch ALL financial events
        3. Filter all events belonging to the specific order_id
        4. Sum:
           + Item Charges
           - Item Fees
           - Promotions
           + Adjustments
           - Service Fees
           + Chargeback Adjustments
        5. Return (net_payout_usd, settlement_id)
        """

        finances = self.get_finances_instance()

        # ------------------------------
        # Step 1 — Fetch ALL settlement groups
        # ------------------------------
        try:
            groups_payload = self.call_sp_api_method(
                finances.list_financial_event_groups,
                max_results=100
            )
        except Exception as e:
            frappe.log_error(f"Cannot load settlement groups: {e}", "Amazon Settlement Error")
            return 0.0, None

        if not groups_payload:
            return 0.0, None

        groups = groups_payload.get("FinancialEventGroupList", [])
        next_group_token = groups_payload.get("NextToken")

        # Pagination: load more groups
        while next_group_token:
            token_payload = self.call_sp_api_method(
                finances.list_financial_event_groups,
                next_token=next_group_token
            )
            groups.extend(token_payload.get("FinancialEventGroupList", []))
            next_group_token = token_payload.get("NextToken")

        # ------------------------------
        # Step 2 — Search each group for this ORDER
        # ------------------------------

        for group in groups:
            group_id = group.get("FinancialEventGroupId")
            settlement_id = group.get("FinancialEventGroupId")  # unique payout cycle ID

            # Fetch all events inside this group
            try:
                events_payload = self.call_sp_api_method(
                    finances.list_financial_events_by_group_id,
                    group_id=group_id,
                    max_results=100
                )
            except Exception as e:
                frappe.log_error(f"Group fetch failed: {e}", "Amazon Settlement Error")
                continue

            if not events_payload:
                continue

            total = 0.0
            found = False

            # Loop through ALL event types
            while True:
                fevents = events_payload.get("FinancialEvents", {})

                # 1️⃣ SHIPMENT EVENTS
                for evt in fevents.get("ShipmentEventList", []):
                    if evt.get("AmazonOrderId") == order_id:
                        found = True
                        for item in evt.get("ShipmentItemList", []):
                            for c in item.get("ItemChargeList", []):
                                total += float(c["ChargeAmount"]["CurrencyAmount"])
                            for f in item.get("ItemFeeList", []):
                                total -= float(f["FeeAmount"]["CurrencyAmount"])
                            for p in item.get("PromotionList", []):
                                total -= float(p["PromotionAmount"]["CurrencyAmount"])

                # 2️⃣ REFUND EVENTS
                for evt in fevents.get("RefundEventList", []):
                    if evt.get("AmazonOrderId") == order_id:
                        found = True
                        for item in evt.get("RefundItemList", []):
                            for c in item.get("ItemChargeAdjustmentList", []):
                                total += float(c["ChargeAmount"]["CurrencyAmount"])
                            for f in item.get("ItemFeeAdjustmentList", []):
                                total -= float(f["FeeAmount"]["CurrencyAmount"])

                # 3️⃣ ADJUSTMENT EVENTS
                for evt in fevents.get("AdjustmentEventList", []):
                    for adj in evt.get("AdjustmentItemList", []):
                        if adj.get("AmazonOrderId") == order_id:
                            found = True
                            total += float(adj.get("QuantityAdjustment", 0))

                # 4️⃣ SERVICE FEE EVENTS
                for evt in fevents.get("ServiceFeeEventList", []):
                    if evt.get("AmazonOrderId") == order_id:
                        found = True
                        for f in evt.get("FeeList", []):
                            total -= float(f["FeeAmount"]["CurrencyAmount"])

                # 5️⃣ CHARGEBACK EVENTS
                for evt in fevents.get("ChargebackEventList", []):
                    if evt.get("AmazonOrderId") == order_id:
                        found = True
                        for adj in evt.get("ChargebackAdjustmentList", []):
                            total += float(adj["ChargeAmount"]["CurrencyAmount"])

                # Pagination inside group
                next_token = events_payload.get("NextToken")
                if not next_token:
                    break

                events_payload = self.call_sp_api_method(
                    finances.list_financial_events_by_group_id,
                    group_id=group_id,
                    next_token=next_token
                )

            if found and total > 0:
                return total, settlement_id

        # No settlement found
        return 0.0, None


    # -------------------------------------------------------------------------
    # PAYMENT ENTRY FROM REAL AMAZON PAYOUT
    # -------------------------------------------------------------------------

    def create_payment_entry_from_si(self, si_name, payout_usd, settlement_id):
        si = frappe.get_doc("Sales Invoice", si_name)

        # Prevent duplicate PE for same settlement
        if si.get("custom_amazon_settlement_id") == settlement_id:
            return None

        company_currency = frappe.db.get_value("Company", si.company, "default_currency")
        amazon_currency = si.currency

        # Determine exchange rate
        if amazon_currency == company_currency:
            exchange_rate = 1
        else:
            exchange_rate = frappe.utils.get_exchange_rate(amazon_currency, company_currency)

        received_amount = payout_usd * exchange_rate

        pe = frappe.new_doc("Payment Entry")
        pe.payment_type = "Receive"
        pe.company = si.company
        pe.party_type = "Customer"
        pe.party = si.customer

        # Multi-currency fields
        pe.paid_amount = payout_usd
        pe.paid_from_account_currency = amazon_currency
        pe.received_amount = received_amount
        pe.received_to_account_currency = company_currency
        pe.source_exchange_rate = exchange_rate

        pe.reference_no = si.name
        pe.reference_date = frappe.utils.today()
        pe.custom_amazon_settlement_id = settlement_id

        payout_acct = self.amz_setting.amazon_payout_account
        if not payout_acct:
            frappe.throw("Set Amazon Payout Account in Amazon SP API Settings")

        pe.paid_to = payout_acct

        pe.append(
            "references",
            {
                "reference_doctype": "Sales Invoice",
                "reference_name": si.name,
                "allocated_amount": received_amount,
            },
        )

        pe.insert(ignore_permissions=True)
        pe.submit()

        # Save settlement ID inside invoice
        si.custom_amazon_settlement_id = settlement_id
        si.save(ignore_permissions=True)

        return pe.name
    
    def update_sales_invoice(self, si_name: str, amazon_order: dict):
        """
        Update existing Sales Invoice.
        """
        try:
            si = frappe.get_doc("Sales Invoice", si_name)
            order_id = amazon_order.get("AmazonOrderId")
            
            # Update due date if needed
            new_due_date = frappe.utils.add_days(frappe.utils.today(), 7)
            if si.due_date != new_due_date:
                si.due_date = new_due_date
            
            # Update taxes/charges if enabled
            if self.amz_setting.taxes_charges:
                charges_and_fees = self.get_charges_and_fees(order_id)
                self.update_invoice_taxes(si, charges_and_fees)
            
            si.save(ignore_permissions=True)
            
            # Only submit if not already submitted
            if si.docstatus == 0:
                si.submit()
            
            frappe.db.commit()
            
        except Exception as e:
            frappe.log_error(
                f"Failed to update Sales Invoice {si_name}: {str(e)}",
                "Amazon Invoice Update Error"
            )

    def update_delivery_note(self, dn_name: str, amazon_order: dict):
        """
        Update existing Delivery Note.
        """
        try:
            dn = frappe.get_doc("Delivery Note", dn_name)
            order_id = amazon_order.get("AmazonOrderId")
            status = amazon_order.get("OrderStatus")
            
            # Update status tracking
            dn.custom_amazon_order_status = status
            
            # For partial shipments, update quantities
            if status == "PartiallyShipped":
                # Get updated order items
                updated_items = self.get_order_items(order_id)
                self.update_delivery_items(dn, updated_items)
            
            dn.save(ignore_permissions=True)
            
            # Only submit if not already submitted
            if dn.docstatus == 0:
                dn.submit()
            
            frappe.db.commit()
            
        except Exception as e:
            frappe.log_error(
                f"Failed to update Delivery Note {dn_name}: {str(e)}",
                "Amazon DN Update Error"
            )

    

    

    def process_order_documents_based_on_status(self, amazon_order, so_name):
        """
        Create/update documents based on current Amazon status.
        """
        order_id = amazon_order.get("AmazonOrderId")
        status = amazon_order.get("OrderStatus")
        
        so = frappe.get_doc("Sales Order", so_name)
        
        # Handle cancellations
        if status == "Canceled":
            self.handle_order_cancellation(order_id, so_name)
            return
        
        # Handle returns/refunds
        self.process_refunds_and_returns(order_id)
        
        # Delivery Note Logic
        if status in ["Shipped", "PartiallyShipped"]:
            # Check if DN already exists
            existing_dn = frappe.db.get_value(
                "Delivery Note",
                {"custom_against_sales_order": so_name, "docstatus": ["<", 2]},
                "name"
            )
            
            if existing_dn:
                # Update existing DN
                self.update_delivery_note(existing_dn, amazon_order)
            else:
                # Create new DN
                self.create_delivery_note_from_so(so_name)
        
        # Sales Invoice Logic
        if status in ["Shipped", "InvoiceUnconfirmed", "PartiallyShipped"]:
            # Check if SI already exists
            existing_si = frappe.db.get_value(
                "Sales Invoice",
                {"custom_against_sales_order": so_name, "docstatus": ["<", 2]},
                "name"
            )
            
            if existing_si:
                # Update existing SI
                self.update_sales_invoice(existing_si, amazon_order)
            else:
                # Create new SI
                si_name = self.create_sales_invoice_from_so(so_name)
                
                # Check for settlement
                if si_name:
                    payout, settlement_id = self.check_amazon_settlement_available(order_id)
                    
                    if payout > 0 and settlement_id:
                        # Check if payment entry already exists
                        existing_pe = frappe.db.get_value(
                            "Payment Entry",
                            {
                                "custom_amazon_settlement_id": settlement_id,
                                "docstatus": ["<", 2]
                            },
                            "name"
                        )
                        
                        if not existing_pe:
                            self.create_payment_entry_from_si(si_name, payout, settlement_id)
                    else:
                        frappe.log_error(
                            f"Settlement NOT available for Order {order_id}",
                            "Amazon Settlement Check"
     
                        )
    
            


        

    # -------------------------------------------------------------------------
    # MAIN GET ORDERS LOOP
    # -------------------------------------------------------------------------

    def get_orders(self, created_after: str) -> list:
        """
        Enhanced: Fetch orders from Amazon → create/update SOs → auto-create/update DN, SI, Payment Entry
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

        processed_orders: list[str] = []

        while True:
            orders_list = payload.get("Orders") or []
            next_token = payload.get("NextToken")

            for order in orders_list:
                # Check if order already exists
                existing_so = frappe.db.get_value(
                    "Sales Order",
                    {"amazon_order_id": order.get("AmazonOrderId")},
                    "name"
                )
                
                if existing_so:
                    # UPDATE EXISTING SALES ORDER
                    self.update_sales_order(existing_so, order)
                    so_name = existing_so
                else:
                    # CREATE NEW SALES ORDER
                    so_name = self.create_sales_order(order)
                
                if so_name:
                    # PROCESS DOCUMENTS BASED ON CURRENT STATUS
                    self.process_order_documents_based_on_status(order, so_name)
                    processed_orders.append(so_name)

            if not next_token:
                break

            payload = self.call_sp_api_method(
                orders_api.get_orders,
                created_after=created_after,
                next_token=next_token,
            )

        return processed_orders


    def update_sales_order(self, so_name: str, amazon_order: dict):
        """
        Update existing Sales Order with latest Amazon data.
        """
        try:
            so = frappe.get_doc("Sales Order", so_name)
            order_id = amazon_order.get("AmazonOrderId")
            current_status = amazon_order.get("OrderStatus")
            
            # Log status change
            if so.custom_amazon_order_status != current_status:
                frappe.log_error(
                    f"Order {order_id} status changed: {so.custom_amazon_order_status} → {current_status}",
                    "Amazon Order Status Update"
                )
                so.custom_amazon_order_status = current_status
            
            # Update order metadata
            so.marketplace_id = amazon_order.get("MarketplaceId")
            so.purchase_date = amazon_order.get("PurchaseDate")
            so.latest_ship_date = amazon_order.get("LatestShipDate")
            
            # Fetch updated order items
            updated_items = self.get_order_items(order_id)
            
            # Update items if changed
            self.update_order_items(so, updated_items)
            
            # Update charges and fees
            if self.amz_setting.taxes_charges:
                charges_and_fees = self.get_charges_and_fees(order_id)
                self.update_taxes_and_charges(so, charges_and_fees)
            
            so.save(ignore_permissions=True)
            
            # Only submit if not already submitted
            if so.docstatus == 0:
                so.submit()
            
            frappe.db.commit()
            
        except Exception as e:
            frappe.log_error(
                f"Failed to update Sales Order {so_name}: {str(e)}",
                "Amazon Order Update Error"
            )

    def generate_reconciliation_report(self):
        """
        Generate report comparing Amazon vs ERPNext status.
        """
        report = {
            "orders": [],
            "mismatches": []
        }
        
        # Get all Amazon orders from ERPNext
        amazon_orders = frappe.get_all(
            "Sales Order",
            filters={"amazon_order_id": ["is", "set"]},
            fields=["name", "amazon_order_id", "amazon_order_status", "status"]
        )
        
        for order in amazon_orders:
            # Get current status from Amazon API
            try:
                orders_api = self.get_orders_instance()
                amazon_data = self.call_sp_api_method(
                    orders_api.get_order_items,
                    order_id=order.amazon_order_id
                )
                
                current_amazon_status = "Unknown"
                if amazon_data and amazon_data.get("OrderStatus"):
                    current_amazon_status = amazon_data.get("OrderStatus")
                
                # Compare
                if order.custom_amazon_order_status != current_amazon_status:
                    report["mismatches"].append({
                        "sales_order": order.name,
                        "amazon_order_id": order.custom_amazon_order_id,
                        "erpnext_status": order.custom_amazon_order_status,
                        "amazon_status": current_amazon_status
                    })
                
                report["orders"].append({
                    "sales_order": order.name,
                    "amazon_order_id": order.custom_amazon_order_id,
                    "erpnext_status": order.custom_amazon_order_status,
                    "amazon_status": current_amazon_status
                })
                
            except Exception as e:
                frappe.log_error(f"Error checking order {order.custom_amazon_order_id}: {str(e)}")
        
        return report
    
    # def schedule_full_sync():
    #     """
    #     Daily full synchronization job.
    #     """
    #     amz_settings = frappe.get_all(
    #         "Amazon SP API Settings",
    #         filters={"is_active": 1, "enable_sync": 1},
    #         fields=["name"]
    #     )
        
    #     for setting in amz_settings:
    #         try:
    #             repo = AmazonRepository(setting.name)
                
    #             # Generate reconciliation report
    #             report = repo.generate_reconciliation_report()
                
    #             # Fix mismatches
    #             for mismatch in report.get("mismatches", []):
    #                 frappe.enqueue(
    #                     method=repo.sync_single_order,
    #                     custom_amazon_order_id=mismatch["custom_amazon_order_id"],
    #                     queue="long",
    #                     timeout=600
    #                 )
                
    #             # Regular sync for last 30 days
    #             from_date = frappe.utils.add_days(frappe.utils.today(), -30)
    #             repo.get_orders(from_date.strftime("%Y-%m-%d"))
                
    #         except Exception as e:
    #             frappe.log_error(f"Full sync failed for {setting.name}: {str(e)}")


    def update_order_items(self, sales_order, updated_items):
        """
        Update Sales Order items with latest Amazon data.
        """
        # Create a map of existing items by SKU
        existing_items = {}
        for item in sales_order.items:
            existing_items[item.item_code] = item
        
        for updated_item in updated_items:
            item_code = updated_item.get("item_code")
            
            if item_code in existing_items:
                # Update existing item
                existing_item = existing_items[item_code]
                if existing_item.qty != updated_item.get("qty"):
                    existing_item.qty = updated_item.get("qty")
                if existing_item.rate != updated_item.get("rate"):
                    existing_item.rate = updated_item.get("rate")
            else:
                # Add new item
                sales_order.append("items", {
                    "item_code": updated_item.get("item_code"),
                    "item_name": updated_item.get("item_name"),
                    "description": updated_item.get("description"),
                    "qty": updated_item.get("qty"),
                    "rate": updated_item.get("rate"),
                    "warehouse": updated_item.get("warehouse"),
                    "stock_uom": "Nos",
                    "conversion_factor": 1,
                })
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