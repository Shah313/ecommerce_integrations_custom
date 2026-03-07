import time
import dateutil
import frappe
from frappe import _
from datetime import datetime, timedelta
import csv
import io
from frappe.utils import flt, nowdate, get_datetime
import hashlib
import logging
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from typing import Optional


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



# def _try_parse_date(s):
#         if not s:
#             return None

#         # common Amazon CSV date formats (best effort)
#         fmts = [
#             "%m/%d/%Y",
#             "%Y-%m-%d",
#             "%m/%d/%Y %H:%M:%S",
#             "%Y-%m-%d %H:%M:%S",
#         ]
#         for f in fmts:
#             try:
#                 return datetime.strptime(s, f)
#             except Exception:
#                 continue
#         return None


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

    def test_finances_api(self):
        """Test if Finances API is working."""
        try:
            finances = self.get_finances_instance()
            from datetime import datetime, timedelta
            today = datetime.utcnow()
            
            # Correct date format
            posted_after = (today - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            posted_before = today.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            
            frappe.log_error(f"Testing finances API with dates: {posted_after} to {posted_before}", "Amazon API")
            
            # FIX: Use capitalized parameter names
            # In get_settlement_reports method:
            payload = self.call_sp_api_method(
                finances.list_financial_event_groups,
                posted_after=posted_after,      # lowercase for method parameter
                posted_before=posted_before,    # lowercase for method parameter  
                max_results=100,                # lowercase for method parameter
            )
            if payload:
                frappe.log_error("Finances API Test: SUCCESS", "Amazon API")
                return True
            else:
                frappe.log_error("Finances API Test: No payload", "Amazon API")
                return False
                
        except Exception as e:
            frappe.log_error(f"Finances API Test Failed: {str(e)[:100]}", "Amazon API")
            return False

    # -------------------------------------------------------------------------
    # CORE SP-API CALL WRAPPER
    # -------------------------------------------------------------------------

    def call_sp_api_method(self, sp_api_method, **kwargs):
        try:
            frappe.log_error(
                title=f"SP API Call: {sp_api_method.__name__}",
                message=frappe.as_json(kwargs, indent=2),
            )

            result = sp_api_method(**kwargs)

            if isinstance(result, dict) and result.get("errors"):
                frappe.log_error(
                    title="Amazon API Error",
                    message=frappe.as_json(result["errors"], indent=2),
                )

            return result.get("payload") if isinstance(result, dict) else None

        except SPAPIError as e:
            frappe.log_error("SPAPIError", f"{e.error} | {e.error_description}")
            raise




    

 


    # -------------------------------------------------------------------------
    # SP-API INSTANCES
    # -------------------------------------------------------------------------

    def get_finances_instance(self) -> Finances:
        return Finances(**self.instance_params)

    def get_orders_instance(self) -> Orders:
        return Orders(**self.instance_params)

    def get_catalog_items_instance(self) -> CatalogItems:
        return CatalogItems(**self.instance_params)
    


    # ============================================================
# SMART SETTLEMENT PROCESSOR (AUDITOR GRADE)
# ============================================================

   # ============================================================
# AMAZON SETTLEMENT → CONSOLIDATED PAYMENT ENTRY (PRODUCTION)
# ============================================================

    # @frappe.whitelist()
    # def create_payment_entry_from_settlement_csv(
    #     self,
    #     file_url: str,
    #     limit_invoices: int = 0,
    # ):
    #     import re
    #     from erpnext.accounts.party import get_party_account

    #     if not file_url:
    #         frappe.throw(_("file_url is required"))

    #     FULL_PAYMENT_TOLERANCE = 0.05

    #     # --------------------------------------------------------
    #     # 🛡️ IDEMPOTENCY
    #     # --------------------------------------------------------
    #     settlement_key = f"AMZ-SETTLEMENT::{file_url}"

    #     if frappe.db.exists(
    #         "Payment Entry",
    #         {"custom_amazon_settlement_key": settlement_key, "docstatus": ["<", 2]},
    #     ):
    #         return {"ok": True, "message": "Settlement already processed"}

    #     # --------------------------------------------------------
    #     # 📥 LOAD CSV
    #     # --------------------------------------------------------
    #     file_doc = frappe.get_doc("File", {"file_url": file_url})
    #     content = file_doc.get_content()

    #     if isinstance(content, bytes):
    #         content = content.decode("utf-8", errors="ignore")

    #     reader = csv.DictReader(io.StringIO(content))
    #     rows = list(reader)

    #     if not rows:
    #         return {"ok": False, "message": "CSV is empty"}

    #     # --------------------------------------------------------
    #     # 🔍 HEADER DETECTION (BULLETPROOF)
    #     # --------------------------------------------------------
    #     def _norm(s: str) -> str:
    #         return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())

    #     headers = list(rows[0].keys())
    #     hmap = {_norm(h): h for h in headers if h}

    #     def pick(*candidates):
    #         for c in candidates:
    #             key = _norm(c)
    #             if key in hmap:
    #                 return hmap[key]
    #         return None

    #     col_order_id = pick("Amazon Order ID", "amazon-order-id", "order-id")
    #     col_type = pick("Transaction Type", "transaction-type", "type")
    #     col_product = pick("Total Product Charges", "total-product-charges")
    #     col_rebate = pick("Total Promotional Rebates", "total-promotional-rebates")
    #     col_fees = pick("Amazon Fees", "amazon-fees")
    #     col_other = pick("Other", "other-amount")

    #     if not col_order_id or not col_type or not col_product:
    #         frappe.throw(
    #             _("Settlement CSV missing required columns. Found headers: {0}")
    #             .format(", ".join(headers))
    #         )

    #     # --------------------------------------------------------
    #     # 🧠 CLASSIFIER
    #     # --------------------------------------------------------
    #     ORDER_KEYWORDS = ["order", "shipment", "payment", "charge"]
    #     REFUND_KEYWORDS = ["refund", "return"]

    #     def classify_row(row):
    #         product = flt(row.get(col_product))
    #         ttype = (row.get(col_type) or "").lower()

    #         if product < 0:
    #             return "refund"

    #         if any(k in ttype for k in REFUND_KEYWORDS):
    #             return "refund"

    #         if product > 0 and any(k in ttype for k in ORDER_KEYWORDS):
    #             return "order"

    #         return "fee"

    #     # --------------------------------------------------------
    #     # 📊 BUCKETS
    #     # --------------------------------------------------------
    #     order_rows = []
    #     created_credit_notes = []
    #     missing_invoices = []

    #     total_allocated_amount = 0.0
    #     total_fees_bucket = 0.0
    #     amazon_positive_total = 0.0

    #     # --------------------------------------------------------
    #     # 🔁 PROCESS ROWS
    #     # --------------------------------------------------------
    #     for row in rows:
    #         category = classify_row(row)

    #         product = flt(row.get(col_product))
    #         rebate = flt(row.get(col_rebate))
    #         fees = flt(row.get(col_fees))
    #         other = flt(row.get(col_other))
    #         order_id = (row.get(col_order_id) or "").strip()

    #         # ================= ORDER =================
    #         if category == "order":
    #             if not order_id:
    #                 continue

    #             amazon_positive_total += product

    #             si_name = frappe.db.get_value(
    #                 "Sales Invoice",
    #                 {"custom_amazon_order_id": order_id, "docstatus": 1},
    #                 "name",
    #             )

    #             if not si_name:
    #                 missing_invoices.append(order_id)
    #                 continue

    #             outstanding = flt(
    #                 frappe.db.get_value("Sales Invoice", si_name, "outstanding_amount")
    #                 or 0
    #             )

    #             if outstanding <= FULL_PAYMENT_TOLERANCE:
    #                 continue

    #             alloc = min(product, outstanding)

    #             if alloc <= FULL_PAYMENT_TOLERANCE:
    #                 continue

    #             order_rows.append((si_name, alloc))
    #             total_allocated_amount += alloc

    #         # ================= REFUND =================
    #         elif category == "refund":
    #             if not order_id:
    #                 continue

    #             original_si = frappe.db.get_value(
    #                 "Sales Invoice",
    #                 {"custom_amazon_order_id": order_id, "docstatus": 1},
    #                 "name",
    #             )

    #             if not original_si:
    #                 continue

    #             existing_cn = frappe.db.get_value(
    #                 "Sales Invoice",
    #                 {"return_against": original_si, "docstatus": ["<", 2]},
    #             )

    #             if existing_cn:
    #                 continue

    #             si_doc = frappe.get_doc("Sales Invoice", original_si)

    #             cn = frappe.copy_doc(si_doc)
    #             cn.is_return = 1
    #             cn.update_stock = 1
    #             cn.set_update_stock = 1
    #             cn.return_against = original_si

    #             for it in cn.items:
    #                 it.qty = -abs(flt(it.qty))
    #                 if hasattr(it, "stock_qty") and it.stock_qty is not None:
    #                     it.stock_qty = -abs(flt(it.stock_qty))
    #                 else:
    #                     it.stock_qty = -abs(flt(it.qty))

    #             cn.set("payments", [])
    #             cn.insert(ignore_permissions=True)
    #             cn.submit()

    #             created_credit_notes.append(cn.name)

    #         # ================= FEES =================
    #         total_fees_bucket += rebate + fees + other

    #     # --------------------------------------------------------
    #     # 🚨 SAFETY
    #     # --------------------------------------------------------
    #     if not order_rows:
    #         frappe.throw(
    #             _("No ORDER PAYMENT rows matched any Sales Invoice.")
    #         )

    #     # --------------------------------------------------------
    #     # 🧮 FINAL PAYOUT (AUDIT CORRECT)
    #     # --------------------------------------------------------
    #     final_payout = amazon_positive_total + total_fees_bucket

    #     if abs(final_payout) <= FULL_PAYMENT_TOLERANCE:
    #         frappe.throw(_("Settlement net is zero"))

    #     # --------------------------------------------------------
    #     # 💰 PAYMENT ENTRY
    #     # --------------------------------------------------------
    #     pe = frappe.new_doc("Payment Entry")
    #     pe.payment_type = "Receive"
    #     pe.company = self.amz_setting.company
    #     pe.party_type = "Customer"
    #     pe.party = self.get_amazon_customer()
    #     pe.custom_amazon_settlement_key = settlement_key

    #     pe.paid_from = get_party_account("Customer", pe.party, pe.company)
    #     pe.paid_to = self.amz_setting.amazon_payout_account

    #     pe.paid_amount = final_payout
    #     pe.received_amount = final_payout

    #     pe.reference_no = f"AMZ-{frappe.utils.now_datetime().strftime('%Y%m%d%H%M%S')}"
    #     pe.reference_date = frappe.utils.today()

    #     # REFERENCES
    #     for si_name, amount in order_rows:
    #         pe.append(
    #             "references",
    #             {
    #                 "reference_doctype": "Sales Invoice",
    #                 "reference_name": si_name,
    #                 "allocated_amount": amount,
    #             },
    #         )

    #     # DEDUCTIONS
    #     if abs(total_fees_bucket) > 0.0001:
    #         pe.append(
    #             "deductions",
    #             {
    #                 "account": self.amz_setting.amazon_fee_account,
    #                 "cost_center": frappe.db.get_value(
    #                     "Company", self.amz_setting.company, "cost_center"
    #                 ),
    #                 "amount": abs(total_fees_bucket),
    #             },
    #         )

    #     pe.insert(ignore_permissions=True)
    #     pe.submit()

    #     return {
    #         "ok": True,
    #         "payment_entry": pe.name,
    #         "orders_matched": len(order_rows),
    #         "credit_notes_created": len(created_credit_notes),
    #         "missing_invoices": missing_invoices[:20],
    #         "final_payout": final_payout,
    #     }
    


    # ============================================================
    
        
    
        
        
        
        
        
        
        
    def process_settlement_csv(
    self,
    file_url: str,
    company: str = None,
    amazon_bank_account: str = None,
    receivable_account: str = None,
    amazon_fees_account: str = None,
    currency: str = None,
    cost_center: str = None,
    exchange_rate: float = 1.0,
    dry_run: bool = False,
):
        """
        Amazon Settlement CSV Processor - WITH CROSS-PERIOD REFUND HANDLING
        FIXED:
        - Previous files ke paid invoices ko recognize karta hai
        - Refund aane par pehle se paid invoice ko credit note se adjust karta hai
        - Accounting sahi hoti hai chahe refund next file mein aaye
        """
        import csv
        import io
        import re
        import hashlib
        from datetime import datetime
        from decimal import Decimal

        import frappe
        from frappe.utils import nowdate
        from erpnext.accounts.party import get_party_account

        # --------------------------------------------------------
        # 1️⃣ DEFAULTS (same as before)
        # --------------------------------------------------------
        if not company:
            company = self.amz_setting.company

        if not amazon_bank_account:
            amazon_bank_account = self.amz_setting.amazon_payout_account

        if not receivable_account:
            customer = self.get_amazon_customer()
            receivable_account = get_party_account("Customer", customer, company)

        if not amazon_fees_account:
            amazon_fees_account = self.amz_setting.amazon_fee_account

        if not currency:
            currency = frappe.db.get_value("Company", company, "default_currency")

        if not cost_center:
            cost_center = frappe.db.get_value("Company", company, "cost_center")

        FULL_PAYMENT_TOLERANCE = 0.05

        # --------------------------------------------------------
        # 2️⃣ SETTLEMENT KEY (idempotency)
        # --------------------------------------------------------
        file_hash = hashlib.sha256(f"{file_url}::{company}".encode()).hexdigest()[:32]
        settlement_key = f"AMZ-CSV-{file_hash}"

        existing_pe = frappe.db.get_value(
            "Payment Entry",
            {"custom_amazon_settlement_key": settlement_key, "docstatus": ["<", 2]},
            "name",
        )
        if existing_pe:
            return {
                "ok": True,
                "message": "Settlement already processed",
                "payment_entry": existing_pe,
                "settlement_key": settlement_key,
            }

        # --------------------------------------------------------
        # 3️⃣ LOAD CSV/EXCEL
        # --------------------------------------------------------
        file_doc = frappe.get_doc("File", {"file_url": file_url})
        content = file_doc.get_content()

        if isinstance(content, bytes):
            content = content.decode("utf-8", errors="ignore")

        if content.startswith("\ufeff"):
            content = content[1:]

        first_line = content.splitlines()[0] if content.splitlines() else ""
        delimiter = "\t" if "\t" in first_line else ","

        # --------------------------------------------------------
        # 4️⃣ PARSE CSV
        # --------------------------------------------------------
        string_io = io.StringIO(content)
        reader = csv.DictReader(string_io, delimiter=delimiter)

        rows = []
        for row in reader:
            normalized = {}
            for k, v in row.items():
                if k:
                    clean_key = k.strip().lower().replace(" ", "_").replace("-", "_")
                    clean_value = v.strip().strip('"') if v else ""
                    normalized[clean_key] = clean_value
            rows.append(normalized)

        if not rows:
            frappe.throw("No data rows found in settlement file")

        data_rows = []
        for row in rows:
            order_id = row.get("order_id", "")
            if order_id and not order_id.startswith("=") and order_id != "---":
                data_rows.append(row)

        if not data_rows:
            data_rows = rows

        # --------------------------------------------------------
        # 5️⃣ GENERATE SETTLEMENT ID
        # --------------------------------------------------------
        file_name = file_doc.file_name or "settlement"
        date_match = re.search(r"(\d{1,2}[_-]\d{1,2}[_-]\d{4})", file_name)
        if date_match:
            settlement_id = f"CSV-{date_match.group(1)}"
        else:
            settlement_id = f"CSV-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        settlement_start = ""
        settlement_end = ""

        # --------------------------------------------------------
        # 6️⃣ COLUMN DETECTION
        # --------------------------------------------------------
        headers = list(data_rows[0].keys()) if data_rows else []

        def find_column(possible_names):
            for name in possible_names:
                for header in headers:
                    if name.lower() in header.lower():
                        return header
            return None

        col_order_id = find_column(["order_id", "orderid", "order id", "order"])
        col_type = find_column(["transaction_type", "transaction type", "type"])
        col_product = find_column(
            ["total_product_charges", "total product charges", "product_charges", "product charges"]
        )
        col_rebate = find_column(
            ["total_promotional_rebates", "total promotional rebates", "promotional_rebates", "promotional rebates"]
        )
        col_fees = find_column(["amazon_fees", "amazon fees", "selling_fees", "selling fees", "fees"])
        col_other = find_column(["other", "other_amount", "other amount"])

        # --------------------------------------------------------
        # 7️⃣ CLASSIFIER
        # --------------------------------------------------------
        ORDER_KEYWORDS = ["order", "shipment", "payment", "sale", "capture"]
        REFUND_KEYWORDS = ["refund", "return", "chargeback", "reversal", "credit"]

        def safe_decimal(x) -> Decimal:
            try:
                return Decimal(str(x or "0"))
            except Exception:
                return Decimal("0")

        def classify_row(row):
            ttype = (row.get(col_type) or "").lower()
            product = safe_decimal(row.get(col_product, "0")) if col_product else Decimal("0")

            if any(k in ttype for k in REFUND_KEYWORDS):
                return "refund"
            if product < 0:
                return "refund"
            if product > 0 and any(k in ttype for k in ORDER_KEYWORDS):
                return "order"
            return "fee"

        # --------------------------------------------------------
        # 8️⃣ CALCULATE TOTALS
        # --------------------------------------------------------
        total_product = Decimal("0")
        total_rebate = Decimal("0")
        total_fees = Decimal("0")
        total_other = Decimal("0")

        for row in rows:
            if col_product:
                total_product += safe_decimal(row.get(col_product, "0"))
            if col_rebate:
                total_rebate += safe_decimal(row.get(col_rebate, "0"))
            if col_fees:
                total_fees += safe_decimal(row.get(col_fees, "0"))
            if col_other:
                total_other += safe_decimal(row.get(col_other, "0"))

        total_fees_bucket = total_rebate + total_fees + total_other
        final_payout = total_product + total_fees_bucket

        # ✅ SIMPLE FLAG: Seller Amazon ko pay karega?
        is_payment_to_amazon = final_payout < Decimal("0")

        # --------------------------------------------------------
        # 9️⃣ SEPARATE ORDER AND REFUND ROWS
        # --------------------------------------------------------
        order_rows = []
        refund_rows = []

        for row in data_rows:
            category = classify_row(row)
            if category == "order":
                order_rows.append(row)
            elif category == "refund":
                refund_rows.append(row)

        # --------------------------------------------------------
        # 🔟 TRACK PAID INVOICES ACROSS FILES
        # --------------------------------------------------------
        # Yeh dictionary track karegi ke kis order ID ki invoice already paid hai
        # chahe wo current file mein ho ya pehle ki files mein
        paid_invoices_map = {}

        # Pehle check karo previously settled payments
        previous_settlements = frappe.db.get_all(
            "Payment Entry",
            filters={
                "custom_amazon_settlement_key": ["is", "set"],
                "docstatus": 1,
                "party": self.get_amazon_customer()
            },
            fields=["name", "custom_amazon_settlement_key"]
        )
        
        for settlement in previous_settlements:
            pe_doc = frappe.get_doc("Payment Entry", settlement.name)
            for ref in pe_doc.references:
                if ref.reference_doctype == "Sales Invoice":
                    invoice = frappe.get_doc("Sales Invoice", ref.reference_name)
                    if invoice.custom_amazon_order_id:
                        paid_invoices_map[invoice.custom_amazon_order_id] = {
                            "invoice": invoice.name,
                            "paid_amount": abs(ref.allocated_amount),
                            "payment_entry": pe_doc.name
                        }

        # --------------------------------------------------------
        # 1️⃣1️⃣ PROCESS ORDER PAYMENTS
        # --------------------------------------------------------
        order_payments = []
        missing_invoices = []
        skipped_zero_outstanding = []
        skipped_small_product = []

        processed_order_ids = set()

        for row in order_rows:
            product = safe_decimal(row.get(col_product, "0") if col_product else "0")
            order_id = (row.get(col_order_id) or "").strip().strip('"')

            if not order_id or order_id == "---":
                continue

            processed_order_ids.add(order_id)

            # Pehle check karo ke yeh order already paid hai kya previous settlement mein
            if order_id in paid_invoices_map:
                # Agar already paid hai to skip karo - yeh duplicate payment hai
                frappe.log_error(
                    f"Order {order_id} already paid in settlement {paid_invoices_map[order_id]['payment_entry']}, skipping",
                    "Amazon Order Already Paid"
                )
                continue

            si_name = frappe.db.get_value(
                "Sales Invoice",
                {"custom_amazon_order_id": order_id, "docstatus": 1},
                "name",
            )

            if not si_name:
                missing_invoices.append(order_id)
                continue

            outstanding = frappe.db.get_value("Sales Invoice", si_name, "outstanding_amount") or 0
            outstanding = Decimal(str(outstanding))

            if outstanding <= Decimal(str(FULL_PAYMENT_TOLERANCE)):
                skipped_zero_outstanding.append({
                    "order_id": order_id,
                    "invoice": si_name,
                    "outstanding": float(outstanding),
                    "product": float(product),
                })
                continue

            alloc = min(product, outstanding)

            remaining_after_alloc = outstanding - alloc
            if remaining_after_alloc <= Decimal(str(FULL_PAYMENT_TOLERANCE)):
                alloc = outstanding

            if alloc <= Decimal(str(FULL_PAYMENT_TOLERANCE)):
                skipped_small_product.append({
                    "order_id": order_id,
                    "invoice": si_name,
                    "outstanding": float(outstanding),
                    "product": float(product),
                    "alloc": float(alloc),
                })
                continue

            order_payments.append({"invoice": si_name, "order_id": order_id, "amount": alloc})

        # --------------------------------------------------------
        # 1️⃣2️⃣ PROCESS REFUNDS (CROSS-PERIOD HANDLING)
        # --------------------------------------------------------
        credit_notes_created = []
        refund_alloc_map = {}

        for row in refund_rows:
            product = safe_decimal(row.get(col_product, "0") if col_product else "0")
            order_id = (row.get(col_order_id) or "").strip().strip('"')

            if not order_id or order_id == "---":
                continue

            if order_id in processed_order_ids:
                frappe.log_error(
                    f"Order {order_id} already processed as payment, now processing refund",
                    "Amazon Refund Info"
                )

            # 🔥 IMPORTANT: Pehle check karo ke yeh order pehle ki file mein paid hua tha?
            original_si = None
            already_paid_in_previous = order_id in paid_invoices_map
            
            if already_paid_in_previous:
                # Agar pehle ki file mein paid hai, to wohi invoice use karo
                original_si = paid_invoices_map[order_id]["invoice"]
                frappe.log_error(
                    f"Refund for order {order_id} - using previously paid invoice {original_si} from settlement {paid_invoices_map[order_id]['payment_entry']}",
                    "Amazon Cross-Period Refund"
                )
            else:
                # Nahi to current file mein dhundho
                original_si = frappe.db.get_value(
                    "Sales Invoice",
                    {"custom_amazon_order_id": order_id, "docstatus": 1},
                    "name",
                )

            if not original_si:
                frappe.log_error(
                    f"Refund for order {order_id} - no original invoice found",
                    "Amazon Refund"
                )
                continue

            # Check for existing credit note
            existing_cn = frappe.db.get_value(
                "Sales Invoice",
                {"return_against": original_si, "docstatus": ["<", 2]},
                "name",
            )

            cn_name = None
            if existing_cn:
                cn_name = existing_cn
                frappe.log_error(
                    f"Using existing credit note {cn_name} for refund {order_id}",
                    "Amazon Refund Info"
                )
            else:
                if not dry_run:
                    try:
                        si_doc = frappe.get_doc("Sales Invoice", original_si)

                        cn = frappe.copy_doc(si_doc)
                        cn.is_return = 1
                        cn.return_against = original_si
                        cn.update_stock = 1
                        cn.custom_amazon_settlement_id = settlement_id
                        cn.custom_amazon_order_id = order_id
                        
                        # 🔥 IMPORTANT: Mark as cross-period refund
                        cn.custom_is_cross_period_refund = 1 if already_paid_in_previous else 0

                        for item in cn.items:
                            item.qty = -abs(item.qty)
                            if hasattr(item, "stock_qty"):
                                item.stock_qty = -abs(item.stock_qty)
                            if hasattr(item, "rejected_qty"):
                                item.rejected_qty = -abs(item.rejected_qty) if item.rejected_qty else 0

                        cn.set("payments", [])
                        cn.insert(ignore_permissions=True)
                        cn.submit()

                        cn_name = cn.name
                        credit_notes_created.append(cn.name)
                        
                        # 🔥 Agar pehle paid invoice thi, to uske outstanding ko update karo
                        if already_paid_in_previous:
                            frappe.db.set_value("Sales Invoice", original_si, "status", "Return")
                            frappe.db.set_value("Sales Invoice", original_si, "outstanding_amount", 0)
                            
                        frappe.log_error(
                            f"Created new credit note {cn_name} for refund {order_id} (cross-period: {already_paid_in_previous})",
                            "Amazon Refund Success"
                        )

                    except Exception as e:
                        frappe.log_error(
                            f"Failed to create credit note for {order_id}: {str(e)}",
                            "Amazon Credit Note Error",
                        )
                        cn_name = None

            if cn_name:
                refund_amount = abs(product)
                refund_alloc_map[cn_name] = refund_alloc_map.get(cn_name, Decimal("0")) + refund_amount

        # --------------------------------------------------------
        # 1️⃣3️⃣ AUDIT LOG
        # --------------------------------------------------------
        frappe.log_error(
            f"Amazon Settlement Audit:\n"
            f"Settlement ID: {settlement_id}\n"
            f"Total Product (net): {total_product}\n"
            f"Total Rebate: {total_rebate}\n"
            f"Total Fees: {total_fees}\n"
            f"Total Other: {total_other}\n"
            f"Fees Bucket (ALL charges): {total_fees_bucket}\n"
            f"Final Payout (deposit): {final_payout}\n"
            f"Is Payment To Amazon: {is_payment_to_amazon}\n"
            f"Order Payments: {len(order_payments)}\n"
            f"Refund CN count: {len(refund_alloc_map)}\n"
            f"Previously Paid Orders in Refund: {len([r for r in refund_rows if r.get(col_order_id, '') in paid_invoices_map])}",
            "Amazon Settlement Debug",
        )

        # --------------------------------------------------------
        # 1️⃣4️⃣ MODE OF PAYMENT HANDLING
        # --------------------------------------------------------
        mode_of_payment = "Amazon Settlement"
        if not frappe.db.exists("Mode of Payment", mode_of_payment):
            try:
                mop = frappe.new_doc("Mode of Payment")
                mop.mode_of_payment = mode_of_payment
                mop.enabled = 1
                mop.type = "Bank"

                if amazon_bank_account:
                    mop.append("accounts", {"company": company, "default_account": amazon_bank_account})

                mop.insert(ignore_permissions=True)
                frappe.db.commit()
            except Exception as e:
                frappe.log_error(f"Could not create Mode of Payment: {str(e)}", "Amazon Settlement")
                mode_of_payment = "Bank"

        # --------------------------------------------------------
        # 1️⃣5️⃣ DRY RUN / NO ORDERS
        # --------------------------------------------------------
        if dry_run or not order_payments:
            return {
                "ok": bool(order_payments),
                "settlement_id": settlement_id,
                "orders_matched": len(order_payments),
                "refund_cn_count": len(refund_alloc_map),
                "fees_bucket": float(total_fees_bucket),
                "final_payout": float(final_payout),
                "payment_direction": f"⚠️ SELLER AMAZON KO PAY KAREGA: {abs(float(final_payout))}" if is_payment_to_amazon else f"✅ AMAZON SELLER KO PAY KAREGA: {float(final_payout)}",
                "total_product": float(total_product),
                "missing_invoices": missing_invoices[:20],
                "skipped_zero_outstanding": skipped_zero_outstanding[:50],
                "skipped_small_product": skipped_small_product[:50],
                "credit_notes": credit_notes_created,
                "dry_run": dry_run,
                "settlement_key": settlement_key,
                "message": "Preview mode - no entries created" if dry_run else "No orders matched",
                "cross_period_refunds": len([r for r in refund_rows if r.get(col_order_id, '') in paid_invoices_map]),
            }

        # --------------------------------------------------------
        # 1️⃣6️⃣ PAYMENT ENTRY CREATION
        # --------------------------------------------------------
        pe = frappe.new_doc("Payment Entry")
        pe.payment_type = "Receive"
        pe.company = company
        pe.party_type = "Customer"
        pe.party = self.get_amazon_customer()
        pe.posting_date = nowdate()
        pe.mode_of_payment = mode_of_payment

        pe.paid_from = receivable_account
        pe.paid_to = amazon_bank_account

        pe.source_exchange_rate = exchange_rate
        pe.target_exchange_rate = exchange_rate

        pe.reference_no = f"AMZ-SET-{settlement_id}"
        pe.reference_date = nowdate()

        pe.custom_amazon_settlement_key = settlement_key
        pe.custom_amazon_settlement_id = settlement_id
        pe.custom_settlement_start_date = settlement_start
        pe.custom_settlement_end_date = settlement_end

        # ✅ REMARKS MEIN CLEARLY NEGATIVE SIGN AUR WARNING
        if is_payment_to_amazon:
            pe.remarks = (
                f"⚠️ AMAZON KO PAY KARNA HAI: -{abs(float(final_payout))} | "
                f"Settlement {settlement_id} | Orders: {len(order_payments)} "
                f"| Refund CNs: {len(refund_alloc_map)}"
            )
        else:
            pe.remarks = (
                f"Amazon Settlement {settlement_id} | Orders: {len(order_payments)} "
                f"| Refund CNs: {len(refund_alloc_map)}"
            )

        # --------------------------------------------------------
        # References (Orders positive + Refund CN negative)
        # --------------------------------------------------------
        total_allocated = Decimal("0")

        for payment in order_payments:
            pe.append(
                "references",
                {
                    "reference_doctype": "Sales Invoice",
                    "reference_name": payment["invoice"],
                    "allocated_amount": float(payment["amount"]),
                },
            )
            total_allocated += payment["amount"]

        for cn_name, refund_amt in refund_alloc_map.items():
            if refund_amt <= Decimal("0"):
                continue

            cn_outstanding = frappe.db.get_value(
                "Sales Invoice", cn_name, "outstanding_amount"
            ) or 0
            cn_outstanding = Decimal(str(abs(cn_outstanding)))

            alloc_refund = min(refund_amt, cn_outstanding)

            if alloc_refund <= Decimal(str(FULL_PAYMENT_TOLERANCE)):
                continue

            pe.append(
                "references",
                {
                    "reference_doctype": "Sales Invoice",
                    "reference_name": cn_name,
                    "allocated_amount": -float(alloc_refund),
                },
            )
            total_allocated -= alloc_refund

        pe.total_allocated_amount = float(total_allocated)
        net_amount = float(total_allocated) + float(total_fees_bucket)

        pe.paid_amount = float(net_amount)
        pe.received_amount = float(net_amount)

        # --------------------------------------------------------
        # Deductions
        # --------------------------------------------------------
        if total_fees_bucket < Decimal("-0.001"):
            pe.append(
                "deductions",
                {
                    "account": amazon_fees_account,
                    "cost_center": cost_center,
                    "amount": abs(float(total_fees_bucket)),
                    "description": f"Amazon charges for settlement {settlement_id}",
                },
            )

        # Consistency check
        net_amount_check = float(total_allocated) + float(total_fees_bucket)
        if abs(net_amount_check - float(final_payout)) > 0.02:
            frappe.log_error(
                f"Payment Entry amount mismatch:\n"
                f"Net Amount: {net_amount_check}\n"
                f"Final Payout: {float(final_payout)}\n"
                f"Difference: {net_amount_check - float(final_payout)}",
                "Amazon Payment Mismatch",
            )

        pe.insert(ignore_permissions=True)
        pe.submit()

        return {
            "ok": True,
            "payment_entry": pe.name,
            "payment_direction": f"⚠️ SELLER PAID AMAZON: -{abs(float(final_payout))}" if is_payment_to_amazon else f"✅ AMAZON PAID SELLER: {float(final_payout)}",
            "settlement_id": settlement_id,
            "orders_matched": len(order_payments),
            "refund_cn_count": len(refund_alloc_map),
            "credit_notes_created": len(credit_notes_created),
            "missing_invoices": missing_invoices[:20],
            "final_payout": float(final_payout),        
            "total_product": float(total_product),
            "fees_bucket": float(total_fees_bucket),
            "total_allocated_refs_net": float(total_allocated),
            "settlement_key": settlement_key,
            "cross_period_refunds": len([r for r in refund_rows if r.get(col_order_id, '') in paid_invoices_map]),
        }

    # -------------------------
    # ✅ FETCH GROUP EVENTS (with pagination)
    # -------------------------
    def get_group_financial_events(self, group_id: str):
        finances = self.get_finances_instance()

        res = finances.list_financial_events_by_group_id(
            group_id=group_id,
            max_results=100,
        )
        payload = res.get("payload") if isinstance(res, dict) and res.get("payload") else res
        if not payload:
            return {}

        all_events = []
        while True:
            fe = payload.get("FinancialEvents", {}) or {}
            all_events.append(fe)

            next_token = payload.get("NextToken")
            if not next_token:
                break

            res = finances.list_financial_events_by_group_id(
                group_id=group_id,
                next_token=next_token,
                max_results=100,
            )
            payload = res.get("payload") if isinstance(res, dict) and res.get("payload") else res
            if not payload:
                break

        # merge event dicts
        merged = {}
        for ev in all_events:
            for k, v in ev.items():
                if not v:
                    continue
                merged.setdefault(k, [])
                merged[k].extend(v)

        return merged

 

   

        # -------------------------------------------------------------------------
    # SETTLEMENT REPORT PROCESSING (CORRECTED)
    # -------------------------------------------------------------------------

    def get_settlement_reports(self, days_back: int = 90):
        finances = self.get_finances_instance()

        posted_before = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        posted_after = (datetime.utcnow() - timedelta(days=days_back)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        payload = self.call_sp_api_method(
            finances.list_financial_event_groups,
            PostedAfter=posted_after,
            PostedBefore=posted_before,
            MaxResultsPerPage=100,
        )

        if not payload:
            return []

        # payload is already correct — DO NOT unwrap again
        groups = payload.get("FinancialEventGroupList", [])

        settlements = []
        for g in groups:
            settlements.append({
                "group_id": g.get("FinancialEventGroupId"),
                "start_date": g.get("FundTransferDate") or g.get("StartDate"),
                "end_date": g.get("FundTransferDate") or g.get("EndDate"),
                "status": g.get("ProcessingStatus"),
                "currency": g.get("OriginalTotal", {}).get("CurrencyCode"),
                "original_amount": float(
                    g.get("OriginalTotal", {}).get("CurrencyAmount", 0) or 0
                ),
                "fund_transfer_date": g.get("FundTransferDate"),
            })

        return settlements




    def get_settlement_details(self, group_id: str):
        """
        Get detailed transactions for a settlement period.
        """
        finances = self.get_finances_instance()
        
        try:
            payload = self.call_sp_api_method(
                finances.list_financial_events_by_group_id,
                group_id=group_id,
                max_results=100
            )
        except Exception as e:
            frappe.log_error(f"Cannot load settlement details {group_id}: {e}", "Amazon Settlement Error")
            return None

        if not payload:
            return None

        details = {
            "group_id": group_id,
            "shipments": [],
            "refunds": [],
            "service_fees": [],
            "other_transactions": [],
            "total_net": 0.0
        }

        while True:
            fevents = payload.get("FinancialEvents", {})

            # 1. Shipment Events (Sales)
            for evt in fevents.get("ShipmentEventList", []):
                shipment = {
                    "order_id": evt.get("AmazonOrderId"),
                    "posted_date": evt.get("PostedDate"),
                    "items": [],
                    "total": 0.0
                }
                
                for item in evt.get("ShipmentItemList", []):
                    item_details = {
                        "sku": item.get("SellerSKU"),
                        "quantity": item.get("QuantityShipped"),
                        "item_charges": 0.0,
                        "item_fees": 0.0,
                        "promotions": 0.0
                    }
                    
                    # Item Charges (Revenue)
                    for charge in item.get("ItemChargeList", []):
                        amount = float(charge.get("ChargeAmount", {}).get("CurrencyAmount", 0))
                        item_details["item_charges"] += amount
                    
                    # Item Fees (Costs)
                    for fee in item.get("ItemFeeList", []):
                        amount = float(fee.get("FeeAmount", {}).get("CurrencyAmount", 0))
                        item_details["item_fees"] += amount
                    
                    # Promotions (Discounts)
                    for promo in item.get("PromotionList", []):
                        amount = float(promo.get("PromotionAmount", {}).get("CurrencyAmount", 0))
                        item_details["promotions"] += amount
                    
                    shipment["items"].append(item_details)
                    shipment["total"] += item_details["item_charges"] - item_details["item_fees"] - item_details["promotions"]
                
                details["shipments"].append(shipment)
                details["total_net"] += shipment["total"]

            # 2. Refund Events
            for evt in fevents.get("RefundEventList", []):
                refund = {
                    "order_id": evt.get("AmazonOrderId"),
                    "posted_date": evt.get("PostedDate"),
                    "total": 0.0
                }
                
                for item in evt.get("RefundItemList", []):
                    # Refund adjustments (negative amounts)
                    for adj in item.get("ItemChargeAdjustmentList", []):
                        amount = float(adj.get("ChargeAmount", {}).get("CurrencyAmount", 0))
                        refund["total"] += amount
                
                details["refunds"].append(refund)
                details["total_net"] += refund["total"]

            # 3. Service Fee Events
            for evt in fevents.get("ServiceFeeEventList", []):
                fee_total = 0.0
                for fee in evt.get("FeeList", []):
                    fee_total += float(fee.get("FeeAmount", {}).get("CurrencyAmount", 0))
                
                details["service_fees"].append({
                    "description": evt.get("FeeDescription"),
                    "total": fee_total,
                    "order_id": evt.get("AmazonOrderId")
                })
                details["total_net"] -= fee_total  # Fees reduce net amount

            # 4. Other Transactions
            # (Add other event types as needed)

            # Pagination
            next_token = payload.get("NextToken")
            if not next_token:
                break

            try:
                payload = self.call_sp_api_method(
                    finances.list_financial_events_by_group_id,
                    group_id=group_id,
                    next_token=next_token
                )
            except Exception as e:
                frappe.log_error(f"Pagination error for {group_id}: {e}", "Amazon Settlement Error")
                break

        return details

    def create_settlement_payment_entry(self, settlement):
        """
        Create ONE payment entry for an entire settlement period.
        """
        # Check if payment already exists for this settlement
        existing_pe = frappe.db.get_value(
            "Payment Entry",
            {"custom_amazon_settlement_id": settlement["group_id"], "docstatus": ["<", 2]},
            "name"
        )
        
        if existing_pe:
            frappe.log_error(f"Payment already exists for settlement {settlement['group_id']}", "Amazon Settlement")
            return existing_pe

        # Get default currency
        company_currency = frappe.db.get_value("Company", self.amz_setting.company, "default_currency")
        settlement_currency = settlement.get("currency", "USD")
        
        # Determine exchange rate
        if settlement_currency == company_currency:
            exchange_rate = 1
        else:
            exchange_rate = frappe.utils.get_exchange_rate(settlement_currency, company_currency)
        
        # Calculate amounts
        original_amount = settlement.get("original_amount", 0)
        received_amount = original_amount * exchange_rate
        
        if original_amount <= 0:
            frappe.log_error(f"Settlement {settlement['group_id']} has zero or negative amount", "Amazon Settlement")
            return None

        invoices = self.get_invoices_for_settlement(settlement) or []


        # Create Payment Entry
        pe = frappe.new_doc("Payment Entry")
        pe.payment_type = "Receive"
        pe.company = self.amz_setting.company
        pe.party_type = "Customer"
        
        # Use a generic Amazon customer or the first invoice's customer
        pe.party = self.get_amazon_customer()
        
        # Multi-currency fields
        pe.paid_amount = original_amount
        pe.paid_from_account_currency = settlement_currency
        pe.received_amount = received_amount
        pe.received_to_account_currency = company_currency
        pe.source_exchange_rate = exchange_rate
        
        pe.reference_no = f"Amazon Settlement {settlement['group_id']}"
        pe.reference_date = settlement.get("fund_transfer_date") or frappe.utils.today()
        pe.custom_amazon_settlement_id = settlement["group_id"]
        pe.custom_settlement_start_date = settlement.get("start_date")
        pe.custom_settlement_end_date = settlement.get("end_date")
        pe.custom_settlement_status = settlement.get("status")

        # Get payout account from settings
        payout_acct = frappe.db.get_value(
            "Amazon SP API Settings", 
            self.amz_setting.name, 
            "amazon_payout_account"
        )
        
        if not payout_acct:
            # Try to find or create Amazon Payout account
            payout_acct = frappe.db.get_value(
                "Account",
                {"account_name": "Amazon Payout Account", "company": self.amz_setting.company},
                "name"
            )
            
            if not payout_acct:
                # Create Amazon Payout account
                payout_acct = self.create_account(
                    "Amazon Payout Account",
                    self.amz_setting.market_place_account_group
                )

        pe.paid_to = payout_acct

        # Add references to invoices
        total_allocated = 0
        for invoice in invoices:
            allocated = invoice.get("outstanding_amount", invoice.get("grand_total", 0))
            pe.append(
                "references",
                {
                    "reference_doctype": "Sales Invoice",
                    "reference_name": invoice["name"],
                    "allocated_amount": allocated,
                    "outstanding_amount": invoice.get("outstanding_amount", 0),
                    "total_amount": invoice.get("grand_total", 0)
                },
            )
            total_allocated += allocated

        # Check allocation matches
        if abs(total_allocated - received_amount) > 0.01:  # Allow small rounding differences
            frappe.msgprint(
                _("Allocation mismatch: Settlement ${0} vs Invoices ${1}").format(
                    received_amount, total_allocated
                ),
                indicator="orange"
            )

        try:
            pe.insert(ignore_permissions=True)
            pe.submit()
            
            # Update invoices with settlement reference
            for invoice in invoices:
                frappe.db.set_value(
                    "Sales Invoice",
                    invoice["name"],
                    "custom_amazon_settlement_id",
                    settlement["group_id"]
                )
            
            frappe.log_error(
                f"Created Payment Entry {pe.name} for Amazon Settlement {settlement['group_id']}",
                "Amazon Settlement"
            )
            
            return pe.name
            
        except Exception as e:
            frappe.log_error(
                f"Failed to create payment entry for settlement {settlement['group_id']}: {str(e)[:200]}",
                "Amazon Settlement Error"
            )
            return None
        


    def get_invoices_for_settlement(self, settlement):
        """
        Find all Sales Invoices that belong to this settlement period.
        Based on order dates matching settlement period.
        """
        start_date = settlement.get("start_date")
        end_date = settlement.get("end_date")
        
        if not start_date or not end_date:
            return []
        
        # Parse dates
        try:
            start = normalize_date(start_date)
            end = normalize_date(end_date)

        except:
            # Try alternative format
            try:
                start = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S.%fZ").date()
                end = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S.%fZ").date()
            except:
                frappe.log_error(f"Cannot parse dates: {start_date}, {end_date}", "Amazon Settlement")
                return []

        # Find invoices for orders within this period
        invoices = frappe.get_all(
            "Sales Invoice",
            filters={
                "custom_amazon_order_id": ["is", "set"],
                "posting_date": ["between", [start, end]],
                "docstatus": 1,  # Only submitted invoices
                "outstanding_amount": [">", 0]  # Only unpaid
            },
            fields=["name", "customer", "grand_total", "outstanding_amount", "posting_date", "custom_amazon_order_id"]
        )
        
        return invoices
    

    
   


    def get_amazon_customer(self):
        """
        Always return the single Amazon Customer configured in settings.
        Auto-create 'Amazon Marketplace' if missing and settings is empty.
        """
        # 1) If user selected customer in settings, use it
        selected = frappe.db.get_value(
            "Amazon SP API Settings", self.amz_setting.name, "custom_customer"
        )
        if selected and frappe.db.exists("Customer", selected):
            return selected

        # 2) fallback: create/find "Amazon Marketplace"
        customer_name = "Amazon Marketplace"
        existing = frappe.db.get_value("Customer", {"customer_name": customer_name}, "name")
        if existing:
            # save into settings for future
            frappe.db.set_value("Amazon SP API Settings", self.amz_setting.name, "custom_customer", existing)
            return existing

        c = frappe.new_doc("Customer")
        c.customer_name = customer_name
        c.customer_group = self.amz_setting.customer_group
        c.territory = self.amz_setting.territory
        c.customer_type = self.amz_setting.customer_type
        c.insert(ignore_permissions=True)

        frappe.db.set_value("Amazon SP API Settings", self.amz_setting.name, "custom_customer", c.name)
        return c.name


    def create_account(self, account_name, parent_account):
        """Create account if doesn't exist"""
        acct = frappe.new_doc("Account")
        acct.account_name = account_name
        acct.company = self.amz_setting.company
        acct.parent_account = parent_account
        acct.account_type = "Bank"
        acct.insert(ignore_permissions=True)
        return acct.name


    def calculate_amazon_reconciliation_totals(self, order_id: str) -> dict:
        """
        Returns NON-accounting reconciliation totals from Amazon Finances API.
        """
        details = self.get_settlement_details_by_order(order_id)

        return {
            "item_total": details.get("item_total", 0),
            "tax_collected": details.get("tax_collected", 0),
            "shipping": details.get("shipping", 0),
            "promotions": details.get("promotions", 0),
            "fees": details.get("fees", 0),
            "net_proceeds": details.get("net_proceeds", 0),
        }


    

    
    def get_settlement_details_by_order(self, order_id):
        """
        Lightweight reconciliation data per order.
        """
        finances = self.get_finances_instance()
        payload = self.call_sp_api_method(
            finances.list_financial_events_by_order_id,
            order_id=order_id,
        )

        totals = {
            "item_total": 0,
            "tax_collected": 0,
            "shipping": 0,
            "promotions": 0,
            "fees": 0,
            "net_proceeds": 0,
        }

        if not payload:
            return totals

        events = payload.get("FinancialEvents", {})

        for shipment in events.get("ShipmentEventList", []):
            for item in shipment.get("ShipmentItemList", []):
                for c in item.get("ItemChargeList", []):
                    amt = float(c.get("ChargeAmount", {}).get("CurrencyAmount", 0) or 0)
                    ctype = c.get("ChargeType")
                    if ctype == "Principal":
                        totals["item_total"] += amt
                    elif "Tax" in ctype:
                        totals["tax_collected"] += amt
                    elif "Shipping" in ctype:
                        totals["shipping"] += amt

                for p in item.get("PromotionList", []):
                    totals["promotions"] += abs(
                        float(p.get("PromotionAmount", {}).get("CurrencyAmount", 0) or 0)
                    )

                for f in item.get("ItemFeeList", []):
                    totals["fees"] += abs(
                        float(f.get("FeeAmount", {}).get("CurrencyAmount", 0) or 0)
                    )

        totals["net_proceeds"] = (
            totals["item_total"]
            + totals["shipping"]
            + totals["tax_collected"]
            - totals["promotions"]
            - totals["fees"]
        )

        return totals





        

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
        frappe.log_error(
            frappe.as_json(payload),
            "AMAZON ORDER DEBUG"
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

                item_tax = float(oi.get("ItemTax", {}).get("Amount", 0) or 0)
                ship_price = float(oi.get("ShippingPrice", {}).get("Amount", 0) or 0)
                ship_tax = float(oi.get("ShippingTax", {}).get("Amount", 0) or 0)

                item_code = self.get_item_code(oi)

                # ERPNext fallback price
                erp_rate = frappe.db.get_value(
                    "Item Price",
                    {
                        "item_code": item_code,
                        "selling": 1
                    },
                    "price_list_rate"
                )

                # Amazon reported item price
                amazon_item_price = float(oi.get("ItemPrice", {}).get("Amount", 0) or 0)

                # ------------------------------------------------------------------
                # FIX: Amazon sometimes sends LINE TOTAL instead of UNIT PRICE
                # ------------------------------------------------------------------
                rate = amazon_item_price

                if qty and qty > 1 and amazon_item_price > 0:
                    rate = amazon_item_price / qty

                # ------------------------------------------------------------------
                # FALLBACK to ERPNext price if Amazon missing
                # ------------------------------------------------------------------
                if rate <= 0 and erp_rate and erp_rate > 0:
                    rate = erp_rate

                # ------------------------------------------------------------------
                # FINAL SAFETY
                # ------------------------------------------------------------------
                if rate <= 0:
                    frappe.log_error(
                        f"Amazon price missing for order {order_id}, item {item_code}. Using fallback price.",
                        "Amazon Pricing Warning"
                    )
                    rate = 0.01

                final_items.append({
                    "item_code": item_code,
                    "item_name": oi.get("SellerSKU"),
                    "description": oi.get("Title"),
                    "rate": rate,
                    "qty": qty,
                    "warehouse": warehouse,
                    "stock_uom": "Nos",
                    "conversion_factor": 1,

                    # Amazon reference fields (non-accounting)
                    "amazon_item_price": amazon_item_price,
                    "amazon_item_tax": item_tax,
                    "amazon_shipping": ship_price,
                    "amazon_shipping_tax": ship_tax,
                })

            if not next_token:
                break

            payload = self.call_sp_api_method(
                orders_api.get_order_items,
                order_id=order_id,
                next_token=next_token,
            )

        return final_items

    # -------------------------------------------------------------------------
    # SALES ORDER CREATION WITH DEADLOCK HANDLING
    # -------------------------------------------------------------------------

    def create_sales_order(self, order: dict) -> str | None:
        """
        Create Sales Order from Amazon Order
        - Uses single Amazon Marketplace customer
        - ERP Item Price FIRST, Amazon price fallback
        - Never allows zero item rate
        """

        order_id = order.get("AmazonOrderId")
        if not order_id:
            return None

        # ---------------- DUPLICATE CHECK ----------------
        existing = frappe.db.get_value(
            "Sales Order",
            {"amazon_order_id": order_id},
            "name"
        )
        if existing:
            return existing

        # ---------------- ITEMS ----------------
        items = self.get_order_items(order_id)
        if not items:
            frappe.log_error(f"No items found for Amazon Order {order_id}", "Amazon SO")
            return None

        # ---------------- CUSTOMER ----------------
        customer = self.get_amazon_customer()

        buyer = order.get("BuyerInfo") or {}
        buyer_email = buyer.get("BuyerEmail")

        # ---------------- SALES ORDER ----------------
        so = frappe.new_doc("Sales Order")
        so.customer = customer
        so.company = self.amz_setting.company
        so.amazon_order_id = order_id
        so.marketplace_id = order.get("MarketplaceId")

        if buyer_email:
            so.custom_amazon_buyer_email = buyer_email

        # ---------------- CURRENCY ----------------
        company_currency = frappe.db.get_value(
            "Company", so.company, "default_currency"
        )
        so.currency = company_currency
        so.conversion_rate = 1.0
        so.plc_conversion_rate = 1.0

        # ---------------- DATES ----------------
        if order.get("PurchaseDate"):
            so.transaction_date = dateutil.parser.parse(
                order.get("PurchaseDate")
            ).date()

        if order.get("LatestShipDate"):
            so.delivery_date = dateutil.parser.parse(
                order.get("LatestShipDate")
            ).date()

        # ---------------- ITEMS TABLE ----------------
        for it in items:
            if not it.get("rate") or it.get("rate") <= 0:
                frappe.throw(
                    f"Item price resolved as 0 for Item {it.get('item_code')}. "
                    "Please set Item Price in ERPNext."
                )

            so.append(
                "items",
                {
                    "item_code": it["item_code"],
                    "item_name": it.get("item_name"),
                    "description": it.get("description"),
                    "qty": it["qty"],
                    "rate": it["rate"],
                    "warehouse": it["warehouse"],
                    "stock_uom": it.get("stock_uom", "Nos"),
                    "conversion_factor": it.get("conversion_factor", 1),
                },
            )

        # ---------------- TAXES (OPTIONAL) ----------------
        if self.amz_setting.taxes_charges:
            charges_and_fees = self.get_charges_and_fees(order_id)

            for row in charges_and_fees.get("charges", []):
                so.append("taxes", row)

            for row in charges_and_fees.get("fees", []):
                so.append("taxes", row)

        # ---------------- INSERT + SUBMIT (SAFE) ----------------
        for attempt in range(3):
            try:
                so.insert(ignore_permissions=True)
                so.submit()
                break
            except frappe.QueryDeadlockError:
                if attempt < 2:
                    time.sleep(1 * (attempt + 1))
                    continue
                frappe.log_error(
                    f"Deadlock creating Sales Order for Amazon Order {order_id}",
                    "Amazon Deadlock"
                )
                return None

        return so.name


    # -------------------------------------------------------------------------
    # DELIVERY NOTE
    # -------------------------------------------------------------------------

    # def create_delivery_note_from_so(self, sales_order_name: str) -> str | None:
    #     try:
    #         existing = frappe.db.get_value(
    #             "Delivery Note",
    #             {"custom_against_sales_order": sales_order_name, "docstatus": ["<", 2]},
    #             "name",
    #         )
    #         if existing:
    #             return existing

    #         so = frappe.get_doc("Sales Order", sales_order_name)

    #         dn = frappe.new_doc("Delivery Note")
    #         dn.company = so.company
    #         dn.customer = self.get_amazon_customer()
    #         dn.custom_amazon_buyer_email = so.custom_amazon_buyer_email

    #         dn.currency = so.currency
    #         dn.custom_against_sales_order = so.name
    #         dn.custom_amazon_order_id = so.amazon_order_id

    #         for it in so.items:
    #             dn.append(
    #                 "items",
    #                 {
    #                     "item_code": it.item_code,
    #                     "item_name": it.item_name,
    #                     "description": it.description,
    #                     "qty": it.qty,
    #                     "rate": it.rate,
    #                     "warehouse": it.warehouse,
    #                     "uom": it.uom,
    #                     "conversion_factor": it.conversion_factor,
    #                     "against_sales_order": so.name,
    #                     "so_detail": it.name,
    #                     "allow_zero_valuation_rate": 1,
    #                 },
    #             )

    #         dn.insert(ignore_permissions=True)
    #         dn.submit()

    #         return dn.name

    #     except Exception as e:
    #         frappe.log_error(
    #             f"DN Error for SO {sales_order_name}: {str(e)[:100]}",
    #             "Amazon DN Error",
    #         )
    #         return None

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
            si.update_stock = 1
            si.set_update_stock = 1
            si.company = so.company
            si.customer = self.get_amazon_customer()
            si.custom_amazon_buyer_email = so.custom_amazon_buyer_email

            si.currency = so.currency
            si.conversion_rate = so.conversion_rate
            si.custom_against_sales_order = so.name
            si.custom_amazon_order_id = so.amazon_order_id
            si.due_date = frappe.utils.add_days(frappe.utils.today(), 7)

            # ---------------------------------------------------
            # COPY ITEMS FROM SALES ORDER
            # ---------------------------------------------------
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

            # ---------------------------------------------------
            # COPY TAX ROWS (ERPNext accounting taxes)
            # ---------------------------------------------------
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

            # ---------------------------------------------------
            # AMAZON SHIPPING + TAX (READ-ONLY TRACKING)
            # ---------------------------------------------------
            try:
                orders_api = self.get_orders_instance()

                payload = self.call_sp_api_method(
                    orders_api.get_order_items,
                    order_id=so.amazon_order_id,
                )

                total_tax = 0
                total_shipping = 0

                items = payload.get("OrderItems", []) if payload else []

                for oi in items:

                    item_tax = float(oi.get("ItemTax", {}).get("Amount", 0) or 0)
                    shipping_price = float(oi.get("ShippingPrice", {}).get("Amount", 0) or 0)
                    shipping_tax = float(oi.get("ShippingTax", {}).get("Amount", 0) or 0)

                    total_tax += item_tax + shipping_tax
                    total_shipping += shipping_price

                frappe.db.set_value(
                    "Sales Invoice",
                    si.name,
                    {
                        "custom_amazon_tax_collected": total_tax,
                        "custom_amazon_shipping": total_shipping,
                    },
                )

            except Exception as e:
                frappe.log_error(
                    f"Failed to fetch Amazon shipping/tax for order {so.amazon_order_id}: {str(e)}",
                    "Amazon Tax/Shipping Fetch Error",
                )

            return si.name

        except Exception as e:
            frappe.log_error(
                f"SI Error for SO {sales_order_name}: {str(e)[:100]}",
                "Amazon SI Error",
            )
            return None



  
    
    
    def update_sales_invoice(self, si_name: str, amazon_order: dict):
        """
        Update existing Sales Invoice and set Amazon reconciliation
        (non-accounting) fields ONLY after submission.
        """
        try:
            si = frappe.get_doc("Sales Invoice", si_name)
            order_id = amazon_order.get("AmazonOrderId")

            # Track if invoice was draft before this update
            was_draft = si.docstatus == 0

            # ❗ Do NOT modify accounting data after submission
            if was_draft:
                si.update_stock = 1
                # Update due date (allowed only in draft)
                new_due_date = frappe.utils.add_days(frappe.utils.today(), 7)
                if si.due_date != new_due_date:
                    si.due_date = new_due_date

                si.save(ignore_permissions=True)
                if si.docstatus == 0:
                    si.submit()

                self._update_amazon_reconciliation_fields(si)

                

                # ---------------- AMAZON RECONCILIATION (NON-ACCOUNTING) ----------------
                try:
                    totals = self.calculate_amazon_reconciliation_totals(order_id)

                    frappe.db.set_value(
                        "Sales Invoice",
                        si.name,
                        {
                            "custom_amazon_item_total": totals.get("item_total", 0),
                            "custom_amazon_tax_collected": totals.get("tax_collected", 0),
                            "custom_amazon_shipping": totals.get("shipping", 0),
                            "custom_amazon_promotions": totals.get("promotions", 0),
                            "custom_amazon_fees": totals.get("fees", 0),
                            "custom_amazon_net_proceeds": totals.get("net_proceeds", 0),
                        },
                        update_modified=False
                    )

                except Exception as e:
                    frappe.log_error(
                        f"Failed to set Amazon reconciliation fields for SI {si.name}: {str(e)[:150]}",
                        "Amazon Reconciliation"
                    )

            else:
                # Invoice already submitted → read-only
                frappe.logger().info(
                    f"Sales Invoice {si.name} already submitted. Skipping reconciliation update."
                )

            frappe.db.commit()

        except Exception as e:
            frappe.log_error(
                f"Failed to update Sales Invoice {si_name}: {str(e)[:150]}",
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
                dn.save(ignore_permissions=True)
                # Get updated order items
                # updated_items = self.get_order_items(order_id)
                # self.update_delivery_items(dn, updated_items)
            
            
            
            # Only submit if not already submitted
            if dn.docstatus == 0:
                dn.submit()
            
            frappe.db.commit()
            
        except Exception as e:
            frappe.log_error(
                f"Failed to update Delivery Note {dn_name}: {str(e)[:100]}",
                "Amazon DN Update Error"
            )


    def process_refunds_and_returns(self, order_id):
        """
        Placeholder for Amazon refunds/returns handling.
        Currently not implemented.
        """
        frappe.log_error(
            f"Refund/Return check skipped for Amazon Order {order_id}",
            "Amazon Refund Placeholder"
        )


    def handle_order_cancellation(self, order_id: str, so_name: str):
        """
        Handle Amazon Canceled order:
        - Cancel SO if possible
        - Cancel related Draft DN/SI if exists
        - If submitted DN/SI exists, don't crash; just log
        """
        try:
            so = frappe.get_doc("Sales Order", so_name)

            # 1) Cancel Draft SI (docstatus=0) linked to this SO
            draft_sis = frappe.get_all(
                "Sales Invoice",
                filters={"custom_against_sales_order": so_name, "docstatus": 0},
                pluck="name"
            )
            for si_name in draft_sis:
                si = frappe.get_doc("Sales Invoice", si_name)
                si.cancel()

            # 2) Cancel Draft DN (docstatus=0) linked to this SO
            draft_dns = frappe.get_all(
                "Delivery Note",
                filters={"custom_against_sales_order": so_name, "docstatus": 0},
                pluck="name"
            )
            for dn_name in draft_dns:
                dn = frappe.get_doc("Delivery Note", dn_name)
                dn.cancel()

            # 3) Cancel Sales Order (only if allowed)
            # If SO is submitted, cancel() may fail if linked docs exist
            if so.docstatus == 1:
                so.cancel()
            elif so.docstatus == 0:
                so.submit()
                so.cancel()

            frappe.db.commit()

        except Exception as e:
            # Don't crash sync job due to cancel restrictions
            frappe.log_error(
                f"Cancel handling failed for Amazon Order {order_id} / SO {so_name}: {str(e)[:200]}",
                "Amazon Cancel Handling Error"
            )



    

    

    def process_order_documents_based_on_status(self, amazon_order, so_name):
        """
        Simplified: Only create SO, DN, SI. Payment entries handled separately.
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
        
        # # Delivery Note Logic
        # if status in ["Shipped", "PartiallyShipped"]:
        #     # Check if DN already exists
        #     existing_dn = frappe.db.get_value(
        #         "Delivery Note",
        #         {"custom_against_sales_order": so_name, "docstatus": ["<", 2]},
        #         "name"
        #     )
            
        #     if existing_dn:
        #         # Update existing DN
        #         self.update_delivery_note(existing_dn, amazon_order)
        #     else:
        #         # Create new DN
        #         self.create_delivery_note_from_so(so_name)
        
        # Sales Invoice Logic (ALWAYS create invoice for shipped orders)
        if status in ["Shipped", "PartiallyShipped"]:
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
                self.create_sales_invoice_from_so(so_name)


    def update_amazon_reconciliation_from_finances_api(self, si_name: str):
        """
        READ-ONLY reconciliation update.
        Safe to run multiple times.
        No accounting impact.
        """

        si = frappe.get_doc("Sales Invoice", si_name)

        # ❗ Must be submitted invoice
        if si.docstatus != 1:
            return

        order_id = si.custom_amazon_order_id
        if not order_id:
            return

        totals = self.calculate_amazon_reconciliation_totals(order_id)

        frappe.db.set_value(
            "Sales Invoice",
            si.name,
            {
                "custom_amazon_item_total": totals.get("item_total", 0),
                "custom_amazon_tax_collected": totals.get("tax_collected", 0),
                "custom_amazon_shipping": totals.get("shipping", 0),
                "custom_amazon_promotions": totals.get("promotions", 0),
                "custom_amazon_fees": totals.get("fees", 0),
                "custom_amazon_net_proceeds": totals.get("net_proceeds", 0),
            },
            update_modified=False
        )



    def get_order_shipping_and_tax(self, order_id: str):
        """
        Extract shipping and tax from Amazon order items.
        """
        items = self.get_order_items(order_id)

        total_tax = 0
        total_shipping = 0

        for item in items:

            # item tax
            total_tax += float(item.get("amazon_item_tax", 0) or 0)

            # shipping tax
            total_tax += float(item.get("amazon_shipping_tax", 0) or 0)

            # shipping amount
            total_shipping += float(item.get("amazon_shipping", 0) or 0)

        return {
            "tax": total_tax,
            "shipping": total_shipping
        }


    
            


        

    # -------------------------------------------------------------------------
    # MAIN GET ORDERS LOOP WITH DEADLOCK HANDLING
    # -------------------------------------------------------------------------

    def get_orders(self, created_after: str) -> list:
        orders_api = self.get_orders_instance()

        # Normalize date
        if "T" not in created_after:
            created_after = datetime.strptime(
                created_after, "%Y-%m-%d"
            ).strftime("%Y-%m-%dT00:00:00Z")

        order_statuses = ",".join([
            "PendingAvailability",
            "Pending",
            "Unshipped",
            "PartiallyShipped",
            "Shipped",
            "InvoiceUnconfirmed",
            "Canceled",
            "Unfulfillable",
        ])

        fulfillment_channels = ",".join(["FBA", "SellerFulfilled"])

        # ✅ FIRST CALL (NO next_token here)
        payload = self.call_sp_api_method(
            orders_api.get_orders,
            created_after=created_after,
            order_statuses=order_statuses,
            fulfillment_channels=fulfillment_channels,
            max_results=50,
        )

        if not payload:
            return []

        processed_orders = []

        # ✅ PAGINATION LOOP
        while True:
            orders_list = payload.get("Orders", []) or []

            for order in orders_list:
                status = order.get("OrderStatus")

                # Skip orders that don't have final pricing yet
                if status in ["Canceled", "Pending", "PendingAvailability","Unshipped"]:
                    continue


                amazon_order_id = order.get("AmazonOrderId")

                existing_so = frappe.db.get_value(
                    "Sales Order",
                    {"amazon_order_id": amazon_order_id},
                    "name"
                )

                if existing_so:
                    so_name = existing_so
                else:
                    so_name = self.create_sales_order(order)

                if so_name:
                    self.process_order_documents_based_on_status(order, so_name)
                    processed_orders.append(so_name)

            # ✅ get next token AFTER processing
            next_token = payload.get("NextToken")
            if not next_token:
                break

            payload = self.call_sp_api_method(
                orders_api.get_orders,
                created_after=created_after,
                order_statuses=order_statuses,
                fulfillment_channels=fulfillment_channels,
                max_results=50,
                next_token=next_token,
            )

            if not payload:
                break

        return processed_orders




    def update_sales_order(self, so_name: str, amazon_order: dict):
        try:
            so = frappe.get_doc("Sales Order", so_name)
            order_id = amazon_order.get("AmazonOrderId")
            current_status = amazon_order.get("OrderStatus")

            # ❗ Do NOT update submitted Sales Order
            if so.docstatus == 0:
                if so.custom_amazon_order_status != current_status:
                    so.custom_amazon_order_status = current_status

                so.marketplace_id = amazon_order.get("MarketplaceId")
                so.purchase_date = amazon_order.get("PurchaseDate")
                so.latest_ship_date = amazon_order.get("LatestShipDate")

                updated_items = self.get_order_items(order_id)
                self.update_order_items(so, updated_items)

                if self.amz_setting.taxes_charges:
                    charges_and_fees = self.get_charges_and_fees(order_id)
                    # self.update_taxes_and_charges(so, charges_and_fees)

                so.save(ignore_permissions=True)

            else:
                # Read-only tracking
                frappe.logger().info(
                    f"Amazon Order {order_id} status is {current_status} (SO submitted)"
                )

            frappe.db.commit()

        except Exception as e:
            frappe.log_error(
                f"SO Update Failed {so_name}: {str(e)[:100]}",
                "Amazon Order Update Error"
            )

    @frappe.whitelist()
    def backfill_amazon_reconciliation_fields(amz_setting_name):
        repo = AmazonRepository(amz_setting_name)

        invoices = frappe.get_all(
            "Sales Invoice",
            filters={
                "custom_amazon_order_id": ["is", "set"],
                "docstatus": 1,
            },
            pluck="name"
        )

        updated = 0

        for si_name in invoices:
            try:
                repo.update_amazon_reconciliation_from_finances_api(si_name)
                updated += 1
            except Exception as e:
                frappe.log_error(
                    f"Reconciliation failed for {si_name}: {str(e)[:150]}",
                    "Amazon Reconciliation"
                )

        return {
            "success": True,
            "updated": updated,
            "total": len(invoices),
        }


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
                frappe.log_error(f"Error checking order {order.custom_amazon_order_id}: {str(e)[:100]}")
        
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


    def get_settlements_from_orders(self, days_back: int = 90):
            """Get settlements indirectly via order data"""
            orders_api = self.get_orders_instance()
            
            # Get orders first
            created_after = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
            
            orders_payload = self.call_sp_api_method(
                orders_api.get_orders,
                created_after=created_after,
                max_results=100
            )
            
            settlements = {}
            
            if orders_payload:
                orders = orders_payload.get("payload", {}).get("Orders", [])
                
                for order in orders:
                    order_id = order.get("AmazonOrderId")
                    
                    # Get financial events for this order
                    financial_data = self.get_financial_data_by_order(order_id)
                    
                    if financial_data and "FinancialEventGroupId" in financial_data:
                        settlement_id = financial_data["FinancialEventGroupId"]
                        
                        if settlement_id not in settlements:
                            settlements[settlement_id] = {
                                "group_id": settlement_id,
                                "orders": [],
                                "total_amount": 0.0
                            }
                        
                        settlements[settlement_id]["orders"].append(order_id)
                        settlements[settlement_id]["total_amount"] += financial_data.get("net_amount", 0)
            
            return list(settlements.values())




        



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
# HOOK ENTRYPOINT FOR SETTLEMENT PROCESSING
# -------------------------------------------------------------------------




@frappe.whitelist()
def backfill_amazon_reconciliation(amz_setting_name):
    repo = AmazonRepository(amz_setting_name)
    return repo.backfill_amazon_reconciliation_fields()

@frappe.whitelist()
def preview_amazon_settlements(amz_setting_name, days_back=90):
    """
    UI/API: Preview settlements without creating Payment Entry
    """
    repo = AmazonRepository(amz_setting_name)
    return repo.preview_settlement_reports(days_back=int(days_back))




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

@frappe.whitelist()
def create_amazon_payments_from_invoices(amz_setting_name, months_back=3):
    """
    Public function to create payment entries from unpaid Amazon invoices.
    Called from UI button.
    """
    try:
        repo = AmazonRepository(amz_setting_name)
        result = repo.create_payments_from_unpaid_invoices(months_back=int(months_back))
        
        # Send realtime update
        frappe.publish_realtime(
            event="amazon_manual_settlement_complete",
            message=result,
            user=frappe.session.user
        )
        
        return result
        
    except Exception as e:
        frappe.log_error(f"Failed to create payments from invoices: {str(e)}", 
                        "Amazon Payment Creation Error")
        return {
            "success": False,
            "message": f"Error: {str(e)}"
        }
    

def normalize_date(value):
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        return value  # already a date

@frappe.whitelist()
def create_payment_from_csv_ui(amz_setting_name, file_url, dry_run=False):
    """
    UI-triggered wrapper for creating Payment Entry from settlement CSV
    """
    repo = AmazonRepository(amz_setting_name)
    
    result = repo.process_settlement_csv(
        file_url=file_url,
        dry_run=frappe.utils.sbool(dry_run)
    )
    
    # Real-time update for UI
    frappe.publish_realtime(
        event="amazon_settlement_complete",
        message=result,
        user=frappe.session.user
    )
    
    return result

@frappe.whitelist()
def create_refunds_from_settlement_csv(amz_setting_name, file_url):
    """
    Wrapper for refund processing (called from UI)
    """

    if not amz_setting_name:
        frappe.throw("Amazon setting name required")

    repo = AmazonRepository(amz_setting_name)
    return repo.process_refunds_from_settlement_csv(file_url)





