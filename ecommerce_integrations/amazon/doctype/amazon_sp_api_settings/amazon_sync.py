import frappe
from ecommerce_integrations.amazon.doctype.amazon_sp_api_settings.amazon_repository import AmazonRepository

@frappe.whitelist()
def sync_amazon_orders(created_after="2020-01-01"):
    # yahan tum apna actual Amazon SP API Settings ka name do
    repo = AmazonRepository("ag8851p7n3")
    orders = repo.get_orders(created_after)
    return orders
