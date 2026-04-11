from . import __version__ as app_version

app_name = "ecommerce_integrations"
app_title = "Ecommerce Integrations"
app_publisher = "Frappe"
app_description = "Ecommerce integrations for ERPNext"
app_icon = "octicon octicon-file-directory"
app_color = "grey"
app_email = "developers@frappe.io"
app_license = "GNU GPL v3.0"
required_apps = ["frappe/erpnext"]

# ──────────────────────────────────────────────
# JS includes per doctype  (ONE dict, merged)
# ──────────────────────────────────────────────
doctype_js = {
    # Shopify – redirect old settings page
    "Shopify Settings": "public/js/shopify/old_settings.js",
    # Amazon
    "Amazon SP API Settings": "public/js/amazon/amazon_sp_api_settings.js",
    # Pipe17
    "Pipe17 Settings": "public/js/pipe17/pipe17_settings.js",
    # Unicommerce
    "Sales Order": [
        "public/js/unicommerce/sales_order.js",
        "public/js/common/ecommerce_transactions.js",
    ],
    "Sales Invoice": [
        "public/js/unicommerce/sales_invoice.js",
        "public/js/common/ecommerce_transactions.js",
    ],
    "Item": "public/js/unicommerce/item.js",
    "Stock Entry": [
        "public/js/unicommerce/stock_entry.js",
        "public/js/pipe17/stock_entry_mobix.js",
    ],
    "Pick List": "public/js/unicommerce/pick_list.js",
}

# ──────────────────────────────────────────────
# Whitelisted method overrides  (ONE dict)
# ──────────────────────────────────────────────
override_whitelisted_methods = {
    "ecommerce_integrations.api.pipe17_api.test_pipe17_connection": (
        "ecommerce_integrations.api.pipe17_api.test_pipe17_connection"
    ),
}

# ──────────────────────────────────────────────
# Document event hooks  (ONE dict, merged)
# ──────────────────────────────────────────────
doc_events = {
    "Item": {
        "after_insert": "ecommerce_integrations.shopify.product.upload_erpnext_item",
        "on_update": "ecommerce_integrations.shopify.product.upload_erpnext_item",
        "validate": [
            "ecommerce_integrations.utils.taxation.validate_tax_template",
            "ecommerce_integrations.unicommerce.product.validate_item",
        ],
    },
    "Sales Order": {
        "on_update_after_submit": (
            "ecommerce_integrations.unicommerce.order.update_shipping_info"
        ),
        "on_cancel": (
            "ecommerce_integrations.unicommerce.status_updater"
            ".ignore_pick_list_on_sales_order_cancel"
        ),
    },
    "Stock Entry": {
        "validate": "ecommerce_integrations.unicommerce.grn.validate_stock_entry_for_grn",
        "on_submit": [
            "ecommerce_integrations.unicommerce.grn.upload_grn",
            "ecommerce_integrations.api.pipe17_api.on_stock_entry_submit",
        ],
        "on_cancel": "ecommerce_integrations.unicommerce.grn.prevent_grn_cancel",
    },
    "Item Price": {
        "on_change": "ecommerce_integrations.utils.price_list.discard_item_prices"
    },
    "Pick List": {
        "validate": "ecommerce_integrations.unicommerce.pick_list.validate"
    },
    "Sales Invoice": {
        "on_submit": "ecommerce_integrations.unicommerce.invoice.on_submit",
        "on_cancel": "ecommerce_integrations.unicommerce.invoice.on_cancel",
    },
}

# ──────────────────────────────────────────────
# Scheduled tasks  (ONE dict, merged)
# ──────────────────────────────────────────────
scheduler_events = {
    # Runs every scheduler tick (~1 min) – only executes when frequency threshold met
    "all": [
        "ecommerce_integrations.shopify.inventory.update_inventory_on_shopify",
    ],
    "hourly": [
        # Shopify – pull orders that may have been missed by webhooks
        "ecommerce_integrations.shopify.order.sync_old_orders",
        # Amazon
        "ecommerce_integrations.amazon.doctype.amazon_sp_api_settings"
        ".amazon_sp_api_settings.schedule_get_order_details",
    ],
    "hourly_long": [
        "ecommerce_integrations.zenoti.doctype.zenoti_settings.zenoti_settings.sync_invoices",
        "ecommerce_integrations.unicommerce.product.upload_new_items",
        "ecommerce_integrations.unicommerce.status_updater.update_sales_order_status",
        "ecommerce_integrations.unicommerce.status_updater.update_shipping_package_status",
    ],
    "daily": [],
    "daily_long": [
        "ecommerce_integrations.zenoti.doctype.zenoti_settings.zenoti_settings.sync_stocks",
    ],
    "weekly": [],
    "monthly": [],
    "cron": {
        # Every 5 minutes – Unicommerce
        "*/5 * * * *": [
            "ecommerce_integrations.unicommerce.order.sync_new_orders",
            "ecommerce_integrations.unicommerce.inventory.update_inventory_on_unicommerce",
            "ecommerce_integrations.unicommerce.delivery_note.prepare_delivery_note",
        ],
    },
}

# ──────────────────────────────────────────────
# Misc
# ──────────────────────────────────────────────
before_uninstall = "ecommerce_integrations.uninstall.before_uninstall"

extend_bootinfo = "ecommerce_integrations.boot.boot_session"

before_tests = "ecommerce_integrations.utils.before_test.before_tests"

default_log_clearing_doctypes = {
    "Ecommerce Integration Log": 120,
}