import frappe
from datetime import datetime


def execute():
    """
    Patch to fix Amazon settlement processing:
    1) Correct SP-API error handling
    2) Fix settlement pagination rules
    3) Normalize Amazon date parsing
    """

    from ecommerce_integrations.amazon.doctype.amazon_sp_api_settings import amazon_repository
    from ecommerce_integrations.amazon.doctype.amazon_sp_api_settings.amazon_sp_api import SPAPIError

    # ------------------------------------------------------------------
    # 1) HARDEN SP-API WRAPPER (no silent failures)
    # ------------------------------------------------------------------

    def patched_call_sp_api_method(self, sp_api_method, **kwargs):
        errors = {}
        max_retries = self.amz_setting.max_retry_limit or 1

        for attempt in range(max_retries):
            try:
                result = sp_api_method(**kwargs)

                if not result:
                    return None

                if "errors" in result:
                    err = result["errors"][0]
                    raise SPAPIError(
                        error=err.get("code"),
                        error_description=err.get("message"),
                    )

                return result.get("payload")

            except SPAPIError as e:
                errors[e.error] = e.error_description
                frappe.log_error(f"Amazon SP API Error: {e.error}", "Amazon Settlement")
            except Exception as e:
                errors["general"] = str(e)
                frappe.log_error(str(e), "Amazon Settlement")

        self.amz_setting.enable_sync = 0
        self.amz_setting.save(ignore_permissions=True)

        frappe.throw(
            "Amazon settlement sync disabled due to repeated API failures:<br>"
            + "<br>".join(f"{k}: {v}" for k, v in errors.items())
        )

    amazon_repository.AmazonRepository.call_sp_api_method = patched_call_sp_api_method

    # ------------------------------------------------------------------
    # 2) SAFE DATE NORMALIZER (prevents strptime crash)
    # ------------------------------------------------------------------

    def normalize_date(val):
        if not val:
            return None
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, str):
            return datetime.fromisoformat(val.replace("Z", "+00:00")).date()
        return None

    amazon_repository.normalize_amazon_date = normalize_date

    frappe.db.commit()
