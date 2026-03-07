# # ecommerce_integrations/shopify/verify_webhook.py

# import frappe
# import requests
# from frappe.utils import get_url

# def run():
#     """Entry point for bench execute - this is what you'll call"""
#     verify_webhook_endpoint()

# def verify_webhook_endpoint():
#     """Check if webhook endpoint is accessible"""
    
#     url = get_url("/api/method/ecommerce_integrations.shopify.connection.store_request_data")
    
#     print("\n" + "="*80)
#     print("🔍 SHOPIFY WEBHOOK VERIFICATION TOOL")
#     print("="*80 + "\n")
    
#     print(f"📍 Endpoint URL: {url}")
#     print(f"📍 Site: {frappe.local.site}")
#     print(f"📍 Environment: {'🔴 PRODUCTION' if not frappe.conf.developer_mode else '🟢 DEVELOPMENT'}")
#     print("-" * 80 + "\n")
    
#     # Test 1: Check if Shopify Setting is configured
#     try:
#         setting = frappe.get_doc("Shopify Setting")
#         print("✅ Shopify Setting found")
#         print(f"   - Enabled: {setting.enable_shopify}")
#         print(f"   - Shop URL: {setting.shopify_url}")
#         print(f"   - Shared Secret: {'✅ Configured' if setting.shared_secret else '❌ MISSING'}")
#     except Exception as e:
#         print(f"❌ ERROR: Shopify Setting not found or not configured: {e}")
#         return
    
#     # Test 2: Check if endpoint is reachable
#     try:
#         # Send a minimal POST to test connectivity
#         headers = {
#             "Content-Type": "application/json",
#             "X-Shopify-Topic": "test_connection",
#             "X-Shopify-Shop-Domain": setting.shopify_url
#         }
        
#         # Use GET for simple connectivity test
#         response = requests.get(url, timeout=10, verify=False)
#         print(f"✅ Endpoint reachable - Status: {response.status_code}")
        
#         # Also test POST (what Shopify actually sends)
#         response2 = requests.post(url, json={"test": True}, headers=headers, timeout=10, verify=False)
#         print(f"✅ POST endpoint reachable - Status: {response2.status_code}")
        
#     except requests.exceptions.ConnectionError:
#         print("❌ ERROR: Cannot connect to the endpoint URL")
#         print("   Check if your site is running and the URL is correct")
#     except Exception as e:
#         print(f"❌ ERROR: {e}")
    
#     # Test 3: Check webhook registration
#     try:
#         from ecommerce_integrations.shopify.connection import temp_shopify_session
#         from shopify.resources import Webhook
        
#         @temp_shopify_session
#         def check_webhooks():
#             webhooks = Webhook.find()
#             print("\n📋 Registered Webhooks:")
#             callback_url = get_url("/api/method/ecommerce_integrations.shopify.connection.store_request_data")
            
#             found = False
#             for wh in webhooks:
#                 print(f"   - {wh.topic}: {wh.address}")
#                 if callback_url in wh.address:
#                     found = True
            
#             if not found:
#                 print("   ⚠️  No webhooks registered for this site URL")
#             return webhooks
        
#         check_webhooks()
#     except Exception as e:
#         print(f"\n⚠️  Could not check webhooks: {e}")
#         print("   This is normal if Shopify credentials are not configured for API access")
    
#     print("\n" + "="*80)
#     print("✅ VERIFICATION COMPLETE")
#     print("="*80 + "\n")
    
#     # Next steps
#     print("📋 NEXT STEPS:")
#     print("1. Create a REAL test order in Shopify (not the test button)")
#     print("2. Use discount code for $0.01 or a test product")
#     print("3. Check Ecommerce Integration Log in ERPNext")
#     print("4. Run diagnostic: bench execute 'ecommerce_integrations.shopify.utils.diagnose_webhook_issue()'\n")

# if __name__ == "__main__":
#     # This allows running as: python -m ecommerce_integrations.shopify.verify_webhook
#     run()