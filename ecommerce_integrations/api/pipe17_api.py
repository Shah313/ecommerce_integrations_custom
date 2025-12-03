import requests
import frappe
import json


@frappe.whitelist()
def test_pipe17_connection():
    """Test connection to Pipe17 API (v3)"""
    try:
        settings = frappe.get_single("Pipe17 Settings")
        api_key = settings.api_key.strip()
        base_url = (settings.base_url or "https://api-v3.pipe17.com/api/v3").rstrip("/")

        headers = {
            "X-Pipe17-Key": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Test both inventory and locations endpoints
        inventory_url = f"{base_url}/inventory?limit=1"
        locations_url = f"{base_url}/locations?limit=1"

        inventory_response = requests.get(inventory_url, headers=headers, timeout=15)
        locations_response = requests.get(locations_url, headers=headers, timeout=15)

        if (
            inventory_response.status_code == 200
            and locations_response.status_code == 200
        ):
            inventory_data = inventory_response.json()
            locations_data = locations_response.json()

            inventory_count = len(inventory_data.get("inventory", []))
            locations_count = len(locations_data.get("locations", []))

            frappe.msgprint(f"✅ Pipe17 Connection Successful!")
            frappe.msgprint(f"📦 Inventory items: {inventory_count}")
            frappe.msgprint(f"🏠 Locations: {locations_count}")
            return {"success": True, "data": inventory_data}
        else:
            frappe.throw(
                f"❌ Connection Failed (Inventory: {inventory_response.status_code}, Locations: {locations_response.status_code})"
            )

    except Exception as e:
        frappe.log_error(f"Pipe17 Connection Error: {str(e)}", "Pipe17 Connector Test")
        frappe.throw(f"Error: {str(e)}")


@frappe.whitelist()
def on_stock_entry_submit(doc, method=None):
    """Triggered automatically on Stock Entry submission - Creates fulfillment in Pipe17"""
    frappe.msgprint("🔄 Starting Mobix Sync...")

    try:
        # Only process Material Transfer entries
        if doc.stock_entry_type != "Material Transfer":
            frappe.msgprint("ℹ️ Not a Material Transfer - Skipping Mobix Sync")
            return

        settings = frappe.get_single("Pipe17 Settings")
        api_key = settings.api_key.strip()
        base_url = (settings.base_url or "https://api-v3.pipe17.com/api/v3").rstrip("/")

        # Validate settings
        if not api_key:
            frappe.throw("Pipe17 API Key is not configured")

        headers = {
            "X-Pipe17-Key": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        created, updated, errors = 0, 0, []

        # Get all available locations from Pipe17
        locations = get_pipe17_locations(base_url, headers)
        if not locations:
            frappe.throw("No locations found in Pipe17. Please create locations first.")

        # Show location mapping to help debug
        frappe.msgprint("📍 Available Mobix Locations:")
        for loc in locations:
            location_id = extract_location_id(loc)
            frappe.msgprint(f"   - {loc.get('name')} (ID: {location_id})")

        for item in doc.items:
            # Only process items with target warehouse (transfers)
            if not item.t_warehouse:
                continue

            sku = item.item_code
            qty = item.qty or 0
            target_warehouse = item.t_warehouse
            item_name = (
                item.item_name or "Unnamed Product"
            )  # Get item name from ERPNext

            frappe.msgprint(
                f"📦 Processing: {sku} - {item_name} - Qty: {qty} - To ERPNext Warehouse: {target_warehouse}"
            )

            try:
                # Find the corresponding location in Pipe17 for the target warehouse
                location_id, location_name = find_matching_location(
                    target_warehouse, locations
                )

                if not location_id:
                    error_msg = f"No Mobix location found for ERPNext warehouse: '{target_warehouse}'. Available locations: {', '.join([loc.get('name') for loc in locations])}"
                    errors.append(error_msg)
                    log_error_short(error_msg, "Pipe17 Location Match")
                    continue

                frappe.msgprint(
                    f"   📍 Mapped to Mobix Location: {location_name} (ID: {location_id})"
                )

                # First, ensure the product exists in Pipe17 with the correct name
                ensure_product_exists(sku, item_name, base_url, headers)

                # Check if item exists in Mobix inventory for this location
                inventory_found, existing_inventory = find_existing_inventory(
                    sku, location_id, location_name, base_url, headers
                )

                if inventory_found and existing_inventory:
                    # Update existing item
                    update_success = update_existing_inventory(
                        existing_inventory,
                        qty,
                        location_id,
                        location_name,
                        base_url,
                        headers,
                        doc.name,
                    )
                    if update_success:
                        updated += 1
                        frappe.msgprint(
                            f"✅ UPDATED: {sku} in {location_name} - Added Qty: {qty}"
                        )
                    else:
                        error_msg = f"Update failed for {sku} in {location_name}"
                        errors.append(error_msg)
                else:
                    # Create new item in the specific location
                    create_success = create_new_inventory(
                        sku,
                        qty,
                        location_id,
                        location_name,
                        base_url,
                        headers,
                        doc.name,
                    )
                    if create_success:
                        created += 1
                        frappe.msgprint(
                            f"✅ CREATED: {sku} in {location_name} - Qty: {qty}"
                        )
                    else:
                        error_msg = f"Create failed for {sku} in {location_name}"
                        errors.append(error_msg)

            except Exception as inner_e:
                error_msg = f"Item {sku} failed: {str(inner_e)[:100]}"
                errors.append(error_msg)
                log_error_short(error_msg, "Pipe17 Item")

        # Show final results
        if errors:
            frappe.msgprint(f"❌ Mobix Sync Completed with {len(errors)} errors")
            for error in errors[:3]:  # Show first 3 errors in message
                frappe.msgprint(f"   ⚠️ {error}")
        else:
            frappe.msgprint(
                f"🎉 Mobix Sync Successful! Created: {created}, Updated: {updated}"
            )

        # Mark as synced to prevent duplicate transfers
        if created > 0 or updated > 0:
            frappe.db.set_value("Stock Entry", doc.name, "custom_mobix_synced", 1)
            frappe.db.commit()
            frappe.msgprint(
                "🔍 Check created/updated items in Mobix: Inventory → Locations → LGU1"
            )

    except Exception as e:
        error_msg = f"Pipe17 Transfer Error: {str(e)[:100]}"
        log_error_short(error_msg, "Pipe17 Transfer")
        frappe.throw(f"Sync failed: {str(e)}")


def ensure_product_exists(sku, item_name, base_url, headers):
    """Ensure product exists in Pipe17 with correct name"""
    try:
        # Check if product already exists
        product_url = f"{base_url}/products?sku={sku}"
        response = requests.get(product_url, headers=headers, timeout=15)

        if response.status_code == 200:
            data = response.json()
            products = data.get("products", [])

            if products:
                # Product exists, check if name needs update
                existing_product = products[0]
                if existing_product.get("name") != item_name:
                    # Update product name
                    product_id = existing_product.get("id")
                    update_url = f"{base_url}/products/{product_id}"
                    update_payload = {"name": item_name, "sku": sku}
                    update_response = requests.patch(
                        update_url, headers=headers, json=update_payload, timeout=15
                    )
                    if update_response.status_code in [200, 201]:
                        frappe.msgprint(f"   ✏️ Updated product name: {item_name}")
            else:
                # Create new product
                create_product_url = f"{base_url}/products"
                create_payload = {"sku": sku, "name": item_name}
                create_response = requests.post(
                    create_product_url, headers=headers, json=create_payload, timeout=15
                )
                if create_response.status_code in [200, 201]:
                    frappe.msgprint(f"   📝 Created product: {item_name}")
                elif create_response.status_code == 409:
                    # Product might exist with different lookup, ignore conflict
                    frappe.msgprint(f"   ℹ️ Product already exists: {item_name}")
                else:
                    frappe.msgprint(
                        f"   ⚠️ Could not create product: {create_response.status_code}"
                    )

    except Exception as e:
        frappe.msgprint(f"   ⚠️ Product setup warning: {str(e)}")
        # Don't fail the whole process if product setup has issues


def find_existing_inventory(sku, location_id, location_name, base_url, headers):
    """Find existing inventory item with multiple lookup strategies"""
    lookup_methods = []

    # Try different lookup strategies
    if location_id and location_id != "None":
        lookup_methods.append(
            f"{base_url}/inventory?sku={sku}&locationId={location_id}"
        )
    else:
        lookup_methods.append(
            f"{base_url}/inventory?sku={sku}&location={location_name}"
        )

    # Also try without location filter
    lookup_methods.append(f"{base_url}/inventory?sku={sku}")

    for url in lookup_methods:
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                inventory = data.get("inventory", [])

                if inventory:
                    frappe.msgprint(
                        f"   🔍 Found {len(inventory)} existing inventory records for {sku}"
                    )
                    # Return the first matching inventory item
                    return True, inventory[0]

        except Exception as e:
            frappe.msgprint(f"   ⚠️ Lookup failed for {url}: {str(e)}")
            continue

    frappe.msgprint(f"   🔍 No existing inventory found for {sku}")
    return False, None


def update_existing_inventory(
    inventory_item, qty_to_add, location_id, location_name, base_url, headers, doc_name
):
    """Update existing inventory item"""
    try:
        # Get current quantity and inventory ID
        current_qty = inventory_item.get("onHand", 0)
        inv_id = inventory_item.get("id")
        new_qty = current_qty + qty_to_add

        frappe.msgprint(
            f"   📊 Current Qty: {current_qty}, Adding: {qty_to_add}, New Qty: {new_qty}"
        )

        # Method 1: Try PATCH with inventory ID
        if inv_id:
            update_url = f"{base_url}/inventory/{inv_id}"
            payload = {
                "onHand": new_qty,
                "committed": 0,
                "ptype": "portal",
                "event": "adjust",
                "locationId": location_id or location_name,
            }

            update_response = requests.patch(
                update_url, headers=headers, json=payload, timeout=15
            )

            if update_response.status_code in [200, 201, 202]:
                return True
            else:
                frappe.msgprint(
                    f"   ⚠️ PATCH failed (HTTP {update_response.status_code}), trying POST method..."
                )

        # Method 2: If PATCH fails or no ID, try POST with adjustment
        payload = {
            "sku": inventory_item.get("sku"),
            "onHand": new_qty,
            "committed": 0,
            "ptype": "portal",
            "event": "adjust",
            "locationId": location_id or location_name,
        }

        create_response = requests.post(
            f"{base_url}/inventory", headers=headers, json=payload, timeout=15
        )

        if create_response.status_code in [200, 201, 202]:
            if create_response.status_code == 202:
                frappe.msgprint(
                    "   ⏳ Request accepted and is being processed asynchronously"
                )
            return True
        else:
            error_msg = f"Update failed for {inventory_item.get('sku')} in {location_name}: HTTP {create_response.status_code} - {create_response.text}"
            log_error_short(error_msg, "Pipe17 Update")
            return False

    except Exception as e:
        error_msg = f"Update error for {inventory_item.get('sku')}: {str(e)}"
        log_error_short(error_msg, "Pipe17 Update")
        return False


def create_new_inventory(
    sku, qty, location_id, location_name, base_url, headers, doc_name
):
    """Create new inventory item"""
    try:
        payload = {
            "sku": sku,
            "onHand": qty,
            "committed": 0,
            "ptype": "portal",
            "event": "adjust",
            "locationId": location_id or location_name,
        }

        create_response = requests.post(
            f"{base_url}/inventory", headers=headers, json=payload, timeout=15
        )

        if create_response.status_code in [200, 201, 202]:
            if create_response.status_code == 202:
                frappe.msgprint(
                    "   ⏳ Request accepted and is being processed asynchronously"
                )
            return True
        else:
            error_details = ""
            try:
                error_data = create_response.json()
                error_details = f" - {error_data.get('message', '')}"
            except:
                pass
            error_msg = f"Create failed for {sku} in {location_name}: HTTP {create_response.status_code}{error_details}"
            log_error_short(error_msg, "Pipe17 Create")
            return False

    except Exception as e:
        error_msg = f"Create error for {sku}: {str(e)}"
        log_error_short(error_msg, "Pipe17 Create")
        return False


def extract_location_id(location):
    """Extract location ID from location object, handling various field names"""
    possible_id_fields = ["id", "locationId", "_id", "location_id", "code"]

    for field in possible_id_fields:
        if location.get(field):
            return location[field]

    return None


def find_matching_location(erpnext_warehouse, pipe17_locations):
    """Find matching Pipe17 location for ERPNext warehouse"""
    # Clean the warehouse name for better matching
    clean_warehouse = erpnext_warehouse.strip().lower()

    # Try different matching strategies
    for location in pipe17_locations:
        location_name = (location.get("name") or "").lower().strip()
        location_id = extract_location_id(location)

        # Exact match
        if location_name == clean_warehouse or (
            location_id and location_id.lower() == clean_warehouse
        ):
            return location_id, location.get("name", "Unknown")

        # Partial match
        if clean_warehouse in location_name or (
            location_id and clean_warehouse in location_id.lower()
        ):
            return location_id, location.get("name", "Unknown")

        # Try matching "Mobix" part
        if "mobix" in clean_warehouse and "mobix" in location_name:
            return location_id, location.get("name", "Unknown")

    # If no match found, use the first available location with warning
    if pipe17_locations:
        first_location = pipe17_locations[0]
        location_id = extract_location_id(first_location)
        return location_id, first_location.get("name", "Unknown")

    return None, None


def log_error_short(message, title="Pipe17 Error"):
    """Log error with truncated message to avoid character limit issues"""
    truncated_msg = message[:100] + "..." if len(message) > 100 else message
    frappe.log_error(truncated_msg, title)


def get_pipe17_locations(base_url, headers):
    """Get all locations from Pipe17"""
    try:
        locations_url = f"{base_url}/locations"
        response = requests.get(locations_url, headers=headers, timeout=15)

        if response.status_code == 200:
            data = response.json()
            locations = data.get("locations", [])
            return locations
        else:
            log_error_short(
                f"Failed to fetch locations: {response.status_code} - {response.text}",
                "Pipe17 Locations",
            )
            return []
    except Exception as e:
        log_error_short(f"Error fetching locations: {str(e)}", "Pipe17 Locations")
        return []


@frappe.whitelist()
def transfer_to_mobix(stock_entry):
    """Manual transfer method called from JS button"""
    try:
        # Check if already synced
        if frappe.db.get_value("Stock Entry", stock_entry, "custom_mobix_synced"):
            frappe.throw(
                "❌ This Stock Entry has already been transferred to Mobix. Duplicate transfers are not allowed."
            )

        doc = frappe.get_doc("Stock Entry", stock_entry)

        # Check if document is submitted
        if doc.docstatus != 1:
            frappe.throw(
                "❌ Please submit the Stock Entry before transferring to Mobix."
            )

        result = on_stock_entry_submit(doc)

        # Mark as synced after successful transfer
        frappe.db.set_value("Stock Entry", stock_entry, "custom_mobix_synced", 1)
        frappe.db.commit()

        return result

    except Exception as e:
        frappe.log_error(f"Transfer to Mobix Error: {str(e)}", "Pipe17 Transfer")
        raise e


@frappe.whitelist()
def reset_mobix_sync_status(stock_entry):
    """Reset sync status for testing purposes (admin only)"""
    # Check if user has permissions
    if not frappe.session.user == "Administrator":
        frappe.throw("Only Administrator can reset sync status.")

    frappe.db.set_value("Stock Entry", stock_entry, "custom_mobix_synced", 0)
    frappe.db.commit()
    frappe.msgprint(
        "✅ Mobix sync status reset. You can now transfer this Stock Entry again."
    )
