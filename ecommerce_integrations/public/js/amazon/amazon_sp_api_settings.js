frappe.ui.form.on("Amazon SP API Settings", {
    refresh(frm) {
        // Button to sync orders (optional)
        frm.add_custom_button("Sync Orders Now", () => {
            frm.call("get_order_details");
        });

        // Button to sync fulfillment
        frm.add_custom_button("Sync Fulfillment (DN + Invoice)", () => {
            frm.call("manual_sync_fulfillment");
        });
    }
});
