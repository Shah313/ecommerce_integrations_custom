// In pipe17_settings.js
frappe.ui.form.on("Pipe17 Settings", {
    refresh: function (frm) {
        frm.add_custom_button(__('Test Connection'), function () {
            frappe.call({
                method: "ecommerce_integrations.api.pipe17_api.test_pipe17_connection",
                freeze: true,
                freeze_message: __("Testing connection with Pipe17..."),
                callback: function (r) {
                    if (!r.exc) {
                        frappe.msgprint(__("✅ Connection Successful!"));
                    }
                }
            });
        });
        
        // // NEW: Button to fetch warehouses
        // frm.add_custom_button(__('Fetch Warehouses'), function () {
        //     frappe.call({
        //         method: "ecommerce_integrations.api.pipe17_api.get_pipe17_warehouses",
        //         freeze: true,
        //         freeze_message: __("Fetching warehouses from Pipe17..."),
        //         callback: function (r) {
        //             if (!r.exc && r.message && r.message.warehouses) {
        //                 let warehouses = r.message.warehouses;
        //                 let message = `<b>Available Warehouses:</b><br><ul>`;
                        
        //                 warehouses.forEach(wh => {
        //                     message += `<li><b>ID:</b> ${wh.id} | <b>Name:</b> ${wh.name || 'N/A'}</li>`;
        //                 });
        //                 message += `</ul>`;
                        
        //                 frappe.msgprint({
        //                     title: __('Pipe17 Warehouses'),
        //                     message: message,
        //                     indicator: 'green'
        //                 });
        //             } else {
        //                 frappe.msgprint({
        //                     title: __('Error'),
        //                     message: __('Could not fetch warehouses. Check API connection.'),
        //                     indicator: 'red'
        //                 });
        //             }
        //         }
        //     });
        // });
    }
});