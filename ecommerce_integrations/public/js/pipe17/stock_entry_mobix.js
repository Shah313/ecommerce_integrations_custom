frappe.ui.form.on("Stock Entry", {
    refresh(frm) {
        // Only show button for "Material Transfer" type and when document is submitted
        if (frm.doc.stock_entry_type === "Material Transfer" && frm.doc.docstatus === 1) {
            
            // Check if already synced to Mobix
            const isSynced = frm.doc.custom_mobix_synced || 0;
            
            if (isSynced) {
                // Show disabled button with message
                frm.add_custom_button("✅ Already Transferred to Harmonix", () => {
                    frappe.msgprint({
                        title: __('Already Transferred'),
                        message: __('This Stock Entry has already been transferred to Harmonix. Duplicate transfers are not allowed.'),
                        indicator: 'blue'
                    });
                }).addClass('btn-default').prop('disabled', true);
            } else {
                // Show active transfer button
                frm.add_custom_button("Transfer to Harmonix", () => {
                    frappe.confirm(
                        __("Are you sure you want to send this transfer to Harmonix inventory?"),
                        () => {
                            frappe.call({
                                method: "ecommerce_integrations.api.pipe17_api.transfer_to_mobix",
                                args: { stock_entry: frm.doc.name },
                                freeze: true,
                                freeze_message: __("Transferring items to Harmonix..."),
                                callback: function (r) {
                                    if (!r.exc) {
                                        frappe.show_alert({
                                            message: __("✅ Transfer completed successfully to Harmonix."),
                                            indicator: "green"
                                        });
                                        // Refresh the form to update the button state
                                        frm.reload_doc();
                                    } else {
                                        frappe.show_alert({
                                            message: __("❌ Transfer failed — check logs."),
                                            indicator: "red"
                                        });
                                    }
                                }
                            });
                        }
                    );
                }).addClass('btn-primary');
            }
            
            // // Add reset button for admin (for testing purposes)
            // if (frappe.user.has_role('System Manager') || frappe.session.user === 'Administrator') {
            //     frm.add_custom_button("Reset Sync Status", () => {
            //         frappe.confirm(
            //             __("Reset sync status? This will allow transferring to Harmonix again. For testing purposes only."),
            //             () => {
            //                 frappe.call({
            //                     method: "ecommerce_integrations.api.pipe17_api.reset_mobix_sync_status",
            //                     args: { stock_entry: frm.doc.name },
            //                     freeze: true,
            //                     freeze_message: __("Resetting sync status..."),
            //                     callback: function (r) {
            //                         if (!r.exc) {
            //                             frappe.show_alert({
            //                                 message: __("✅ Sync status reset."),
            //                                 indicator: "green"
            //                             });
            //                             frm.reload_doc();
            //                         }
            //                     }
            //                 });
            //             }
            //         );
            //     }).addClass('btn-default');
            // }
        }
    },
    
    // Hide button when document is not submitted
    onload(frm) {
        if (frm.doc.docstatus !== 1) {
            // Remove any existing transfer buttons
            frm.page.remove_inner_button("Transfer to Harmonix");
            frm.page.remove_inner_button("✅ Already Transferred to Harmonix");
        }
    }
});