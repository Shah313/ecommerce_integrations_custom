// Copyright (c) 2022, Frappe and contributors
// For license information, please see license.txt

frappe.ui.form.on("Amazon SP API Settings", {
	refresh(frm) {
		// Existing logic
		if (frm.doc.__islocal && !frm.doc.amazon_fields_map) {
			frm.trigger("set_default_fields_map");
		}

		frm.trigger("set_queries");

		frm.set_df_property("amazon_fields_map", "cannot_add_rows", true);
		frm.set_df_property("amazon_fields_map", "cannot_delete_rows", true);

		 if (!frm.doc.__islocal) {

            frm.add_custom_button(
                __("Preview Settlements (Dry Run)"),
                () => {
                    frappe.call({
                        method: "ecommerce_integrations.amazon.doctype.amazon_sp_api_settings.amazon_repository.preview_amazon_settlements",
                        args: {
                            amz_setting_name: frm.doc.name,
                            days_back: frm.doc.after_date
							? frappe.datetime.get_diff(
								frappe.datetime.now_date(),
								frm.doc.after_date
							)
							: 90

                        },
                        freeze: true,
                        callback(r) {
                            if (r.message) {
                                console.table(r.message);
                                frappe.msgprint({
                                    title: __("Settlement Preview"),
                                    message: __("Check browser console for settlement preview."),
                                    indicator: "blue"
                                });
                            }
                        }
                    });
                },
                __("Amazon")
            );

        }

		// ---------------------------------------------------------
		// ✅ AMAZON RECONCILIATION BACKFILL BUTTON
		// ---------------------------------------------------------
		// if (!frm.doc.__islocal && frm.doc.is_active) {
		// 	frm.add_custom_button(
		// 		__("Backfill Amazon Reconciliation"),
		// 		function () {
		// 			frappe.confirm(
		// 				__(
		// 					"This will fetch Amazon Tax, Shipping, Promotions, Fees, and Net Proceeds " +
		// 					"for already-created Sales Invoices. This does NOT affect accounting totals.\n\nContinue?"
		// 				),
		// 				function () {
		// 					frappe.call({
		// 						method:
		// 							"ecommerce_integrations.amazon.doctype.amazon_sp_api_settings.amazon_repository.backfill_amazon_reconciliation",
		// 						args: {
		// 							amz_setting_name: frm.doc.name,
		// 						},
		// 						freeze: true,
		// 						freeze_message: __("Backfilling Amazon reconciliation data..."),
		// 						callback: function (r) {
		// 							if (r.message && r.message.success) {
		// 								frappe.msgprint({
		// 									title: __("Reconciliation Backfill Complete"),
		// 									message: __(
		// 										"Updated {0} out of {1} Sales Invoices."
		// 									).format(r.message.updated, r.message.total),
		// 									indicator: "green",
		// 								});
		// 							} else {
		// 								frappe.msgprint({
		// 									title: __("Backfill Finished"),
		// 									message: __(
		// 										"Backfill completed but some invoices may have failed. Please check Error Log."
		// 									),
		// 									indicator: "orange",
		// 								});
		// 							}
		// 						},
		// 					});
		// 				}
		// 			);
		// 		},
		// 		__("Reconciliation")
		// 	);
		// }
	},

	set_default_fields_map(frm) {
		frappe.call({
			method: "set_default_fields_map",
			doc: frm.doc,
			callback: (r) => {
				if (!r.exc) {
					refresh_field("amazon_fields_map");
				}
			},
		});
	},

	set_queries(frm) {
		frm.set_query("warehouse", () => {
			return {
				filters: {
					is_group: 0,
					company: frm.doc.company,
				},
			};
		});

		frm.set_query("market_place_account_group", () => {
			return {
				filters: {
					is_group: 1,
					company: frm.doc.company,
				},
			};
		});
	},
});
