// frappe.ui.form.on('Pipe17 Settings', {
//     refresh(frm) {
//         if (!frm.is_new()) {
//             frm.add_custom_button('Test Connection', () => {
//                 frappe.call({
//                     method: 'ecommerce_integrations.api.pipe17_api.test_pipe17_connection',
//                     callback: function(r) {
//                         if (r.message) {
//                             frappe.msgprint(`Status: ${r.message.status}<br>Response: ${r.message.text}`);
//                         }
//                     }
//                 });
//             }).addClass('btn-primary');
//         }
//     }
// });
