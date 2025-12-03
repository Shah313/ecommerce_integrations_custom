# test_fulfillment_api.py

import frappe
import unittest
import os
import json
import responses

from ecommerce_integrations.amazon.doctype.amazon_sp_api_settings.amazon_sp_api import Util
from requests import request
from requests.exceptions import HTTPError

# ---------------------------------------
# 🔹 Load test JSON 
# ---------------------------------------
file_path = os.path.join(os.path.dirname(__file__), "test_data.json")
with open(file_path) as json_file:
    DATA = json.load(json_file)

# ---------------------------------------
# 🔹 Base test class (same as TestSPAPI)
# ---------------------------------------
class TestSPAPI:
    expected_response = {}

    @responses.activate
    def make_request(self, method="GET", append_to_base_uri="", params=None, data=None):
        if isinstance(params, dict):
            params = Util.remove_empty(params)

        url = self.endpoint + self.BASE_URI + append_to_base_uri

        responses.add(
            responses.GET if method == "GET" else responses.POST,
            url,
            status=self.expected_response.get("status", 200),
            json=self.expected_response.get("json", {}),
        )

        response = request(method=method, url=url, params=None, data=None)
        return response.json()

# ---------------------------------------
# 🔹 Fulfillment Test Client
# ---------------------------------------
class TestFulfillmentClient(TestSPAPI):

    BASE_URI = "/fba/outbound/2020-07-01"

    def list_shipments(self, created_after: str, next_token=None):
        self.expected_response = DATA.get("list_fulfillments_200")

        append_to_base_uri = "/shipments"

        return self.make_request(
            append_to_base_uri=append_to_base_uri,
            params={"CreatedAfter": created_after, "NextToken": next_token},
        )

# ---------------------------------------
# 🔹 Unit Test
# ---------------------------------------
class TestFulfillmentAPI(unittest.TestCase):

    def test_list_shipments(self):
        client = TestFulfillmentClient()

        # Fake marketplace endpoint for testing
        client.endpoint = "https://sellingpartnerapi-na.amazon.com"

        result = client.list_shipments("2024-01-01")

        self.assertIn("payload", result)

        frappe.msgprint("Fulfillment API Test Passed Successfully!")

