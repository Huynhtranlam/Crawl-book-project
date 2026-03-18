from __future__ import annotations

import unittest
from unittest.mock import patch

from dashboard import setup_metabase


class AsItemsTests(unittest.TestCase):
    def test_as_items_accepts_list_response(self) -> None:
        response = [{"id": 1}]

        items = setup_metabase.as_items(response)

        self.assertEqual(response, items)

    def test_as_items_accepts_data_wrapper(self) -> None:
        response = {"data": [{"id": 2}]}

        items = setup_metabase.as_items(response)

        self.assertEqual(response["data"], items)

    def test_as_items_rejects_unexpected_shape(self) -> None:
        with self.assertRaises(setup_metabase.MetabaseError):
            setup_metabase.as_items({"unexpected": []})


class EnsureDatabaseTests(unittest.TestCase):
    @patch("dashboard.setup_metabase.request_json")
    def test_ensure_database_returns_existing_database_id(self, request_json_mock) -> None:
        request_json_mock.return_value = {"data": [{"id": 7, "name": setup_metabase.DATABASE_NAME}]}

        database_id = setup_metabase.ensure_database("session-id")

        self.assertEqual(7, database_id)
        request_json_mock.assert_called_once_with("GET", "/api/database", session_id="session-id")

    @patch("dashboard.setup_metabase.request_json")
    def test_ensure_database_creates_database_when_missing(self, request_json_mock) -> None:
        request_json_mock.side_effect = [
            {"data": [{"id": 1, "name": "Sample Database"}]},
            {"id": 9},
        ]

        database_id = setup_metabase.ensure_database("session-id")

        self.assertEqual(9, database_id)
        self.assertEqual(2, request_json_mock.call_count)
        self.assertEqual("POST", request_json_mock.call_args_list[1].args[0])
        self.assertEqual("/api/database", request_json_mock.call_args_list[1].args[1])


class DashboardChartTests(unittest.TestCase):
    def test_dashboard_has_at_least_three_charts(self) -> None:
        self.assertGreaterEqual(len(setup_metabase.CHARTS), 3)

    def test_dashboard_queries_target_btc_marts(self) -> None:
        for chart in setup_metabase.CHARTS:
            self.assertIn("mart_btc_", chart["query"])

    def test_dashboard_charts_define_interval_table(self) -> None:
        for chart in setup_metabase.CHARTS:
            self.assertIn(chart["interval_table"], {"mart_btc_ohlcv", "mart_btc_price_latest"})

    def test_dashboard_interval_parameter_uses_static_dropdown_values(self) -> None:
        parameters = setup_metabase._dashboard_parameters()

        self.assertEqual(1, len(parameters))
        parameter = parameters[0]
        self.assertEqual("dashboard-candle-interval", parameter["id"])
        self.assertEqual("string", parameter["sectionId"])
        self.assertEqual(["5m"], parameter["default"])
        self.assertEqual("static-list", parameter["values_source_type"])
        self.assertEqual(
            ["1m", "5m", "15m", "1h", "4h", "1d", "1w"],
            parameter["values_source_config"]["values"],
        )

    def test_card_payload_includes_interval_parameter_definition(self) -> None:
        payload = setup_metabase._card_payload(
            3,
            setup_metabase.CHARTS[0],
            {"mart_btc_price_latest": 124, "mart_btc_ohlcv": 101},
        )

        self.assertEqual("BTC Market Snapshot", payload["name"])
        self.assertEqual(1, len(payload["parameters"]))
        self.assertEqual("interval_value", payload["parameters"][0]["id"])
        self.assertEqual(
            ["dimension", ["template-tag", "interval_value"]],
            payload["parameters"][0]["target"],
        )
        self.assertEqual(
            ["field", 124, None],
            payload["dataset_query"]["native"]["template-tags"]["interval_value"]["dimension"],
        )

    def test_dashcard_parameter_mapping_includes_card_id(self) -> None:
        mapping = setup_metabase._dashcard_parameter_mappings(49)[0]

        self.assertEqual("dashboard-candle-interval", mapping["parameter_id"])
        self.assertEqual(49, mapping["card_id"])
        self.assertEqual(
            ["dimension", ["template-tag", "interval_value"]],
            mapping["target"],
        )


if __name__ == "__main__":
    unittest.main()
