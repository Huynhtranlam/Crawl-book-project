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


if __name__ == "__main__":
    unittest.main()
