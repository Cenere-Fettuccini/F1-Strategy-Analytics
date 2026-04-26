import unittest
from unittest.mock import patch, MagicMock
import json
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

# Add the project root to the Python path so it can find the 'warehouse' module
sys.path.append(str(Path(__file__).parent.parent))

from warehouse.ingestion.catalog_manager import CatalogManager
from warehouse.ingestion.collector import DataCollector
from warehouse.ingestion.fetch_calendar import CalendarFetcher
from warehouse.ingestion.orchestrator import IngestionOrchestrator

class TestCatalogManager(unittest.TestCase):
    def setUp(self):
        # We will patch the CATALOG_PATH inside CatalogManager to point to a dummy file
        self.test_catalog_path = Path("test_catalog.json")
        self.patcher = patch('warehouse.ingestion.catalog_manager.CATALOG_PATH', self.test_catalog_path)
        self.patcher.start()
        
        # Clean up any old test files
        if self.test_catalog_path.exists():
            self.test_catalog_path.unlink()
            
        self.manager = CatalogManager()

    def tearDown(self):
        self.patcher.stop()
        if self.test_catalog_path.exists():
            self.test_catalog_path.unlink()

    def test_ensure_catalog_creates_file(self):
        self.assertTrue(self.test_catalog_path.exists())
        with open(self.test_catalog_path, 'r') as f:
            data = json.load(f)
        self.assertEqual(data, {})

    def test_register_and_read_session(self):
        session_key = "2024/1/Race"
        meta = {"year": 2024, "round": 1}
        
        self.assertFalse(self.manager.is_session_downloaded(session_key))
        
        self.manager.register_session_success(session_key, meta)
        
        self.assertTrue(self.manager.is_session_downloaded(session_key))
        completed = self.manager.get_all_completed_sessions()
        self.assertIn(session_key, completed)


class TestDataCollector(unittest.TestCase):
    def setUp(self):
        self.collector = DataCollector()
        self.collector.backoff = 0.01  # Speed up tests by reducing sleep time

    def test_retry_wrapper_success_first_try(self):
        mock_func = MagicMock(return_value="success")
        result = self.collector._retry_wrapper(mock_func)
        self.assertEqual(result, "success")
        self.assertEqual(mock_func.call_count, 1)

    def test_retry_wrapper_success_after_retries(self):
        mock_func = MagicMock(side_effect=[ValueError("fail"), ValueError("fail"), "success"])
        result = self.collector._retry_wrapper(mock_func)
        self.assertEqual(result, "success")
        self.assertEqual(mock_func.call_count, 3)

    def test_retry_wrapper_exhausts_retries(self):
        mock_func = MagicMock(side_effect=RuntimeError("fail"))
        with self.assertRaises(RuntimeError):
            self.collector._retry_wrapper(mock_func)
        self.assertEqual(mock_func.call_count, self.collector.max_retries)


class TestCalendarFetcher(unittest.TestCase):
    @patch('fastf1.get_event_schedule')
    @patch('warehouse.ingestion.fetch_calendar.SUPPORTED_SEASONS', [2024])
    def test_get_all_available_sessions_filters_future(self, mock_get_schedule):
        # Create a mock pandas dataframe simulating the FastF1 schedule
        now = pd.Timestamp.utcnow().tz_localize(None)
        past_date = now - pd.Timedelta(days=1)
        future_date = now + pd.Timedelta(days=1)
        
        mock_df = pd.DataFrame([
            {
                'EventName': 'Past Race',
                'RoundNumber': 1,
                'Session1': 'Race',
                'Session1DateUtc': past_date
            },
            {
                'EventName': 'Future Race',
                'RoundNumber': 2,
                'Session1': 'Race',
                'Session1DateUtc': future_date
            }
        ])
        mock_get_schedule.return_value = mock_df

        fetcher = CalendarFetcher()
        sessions = fetcher.get_all_available_sessions()
        
        # Should only return the past race
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0]['session_key'], '2024/1/Race')


class TestIngestionOrchestrator(unittest.TestCase):
    @patch('warehouse.ingestion.orchestrator.CalendarFetcher')
    @patch('warehouse.ingestion.orchestrator.CatalogManager')
    @patch('warehouse.ingestion.orchestrator.DataCollector')
    def test_orchestrator_skips_existing(self, mock_collector_class, mock_manager_class, mock_fetcher_class):
        # Setup mocks
        mock_fetcher = mock_fetcher_class.return_value
        mock_manager = mock_manager_class.return_value
        mock_collector = mock_collector_class.return_value
        
        # Return 2 sessions from calendar
        mock_fetcher.get_all_available_sessions.return_value = [
            {'session_key': '2024/1/Race'},
            {'session_key': '2024/2/Race'}
        ]
        
        # Say 1 session is already downloaded
        mock_manager.get_all_completed_sessions.return_value = {'2024/1/Race'}
        
        # Simulate successful download for the missing one
        mock_collector.collect_session.return_value = True

        orchestrator = IngestionOrchestrator()
        orchestrator.run_ingestion()

        # Should only call collect_session once (for the missing 2024/2/Race)
        self.assertEqual(mock_collector.collect_session.call_count, 1)
        args, _ = mock_collector.collect_session.call_args
        self.assertEqual(args[0]['session_key'], '2024/2/Race')
        
        # Should register success once
        self.assertEqual(mock_manager.register_session_success.call_count, 1)


if __name__ == '__main__':
    unittest.main()
