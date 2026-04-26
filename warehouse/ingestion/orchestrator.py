import logging
from .fetch_calendar import CalendarFetcher
from .catalog_manager import CatalogManager
from .collector import DataCollector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IngestionOrchestrator:
    def __init__(self):
        self.calendar_fetcher = CalendarFetcher()
        self.catalog_manager = CatalogManager()
        self.collector = DataCollector()

    def run_ingestion(self):
        logger.info("Starting ingestion run...")
        
        # 1. Get all events
        all_sessions = self.calendar_fetcher.get_all_available_sessions()
        logger.info("Discovered %d total sessions.", len(all_sessions))
        
        # 2. Check existing
        completed_sessions = self.catalog_manager.get_all_completed_sessions()
        logger.info("%d sessions already in catalog.", len(completed_sessions))
        
        # 3. Filter for missing
        missing_sessions = [s for s in all_sessions if s['session_key'] not in completed_sessions]
        logger.info("Found %d missing sessions to process.", len(missing_sessions))
        
        # 4. Cycle through missing
        for session_meta in missing_sessions:
            session_key = session_meta['session_key']
            logger.info("Triggering collection for: %s", session_key)
            
            success = self.collector.collect_session(session_meta)
            
            if success:
                # 5. Register success
                self.catalog_manager.register_session_success(session_key, session_meta)
                logger.info("Successfully ingested %s", session_key)
            else:
                logger.error("Collection aborted for %s. Will retry next run.", session_key)

if __name__ == "__main__":
    orchestrator = IngestionOrchestrator()
    orchestrator.run_ingestion()
