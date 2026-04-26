"""
Module for managing the ingestion catalog.
"""
import json
import logging
from warehouse.config import CATALOG_PATH

logger = logging.getLogger(__name__)

class CatalogManager:
    """
    Manages the data entry index for the warehouse.
    
    Reads from and writes to the bronze catalog JSON file to track 
    which sessions have been fully downloaded, preventing redundant 
    API calls during ingestion.
    """
    def __init__(self):
        self.catalog_path = CATALOG_PATH
        self._ensure_catalog()

    def _ensure_catalog(self):
        """Ensure the catalog JSON file exists."""
        if not self.catalog_path.exists():
            with open(self.catalog_path, 'w', encoding='utf-8') as f:
                json.dump({}, f)

    def _read_catalog(self):
        with open(self.catalog_path, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}

    def _write_catalog(self, data):
        with open(self.catalog_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)

    def is_session_downloaded(self, session_key: str) -> bool:
        """Check if a specific session is already fully downloaded."""
        catalog = self._read_catalog()
        return session_key in catalog and catalog[session_key].get("status") == "completed"

    def register_session_success(self, session_key: str, metadata: dict):
        """Register a session as fully downloaded."""
        catalog = self._read_catalog()
        catalog[session_key] = {
            "status": "completed",
            "metadata": metadata
        }
        self._write_catalog(catalog)
        logger.info("Session %s registered in catalog.", session_key)
        
    def get_all_completed_sessions(self) -> set:
        catalog = self._read_catalog()
        return {k for k, v in catalog.items() if v.get("status") == "completed"}
