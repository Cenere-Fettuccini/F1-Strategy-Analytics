import logging
from typing import List, Dict
import fastf1
from warehouse.config import FF1_CACHE_DIR, SUPPORTED_SEASONS

logger = logging.getLogger(__name__)

# Initialize FastF1 cache
fastf1.Cache.enable_cache(str(FF1_CACHE_DIR))

class CalendarFetcher:
    def get_all_available_sessions(self) -> List[Dict]:
        """
        Fetches the calendar for all SUPPORTED_SEASONS and extracts
        individual sessions (FP1, FP2, FP3, Q, S, R).
        Returns a list of dictionaries with session identifiers.
        """
        import pandas as pd
        
        all_sessions = []
        now_utc = pd.Timestamp.utcnow().tz_localize(None)
        
        for year in SUPPORTED_SEASONS:
            logger.info("Fetching calendar for season %s", year)
            try:
                schedule = fastf1.get_event_schedule(year)
                # Filter out testing events
                for _, event in schedule.iterrows():
                    event_name = event['EventName']
                    # Filter out testing events using EventFormat instead of hardcoded strings
                    if event.get('EventFormat') == 'testing':
                        continue
                    
                    # Each event has up to 5 sessions
                    for session_num in range(1, 6):
                        session_name_col = f'Session{session_num}'
                        session_date_col = f'Session{session_num}DateUtc'
                        
                        if session_name_col in event and str(event[session_name_col]) != "None":
                            session_date = event.get(session_date_col)
                            
                            # Ensure the session has happened (start time + 2 hours buffer)
                            if pd.notna(session_date) and (session_date + pd.Timedelta(hours=2)) < now_utc:
                                session_name = event[session_name_col]
                                session_key = f"{year}/{event['RoundNumber']}/{session_name.replace(' ', '_')}"
                                
                                all_sessions.append({
                                    "year": year,
                                    "round": event['RoundNumber'],
                                    "event_name": event_name,
                                    "session_name": session_name,
                                    "session_key": session_key
                                })
                            else:
                                logger.debug("Skipping future/current session: %s", event.get(session_name_col))
            except Exception as e:
                logger.error("Failed to fetch calendar for %s: %s", year, e)
                
        return all_sessions
