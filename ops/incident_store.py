"""
Incident management and storage for the data pipeline.
Tracks incidents, escalations, and resolution status.
"""

import json
import logging
import os
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class IncidentStore:
    """SQLite-based incident storage with JSONL fallback."""
    
    def __init__(self, db_path: str = "/data/ops/incidents.db", 
                 jsonl_path: str = "/data/ops/incidents.jsonl"):
        self.db_path = db_path
        self.jsonl_path = jsonl_path
        self._ensure_directories()
        self._init_database()
    
    def _ensure_directories(self):
        """Ensure the necessary directories exist."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.jsonl_path), exist_ok=True)
    
    def _init_database(self):
        """Initialize the SQLite database with required tables."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS incidents (
                        id TEXT PRIMARY KEY,
                        incident_type TEXT NOT NULL,
                        summary TEXT NOT NULL,
                        context TEXT,  -- JSON string
                        severity TEXT DEFAULT 'medium',
                        status TEXT DEFAULT 'open',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        resolved_at TEXT,
                        escalated BOOLEAN DEFAULT 0,
                        escalated_at TEXT,
                        resolution_notes TEXT,
                        created_by TEXT DEFAULT 'pipeline_agent'
                    )
                """)
                
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_incidents_created_at 
                    ON incidents(created_at)
                """)
                
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_incidents_type 
                    ON incidents(incident_type)
                """)
                
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_incidents_status 
                    ON incidents(status)
                """)
                
                conn.commit()
                logger.info("Incident database initialized")
                
        except Exception as e:
            logger.error(f"Failed to initialize incident database: {e}")
            logger.info("Falling back to JSONL storage")

    def log_incident(self, incident_type: str, summary: str, 
                    context: Dict[str, Any] = None, severity: str = "medium",
                    created_by: str = "pipeline_agent") -> str:
        """
        Log a new incident.
        
        Args:
            incident_type: Type of incident (pipeline_failure, data_quality, etc.)
            summary: Brief description of the incident
            context: Additional context information
            severity: Severity level (low, medium, high, critical)
            created_by: Who/what created the incident
            
        Returns:
            Incident ID
        """
        incident_id = str(uuid.uuid4())
        timestamp = datetime.now().isoformat()
        
        incident_data = {
            'id': incident_id,
            'incident_type': incident_type,
            'summary': summary,
            'context': context or {},
            'severity': severity,
            'status': 'open',
            'created_at': timestamp,
            'updated_at': timestamp,
            'resolved_at': None,
            'escalated': False,
            'escalated_at': None,
            'resolution_notes': None,
            'created_by': created_by
        }
        
        # Try SQLite first, fallback to JSONL
        if self._store_in_sqlite(incident_data):
            logger.info(f"Incident {incident_id} logged to database")
        else:
            self._store_in_jsonl(incident_data)
            logger.info(f"Incident {incident_id} logged to JSONL")
        
        return incident_id
    
    def _store_in_sqlite(self, incident_data: Dict[str, Any]) -> bool:
        """Store incident in SQLite database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO incidents (
                        id, incident_type, summary, context, severity, status,
                        created_at, updated_at, resolved_at, escalated, 
                        escalated_at, resolution_notes, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    incident_data['id'],
                    incident_data['incident_type'],
                    incident_data['summary'],
                    json.dumps(incident_data['context']),
                    incident_data['severity'],
                    incident_data['status'],
                    incident_data['created_at'],
                    incident_data['updated_at'],
                    incident_data['resolved_at'],
                    incident_data['escalated'],
                    incident_data['escalated_at'],
                    incident_data['resolution_notes'],
                    incident_data['created_by']
                ))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Failed to store incident in SQLite: {e}")
            return False
    
    def _store_in_jsonl(self, incident_data: Dict[str, Any]):
        """Store incident in JSONL file as fallback."""
        try:
            with open(self.jsonl_path, 'a') as f:
                f.write(json.dumps(incident_data) + '\n')
        except Exception as e:
            logger.error(f"Failed to store incident in JSONL: {e}")
    
    def get_incident(self, incident_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a specific incident by ID."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT * FROM incidents WHERE id = ?", 
                    (incident_id,)
                )
                row = cursor.fetchone()
                
                if row:
                    incident = dict(row)
                    incident['context'] = json.loads(incident['context']) if incident['context'] else {}
                    return incident
                    
        except Exception as e:
            logger.error(f"Failed to retrieve incident from database: {e}")
        
        # Fallback to JSONL
        return self._get_incident_from_jsonl(incident_id)
    
    def _get_incident_from_jsonl(self, incident_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve incident from JSONL file."""
        try:
            if os.path.exists(self.jsonl_path):
                with open(self.jsonl_path, 'r') as f:
                    for line in f:
                        incident = json.loads(line.strip())
                        if incident['id'] == incident_id:
                            return incident
        except Exception as e:
            logger.error(f"Failed to retrieve incident from JSONL: {e}")
        
        return None
    
    def get_recent_incidents(self, days: int = 7, 
                           incident_type: str = None) -> List[Dict[str, Any]]:
        """Get recent incidents within the specified number of days."""
        cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                if incident_type:
                    cursor = conn.execute("""
                        SELECT * FROM incidents 
                        WHERE created_at >= ? AND incident_type = ?
                        ORDER BY created_at DESC
                    """, (cutoff_date, incident_type))
                else:
                    cursor = conn.execute("""
                        SELECT * FROM incidents 
                        WHERE created_at >= ?
                        ORDER BY created_at DESC
                    """, (cutoff_date,))
                
                incidents = []
                for row in cursor.fetchall():
                    incident = dict(row)
                    incident['context'] = json.loads(incident['context']) if incident['context'] else {}
                    incidents.append(incident)
                
                return incidents
                
        except Exception as e:
            logger.error(f"Failed to retrieve recent incidents from database: {e}")
        
        # Fallback to JSONL
        return self._get_recent_incidents_from_jsonl(days, incident_type)
    
    def _get_recent_incidents_from_jsonl(self, days: int, 
                                       incident_type: str = None) -> List[Dict[str, Any]]:
        """Get recent incidents from JSONL file."""
        cutoff_date = datetime.now() - timedelta(days=days)
        incidents = []
        
        try:
            if os.path.exists(self.jsonl_path):
                with open(self.jsonl_path, 'r') as f:
                    for line in f:
                        incident = json.loads(line.strip())
                        incident_date = datetime.fromisoformat(incident['created_at'])
                        
                        if incident_date >= cutoff_date:
                            if not incident_type or incident['incident_type'] == incident_type:
                                incidents.append(incident)
                
                # Sort by created_at descending
                incidents.sort(key=lambda x: x['created_at'], reverse=True)
                
        except Exception as e:
            logger.error(f"Failed to retrieve incidents from JSONL: {e}")
        
        return incidents
    
    def update_incident(self, incident_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing incident."""
        updates['updated_at'] = datetime.now().isoformat()
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Build dynamic UPDATE query
                set_clauses = []
                values = []
                
                for key, value in updates.items():
                    if key == 'context':
                        set_clauses.append(f"{key} = ?")
                        values.append(json.dumps(value))
                    else:
                        set_clauses.append(f"{key} = ?")
                        values.append(value)
                
                values.append(incident_id)
                
                query = f"UPDATE incidents SET {', '.join(set_clauses)} WHERE id = ?"
                conn.execute(query, values)
                conn.commit()
                
                logger.info(f"Incident {incident_id} updated")
                return True
                
        except Exception as e:
            logger.error(f"Failed to update incident: {e}")
            return False
    
    def escalate_incident(self, incident_id: str, escalation_notes: str = None) -> bool:
        """Mark an incident as escalated."""
        updates = {
            'escalated': True,
            'escalated_at': datetime.now().isoformat()
        }
        
        if escalation_notes:
            updates['resolution_notes'] = escalation_notes
        
        return self.update_incident(incident_id, updates)
    
    def resolve_incident(self, incident_id: str, resolution_notes: str = None) -> bool:
        """Mark an incident as resolved."""
        updates = {
            'status': 'resolved',
            'resolved_at': datetime.now().isoformat()
        }
        
        if resolution_notes:
            updates['resolution_notes'] = resolution_notes
        
        return self.update_incident(incident_id, updates)
    
    def get_incident_stats(self, days: int = 30) -> Dict[str, Any]:
        """Get incident statistics for the specified period."""
        cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Total incidents
                total_cursor = conn.execute(
                    "SELECT COUNT(*) as count FROM incidents WHERE created_at >= ?",
                    (cutoff_date,)
                )
                total_incidents = total_cursor.fetchone()[0]
                
                # By type
                type_cursor = conn.execute("""
                    SELECT incident_type, COUNT(*) as count 
                    FROM incidents 
                    WHERE created_at >= ? 
                    GROUP BY incident_type
                """, (cutoff_date,))
                by_type = {row[0]: row[1] for row in type_cursor.fetchall()}
                
                # By severity
                severity_cursor = conn.execute("""
                    SELECT severity, COUNT(*) as count 
                    FROM incidents 
                    WHERE created_at >= ? 
                    GROUP BY severity
                """, (cutoff_date,))
                by_severity = {row[0]: row[1] for row in severity_cursor.fetchall()}
                
                # By status
                status_cursor = conn.execute("""
                    SELECT status, COUNT(*) as count 
                    FROM incidents 
                    WHERE created_at >= ? 
                    GROUP BY status
                """, (cutoff_date,))
                by_status = {row[0]: row[1] for row in status_cursor.fetchall()}
                
                # Escalated count
                escalated_cursor = conn.execute(
                    "SELECT COUNT(*) as count FROM incidents WHERE created_at >= ? AND escalated = 1",
                    (cutoff_date,)
                )
                escalated_count = escalated_cursor.fetchone()[0]
                
                return {
                    'period_days': days,
                    'total_incidents': total_incidents,
                    'escalated_incidents': escalated_count,
                    'by_type': by_type,
                    'by_severity': by_severity,
                    'by_status': by_status,
                    'generated_at': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Failed to get incident stats: {e}")
            return {
                'error': str(e),
                'generated_at': datetime.now().isoformat()
            }

# Global incident store instance
_incident_store = None

def get_incident_store() -> IncidentStore:
    """Get the global incident store instance."""
    global _incident_store
    if _incident_store is None:
        _incident_store = IncidentStore()
    return _incident_store

# Convenience functions for direct import
def log_incident(incident_type: str, summary: str, context: Dict[str, Any] = None, 
                severity: str = "medium") -> str:
    """Log a new incident."""
    store = get_incident_store()
    return store.log_incident(incident_type, summary, context, severity)

def get_incident(incident_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific incident."""
    store = get_incident_store()
    return store.get_incident(incident_id)

def get_recent_incidents(days: int = 7) -> List[Dict[str, Any]]:
    """Get recent incidents."""
    store = get_incident_store()
    return store.get_recent_incidents(days)

def escalate_incident(incident_id: str, escalation_notes: str = None) -> bool:
    """Escalate an incident."""
    store = get_incident_store()
    
    # Send notification about escalation
    try:
        from ops.notifications import notify_pipeline_failure
        incident = store.get_incident(incident_id)
        if incident:
            notify_pipeline_failure(
                f"Incident escalated: {incident['summary']}",
                "incident_management",
                context={
                    'incident_id': incident_id,
                    'incident_type': incident['incident_type'],
                    'severity': incident['severity'],
                    'escalation_notes': escalation_notes
                }
            )
    except Exception as e:
        logger.error(f"Failed to send escalation notification: {e}")
    
    return store.escalate_incident(incident_id, escalation_notes)

def resolve_incident(incident_id: str, resolution_notes: str = None) -> bool:
    """Resolve an incident."""
    store = get_incident_store()
    return store.resolve_incident(incident_id, resolution_notes)

def get_incident_stats(days: int = 30) -> Dict[str, Any]:
    """Get incident statistics."""
    store = get_incident_store()
    return store.get_incident_stats(days)

def main():
    """Test the incident store functionality."""
    print("Testing incident store...")
    
    # Log a test incident
    incident_id = log_incident(
        incident_type="test_incident",
        summary="Test incident for development",
        context={"component": "test", "details": "This is a test"},
        severity="low"
    )
    print(f"Created incident: {incident_id}")
    
    # Retrieve the incident
    incident = get_incident(incident_id)
    print(f"Retrieved incident: {incident}")
    
    # Get recent incidents
    recent = get_recent_incidents(days=1)
    print(f"Recent incidents: {len(recent)}")
    
    # Get stats
    stats = get_incident_stats(days=30)
    print(f"Incident stats: {stats}")
    
    # Resolve the test incident
    resolve_incident(incident_id, "Test completed successfully")
    print(f"Resolved incident: {incident_id}")

if __name__ == "__main__":
    main()