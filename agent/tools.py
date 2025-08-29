"""
Agent Tools for Data Pipeline Remediation
Provides tools for the decision agent to take corrective actions.
"""

import json
import logging
import os
import requests
import shutil
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

class ToolResult(BaseModel):
    """Standard result format for all tools."""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

class AirflowTool:
    """Tool for interacting with Airflow REST API."""
    
    def __init__(self, airflow_url: str = "http://localhost:8080", 
                 username: str = "airflow", password: str = "airflow"):
        self.base_url = airflow_url.rstrip('/')
        self.auth = (username, password)
        self.session = requests.Session()
        self.session.auth = self.auth
    
    def retrigger_task(self, dag_id: str, task_id: str, execution_date: Optional[str] = None) -> ToolResult:
        """Retrigger a specific Airflow task."""
        try:
            if not execution_date:
                # Get the latest DAG run
                dag_runs_url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns"
                response = self.session.get(dag_runs_url, params={"limit": 1, "order_by": "-execution_date"})
                
                if response.status_code != 200:
                    return ToolResult(
                        success=False,
                        message=f"Failed to get DAG runs: {response.status_code}"
                    )
                
                dag_runs = response.json()
                if not dag_runs.get('dag_runs'):
                    return ToolResult(
                        success=False,
                        message=f"No DAG runs found for {dag_id}"
                    )
                
                execution_date = dag_runs['dag_runs'][0]['execution_date']
            
            # Clear the task to retrigger it
            clear_url = f"{self.base_url}/api/v1/dags/{dag_id}/clearTaskInstances"
            clear_payload = {
                "dry_run": False,
                "task_ids": [task_id],
                "dag_run_id": execution_date,
                "include_subdags": False,
                "include_parentdag": False,
                "reset_dag_runs": False
            }
            
            response = self.session.post(clear_url, json=clear_payload)
            
            if response.status_code == 200:
                return ToolResult(
                    success=True,
                    message=f"Successfully retriggered task {task_id} in DAG {dag_id}",
                    data={"dag_id": dag_id, "task_id": task_id, "execution_date": execution_date}
                )
            else:
                return ToolResult(
                    success=False,
                    message=f"Failed to retrigger task: {response.status_code} - {response.text}"
                )
                
        except Exception as e:
            logger.error(f"Error retriggering task: {e}")
            return ToolResult(
                success=False,
                message=f"Error retriggering task: {str(e)}"
            )

class SchemaRemapTool:
    """Tool for applying schema remapping configurations."""
    
    def __init__(self, config_path: str = "/data/ops/schema_remap.json"):
        self.config_path = config_path
        
    def apply_schema_remap(self, mapping: Dict[str, Any]) -> ToolResult:
        """Apply schema remapping configuration."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            
            # Load existing mappings if they exist
            existing_mappings = {}
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    existing_mappings = json.load(f)
            
            # Update with new mapping
            remap_config = {
                "version": existing_mappings.get("version", 0) + 1,
                "updated_at": datetime.now().isoformat(),
                "mappings": {**existing_mappings.get("mappings", {}), **mapping},
                "applied": False  # Flag for Spark job to check
            }
            
            # Write the configuration
            with open(self.config_path, 'w') as f:
                json.dump(remap_config, f, indent=2)
            
            logger.info(f"Applied schema remap: {mapping}")
            
            return ToolResult(
                success=True,
                message=f"Schema remap applied successfully",
                data=remap_config
            )
            
        except Exception as e:
            logger.error(f"Error applying schema remap: {e}")
            return ToolResult(
                success=False,
                message=f"Error applying schema remap: {str(e)}"
            )

class QuarantineTool:
    """Tool for quarantining problematic records."""
    
    def __init__(self, quarantine_path: str = "/data/quarantine"):
        self.quarantine_path = quarantine_path
        
    def quarantine_records(self, filter_condition: str, source_path: str, 
                          reason: str = "data_quality_failure") -> ToolResult:
        """Move records matching filter to quarantine."""
        try:
            # Ensure quarantine directory exists
            quarantine_dir = Path(self.quarantine_path) / reason / datetime.now().strftime("%Y%m%d_%H%M%S")
            quarantine_dir.mkdir(parents=True, exist_ok=True)
            
            # Create quarantine metadata
            metadata = {
                "quarantined_at": datetime.now().isoformat(),
                "reason": reason,
                "filter_condition": filter_condition,
                "source_path": source_path,
                "quarantine_path": str(quarantine_dir)
            }
            
            # Write metadata
            with open(quarantine_dir / "metadata.json", 'w') as f:
                json.dump(metadata, f, indent=2)
            
            # Log the quarantine action (actual record movement would be done by Spark job)
            logger.info(f"Quarantine setup for filter: {filter_condition}")
            
            return ToolResult(
                success=True,
                message=f"Quarantine configured for records matching: {filter_condition}",
                data=metadata
            )
            
        except Exception as e:
            logger.error(f"Error setting up quarantine: {e}")
            return ToolResult(
                success=False,
                message=f"Error setting up quarantine: {str(e)}"
            )

class NotificationTool:
    """Tool for sending notifications."""
    
    def __init__(self):
        self.slack_webhook = os.getenv("ALERT_SLACK_WEBHOOK_URL")
        self.alert_email = os.getenv("ALERT_EMAIL")
    
    def notify_ops(self, message: str, severity: str = "info", 
                   title: str = "Data Pipeline Alert") -> ToolResult:
        """Send notification to operations team."""
        try:
            # Import notification functions
            import sys
            sys.path.append('/workspace')
            from ops.notifications import send_slack, send_email
            
            results = []
            
            # Send Slack notification if configured
            if self.slack_webhook:
                slack_result = send_slack(f"*{title}*\n{message}\nSeverity: {severity}")
                results.append(f"Slack: {slack_result}")
            
            # Send email notification if configured
            if self.alert_email:
                email_result = send_email(title, f"{message}\n\nSeverity: {severity}")
                results.append(f"Email: {email_result}")
            
            if not results:
                results.append("No notification channels configured")
            
            return ToolResult(
                success=True,
                message="Notification sent successfully",
                data={"results": results, "severity": severity}
            )
            
        except Exception as e:
            logger.error(f"Error sending notification: {e}")
            return ToolResult(
                success=False,
                message=f"Error sending notification: {str(e)}"
            )

class IncidentTool:
    """Tool for managing incidents."""
    
    def __init__(self):
        pass
    
    def escalate_incident(self, summary: str, context: Dict[str, Any], 
                         severity: str = "medium") -> ToolResult:
        """Escalate an incident to the incident management system."""
        try:
            # Import incident store functions
            import sys
            sys.path.append('/workspace')
            from ops.incident_store import log_incident, escalate_incident
            
            # Log the incident
            incident_id = log_incident(
                incident_type="pipeline_failure",
                summary=summary,
                context=context,
                severity=severity
            )
            
            # Escalate if severity is high
            if severity.lower() in ["high", "critical"]:
                escalate_result = escalate_incident(incident_id)
                
                return ToolResult(
                    success=True,
                    message=f"Incident {incident_id} logged and escalated",
                    data={"incident_id": incident_id, "escalated": True}
                )
            else:
                return ToolResult(
                    success=True,
                    message=f"Incident {incident_id} logged",
                    data={"incident_id": incident_id, "escalated": False}
                )
                
        except Exception as e:
            logger.error(f"Error escalating incident: {e}")
            return ToolResult(
                success=False,
                message=f"Error escalating incident: {str(e)}"
            )

class PipelineTools:
    """Collection of all pipeline remediation tools."""
    
    def __init__(self):
        self.airflow = AirflowTool()
        self.schema_remap = SchemaRemapTool()
        self.quarantine = QuarantineTool()
        self.notification = NotificationTool()
        self.incident = IncidentTool()
    
    def get_available_tools(self) -> List[str]:
        """Get list of available tool methods."""
        return [
            "retrigger_task",
            "apply_schema_remap", 
            "quarantine_records",
            "notify_ops",
            "escalate_incident"
        ]
    
    def execute_tool(self, tool_name: str, **kwargs) -> ToolResult:
        """Execute a tool by name with provided kwargs."""
        try:
            if tool_name == "retrigger_task":
                return self.airflow.retrigger_task(**kwargs)
            elif tool_name == "apply_schema_remap":
                return self.schema_remap.apply_schema_remap(**kwargs)
            elif tool_name == "quarantine_records":
                return self.quarantine.quarantine_records(**kwargs)
            elif tool_name == "notify_ops":
                return self.notification.notify_ops(**kwargs)
            elif tool_name == "escalate_incident":
                return self.incident.escalate_incident(**kwargs)
            else:
                return ToolResult(
                    success=False,
                    message=f"Unknown tool: {tool_name}"
                )
        except Exception as e:
            logger.error(f"Error executing tool {tool_name}: {e}")
            return ToolResult(
                success=False,
                message=f"Error executing tool {tool_name}: {str(e)}"
            )