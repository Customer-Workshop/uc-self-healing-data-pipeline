"""
Notification utilities for pipeline alerts and incidents.
Supports Slack webhooks and email notifications.
"""

import json
import logging
import os
import smtplib
from datetime import datetime
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from typing import Optional, Dict, Any

import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class NotificationManager:
    """Manages various notification channels for the data pipeline."""
    
    def __init__(self):
        self.slack_webhook_url = os.getenv('ALERT_SLACK_WEBHOOK_URL')
        self.alert_email = os.getenv('ALERT_EMAIL')
        self.smtp_server = os.getenv('SMTP_SERVER', 'localhost')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.smtp_user = os.getenv('SMTP_USER')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        
        # Email template settings
        self.from_email = os.getenv('FROM_EMAIL', 'pipeline@dataeng.local')
        self.email_enabled = bool(self.alert_email)
        self.slack_enabled = bool(self.slack_webhook_url)
        
        logger.info(f"Notifications initialized - Slack: {self.slack_enabled}, Email: {self.email_enabled}")

def send_slack(message: str, title: str = "Data Pipeline Alert", 
               severity: str = "info", channel: str = None) -> str:
    """
    Send notification to Slack via webhook.
    
    Args:
        message: The message content to send
        title: The title/header for the message
        severity: Severity level (info, warning, error, critical)
        channel: Optional specific channel to send to
        
    Returns:
        Status message indicating success or failure
    """
    try:
        webhook_url = os.getenv('ALERT_SLACK_WEBHOOK_URL')
        
        if not webhook_url:
            logger.warning("Slack webhook URL not configured")
            return "Slack webhook not configured"
        
        # Color coding based on severity
        color_map = {
            'info': '#36a64f',      # green
            'warning': '#ffaa00',   # orange
            'error': '#ff0000',     # red
            'critical': '#8b0000'   # dark red
        }
        
        color = color_map.get(severity.lower(), '#36a64f')
        
        # Create Slack message payload
        payload = {
            "text": title,
            "attachments": [
                {
                    "color": color,
                    "fields": [
                        {
                            "title": "Severity",
                            "value": severity.upper(),
                            "short": True
                        },
                        {
                            "title": "Timestamp",
                            "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
                            "short": True
                        },
                        {
                            "title": "Message",
                            "value": message,
                            "short": False
                        }
                    ],
                    "footer": "Data Pipeline Orchestrator",
                    "footer_icon": ":robot_face:"
                }
            ]
        }
        
        # Add channel if specified
        if channel:
            payload["channel"] = channel
        
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info("Slack notification sent successfully")
            return "Slack notification sent successfully"
        else:
            error_msg = f"Slack notification failed: {response.status_code} - {response.text}"
            logger.error(error_msg)
            return error_msg
            
    except Exception as e:
        error_msg = f"Error sending Slack notification: {str(e)}"
        logger.error(error_msg)
        return error_msg

def send_email(subject: str, body: str, to_email: str = None, 
               severity: str = "info", html_body: str = None) -> str:
    """
    Send email notification.
    
    Args:
        subject: Email subject line
        body: Email body content (plain text)
        to_email: Recipient email (uses ALERT_EMAIL if not specified)
        severity: Severity level for styling
        html_body: Optional HTML version of the body
        
    Returns:
        Status message indicating success or failure
    """
    try:
        recipient = to_email or os.getenv('ALERT_EMAIL')
        
        if not recipient:
            logger.warning("No email recipient configured")
            return "Email recipient not configured"
        
        # Use simple print for local development if SMTP not configured
        smtp_server = os.getenv('SMTP_SERVER')
        if not smtp_server or smtp_server == 'localhost':
            print("=" * 50)
            print(f"EMAIL NOTIFICATION")
            print(f"To: {recipient}")
            print(f"Subject: {subject}")
            print(f"Severity: {severity}")
            print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print("-" * 50)
            print(body)
            print("=" * 50)
            logger.info("Email notification printed to console (SMTP not configured)")
            return "Email notification sent to console"
        
        # Create message
        msg = MimeMultipart('alternative')
        msg['Subject'] = f"[{severity.upper()}] {subject}"
        msg['From'] = os.getenv('FROM_EMAIL', 'pipeline@dataeng.local')
        msg['To'] = recipient
        msg['X-Priority'] = '1' if severity.lower() in ['error', 'critical'] else '3'
        
        # Add timestamp to body
        timestamped_body = f"{body}\n\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
        
        # Plain text part
        text_part = MimeText(timestamped_body, 'plain')
        msg.attach(text_part)
        
        # HTML part if provided
        if html_body:
            html_part = MimeText(html_body, 'html')
            msg.attach(html_part)
        
        # Send the email
        smtp_port = int(os.getenv('SMTP_PORT', '587'))
        smtp_user = os.getenv('SMTP_USER')
        smtp_password = os.getenv('SMTP_PASSWORD')
        
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            if smtp_port == 587:
                server.starttls()
            
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            
            server.send_message(msg)
        
        logger.info(f"Email notification sent to {recipient}")
        return f"Email notification sent to {recipient}"
        
    except Exception as e:
        error_msg = f"Error sending email notification: {str(e)}"
        logger.error(error_msg)
        return error_msg

def send_notification(message: str, title: str = "Pipeline Alert", 
                     severity: str = "info", channels: list = None) -> Dict[str, str]:
    """
    Send notification to all configured channels.
    
    Args:
        message: The message content
        title: The notification title
        severity: Severity level (info, warning, error, critical)
        channels: List of channels to send to ['slack', 'email'] or None for all
        
    Returns:
        Dictionary with results from each channel
    """
    results = {}
    
    # Default to all channels if none specified
    if channels is None:
        channels = ['slack', 'email']
    
    # Send Slack notification
    if 'slack' in channels:
        results['slack'] = send_slack(message, title, severity)
    
    # Send email notification
    if 'email' in channels:
        results['email'] = send_email(title, message, severity=severity)
    
    return results

def notify_pipeline_failure(error_message: str, component: str, 
                          context: Dict[str, Any] = None) -> Dict[str, str]:
    """
    Send notifications specifically for pipeline failures.
    
    Args:
        error_message: Description of the failure
        component: Which component failed (kafka, spark, airflow, etc.)
        context: Additional context information
        
    Returns:
        Dictionary with notification results
    """
    title = f"Pipeline Failure - {component.title()}"
    
    # Build detailed message
    message_parts = [
        f"Component: {component}",
        f"Error: {error_message}",
    ]
    
    if context:
        message_parts.append("Additional Context:")
        for key, value in context.items():
            message_parts.append(f"  {key}: {value}")
    
    message = "\n".join(message_parts)
    
    return send_notification(
        message=message,
        title=title,
        severity="error"
    )

def notify_data_quality_issue(issue_type: str, affected_records: int, 
                            total_records: int, details: str = None) -> Dict[str, str]:
    """
    Send notifications for data quality issues.
    
    Args:
        issue_type: Type of DQ issue (validation_failure, schema_drift, etc.)
        affected_records: Number of records affected
        total_records: Total number of records processed
        details: Additional details about the issue
        
    Returns:
        Dictionary with notification results
    """
    failure_rate = (affected_records / total_records * 100) if total_records > 0 else 0
    
    title = f"Data Quality Issue - {issue_type.replace('_', ' ').title()}"
    
    message_parts = [
        f"Issue Type: {issue_type}",
        f"Affected Records: {affected_records:,} out of {total_records:,}",
        f"Failure Rate: {failure_rate:.2f}%",
    ]
    
    if details:
        message_parts.append(f"Details: {details}")
    
    message = "\n".join(message_parts)
    
    # Determine severity based on failure rate
    if failure_rate > 10:
        severity = "critical"
    elif failure_rate > 5:
        severity = "error"
    elif failure_rate > 1:
        severity = "warning"
    else:
        severity = "info"
    
    return send_notification(
        message=message,
        title=title,
        severity=severity
    )

def notify_schema_drift(drift_details: Dict[str, Any]) -> Dict[str, str]:
    """
    Send notifications for schema drift events.
    
    Args:
        drift_details: Details about the schema changes detected
        
    Returns:
        Dictionary with notification results
    """
    title = "Schema Drift Detected"
    
    message_parts = [
        "Schema evolution detected in incoming data:",
    ]
    
    if 'new_fields' in drift_details:
        message_parts.append(f"New Fields: {', '.join(drift_details['new_fields'])}")
    
    if 'removed_fields' in drift_details:
        message_parts.append(f"Removed Fields: {', '.join(drift_details['removed_fields'])}")
    
    if 'type_changes' in drift_details:
        message_parts.append(f"Type Changes: {drift_details['type_changes']}")
    
    if 'version' in drift_details:
        message_parts.append(f"Schema Version: {drift_details['version']}")
    
    message = "\n".join(message_parts)
    
    return send_notification(
        message=message,
        title=title,
        severity="warning"
    )

# For backward compatibility and direct imports
def main():
    """Test notification functionality."""
    print("Testing notification system...")
    
    # Test Slack
    slack_result = send_slack(
        message="Test message from the data pipeline notification system",
        title="Notification System Test",
        severity="info"
    )
    print(f"Slack result: {slack_result}")
    
    # Test Email
    email_result = send_email(
        subject="Notification System Test",
        body="This is a test message from the data pipeline notification system.",
        severity="info"
    )
    print(f"Email result: {email_result}")

if __name__ == "__main__":
    main()