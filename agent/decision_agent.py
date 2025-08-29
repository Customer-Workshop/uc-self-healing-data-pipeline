#!/usr/bin/env python3
"""
Decision Agent for Autonomous Data Pipeline Remediation
Uses LangChain to make intelligent decisions based on pipeline metrics and data quality results.
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path

from langchain.llms import Ollama
from langchain_openai import OpenAI
from langchain.schema import HumanMessage, SystemMessage, AIMessage
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Import local tools
sys.path.append('/workspace')
from agent.tools import PipelineTools, ToolResult

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DecisionContext(BaseModel):
    """Context information for decision making."""
    run_report: Dict[str, Any]
    ge_results: Dict[str, Any]
    recent_incidents: List[Dict[str, Any]]
    pipeline_config: Dict[str, Any]
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

class DecisionResult(BaseModel):
    """Result of the decision making process."""
    decision: str
    reasoning: str
    actions_taken: List[Dict[str, Any]]
    confidence: float
    escalation_required: bool
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

class PipelineDecisionAgent:
    """Autonomous decision agent for data pipeline remediation."""
    
    def __init__(self):
        self.tools = PipelineTools()
        self.llm = self._initialize_llm()
        self.decision_history = []
        self.config = self._load_config()
        
        # Decision thresholds from configuration
        self.thresholds = {
            'late_arrival_threshold': 0.15,  # 15% late arrivals
            'dq_failure_threshold': 0.05,    # 5% DQ failures
            'schema_drift_auto_remap': True,
            'incident_escalation_threshold': 3,  # consecutive failures
            'confidence_threshold': 0.7      # minimum confidence for automatic actions
        }
        
        logger.info("Decision agent initialized")
    
    def _initialize_llm(self):
        """Initialize the LLM based on configuration."""
        llm_provider = os.getenv('LLM_PROVIDER', 'ollama').lower()
        
        if llm_provider == 'openai':
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key:
                logger.warning("OpenAI API key not found, falling back to Ollama")
                return self._init_ollama()
            
            return OpenAI(
                openai_api_key=api_key,
                model_name="gpt-3.5-turbo-instruct",
                temperature=0.1,
                max_tokens=1000
            )
        else:
            return self._init_ollama()
    
    def _init_ollama(self):
        """Initialize Ollama LLM."""
        model = os.getenv('OLLAMA_MODEL', 'llama3.1')
        try:
            return Ollama(
                model=model,
                base_url="http://localhost:11434",
                temperature=0.1
            )
        except Exception as e:
            logger.error(f"Failed to initialize Ollama: {e}")
            # Return a mock LLM for testing
            return None
    
    def _load_config(self) -> Dict[str, Any]:
        """Load agent configuration."""
        config_path = "/workspace/airflow/include/config.yml"
        # For now, return default config since YAML parsing would need additional dependency
        return {
            'monitoring': {
                'late_arrival_threshold_minutes': 15,
                'schema_drift_action': 'remap',
                'dq_failure_threshold': 0.05,
                'incident_escalation_threshold': 3
            }
        }
    
    def load_context(self, run_report_path: str, ge_results_path: str) -> DecisionContext:
        """Load decision context from file paths."""
        try:
            # Load run report
            run_report = {}
            if os.path.exists(run_report_path):
                with open(run_report_path, 'r') as f:
                    run_report = json.load(f)
            
            # Load Great Expectations results
            ge_results = {}
            if os.path.exists(ge_results_path):
                with open(ge_results_path, 'r') as f:
                    ge_results = json.load(f)
            
            # Load recent incidents
            recent_incidents = self._load_recent_incidents()
            
            return DecisionContext(
                run_report=run_report,
                ge_results=ge_results,
                recent_incidents=recent_incidents,
                pipeline_config=self.config
            )
            
        except Exception as e:
            logger.error(f"Error loading context: {e}")
            return DecisionContext(
                run_report={},
                ge_results={},
                recent_incidents=[],
                pipeline_config=self.config
            )
    
    def _load_recent_incidents(self, days: int = 7) -> List[Dict[str, Any]]:
        """Load recent incidents from incident store."""
        try:
            from ops.incident_store import get_recent_incidents
            return get_recent_incidents(days=days)
        except Exception as e:
            logger.error(f"Error loading recent incidents: {e}")
            return []
    
    def _create_decision_prompt(self, context: DecisionContext) -> str:
        """Create the decision prompt for the LLM."""
        prompt_template = """You are an autonomous data pipeline operations specialist responsible for analyzing pipeline metrics and taking corrective actions. Based on the provided context, determine the appropriate remediation actions.

CONTEXT ANALYSIS:
=================

Pipeline Run Report:
{run_report}

Data Quality Results:
{ge_results}

Recent Incidents (last 7 days):
{recent_incidents}

DECISION FRAMEWORK:
==================

Consider these conditions and recommended actions:

1. SCHEMA DRIFT:
   - If schema drift detected with additive nullable fields → Apply schema remapping
   - If schema drift with breaking changes → Quarantine and escalate

2. LATE ARRIVALS:
   - If late arrival rate < 15% → Retrigger next micro-batch
   - If late arrival rate > 15% → Escalate for investigation

3. DATA QUALITY FAILURES:
   - If DQ failure rate < 5% → Log and continue
   - If DQ failures localized (specific country/plan) → Quarantine affected records
   - If DQ failure rate > 5% → Escalate incident

4. REPEATED FAILURES:
   - If 3+ consecutive failures of same type → Escalate incident
   - If processing errors detected → Retrigger with different configuration

5. ESCALATION CRITERIA:
   - Critical data quality issues affecting >10% of records
   - Infrastructure failures (Kafka, Spark, Snowflake connectivity)
   - Schema changes requiring manual intervention
   - Repeated automatic remediation failures

Available Tools:
- retrigger_task(dag_id, task_id): Restart a failed or problematic task
- apply_schema_remap(mapping): Apply schema mapping for drift handling
- quarantine_records(filter_condition, source_path, reason): Isolate problematic records
- notify_ops(message, severity, title): Send notifications to operations team
- escalate_incident(summary, context, severity): Create and escalate incidents

RESPONSE FORMAT:
================

Provide your analysis and decision in this exact format:

ANALYSIS:
- Current pipeline status: [healthy/degraded/critical]
- Key issues identified: [list main issues]
- Risk assessment: [low/medium/high]

DECISION:
[Primary action to take]

REASONING:
[Explain why this decision was made based on the data]

ACTIONS:
[List specific tool calls with parameters in JSON format]

CONFIDENCE:
[0.0-1.0 confidence score in this decision]

ESCALATION:
[true/false - whether human intervention is required]

Now analyze the current situation and provide your decision:"""

        return prompt_template.format(
            run_report=json.dumps(context.run_report, indent=2),
            ge_results=json.dumps(context.ge_results, indent=2),
            recent_incidents=json.dumps(context.recent_incidents, indent=2)
        )
    
    def _parse_llm_response(self, response: str) -> DecisionResult:
        """Parse the LLM response into structured decision result."""
        try:
            # Extract sections from response
            sections = self._extract_sections(response)
            
            # Parse actions (look for JSON formatted tool calls)
            actions = self._extract_actions(sections.get('actions', ''))
            
            # Parse confidence
            confidence = self._extract_confidence(sections.get('confidence', '0.7'))
            
            # Parse escalation flag
            escalation = self._extract_escalation(sections.get('escalation', 'false'))
            
            return DecisionResult(
                decision=sections.get('decision', 'No decision made').strip(),
                reasoning=sections.get('reasoning', 'No reasoning provided').strip(),
                actions_taken=[],  # Will be populated after executing actions
                confidence=confidence,
                escalation_required=escalation
            )
            
        except Exception as e:
            logger.error(f"Error parsing LLM response: {e}")
            # Return fallback decision
            return DecisionResult(
                decision="Error in decision making - manual review required",
                reasoning=f"Failed to parse LLM response: {str(e)}",
                actions_taken=[],
                confidence=0.0,
                escalation_required=True
            )
    
    def _extract_sections(self, response: str) -> Dict[str, str]:
        """Extract sections from LLM response."""
        sections = {}
        current_section = None
        current_content = []
        
        for line in response.split('\n'):
            line = line.strip()
            if line.upper().endswith(':') and len(line) < 20:
                if current_section:
                    sections[current_section.lower()] = '\n'.join(current_content)
                current_section = line[:-1]
                current_content = []
            elif current_section:
                current_content.append(line)
        
        # Add the last section
        if current_section:
            sections[current_section.lower()] = '\n'.join(current_content)
        
        return sections
    
    def _extract_actions(self, actions_text: str) -> List[Dict[str, Any]]:
        """Extract action tool calls from actions section."""
        actions = []
        try:
            # Look for JSON-like patterns in the actions text
            import re
            json_pattern = r'\{[^}]+\}'
            matches = re.findall(json_pattern, actions_text)
            
            for match in matches:
                try:
                    action = json.loads(match)
                    actions.append(action)
                except json.JSONDecodeError:
                    # Try to fix common JSON issues
                    fixed_match = match.replace("'", '"')
                    try:
                        action = json.loads(fixed_match)
                        actions.append(action)
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse action: {match}")
        except Exception as e:
            logger.error(f"Error extracting actions: {e}")
        
        return actions
    
    def _extract_confidence(self, confidence_text: str) -> float:
        """Extract confidence score from text."""
        try:
            # Look for decimal numbers
            import re
            numbers = re.findall(r'\d+\.?\d*', confidence_text)
            if numbers:
                confidence = float(numbers[0])
                return min(max(confidence, 0.0), 1.0)  # Clamp between 0 and 1
        except:
            pass
        return 0.5  # Default confidence
    
    def _extract_escalation(self, escalation_text: str) -> bool:
        """Extract escalation flag from text."""
        return 'true' in escalation_text.lower()
    
    def execute_actions(self, actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Execute the decided actions using available tools."""
        executed_actions = []
        
        for action in actions:
            try:
                tool_name = action.get('tool')
                tool_params = action.get('params', {})
                
                if not tool_name:
                    logger.warning(f"No tool specified in action: {action}")
                    continue
                
                logger.info(f"Executing tool: {tool_name} with params: {tool_params}")
                result = self.tools.execute_tool(tool_name, **tool_params)
                
                executed_actions.append({
                    'tool': tool_name,
                    'params': tool_params,
                    'result': result.dict(),
                    'timestamp': datetime.now().isoformat()
                })
                
                logger.info(f"Tool execution result: {result.message}")
                
            except Exception as e:
                logger.error(f"Error executing action {action}: {e}")
                executed_actions.append({
                    'tool': action.get('tool', 'unknown'),
                    'params': action.get('params', {}),
                    'result': {
                        'success': False,
                        'message': f"Execution error: {str(e)}",
                        'timestamp': datetime.now().isoformat()
                    },
                    'timestamp': datetime.now().isoformat()
                })
        
        return executed_actions
    
    def make_decision(self, run_report_path: str, ge_results_path: str) -> DecisionResult:
        """Main decision making method."""
        logger.info("=== Starting Decision Analysis ===")
        
        try:
            # Load context
            context = self.load_context(run_report_path, ge_results_path)
            logger.info("Context loaded successfully")
            
            # If no LLM available, use rule-based fallback
            if self.llm is None:
                return self._rule_based_decision(context)
            
            # Create decision prompt
            prompt = self._create_decision_prompt(context)
            
            # Get LLM decision
            response = self.llm(prompt)
            logger.info("LLM decision received")
            
            # Parse response
            decision_result = self._parse_llm_response(response)
            
            # Execute actions if confidence is high enough
            if decision_result.confidence >= self.thresholds['confidence_threshold']:
                actions = self._extract_actions(response)
                executed_actions = self.execute_actions(actions)
                decision_result.actions_taken = executed_actions
            else:
                logger.warning(f"Low confidence decision ({decision_result.confidence}), skipping automatic actions")
            
            # Log decision
            self._log_decision(decision_result, context)
            
            return decision_result
            
        except Exception as e:
            logger.error(f"Error in decision making: {e}")
            return DecisionResult(
                decision="Error in autonomous decision making",
                reasoning=f"Exception occurred: {str(e)}",
                actions_taken=[],
                confidence=0.0,
                escalation_required=True
            )
    
    def _rule_based_decision(self, context: DecisionContext) -> DecisionResult:
        """Fallback rule-based decision making when LLM is not available."""
        run_report = context.run_report
        ge_results = context.ge_results
        
        actions_taken = []
        decision = "Rule-based analysis"
        reasoning = []
        escalation_required = False
        
        # Check for schema drift
        if run_report.get('schema_drift_detected', False):
            reasoning.append("Schema drift detected")
            if self.thresholds['schema_drift_auto_remap']:
                result = self.tools.schema_remap.apply_schema_remap({
                    "auto_remap": True,
                    "timestamp": datetime.now().isoformat()
                })
                actions_taken.append({
                    'tool': 'apply_schema_remap',
                    'result': result.dict()
                })
                reasoning.append("Applied automatic schema remapping")
        
        # Check late arrivals
        total_records = run_report.get('total_records', 0)
        late_records = run_report.get('late_records', 0)
        
        if total_records > 0:
            late_rate = late_records / total_records
            if late_rate > self.thresholds['late_arrival_threshold']:
                reasoning.append(f"High late arrival rate: {late_rate:.2%}")
                escalation_required = True
        
        # Check DQ failures
        dq_failures = run_report.get('dq_failures', 0)
        if total_records > 0:
            dq_failure_rate = dq_failures / total_records
            if dq_failure_rate > self.thresholds['dq_failure_threshold']:
                reasoning.append(f"High DQ failure rate: {dq_failure_rate:.2%}")
                escalation_required = True
        
        # Default notification
        if not reasoning:
            reasoning.append("Pipeline running normally")
        
        result = self.tools.notification.notify_ops(
            message=f"Pipeline status: {'; '.join(reasoning)}",
            severity="info" if not escalation_required else "warning"
        )
        actions_taken.append({
            'tool': 'notify_ops',
            'result': result.dict()
        })
        
        return DecisionResult(
            decision=decision,
            reasoning="; ".join(reasoning),
            actions_taken=actions_taken,
            confidence=0.8,  # Rule-based decisions have high confidence
            escalation_required=escalation_required
        )
    
    def _log_decision(self, decision: DecisionResult, context: DecisionContext):
        """Log the decision for audit trail."""
        try:
            log_entry = {
                'timestamp': decision.timestamp,
                'decision': decision.decision,
                'reasoning': decision.reasoning,
                'confidence': decision.confidence,
                'escalation_required': decision.escalation_required,
                'actions_count': len(decision.actions_taken),
                'context_summary': {
                    'total_records': context.run_report.get('total_records', 0),
                    'late_records': context.run_report.get('late_records', 0),
                    'dq_failures': context.run_report.get('dq_failures', 0),
                    'schema_drift': context.run_report.get('schema_drift_detected', False)
                }
            }
            
            # Ensure log directory exists
            os.makedirs('/data/ops', exist_ok=True)
            
            # Append to decision log
            with open('/data/ops/decision_log.jsonl', 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
            
            self.decision_history.append(log_entry)
            
        except Exception as e:
            logger.error(f"Error logging decision: {e}")

def main():
    """Main entry point for the decision agent."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Data Pipeline Decision Agent")
    parser.add_argument('--run-report', required=True, help="Path to run report JSON")
    parser.add_argument('--ge-results', required=True, help="Path to Great Expectations results JSON")
    parser.add_argument('--output', help="Path to save decision result JSON")
    
    args = parser.parse_args()
    
    logger.info("=== Data Pipeline Decision Agent ===")
    logger.info(f"Run report: {args.run_report}")
    logger.info(f"GE results: {args.ge_results}")
    
    try:
        agent = PipelineDecisionAgent()
        decision = agent.make_decision(args.run_report, args.ge_results)
        
        logger.info(f"Decision: {decision.decision}")
        logger.info(f"Confidence: {decision.confidence}")
        logger.info(f"Actions taken: {len(decision.actions_taken)}")
        logger.info(f"Escalation required: {decision.escalation_required}")
        
        # Save decision result if output path provided
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(decision.dict(), f, indent=2)
            logger.info(f"Decision result saved to {args.output}")
        
        # Exit with non-zero code if escalation required
        if decision.escalation_required:
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Agent execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()