"""
Unit tests for the decision agent and tools.
"""

import json
import os
import pytest
import tempfile
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

import sys
sys.path.append('/workspace')

from agent.decision_agent import PipelineDecisionAgent, DecisionContext, DecisionResult
from agent.tools import PipelineTools, ToolResult, AirflowTool, SchemaRemapTool, QuarantineTool


class TestPipelineTools:
    """Test suite for pipeline tools."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.tools = PipelineTools()
    
    def test_schema_remap_tool(self):
        """Test schema remapping functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "schema_remap.json")
            remap_tool = SchemaRemapTool(config_path)
            
            # Test applying schema remap
            mapping = {
                "marketing_opt_in": {"type": "boolean", "nullable": True},
                "customer_segment": {"type": "string", "nullable": True}
            }
            
            result = remap_tool.apply_schema_remap(mapping)
            
            assert result.success is True
            assert "Schema remap applied successfully" in result.message
            assert os.path.exists(config_path)
            
            # Verify config file content
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            assert config["version"] == 1
            assert config["mappings"] == mapping
            assert config["applied"] is False
    
    def test_quarantine_tool(self):
        """Test quarantine functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            quarantine_tool = QuarantineTool(temp_dir)
            
            result = quarantine_tool.quarantine_records(
                filter_condition="country = 'INVALID'",
                source_path="/data/input",
                reason="invalid_country"
            )
            
            assert result.success is True
            assert "Quarantine configured" in result.message
            
            # Check that metadata was created
            metadata_files = []
            for root, dirs, files in os.walk(temp_dir):
                if "metadata.json" in files:
                    metadata_files.append(os.path.join(root, "metadata.json"))
            
            assert len(metadata_files) == 1
            
            with open(metadata_files[0], 'r') as f:
                metadata = json.load(f)
            
            assert metadata["reason"] == "invalid_country"
            assert metadata["filter_condition"] == "country = 'INVALID'"
    
    @patch('requests.Session.post')
    def test_airflow_retrigger_task(self, mock_post):
        """Test Airflow task retriggering."""
        # Mock successful DAG runs response
        mock_dag_runs = MagicMock()
        mock_dag_runs.status_code = 200
        mock_dag_runs.json.return_value = {
            'dag_runs': [{'execution_date': '2024-01-01T00:00:00Z'}]
        }
        
        # Mock successful clear task response
        mock_clear = MagicMock()
        mock_clear.status_code = 200
        
        with patch('requests.Session.get', return_value=mock_dag_runs):
            mock_post.return_value = mock_clear
            
            airflow_tool = AirflowTool()
            result = airflow_tool.retrigger_task("test_dag", "test_task")
            
            assert result.success is True
            assert "Successfully retriggered task" in result.message
    
    def test_tool_execution(self):
        """Test tool execution through the main interface."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock the schema remap tool path
            with patch('agent.tools.SchemaRemapTool.__init__') as mock_init:
                mock_init.return_value = None
                
                with patch.object(self.tools.schema_remap, 'apply_schema_remap') as mock_remap:
                    mock_remap.return_value = ToolResult(
                        success=True,
                        message="Test remap successful"
                    )
                    
                    result = self.tools.execute_tool(
                        "apply_schema_remap",
                        mapping={"test_field": "test_value"}
                    )
                    
                    assert result.success is True
                    assert "Test remap successful" in result.message


class TestDecisionAgent:
    """Test suite for the decision agent."""
    
    def setup_method(self):
        """Set up test fixtures."""
        with patch('agent.decision_agent.PipelineDecisionAgent._initialize_llm'):
            self.agent = PipelineDecisionAgent()
            self.agent.llm = None  # Use rule-based fallback
    
    def test_decision_context_loading(self):
        """Test loading of decision context from files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test run report
            run_report = {
                "total_records": 1000,
                "late_records": 50,
                "dq_failures": 25,
                "schema_drift_detected": True,
                "timestamp": datetime.now().isoformat()
            }
            
            run_report_path = os.path.join(temp_dir, "run_report.json")
            with open(run_report_path, 'w') as f:
                json.dump(run_report, f)
            
            # Create test GE results
            ge_results = {
                "status": "success",
                "summary": {
                    "total_expectations": 15,
                    "success_count": 13,
                    "failure_count": 2,
                    "overall_success_rate": 0.867
                }
            }
            
            ge_results_path = os.path.join(temp_dir, "ge_results.json")
            with open(ge_results_path, 'w') as f:
                json.dump(ge_results, f)
            
            # Load context
            context = self.agent.load_context(run_report_path, ge_results_path)
            
            assert context.run_report["total_records"] == 1000
            assert context.run_report["late_records"] == 50
            assert context.ge_results["summary"]["failure_count"] == 2
            assert context.run_report["schema_drift_detected"] is True
    
    def test_rule_based_decision_schema_drift(self):
        """Test rule-based decision making for schema drift."""
        context = DecisionContext(
            run_report={
                "total_records": 1000,
                "late_records": 10,
                "dq_failures": 5,
                "schema_drift_detected": True
            },
            ge_results={"summary": {"overall_success_rate": 0.95}},
            recent_incidents=[],
            pipeline_config={}
        )
        
        with patch.object(self.agent.tools.schema_remap, 'apply_schema_remap') as mock_remap:
            mock_remap.return_value = ToolResult(success=True, message="Remap applied")
            
            with patch.object(self.agent.tools.notification, 'notify_ops') as mock_notify:
                mock_notify.return_value = ToolResult(success=True, message="Notification sent")
                
                decision = self.agent._rule_based_decision(context)
                
                assert decision.decision == "Rule-based analysis"
                assert "Schema drift detected" in decision.reasoning
                assert len(decision.actions_taken) >= 1
                assert decision.confidence == 0.8
    
    def test_rule_based_decision_high_late_arrivals(self):
        """Test rule-based decision for high late arrival rate."""
        context = DecisionContext(
            run_report={
                "total_records": 1000,
                "late_records": 200,  # 20% late arrival rate
                "dq_failures": 5,
                "schema_drift_detected": False
            },
            ge_results={"summary": {"overall_success_rate": 0.95}},
            recent_incidents=[],
            pipeline_config={}
        )
        
        with patch.object(self.agent.tools.notification, 'notify_ops') as mock_notify:
            mock_notify.return_value = ToolResult(success=True, message="Notification sent")
            
            decision = self.agent._rule_based_decision(context)
            
            assert "High late arrival rate" in decision.reasoning
            assert decision.escalation_required is True
    
    def test_rule_based_decision_high_dq_failures(self):
        """Test rule-based decision for high DQ failure rate."""
        context = DecisionContext(
            run_report={
                "total_records": 1000,
                "late_records": 10,
                "dq_failures": 100,  # 10% failure rate
                "schema_drift_detected": False
            },
            ge_results={"summary": {"overall_success_rate": 0.90}},
            recent_incidents=[],
            pipeline_config={}
        )
        
        with patch.object(self.agent.tools.notification, 'notify_ops') as mock_notify:
            mock_notify.return_value = ToolResult(success=True, message="Notification sent")
            
            decision = self.agent._rule_based_decision(context)
            
            assert "High DQ failure rate" in decision.reasoning
            assert decision.escalation_required is True
    
    def test_make_decision_with_missing_files(self):
        """Test decision making when input files are missing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            run_report_path = os.path.join(temp_dir, "missing_run_report.json")
            ge_results_path = os.path.join(temp_dir, "missing_ge_results.json")
            
            with patch.object(self.agent.tools.notification, 'notify_ops') as mock_notify:
                mock_notify.return_value = ToolResult(success=True, message="Notification sent")
                
                decision = self.agent.make_decision(run_report_path, ge_results_path)
                
                assert decision.decision == "Rule-based analysis"
                assert "Pipeline running normally" in decision.reasoning
    
    def test_extract_sections(self):
        """Test extraction of sections from LLM response."""
        response = """
        ANALYSIS:
        Current pipeline status: healthy
        Key issues: none
        
        DECISION:
        Continue normal operation
        
        REASONING:
        All metrics are within normal ranges
        
        CONFIDENCE:
        0.9
        
        ESCALATION:
        false
        """
        
        sections = self.agent._extract_sections(response)
        
        assert "analysis" in sections
        assert "decision" in sections
        assert "reasoning" in sections
        assert "confidence" in sections
        assert "escalation" in sections
        
        assert "healthy" in sections["analysis"]
        assert "Continue normal operation" in sections["decision"]
    
    def test_extract_confidence(self):
        """Test confidence extraction from text."""
        assert self.agent._extract_confidence("0.85") == 0.85
        assert self.agent._extract_confidence("confidence is 0.92") == 0.92
        assert self.agent._extract_confidence("95%") == 95.0  # Will be clamped to 1.0
        assert self.agent._extract_confidence("no number") == 0.5
    
    def test_extract_escalation(self):
        """Test escalation flag extraction."""
        assert self.agent._extract_escalation("true") is True
        assert self.agent._extract_escalation("TRUE") is True
        assert self.agent._extract_escalation("false") is False
        assert self.agent._extract_escalation("escalation required: true") is True


class TestDecisionScenarios:
    """Test specific decision scenarios."""
    
    def setup_method(self):
        """Set up test fixtures."""
        with patch('agent.decision_agent.PipelineDecisionAgent._initialize_llm'):
            self.agent = PipelineDecisionAgent()
            self.agent.llm = None  # Use rule-based fallback
    
    def test_normal_operation_scenario(self):
        """Test decision for normal pipeline operation."""
        context = DecisionContext(
            run_report={
                "total_records": 1000,
                "late_records": 5,  # 0.5% late arrival
                "dq_failures": 2,   # 0.2% failure rate
                "schema_drift_detected": False
            },
            ge_results={"summary": {"overall_success_rate": 0.998}},
            recent_incidents=[],
            pipeline_config={}
        )
        
        with patch.object(self.agent.tools.notification, 'notify_ops') as mock_notify:
            mock_notify.return_value = ToolResult(success=True, message="Notification sent")
            
            decision = self.agent._rule_based_decision(context)
            
            assert decision.escalation_required is False
            assert "Pipeline running normally" in decision.reasoning
    
    def test_multiple_issues_scenario(self):
        """Test decision when multiple issues are present."""
        context = DecisionContext(
            run_report={
                "total_records": 1000,
                "late_records": 180,  # 18% late arrival
                "dq_failures": 80,    # 8% failure rate
                "schema_drift_detected": True
            },
            ge_results={"summary": {"overall_success_rate": 0.80}},
            recent_incidents=[
                {"type": "pipeline_failure", "timestamp": (datetime.now() - timedelta(hours=2)).isoformat()},
                {"type": "data_quality", "timestamp": (datetime.now() - timedelta(hours=1)).isoformat()}
            ],
            pipeline_config={}
        )
        
        with patch.object(self.agent.tools.schema_remap, 'apply_schema_remap') as mock_remap:
            mock_remap.return_value = ToolResult(success=True, message="Remap applied")
            
            with patch.object(self.agent.tools.notification, 'notify_ops') as mock_notify:
                mock_notify.return_value = ToolResult(success=True, message="Notification sent")
                
                decision = self.agent._rule_based_decision(context)
                
                assert decision.escalation_required is True
                assert "Schema drift detected" in decision.reasoning
                assert "High late arrival rate" in decision.reasoning
                assert "High DQ failure rate" in decision.reasoning


if __name__ == "__main__":
    pytest.main([__file__, "-v"])