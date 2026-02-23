"""
LangGraph Agent - Professional Version
Defines the workflow graph with proper error handling and type safety
"""

import logging
from typing import Dict, Any, Optional, Literal, Callable, List
from dataclasses import dataclass, field
from enum import Enum
from langgraph.graph import Graph, END
from agents.nodes import AgentNodes

logger = logging.getLogger(__name__)

# ============================================================================
# Type Definitions
# ============================================================================

class WorkflowStep(Enum):
    """Workflow step identifiers"""
    PARSE_INTENT = "parse_intent"
    GET_SCHEMA = "get_schema"
    GENERATE_SQL = "generate_sql"
    VALIDATE_SQL = "validate_sql"
    EXECUTE_QUERY = "execute_query"
    FORMAT_RESPONSE = "format_response"
    HANDLE_ERROR = "handle_error"

@dataclass
class AgentResponse:
    """Structured agent response"""
    success: bool
    response: str
    intent: Optional[str] = None
    target_database: Optional[str] = None
    queries: Dict[str, Any] = field(default_factory=dict)
    results_count: Dict[str, int] = field(default_factory=dict)
    error: Optional[str] = None
    workflow_steps: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'success': self.success,
            'response': self.response,
            'intent': self.intent,
            'target_database': self.target_database,
            'queries': self.queries,
            'results_count': self.results_count,
            'error': self.error,
            'workflow_steps': self.workflow_steps,
            'metadata': self.metadata
        }

# ============================================================================
# Routing Decision Functions
# ============================================================================

class WorkflowRouter:
    """Handles routing decisions in the workflow"""
    
    @staticmethod
    def check_early_exit(state: Dict[str, Any]) -> Literal["continue", "exit"]:
        """
        Check if we should exit early (greeting, irrelevant question, etc.)
        
        Returns:
            "exit" if early_exit flag is True, otherwise "continue"
        """
        # CRITICAL FIX: Check if early_exit has a truthy value
        if state.get('early_exit'):
            logger.info("Early exit triggered")
            return "exit"
        return "continue"
    
    @staticmethod
    def should_validate(state: Dict[str, Any]) -> Literal["validate", "error"]:
        """
        Decision: Should we validate the generated queries?
        
        Returns:
            "validate" if queries were generated successfully, otherwise "error"
        """
        # CRITICAL FIX: Check if error has a truthy value (not just if key exists!)
        error = state.get('error')
        queries = state.get('queries', {})
        
        # Check for actual error value and non-empty queries
        if error:
            logger.warning(f"Skipping validation - error detected: {error}")
            return "error"
        
        if not queries:
            logger.warning("Skipping validation - no queries generated")
            state['error'] = "No queries were generated"
            return "error"
        
        logger.debug(f"Proceeding to validation with {len(queries)} queries")
        return "validate"
    
    @staticmethod
    def should_execute(state: Dict[str, Any]) -> Literal["execute", "error"]:
        """
        Decision: Should we execute the validated queries?
        
        Returns:
            "execute" if queries passed validation, otherwise "error"
        """
        # CRITICAL FIX: Check if error has a truthy value
        error = state.get('error')
        validated = state.get('validated_queries', {})
        
        if error:
            logger.warning(f"Skipping execution - validation error: {error}")
            return "error"
        
        if not validated:
            logger.warning("Skipping execution - no validated queries")
            state['error'] = "Query validation failed"
            return "error"
        
        logger.debug(f"Proceeding to execution with {len(validated)} validated queries")
        return "execute"
    
    @staticmethod
    def should_format(state: Dict[str, Any]) -> Literal["format", "error"]:
        """
        Decision: Should we format the results?
        
        Returns:
            "format" if results exist, otherwise "error"
        """
        # CRITICAL FIX: Check for actual error value and results
        error = state.get('error')
        results = state.get('results', {})
        
        # If we have results, format them even if there's an error
        if results:
            logger.debug(f"Proceeding to formatting with {len(results)} result sets")
            return "format"
        
        if error:
            logger.warning(f"Skipping formatting - execution error: {error}")
            return "error"
        
        logger.warning("Skipping formatting - no results")
        state['error'] = "Query execution produced no results"
        return "error"

# ============================================================================
# Progress Tracker
# ============================================================================

class ProgressTracker:
    """Tracks workflow progress for observability"""
    
    def __init__(self):
        self.callbacks: List[Callable[[str, Dict], None]] = []
    
    def add_callback(self, callback: Callable[[str, Dict], None]):
        """Add a progress callback"""
        self.callbacks.append(callback)
    
    def notify(self, step: str, state: Dict[str, Any]):
        """Notify all callbacks of progress"""
        for callback in self.callbacks:
            try:
                callback(step, state)
            except Exception as e:
                logger.error(f"Progress callback failed: {e}")

# ============================================================================
# Main Agent Class
# ============================================================================

class DatabaseQueryAgent:
    """
    LangGraph agent for database queries with improved error handling
    """
    
    def __init__(
        self,
        nodes: AgentNodes,
        enable_progress_tracking: bool = False,
        max_retries: int = 0
    ):
        """
        Initialize the agent
        
        Args:
            nodes: AgentNodes instance with all workflow nodes
            enable_progress_tracking: Enable progress callbacks
            max_retries: Number of retries for failed queries (0 = no retry)
        """
        self.nodes = nodes
        self.router = WorkflowRouter()
        self.progress_tracker = ProgressTracker() if enable_progress_tracking else None
        self.max_retries = max_retries
        self.graph = self._build_graph()
    
    def _build_graph(self) -> Graph:
        """Build the LangGraph workflow with proper routing"""
        
        # Create the graph
        workflow = Graph()
        
        # Add nodes with progress tracking wrappers
        workflow.add_node("parse_intent", self._wrap_node(self.nodes.parse_intent, "parse_intent"))
        workflow.add_node("get_schema", self._wrap_node(self.nodes.get_schema, "get_schema"))
        workflow.add_node("generate_sql", self._wrap_node(self.nodes.generate_sql, "generate_sql"))
        workflow.add_node("validate_sql", self._wrap_node(self.nodes.validate_sql, "validate_sql"))
        workflow.add_node("execute_query", self._wrap_node(self.nodes.execute_query, "execute_query"))
        workflow.add_node("format_response", self._wrap_node(self.nodes.format_response, "format_response"))
        workflow.add_node("handle_error", self._wrap_node(self.nodes.handle_error, "handle_error"))
        
        # Set entry point
        workflow.set_entry_point("parse_intent")
        
        # Define edges (workflow flow)
        # Conditional edge after parse_intent (handle early exits like greetings)
        workflow.add_conditional_edges(
            "parse_intent",
            self.router.check_early_exit,
            {
                "continue": "get_schema",
                "exit": END
            }
        )
        
        # Linear flow from schema to generation
        workflow.add_edge("get_schema", "generate_sql")
        
        # Conditional edge after generate_sql
        workflow.add_conditional_edges(
            "generate_sql",
            self.router.should_validate,
            {
                "validate": "validate_sql",
                "error": "handle_error"
            }
        )
        
        # Conditional edge after validate_sql
        workflow.add_conditional_edges(
            "validate_sql",
            self.router.should_execute,
            {
                "execute": "execute_query",
                "error": "handle_error"
            }
        )
        
        # Conditional edge after execute_query
        workflow.add_conditional_edges(
            "execute_query",
            self.router.should_format,
            {
                "format": "format_response",
                "error": "handle_error"
            }
        )
        
        # End edges
        workflow.add_edge("format_response", END)
        workflow.add_edge("handle_error", END)
        
        return workflow.compile()
    
    def _wrap_node(
        self,
        node_func: Callable[[Dict], Dict],
        step_name: str
    ) -> Callable[[Dict], Dict]:
        """
        Wrap a node function with progress tracking and error handling
        
        Args:
            node_func: The original node function
            step_name: Name of the workflow step
        
        Returns:
            Wrapped function with tracking
        """
        def wrapped(state: Dict[str, Any]) -> Dict[str, Any]:
            # Track workflow step
            if 'workflow_steps' not in state:
                state['workflow_steps'] = []
            state['workflow_steps'].append(step_name)
            
            # Notify progress
            if self.progress_tracker:
                self.progress_tracker.notify(step_name, state)
            
            # Execute node
            try:
                logger.debug(f"Executing node: {step_name}")
                result = node_func(state)
                logger.debug(f"Node {step_name} completed")
                return result
            except Exception as e:
                logger.error(f"Node {step_name} failed: {e}", exc_info=True)
                # ⭐ NEVER show technical errors to users - always use friendly message
                # The error handler will convert this to a user-friendly message
                state['error'] = f"Error in {step_name}: {str(e)}"
                # If this is format_response error, set a friendly message immediately
                if step_name == 'format_response':
                    state['final_response'] = "I'm still learning how to display this information. Please try rephrasing your query or ask for specific details."
                return state
        
        return wrapped
    
    def add_progress_callback(self, callback: Callable[[str, Dict], None]):
        """
        Add a callback for progress updates
        
        Args:
            callback: Function(step_name, state) called at each step
        """
        if not self.progress_tracker:
            self.progress_tracker = ProgressTracker()
        self.progress_tracker.add_callback(callback)
    
    def process_message(
        self,
        user_message: str,
        session_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process a user message through the agent workflow
        
        Args:
            user_message: User's natural language query
            session_id: Optional session identifier for tracking
            metadata: Optional additional metadata
        
        Returns:
            Dictionary with response data (backward compatible)
        """
        logger.info(f"Processing message: {user_message[:100]}...")
        
        # Create initial state with proper initialization
        initial_state = {
            'user_message': user_message,
            'session_id': session_id,
            'intent': None,
            'target_database': None,
            'schema': None,
            'query_plan': None,
            'queries': {},  # Empty dict, not None
            'validated_queries': {},  # Empty dict, not None
            'results': {},  # Empty dict, not None
            'final_response': None,
            'error': None,  # None means no error (not truthy!)
            'early_exit': False,  # Explicitly False
            'workflow_steps': [],
            'metadata': metadata or {}
        }
        
        try:
            # Run the workflow
            final_state = self.graph.invoke(initial_state)
            
            # Build response (backward compatible dict format)
            has_error = bool(final_state.get('error'))
            has_final_response = bool(final_state.get('final_response'))
            
            # CRITICAL: If we have a final_response, treat as SUCCESS (even if error occurred)
            # The error handler converted technical errors to user-friendly messages
            response = {
                'success': has_final_response or not has_error,  # Success if we have a response OR no error
                'response': final_state.get('final_response') or 'No response generated',
                'intent': final_state.get('intent'),
                'target_database': final_state.get('target_database'),
                'queries': final_state.get('validated_queries', {}),
                'results_count': {
                    db: len(data) for db, data in final_state.get('results', {}).items()
                },
                # IMPORTANT: Don't send technical error to frontend when we have a user-friendly message!
                # The error handler already converted it to final_response
                'error': None,  # Never send technical errors to frontend!
                'workflow_steps': final_state.get('workflow_steps', []),
                'metadata': final_state.get('metadata', {})
            }
            
            if response['success']:
                logger.info(f"Message processed successfully through {len(response.get('workflow_steps', []))} steps")
            else:
                logger.warning(f"Message processing failed: {response['error']}")
            
            return response
            
        except Exception as e:
            logger.error(f"Agent workflow crashed: {e}", exc_info=True)
            # ⭐ NEVER show technical errors to users - always use friendly message
            return {
                'success': False,
                'response': "I'm still learning and encountered an issue. Please try rephrasing your query or ask for help with: 'What can you show me?'",
                'error': None,  # Never send technical errors to frontend!
                'workflow_steps': initial_state.get('workflow_steps', [])
            }
    
    def get_workflow_visualization(self) -> str:
        """
        Get a text visualization of the workflow
        
        Returns:
            String representation of workflow
        """
        viz = """
DatabaseQueryAgent Workflow:

1. parse_intent
   ├─ early_exit? → END
   └─ continue → 2

2. get_schema → 3

3. generate_sql
   ├─ error? → 7 (handle_error)
   └─ success → 4

4. validate_sql
   ├─ error? → 7 (handle_error)
   └─ success → 5

5. execute_query
   ├─ error? → 7 (handle_error)
   └─ success → 6

6. format_response → END

7. handle_error → END
"""
        return viz

# ============================================================================
# Factory Functions
# ============================================================================

def create_agent(
    nodes: AgentNodes,
    enable_progress: bool = False,
    max_retries: int = 0
) -> DatabaseQueryAgent:
    """
    Factory function to create DatabaseQueryAgent
    
    Args:
        nodes: Configured AgentNodes instance
        enable_progress: Enable progress tracking
        max_retries: Number of retry attempts
    
    Returns:
        Configured DatabaseQueryAgent
    """
    return DatabaseQueryAgent(
        nodes=nodes,
        enable_progress_tracking=enable_progress,
        max_retries=max_retries
    )

