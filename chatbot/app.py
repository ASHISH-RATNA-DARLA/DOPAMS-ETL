"""
Flask Application Entry Point
Main application setup and initialization
"""
from flask import Flask, render_template
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import logging
import sys
import os

from config import Config
from database.postgres_executor import PostgreSQLExecutor
from database.mongo_executor import MongoDBExecutor
from database.schema_manager import SchemaManager
from cache.redis_manager import RedisManager
from agents.llm_client_universal import UniversalLLMClient
from agents.nodes import AgentNodes
from agents.langgraph_agent import DatabaseQueryAgent
from api.routes import init_routes, api_bp

# Configure logging
log_level = logging.DEBUG if Config.DEBUG else logging.INFO

# Get log file from environment variable or use default
log_file = os.getenv('LOG_FILE', 'app.log')

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file)
    ]
)

logger = logging.getLogger(__name__)


def create_app():
    """Create and configure Flask application"""
    
    app = Flask(__name__)
    app.config['SECRET_KEY'] = Config.SECRET_KEY
    
    # Enable CORS
    CORS(app, resources={r"/api/*": {"origins": "*"}})
    
    # Initialize rate limiter
    limiter = Limiter(
        app=app,
        key_func=get_remote_address,
        default_limits=[f"{Config.RATE_LIMIT} per minute"],
        storage_uri="memory://"
    )
    
    logger.info("Initializing application components...")
    
    try:
        # Initialize database executors
        logger.info("Connecting to PostgreSQL...")
        postgres_executor = PostgreSQLExecutor()
        
        logger.info("Connecting to MongoDB...")
        mongo_executor = MongoDBExecutor()
        
        # Initialize cache manager
        logger.info("Connecting to Redis...")
        cache_manager = RedisManager()
        
        # Initialize schema manager
        logger.info("Initializing schema manager...")
        schema_manager = SchemaManager(postgres_executor, mongo_executor)
        
        # Initialize LLM client
        logger.info(f"Initializing LLM client (provider: {Config.LLM_CONFIG['provider']})...")
        llm_client = UniversalLLMClient()
        
        # Initialize agent nodes
        logger.info("Setting up agent nodes...")
        agent_nodes = AgentNodes(
            llm_client=llm_client,
            schema_manager=schema_manager,
            postgres_executor=postgres_executor,
            mongo_executor=mongo_executor,
            cache_manager=cache_manager
        )
        
        # Initialize LangGraph agent
        logger.info("Building LangGraph workflow...")
        agent = DatabaseQueryAgent(agent_nodes)
        
        # Initialize routes
        logger.info("Registering API routes...")
        init_routes(agent, cache_manager, postgres_executor, mongo_executor)
        app.register_blueprint(api_bp)
        
        logger.info("Application initialized successfully!")
        
    except Exception as e:
        logger.error(f"Failed to initialize application: {e}", exc_info=True)
        sys.exit(1)
    
    # Root route - serve chatbot UI
    @app.route('/')
    def index():
        """Serve the chatbot web interface"""
        return render_template('index.html')
    
    return app


if __name__ == '__main__':
    app = create_app()
    
    logger.info(f"Starting Flask server on port {Config.PORT}...")
    logger.info(f"Debug mode: {Config.DEBUG}")
    
    app.run(
        host='0.0.0.0',
        port=Config.PORT,
        debug=Config.DEBUG
    )



