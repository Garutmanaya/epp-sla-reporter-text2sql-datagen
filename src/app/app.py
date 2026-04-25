import logging
import time
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel, Field
import uvicorn

from model.inference import Text2SQLInference
from common.config_manager import ConfigManager

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Text2SQL_API")

# =========================================
# MODELS (Pydantic Schemas)
# =========================================

class QueryRequest(BaseModel):
    """
    Schema for a single Text2SQL prediction request.
    """
    question: str = Field(
        ..., 
        description="The natural language question to be converted to SQL.",
        example="What is the average latency for AtlasRegistrar in EU yesterday?"
    )
    db_id: str = Field(
        "epp_registry", 
        description="The target database identifier defined in the schema manager.",
        example="epp_registry"
    )

class BatchQueryRequest(BaseModel):
    """
    Schema for multiple Text2SQL prediction requests.
    """
    queries: List[QueryRequest] = Field(..., min_items=1)

class QueryResponse(BaseModel):
    """
    Schema for a single Text2SQL prediction result.
    """
    question: str = Field(..., description="The original input question.")
    sql: str = Field(..., description="The generated SQL query.")
    detected_schema: str = Field(..., description="The serialized schema prompt sent to the model.")
    latency_ms: float = Field(..., description="Time taken to generate the SQL in milliseconds.")

class BatchQueryResponse(BaseModel):
    """
    Schema for the batch prediction result.
    """
    results: List[QueryResponse] = Field(..., description="List of individual query results.")
    total_latency_ms: float = Field(..., description="Total time taken for the entire batch.")

# =========================================
# APP INITIALIZATION
# =========================================

app = FastAPI(
    title="EPP Text2SQL API",
    description="Natural Language to SQL conversion API for SLA Reporting. Supports Single, Batch, and AWS SageMaker invocations.",
    version="1.0.0"
)

# Global inference engine loaded into memory once
engine: Optional[Text2SQLInference] = None

@app.on_event("startup")
def load_model():
    """
    Loads the Text2SQL model on server startup to minimize inference latency.
    """
    global engine
    cfg = ConfigManager()
    mode = cfg.training_mode
    size = cfg.model_size
    
    logger.info(f"Initializing Inference Engine (Mode: {mode}, Size: {size})...")
    try:
        engine = Text2SQLInference(mode=mode, model_size=size)
        logger.info("Model loaded and ready for inference.")
    except Exception as e:
        logger.error(f"Failed to load model: {str(e)}")
        raise RuntimeError(f"Critical error: Model could not be initialized. {e}")

# =========================================
# ENDPOINTS
# =========================================

@app.get("/ping", tags=["Management"], summary="Health Check")
@app.get("/health", tags=["Management"], summary="Health Check")
def health_check():
    """
    Standard health check endpoint for Load Balancers and AWS SageMaker.
    Returns 200 OK if the model is loaded and ready.
    """
    if engine is not None:
        return {"status": "healthy", "model_loaded": True}
    return {"status": "unhealthy", "model_loaded": False}

@app.post("/predict", response_model=QueryResponse, tags=["Inference"], summary="Single SQL Generation")
async def predict_single(request: QueryRequest):
    """
    Converts a single Natural Language question into a valid SQL query.
    """
    start_time = time.time()
    try:
        result = engine.predict(request.question, request.db_id)
        latency = (time.time() - start_time) * 1000
        
        return {
            **result,
            "latency_ms": round(latency, 2)
        }
    except Exception as e:
        logger.error(f"Inference error: {e}")
        raise HTTPException(status_code=500, detail="Internal Model Error")

@app.post("/predict/batch", response_model=BatchQueryResponse, tags=["Inference"], summary="Batch SQL Generation")
async def predict_batch(request: BatchQueryRequest):
    """
    Processes multiple questions in a single request. 
    """
    start_time = time.time()
    results = []
    
    try:
        for q in request.queries:
            step_start = time.time()
            res = engine.predict(q.question, q.db_id)
            step_lat = (time.time() - step_start) * 1000
            results.append({**res, "latency_ms": round(step_lat, 2)})
            
        total_latency = (time.time() - start_time) * 1000
        return {
            "results": results,
            "total_latency_ms": round(total_latency, 2)
        }
    except Exception as e:
        logger.error(f"Batch inference error: {e}")
        raise HTTPException(status_code=500, detail="Internal Batch Processing Error")

@app.post("/invocations", response_model=QueryResponse, tags=["SageMaker"], summary="SageMaker Inference")
async def sagemaker_invoke(request: QueryRequest):
    """
    Endpoint mapping for AWS SageMaker. 
    """
    return await predict_single(request)

# =========================================
# EXECUTION
# =========================================

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)

# ==============================================================================
# CURL EXAMPLES FOR TESTING
# ==============================================================================
#
# 1. HEALTH CHECK / PING
# curl -X GET http://localhost:8080/health
#
# 2. SINGLE PREDICTION
# curl -X 'POST' \
#   'http://localhost:8080/predict' \
#   -H 'accept: application/json' \
#   -H 'Content-Type: application/json' \
#   -d '{
#   "question": "Show average latency for AtlasRegistrar in EU yesterday",
#   "db_id": "epp_registry"
# }'
#
# 3. BATCH PREDICTION
# curl -X 'POST' \
#   'http://localhost:8080/predict/batch' \
#   -H 'accept: application/json' \
#   -H 'Content-Type: application/json' \
#   -d '{
#   "queries": [
#     {"question": "How many records exist for customer Atlas?", "db_id": "epp_registry"},
#     {"question": "List top 5 latencies from last week", "db_id": "epp_registry"}
#   ]
# }'
#
# 4. AWS SAGEMAKER INVOCATION
# curl -X 'POST' \
#   'http://localhost:8080/invocations' \
#   -H 'accept: application/json' \
#   -H 'Content-Type: application/json' \
#   -d '{
#   "question": "Show all maintenance windows for v2 deployments",
#   "db_id": "epp_registry"
# }'
# ==============================================================================
