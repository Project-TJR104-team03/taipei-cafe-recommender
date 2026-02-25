# app/agents/base_agent.py
import os
import logging
import vertexai
from vertexai.generative_models import GenerativeModel

logger = logging.getLogger("AI_Agent")

class BaseAgent:
    def __init__(self, model_name="gemini-2.0-flash-001"):
        # 取得 GCP 專案設定
        project_id = os.getenv("GCP_PROJECT_ID")
        location = os.getenv("GCP_LOCATION", "us-central1")
        
        try:
            # 使用 Vertex AI 初始化
            vertexai.init(project=project_id, location=location)
            self.model = GenerativeModel(model_name)
            logger.info(f"✅ Vertex AI 初始化成功，使用模型: {model_name}")
        except Exception as e:
            logger.error(f"❌ Vertex AI 初始化失敗: {e}")
            self.model = None