# app/agents/intent_agent.py
import json
import logging
from datetime import datetime
from agents.base_agent import BaseAgent
from templates.prompts import USER_INTENT_SYSTEM_PROMPT_TEMPLATE
from vertexai.generative_models import GenerationConfig # 🔥 改用 Vertex AI 的 Config

logger = logging.getLogger("Coffee_Recommender")

class IntentAgent(BaseAgent):
    def analyze_user_intent(self, user_message: str) -> dict:
        if not self.model: return {}

        now = datetime.now() 
        weekday_map = ["一", "二", "三", "四", "五", "六", "日"]
        
        dynamic_system_prompt = USER_INTENT_SYSTEM_PROMPT_TEMPLATE.format(
            current_time_str=now.strftime("%Y-%m-%d %H:%M"),
            weekday_str=weekday_map[now.weekday()]
        )
        
        # Vertex AI 建議將 System Prompt 放在 user message 前面
        full_prompt = f"""
        {dynamic_system_prompt}
        
        使用者輸入：{user_message}
        """

        try:
            generation_config = GenerationConfig(
                response_mime_type="application/json",
                temperature=0.0
            )

            response = self.model.generate_content(
                full_prompt,
                generation_config=generation_config
            )
            
            if response.text:
                clean_text = response.text.replace("```json", "").replace("```", "").strip()
                result = json.loads(clean_text)
                # logger.info(f"🤖 [Vertex AI 分析結果]: {json.dumps(result, ensure_ascii=False)}")
                return result
            return {}

        except Exception as e:
            logger.error(f"❌ Vertex AI 解析失敗: {e}")
            return {}