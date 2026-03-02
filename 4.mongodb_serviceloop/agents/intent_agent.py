# app/agents/intent_agent.py
import json
import logging
from datetime import datetime
from agents.base_agent import BaseAgent
from vertexai.generative_models import GenerationConfig 

logger = logging.getLogger("Coffee_Recommender")

# 🔥 直接將 Prompt 放在這裡，未來修改意圖邏輯只需動這一個檔案
USER_INTENT_SYSTEM_PROMPT_TEMPLATE = """
### Role
你是一個專業的台北咖啡廳需求分析專家。
現在的時間是：{current_time_str} (星期 {weekday_str})。

### Task
請分析使用者的輸入，判斷他想去的「時間點」以及「需求維度」。

### Output Format (JSON ONLY)
{{
  "workability": float (0.0-1.0),
  "atmosphere": float (0.0-1.0),
  "product_quality": float (0.0-1.0),
  "pet_friendly": float (0.0-1.0),
  "time_filter": {{
      "filter_open_now": boolean,
      "target_iso_datetime": string
  }},
  "extracted_keywords": list[str]
}}

### Rules
1. 參照「現在的時間」來計算使用者口中的「明天」、「週五」、「晚上」是具體哪個日期時間。
2. 若使用者只說「晚上」，預設為 19:00。
3. 若使用者只說「下午」，預設為 14:00。
4. 若使用者只說「早上」，預設為 09:00。
"""

class IntentAgent(BaseAgent):
    def analyze_user_intent(self, user_message: str) -> dict:
        if not self.model: return {}

        now = datetime.now() 
        weekday_map = ["一", "二", "三", "四", "五", "六", "日"]
        
        dynamic_system_prompt = USER_INTENT_SYSTEM_PROMPT_TEMPLATE.format(
            current_time_str=now.strftime("%Y-%m-%d %H:%M"),
            weekday_str=weekday_map[now.weekday()]
        )
        
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
                # logger.info(f"🤖 [Intent AI 分析結果]: {json.dumps(result, ensure_ascii=False)}")
                return result
            return {}

        except Exception as e:
            logger.error(f"❌ Intent AI 解析失敗: {e}")
            return {}