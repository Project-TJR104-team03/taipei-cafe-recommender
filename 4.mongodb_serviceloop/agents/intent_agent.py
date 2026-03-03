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

### Rules
1. 參照「現在的時間」來計算使用者口中的「明天」、「週五」、「晚上」是具體哪個日期時間。
2. 若使用者只說「晚上」，預設為 19:00。
3. 若使用者只說「下午」，預設為 14:00。
4. 若使用者只說「早上」，預設為 09:00。
5. ⚠️ 若使用者完全沒有提到時間，請將 has_time 設為 false。

### Output Format (絕對鐵律)
請你務必、絕對只能回傳以下的 JSON 格式，不要包含任何其他說明文字：
{{
    "has_time": true 或 false,
    "target_time": "YYYY-MM-DD HH:MM", 
    "time_flexibility": "使用者提到的原始時間字眼，例如: 明天早上10點以後",
    "intents": ["提取出的其他需求", "例如: 親子友善"]
}}
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