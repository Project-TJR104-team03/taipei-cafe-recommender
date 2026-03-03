# app/agents/preference_agent.py
import json
import logging
import asyncio
from agents.base_agent import BaseAgent
from vertexai.generative_models import GenerationConfig

logger = logging.getLogger("Coffee_Recommender")

PREFERENCE_SYSTEM_PROMPT = """
【任務】
你是一位頂級的「使用者行為與心理分析師」。
你需要分析該使用者過去的「搜尋紀錄」、「收藏清單」以及「黑名單與拒絕原因」，
並精煉出該使用者的「咖啡廳偏好畫像 (Persona)」。

【輸入資料】
{behavior_data}

【輸出規定】
請務必回傳以下 JSON 格式：
{{
    "persona_label": "給這個使用者的行為一句話精準定義，例如：深夜工作甜點控",
    "preferred_tags": ["從資料中推斷出他最在意的 3~5 個正面特徵，例如: 深夜, 安靜, 插座"],
    "avoid_tags": ["從黑名單或拒絕原因中，推斷他最討厭的 2~3 個地雷特徵，例如: 吵鬧, 太貴, 沒插座"],
    "analysis_summary": "簡短的一段話說明為什麼給出這樣的設定"
}}
"""
class PreferenceAgent(BaseAgent):
    async def analyze_user_preferences(self, behavior_data: dict) -> dict:
        if not self.model or not behavior_data: return {}

        full_prompt = PREFERENCE_SYSTEM_PROMPT.format(
            behavior_data=json.dumps(behavior_data, ensure_ascii=False)
        )

        try:
            generation_config = GenerationConfig(
                response_mime_type="application/json",
                temperature=0.2 
            )

            # 確保不卡死 FastAPI 主線程
            response = await asyncio.to_thread(
                self.model.generate_content,
                full_prompt,
                generation_config=generation_config
            )

            if response.text:
                clean_text = response.text.replace("```json", "").replace("```", "").strip()
                result = json.loads(clean_text)
                logger.info(f"🧠 [Preference AI] 成功更新偏好畫像: {result.get('persona_label')}")
                return result
            return {}

        except Exception as e:
            logger.error(f"❌ Preference AI 偏好分析失敗: {e}")
            return {}