# app/agents/reason_agent.py
import json
import logging
import asyncio
import time
from agents.base_agent import BaseAgent
from vertexai.generative_models import GenerationConfig

logger = logging.getLogger("Coffee_Recommender")

# 🔥 換回你原本精心設計的神級 Prompt (嚴格限制字數、語氣與格式)
REASON_SYSTEM_PROMPT = """
【任務】
你是一位熱情且專業的咖啡廳推薦專家。
使用者目前的搜尋需求為：「{user_query}」
名單上的咖啡廳「已經確定符合」此需求。請根據資料中的 tags 與 info (可能為網友真實評論或店家介紹)，為每家店寫出「一句話的專屬正面推薦理由」。

【資料】
{cafe_info_json}

【輸出規定】
1. 必須是「正面、肯定」的推銷語氣。如果 info 是網友評論，請將其修飾為推薦口吻 (例如：可以寫「網友大推這裡的甜點...」或直接描述優點)。如果 info 沒提到需求，請利用 tags 造句。
2. ⚠️ 絕對禁止出現「資訊較少」、「未提及」、「無相關資訊」、「偏重咖啡」等任何帶有否定、抱歉或不確定的字眼！
3. 語氣自然，絕對不要出現「整體而言」、「這是一家」等廢言。為了版面簡潔，請不要在理由中重複店家的名稱。
4. ⚠️ 字數嚴格限制在 15 到 20 字以內！必須是一句「完整結束」的精華短句，不要使用括號 () 備註。
5. ⚠️ 必須回傳單一的 JSON Object (字典) 格式，絕對不要回傳 List (陣列)！格式範例如下：
{{
    "place_id_1": "營業至深夜，且提供插座適合辦公",
    "place_id_2": "如同網友推薦，這裡提供多樣美味甜點"
}}
"""

class ReasonAgent(BaseAgent):
    async def generate_reasons_batch(self, user_query: str, cafes: list) -> dict:
        if not self.model or not cafes:
            return {}

        # 🔥 補回原本強大的資料萃取邏輯：抓取 matched_review 或 summary
        cafe_info_list = []
        for c in cafes:
            info = c.get("matched_review", "") 
            if not info:
                info = c.get("summary", "")
            
            tags = c.get("tags", [])
            if not tags and "ai_tags" in c:
                tags = [t.get("tag", "") for t in c.get("ai_tags", []) if isinstance(t, dict)]
            
            cafe_info_list.append({
                "id": str(c.get("place_id", c.get("_id"))),
                "tags": tags[:5], 
                "info": info[:250] # 保留評論前 250 字給 AI 參考，讓他有素材可以寫
            })

        # 組合 Prompt
        full_prompt = REASON_SYSTEM_PROMPT.format(
            user_query=user_query,
            cafe_info_json=json.dumps(cafe_info_list, ensure_ascii=False)
        )

        try:
            # 🌟 1. 計算輸入的 Token 數量
            token_info = self.model.count_tokens(full_prompt)
            input_tokens = token_info.total_tokens
            
            # ✂️ [瘦身] 精簡輸入 Log
            logger.info(f"🟢 [ReasonAgent] 輸入 | 需求: '{user_query}' | 候選: {len(cafes)} 家")
            logger.debug(f"==== 🟢 [ReasonAgent] 完整 Prompt ====\n{full_prompt}\n======================================")

            generation_config = GenerationConfig(
                response_mime_type="application/json",
                temperature=0.2 
            )

            # 🌟 2. 開始計時並呼叫 AI
            start_time = time.time()
            response = await asyncio.to_thread(
                self.model.generate_content,
                full_prompt,
                generation_config=generation_config
            )
            elapsed_time = time.time() - start_time

            if response.text:
                clean_text = response.text.replace("```json", "").replace("```", "").strip()
                parsed_data = json.loads(clean_text)
                
                # 🛡️ 神級防呆機制保留
                if isinstance(parsed_data, list):
                    logger.warning("⚠️ AI 回傳了 List 格式，正在自動修正為 Dict...")
                    reasons_dict = {}
                    for item in parsed_data:
                        if isinstance(item, dict):
                            item_id = item.get("id", item.get("place_id"))
                            item_reason = item.get("reason", item.get("理由"))
                            if item_id and item_reason:
                                reasons_dict[item_id] = item_reason
                            else:
                                reasons_dict.update(item)
                    parsed_data = reasons_dict
                
                elif not isinstance(parsed_data, dict):
                    parsed_data = {}

                # ✂️ [瘦身] 將耗時、Token 與壓平後的 JSON 合併成精華一行！
                logger.info(f"🔵 [ReasonAgent] 輸出 | 耗時: {elapsed_time:.2f}s | Token: {input_tokens} | 解析: {json.dumps(parsed_data, ensure_ascii=False)}")
                return parsed_data
            return {}

        except Exception as e:
            logger.error(f"❌ Reason AI 理由生成失敗: {e}")
            return {}