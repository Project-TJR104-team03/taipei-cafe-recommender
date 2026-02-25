# app/agents/chat_agent.py
import json
import logging
from agents.base_agent import BaseAgent
from vertexai.generative_models import GenerationConfig # 🔥 換成 Vertex AI 的 Config 套件

logger = logging.getLogger("Coffee_Recommender")

class ChatAgent(BaseAgent):
    def analyze_chat_intent(self, user_msg: str) -> dict:
        """
        判斷是純閒聊 (Chat) 還是找店 (Search)，並生成幽默回應。
        """
        # 🔥 修改點 1：因為 BaseAgent 是用 self.model 初始化，所以這裡改成判斷 self.model
        if not self.model:
             # 若無 AI，回傳預設值
            return {
                "mode": "search", "tags": [], "keyword": user_msg, 
                "opening": "正在搜尋中...", "closing": "希望您喜歡！"
            }

        valid_tags = "不限時, 安靜, 甜點, 插座, wifi, 景觀, 復古, 寵物, 深夜, 舒適, 商業, 約會, 讀書"
        
        prompt = f"""
        【角色設定】
        你是一個幽默、溫暖的 AI 咖啡廳助手。
        你的任務是判斷使用者的輸入，過濾掉閒聊廢話，並精準萃取出「完整保留所有條件」的搜尋關鍵字。

        【可用標籤清單】
        {valid_tags}

        【判斷邏輯】
        請回傳 JSON 格式：
        情況 A：使用者想找咖啡廳 (Search Mode)
        - 回傳格式： {{ "mode": "search", "tags": ["選出的標籤"], "keyword": "完整的搜尋條件", "opening": "開場白...", "closing": "結尾..." }}
        - ⚠️ 關鍵指令：你的 "keyword" 必須「完整保留」使用者提到的時間 (如半夜、明天)、地點 (如中山站) 與特殊需求 (如插座、貓咪)。絕對不能擅自將其縮減為「咖啡廳」。

        情況 B：使用者純粹閒聊 (Chat Mode)
        - 回傳格式： {{ "mode": "chat", "reply": "幽默回應(50字內)" }}

        【範例訓練】
        使用者：「哈哈謝謝你！那你可以幫我找找半夜有開的安靜咖啡廳嗎？」
        你的輸出："keyword": "半夜有開的安靜咖啡廳" (過濾了閒聊，但完美保留了時間與需求)

        使用者：「明天下午信義區哪裡有肉桂捲？」
        你的輸出："keyword": "明天下午信義區肉桂捲"

        【真實輸入】 "{user_msg}"
        """

        try:
            # 🔥 修改點 2：使用 Vertex AI 的 GenerationConfig 設定 JSON 輸出
            generation_config = GenerationConfig(
                response_mime_type="application/json"
            )

            # 🔥 修改點 3：改用 self.model.generate_content 呼叫
            response = self.model.generate_content(
                prompt,
                generation_config=generation_config
            )
            
            clean_text = response.text.replace("```json", "").replace("```", "").strip()
            return json.loads(clean_text)
        except Exception as e:
            logger.error(f"❌ Chat Agent 分析失敗: {e}")
            return {"mode": "search", "keyword": user_msg}