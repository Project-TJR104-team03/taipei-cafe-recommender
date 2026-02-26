# app/agents/chat_agent.py
import json
import logging
from agents.base_agent import BaseAgent
from vertexai.generative_models import GenerationConfig # 🔥 換成 Vertex AI 的 Config 套件
from constants import TAG_EMOJI_MAP

logger = logging.getLogger("Coffee_Recommender")

class ChatAgent(BaseAgent):
    def analyze_chat_intent(self, user_msg: str) -> dict:
        """
        判斷是純閒聊 (Chat) 還是找店 (Search)，精準萃取 48 種標準標籤與搜尋關鍵字，並生成幽默回應。
        """
        # 🔥 修改點 1：因為 BaseAgent 是用 self.model 初始化，所以這裡改成判斷 self.model
        if not self.model:
             # 若無 AI，回傳預設值
            return {
                "mode": "search", "tags": [], "keyword": user_msg, 
                "opening": "正在搜尋中...", "closing": "希望您喜歡！"
            }

        # 🔥 動態把 constants 裡面的 48 個 Key 抽出來，組合成字串交給 AI
        valid_tags_list = list(TAG_EMOJI_MAP.keys())
        valid_tags = ", ".join(valid_tags_list)
        
        prompt = f"""
        【角色設定】
        你是一個幽默、溫暖的 AI 咖啡廳助手。
        你的任務是判斷使用者的輸入，過濾掉閒聊廢話，並精準萃取出「完整保留所有條件」的搜尋關鍵字，同時將隱性需求「翻譯」成標準標籤。

        【可用標籤清單】(你只能從這裡面挑選)
        {valid_tags}

        【判斷邏輯】
        請回傳 JSON 格式：
        情況 A：使用者想找咖啡廳 (Search Mode)
        - 回傳格式： {{ "mode": "search", "tags": ["選出的標籤"], "keyword": "完整的搜尋條件", "opening": "開場白...", "closing": "結尾..." }}
        - 🏷️ 標籤指令：請嚴格從【可用標籤清單】中挑選出最符合使用者意圖的 1 到 3 個標籤，放入 tags 陣列中。如果使用者說「有貓咪」，請翻譯為「店貓」；說「筆電沒電」，請翻譯為「插座」；說「可以坐很久」，請翻譯為「不限時」。絕對不能自己發明標籤！
        - ⚠️ 關鍵指令：你的 "keyword" 必須「完整保留」使用者提到的時間 (如半夜、明天)、地點 (如中山站) 與特殊需求 (如插座、貓咪)。絕對不能擅自將其縮減為「咖啡廳」。

        情況 B：使用者純粹閒聊 (Chat Mode)
        - 回傳格式： {{ "mode": "chat", "reply": "幽默回應(50字內)" }}

        【範例訓練】
        使用者：「哈哈謝謝你！那你可以幫我找找半夜有開的安靜咖啡廳嗎？」
        你的輸出：{{"mode": "search", "tags": ["深夜", "安靜"], "keyword": "半夜有開的安靜咖啡廳", "opening": "沒問題，馬上為您尋找半夜有營業的安靜空間...", "closing": "希望有適合您的好地方！"}}

        使用者：「我筆電快沒電了，中山站附近哪裡可以待著」
        你的輸出：{{"mode": "search", "tags": ["插座", "工作友善"], "keyword": "中山站附近筆電快沒電可以待著", "opening": "收到！馬上幫您找中山站附近有插座的咖啡廳...", "closing": "快去拯救您的筆電吧！"}}

        【真實輸入】 "{user_msg}"
        """

        try:
            generation_config = GenerationConfig(
                response_mime_type="application/json",
                temperature=0.2 # 🔥 稍微降低溫度，讓 AI 挑選標籤時更精準、更守規矩
            )

            response = self.model.generate_content(
                prompt,
                generation_config=generation_config
            )
            
            clean_text = response.text.replace("```json", "").replace("```", "").strip()
            result = json.loads(clean_text)
            
            # 🛡️ 雙重防呆機制：確保 AI 吐出來的 tags 真的存在於我們的 48 個名單內
            if "tags" in result and isinstance(result["tags"], list):
                result["tags"] = [t for t in result["tags"] if t in valid_tags_list]
                
            logger.info(f"🤖 [ChatAgent 解析成功]: {json.dumps(result, ensure_ascii=False)}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Chat Agent 分析失敗: {e}")
            return {"mode": "search", "keyword": user_msg, "tags": []}