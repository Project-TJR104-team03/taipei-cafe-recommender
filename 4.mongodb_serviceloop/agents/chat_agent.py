# app/agents/chat_agent.py
import json
import logging
from agents.base_agent import BaseAgent
from vertexai.generative_models import GenerationConfig 
from constants import TAG_EMOJI_MAP

logger = logging.getLogger("Coffee_Recommender")

class ChatAgent(BaseAgent):
    def analyze_chat_intent(self, user_msg: str) -> dict:
        """
        判斷是純閒聊 (Chat) 還是找店 (Search)，精準萃取 48 種標準標籤與搜尋關鍵字，並生成幽默回應。
        """
        if not self.model:
            return {
                "mode": "search", "tags": [], "keyword": user_msg, 
                "opening": "正在搜尋中...", "closing": "希望您喜歡！"
            }

        valid_tags_list = list(TAG_EMOJI_MAP.keys())
        valid_tags = ", ".join(valid_tags_list)
        
        # ✨ 大幅翻新 Prompt：注入「資深吃貨網友」的靈魂
        prompt = f"""
        【角色設定】
        你現在不是死板的 AI 客服，而是一個超級懂喝、說話接地氣的「資深咖啡廳評鑑網友」。
        你的語氣要像 Google Maps 或 PTT/Dcard 上的真實評論一樣：熱情、直白、生動。
        請多使用網路習慣用語（例如：超推、大推、絕配、氣氛超讚、寶藏愛店、雷店退散）。

        【可用標籤清單】(你只能從這裡面挑選)
        {valid_tags}

        【判斷邏輯】
        請回傳 JSON 格式：
        情況 A：使用者想找咖啡廳 (Search Mode)
        - 回傳格式： {{ "mode": "search", "tags": ["選出的標籤"], "keyword": "完整的搜尋條件", "opening": "開場白...", "closing": "結尾..." }}
        - 🏷️ 標籤指令：請嚴格從【可用標籤清單】中挑選出最符合使用者意圖的 1 到 3 個標籤。
        - ⚠️ 關鍵指令：你的 "keyword" 必須「完整保留」使用者提到的時間與地點。
        - 🗣️ 語氣指令：
            - opening：像熱情網友幫忙找店的口吻（如：「收到！馬上幫你撈幾家網評超讚的店...」）。
            - closing：像評論家給的結語（如：「這幾家氣氛都超讚，快去踩點看看！」）。

        情況 B：使用者純粹閒聊 (Chat Mode)
        - 回傳格式： {{ "mode": "chat", "reply": "幽默回應(50字內)" }}
        - 🗣️ 語氣指令：把使用者當朋友，用愛喝咖啡的吃貨口吻跟他瞎扯。

        【範例訓練】
        使用者：「你可以幫我找找半夜有開的安靜咖啡廳嗎？」
        你的輸出：{{"mode": "search", "tags": ["深夜", "安靜"], "keyword": "半夜有開的安靜咖啡廳", "opening": "沒問題！馬上幫你特搜幾家半夜還開著的寶藏愛店，絕對夠安靜好做事🤫", "closing": "這幾間網友都大推，半夜不怕沒地方去啦！"}}

        使用者：「我筆電快沒電了，中山站附近哪裡可以待著」
        你的輸出：{{"mode": "search", "tags": ["插座", "工作友善"], "keyword": "中山站附近筆電快沒電可以待著", "opening": "收到！中山站周邊有插座的續命好店馬上幫你掃出來⚡", "closing": "快去拯救你的筆電吧！這幾家坐起來都超舒服的💻"}}
        
        使用者：「今天天氣好差心情不好」
        你的輸出：{{"mode": "chat", "reply": "天氣差真的超厭世的啦！這種時候最適合躲進咖啡廳吃塊超讚的肉桂捲了，要不要我幫你找找？🍰"}}

        【真實輸入】 "{user_msg}"
        """

        try:
            # ✨ 微調：稍微把 temperature 從 0.2 調高到 0.4，讓語氣產生更多變化，但仍保持 JSON 格式穩定
            generation_config = GenerationConfig(
                response_mime_type="application/json",
                temperature=0.4 
            )

            response = self.model.generate_content(
                prompt,
                generation_config=generation_config
            )
            
            clean_text = response.text.replace("```json", "").replace("```", "").strip()
            result = json.loads(clean_text)
            
            # 防呆機制
            if "tags" in result and isinstance(result["tags"], list):
                result["tags"] = [t for t in result["tags"] if t in valid_tags_list]
                
            logger.info(f"🤖 [ChatAgent 解析成功]: {json.dumps(result, ensure_ascii=False)}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Chat Agent 分析失敗: {e}")
            return {"mode": "search", "keyword": user_msg, "tags": []}