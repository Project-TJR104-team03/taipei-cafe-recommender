# app/agents/chat_agent.py
import json
import logging
import time
from agents.base_agent import BaseAgent
from vertexai.generative_models import GenerationConfig 
from constants import STANDARD_TAGS

logger = logging.getLogger("Coffee_Recommender")

class ChatAgent(BaseAgent):
    def manage_dialogue_and_cart(self, user_msg: str, chat_window: list = None, current_cart: list = None, last_session_cart: list = None) -> dict:
        """
        [終極大腦] 具備語意狀態機的對話總管。
        同時處理閒聊、找店意圖，並執行購物車增/刪/改與隔夜反問！
        """
        # 防呆初始化
        chat_window = chat_window or []
        current_cart = current_cart or []
        last_session_cart = last_session_cart or []
        
        if not self.model:
            return {
                "mode": "search", "keyword": user_msg, "updated_cart": [], "tags": [],
                "opening": "正在搜尋中...", "closing": "希望您喜歡！", "cart_action": "clear"
            }

        valid_tags_list = STANDARD_TAGS
        valid_tags = ", ".join(valid_tags_list)
        
        # 將狀態轉為字串給 AI 看
        chat_history_str = json.dumps(chat_window, ensure_ascii=False)
        current_cart_str = json.dumps(current_cart, ensure_ascii=False)
        last_session_str = json.dumps(last_session_cart, ensure_ascii=False)

        prompt = f"""
        【角色設定】
        你現在不是死板的 AI 客服，而是一個超級懂喝、說話接地氣的「資深咖啡廳評鑑網友」。
        你的語氣要像 Google Maps 或 PTT/Dcard 上的真實評論一樣：熱情、直白、生動。
        請多使用網路習慣用語（例如：超推、大推、絕配、氣氛超讚、寶藏愛店、雷店退散）。
        ⚠️ 絕對語言限制：請「百分之百」使用【繁體中文（台灣習慣用語）】進行回覆！除了保留原本的英文店名外，【嚴禁】夾雜任何俄文、日文或簡體字！

        【當前系統狀態 RAM】(你必須根據這些記憶來判斷使用者的意思！)
        - 歷史對話視窗 (chat_window)：{chat_history_str}
        - 目前條件購物車 (current_cart)：{current_cart_str}
        - 昨日備份購物車 (last_session_cart)：{last_session_str}
        - 使用者最新輸入："{user_msg}"

        【可用標籤清單】(你只能從這裡面挑選標籤)
        {valid_tags}

        【判斷邏輯與購物車劇本】
        請回傳 JSON 格式。你擁有「購物車管理權」，請根據情況決定：

        情況 A：使用者想找咖啡廳 (Search Mode)
        - ⚠️ 絕對鐵律：只要包含「任何一家咖啡廳的名字（如：星巴克、北風社、always day one等）」或「看起來像在找店」，強制判定為 Search！
        - 劇本 A【融合追加 (add)】：使用者在原購物車基礎上新增條件（如：「那有貓咪的嗎」）。保留舊條件，加入新條件。
        - 劇本 B【精準替換 (replace)】：使用者改變心意，換地點、換時間或衝突條件（如：「那改去松山」、「改成明天早上」、「現在去好了」）。
           - 動作：🚨 唯一性鐵律！購物車內【永遠只能有一個地點】與【一個時間點】。拔除衝突的舊地點、舊時間或舊條件，換成新的。
           - ⏳ 時間重置特例：如果使用者說「現在去好了」、「改成現在」，請【直接拔除】購物車內的舊時間，不需要保留「現在」這兩個字。
           - 🛡️ 繼承鐵律：【絕對必須保留】原本不衝突的其他條件（例如：安靜等），絕對不可以把它們弄丟！
           - 🧹 斷捨離鐵律：在替換地點或時間時，你的 "keyword" 裡面【絕對不可以】再把舊的名稱寫出來！
        - 劇本 C【清空重組 (clear)】：另起爐灶或指定店名。清空舊車，只放新條件。
           - 🗣️ 語氣指令：opening 像熱情網友幫忙找店的口吻（如：「收到！馬上幫你撈幾家網評超讚的店...」）；closing 像評論家給的結語（如：「這幾家氣氛都超讚，快去踩點看看！」）。
           - ⚠️ keyword：必須將 updated_cart 裡的條件融合成一句完整的搜尋關鍵字。

        情況 B：使用者純粹閒聊或跨日反問 (Chat Mode)
        - 劇本 D【跨日反問 (ask_restore)】：目前 current_cart 是空的，但 last_session_cart 有東西，且使用者輸入破碎條件（如：「有賣甜點的嗎」）。親切反問是否要延續昨天條件。
           - ⚠️ 記憶暫存鐵律：你必須將使用者「剛剛輸入的新條件」放進 updated_cart 暫存，絕對不可以回傳空陣列 []，否則你會忘記他剛剛的需求！
        - 劇本 E【純閒聊 (none)】：抱怨天氣、閒聊廢話。購物車保持原樣 (none)。
           - 🗣️ 語氣指令：把使用者當朋友，用愛喝咖啡的吃貨口吻瞎扯。

        【最高優先級與特殊鐵律】
        - ⚠️ 【最高優先級鐵律】：如果是具體店名 (如 dine in cafe, always day one)時，你的處理方式如下：
           1. 清除風格條件：必須強制清除購物車內所有的「風格與設施標籤（如：安靜、深夜、有貓、插座等）」，絕對不能保留！
           2. 保留或新增地點：你【允許且必須】保留舊有的「地點名稱（如：東門、南港）」，或是新增使用者剛提到的地點。
           3. 輸出限制：你的 keyword 與 updated_cart 裡面，永遠只能是「地點 + 店名」或是單純的「店名」，絕不能夾帶任何其他形容詞或標籤！
        - 🛡️ 【比喻與排除豁免 (極度重要)】：如果使用者是把店名當作「比喻/參考」（例如：「像星巴克一樣氛圍的」、「跟路易莎差不多的」）或是「排除」（例如：「不要星巴克」），請【絕對不要】觸發上面的店名清空鐵律！你應該把這整句話當作一般的「風格條件」來處理，將比喻完整保留在 keyword 中，交給語意搜尋引擎處理。   
        - 🏷️ 購物車內容限制：updated_cart 裡面只能放【地點名稱】、【可用標籤清單】以及【明確的時間條件 (如：明天早上、晚上7點)】！絕對不要放廢話！
        - 🧹 條件不重複：確保 updated_cart 條件【絕對不重複】。
        - 🛡️ 記憶防護罩：執行劇本 A 與 B 時，絕對要保留原本購物車內的「時間條件」與「其他需求標籤」，除非使用者明確說不要了。
        - 📍 數量唯一性原則：通常情況下，購物車只會有【一個地點】與【一個時間】。**【唯一例外】**：如果使用者「明確」說出要找「A跟B之間 / 兩者中間」的店，你才允許把【兩個地點】同時放進 updated_cart 和 keyword 中！

        【回傳 JSON 格式鐵律】
        {{
            "mode": "search" 或 "chat",
            "reply": "Chat模式下的幽默回應或反問",
            "opening": "Search模式的開場白",
            "closing": "Search模式的結語",
            "keyword": "完整的搜尋條件 (Search模式必填)",
            "tags": ["嚴格從可用清單挑選的 1~3 個標籤"],
            "cart_action": "add" | "replace" | "clear" | "ask_restore" | "none",
            "updated_cart": ["更新後的條件或地點"]
        }}
        
        【範例訓練 (極度重要)】
        
        範例一 (劇本C：全新搜尋)
        - 狀態：current_cart=[]
        - 使用者：「幫我找半夜有開的安靜咖啡廳」
        - 你的輸出：{{"mode": "search", "reply": "", "opening": "沒問題！馬上幫你特搜幾家半夜還開著的寶藏愛店🤫", "closing": "這幾間網友都大推，半夜不怕沒地方去啦！", "keyword": "半夜有開的安靜咖啡廳", "tags": ["深夜", "安靜"], "cart_action": "clear", "updated_cart": ["深夜", "安靜"]}}

        範例二 (劇本A：追加條件)
        - 狀態：current_cart=["中山站", "工作友善"]
        - 使用者：「那這幾家有晚上8點營業的嗎」
        - 你的輸出：{{"mode": "search", "reply": "", "opening": "收到！要能工作又開得晚的續命好店馬上掃出來⚡", "closing": "快去拯救你的筆電吧！💻", "keyword": "中山站 工作友善 晚上8點營業", "tags": ["深夜"], "cart_action": "add", "updated_cart": ["中山站", "工作友善", "深夜"]}}

        範例三 (劇本E：純閒聊)
        - 狀態：current_cart=["士林"]
        - 使用者：「今天天氣好差心情不好」
        - 你的輸出：{{"mode": "chat", "reply": "天氣差真的超厭世啦！這種時候最適合躲進咖啡廳吃塊超讚的肉桂捲了，要不要我幫你找找？🍰", "opening": "", "closing": "", "keyword": "", "tags": [], "cart_action": "none", "updated_cart": ["士林"]}}

        範例四 (劇本D：跨日反問)
        - 狀態：current_cart=[], last_session_cart=["信義區", "插座"]
        - 使用者：「有賣甜點的嗎」
        - 你的輸出：{{"mode": "chat", "reply": "歡迎回來！您是要找昨天『信義區+有插座』附近，而且有賣甜點的咖啡廳嗎？還是今天要換個地方找呢？🍰", "opening": "", "closing": "", "keyword": "", "tags": [], "cart_action": "ask_restore", "updated_cart": ["甜點"]}}
        
        範例五 (劇本B：地點替換)
        - 狀態：current_cart=["忠孝復興", "安靜", "早上9點營業"]
        - 使用者：「松山附近呢，適合念書的」
        - 你的輸出：{{"mode": "search", "reply": "", "opening": "收到！馬上幫你轉移陣地到松山...", "closing": "這幾間松山的店超適合看書！", "keyword": "松山 安靜 早上9點營業 工作友善", "tags": ["安靜", "工作友善"], "cart_action": "replace", "updated_cart": ["松山", "安靜", "早上9點營業", "工作友善"]}}
        
        範例六 (劇本C：精準店名直達車)
        - 狀態：current_cart=["松山", "甜點", "早上10點營業"]
        - 使用者：「是店名 dine in cafe」 或 「找 always day one」
        - 你的輸出：{{"mode": "search", "reply": "", "opening": "沒問題！馬上幫你精準定位這家神店...", "closing": "這家真的讚，快去看看！", "keyword": "dine in cafe", "tags": [], "cart_action": "clear", "updated_cart": ["dine in cafe"]}}
        
        範例七 (隱藏技：中間點定位)
        - 狀態：current_cart=["安靜"]
        - 使用者：「那找北車跟中山中間的店好了」
        - 你的輸出：{{"mode": "search", "reply": "", "opening": "內行的！馬上幫你鎖定北車與中山的黃金交叉點...", "closing": "這幾家剛好在中間，交通超方便！", "keyword": "北車 中山 安靜", "tags": ["安靜"], "cart_action": "add", "updated_cart": ["北車", "中山", "安靜"]}}
        
        範例八 (保留地點的連鎖店/店名搜尋)
        - 狀態：current_cart=["東門", "深夜", "安靜"]
        - 使用者：「找星巴克」
        - 你的輸出：{{"mode": "search", "reply": "", "opening": "收到！馬上為您鎖定東門附近的星巴克...", "closing": "連鎖店最方便了，快去喝一杯吧！", "keyword": "東門 星巴克", "tags": [], "cart_action": "replace", "updated_cart": ["東門", "星巴克"]}}
        
        範例九 (豁免條款：將店名當作比喻或氛圍參考)
        - 狀態：current_cart=["信義區"]
        - 使用者：「要找跟星巴克氛圍一樣的，可以坐很久」
        - 你的輸出：{{"mode": "search", "reply": "", "opening": "懂你想找那種無拘無束、可以自帶筆電窩著的氛圍！馬上幫你撈...", "closing": "這幾家的氣氛絕對不輸星巴克，快去試試！", "keyword": "信義區 跟星巴克氛圍一樣的 可以坐很久", "tags": ["工作友善"], "cart_action": "add", "updated_cart": ["信義區", "工作友善"]}}
        """

        try:
            # 🌟 1. 計算輸入的 Token 數量
            token_info = self.model.count_tokens(prompt)
            input_tokens = token_info.total_tokens
            
            # ✂️ [瘦身] 精簡輸入 Log，完整 Prompt 降級為 debug 備用
            logger.info(f"🟢 [ChatAgent] 輸入 | 狀態: search_cart={current_cart_str} | 訊息: \"{user_msg}\"")
            logger.debug(f"==== 🟢 [ChatAgent] 完整 Prompt ====\n{prompt}\n===================================")

            generation_config = GenerationConfig(
                response_mime_type="application/json",
                temperature=0.3 
            )

            # 🌟 2. 開始計時並呼叫 AI
            start_time = time.time()
            response = self.model.generate_content(prompt, generation_config=generation_config)
            elapsed_time = time.time() - start_time
            
            clean_text = response.text.replace("```json", "").replace("```", "").strip()
            result = json.loads(clean_text)
            
            # 確保欄位齊全
            result.setdefault("tags", [])
            result.setdefault("updated_cart", [])
            result.setdefault("cart_action", "clear")
            
            # 防呆機制：標籤必須在標準清單內
            if isinstance(result["tags"], list):
                result["tags"] = [t for t in result["tags"] if t in valid_tags_list]
                
            # 防呆機制：過濾掉不在清單內，且不像是地點的怪異購物車標籤
            if isinstance(result["updated_cart"], list):
                filtered_cart = []
                for t in result["updated_cart"]:
                    if t in valid_tags_list or "區" in t or "站" in t or "市" in t or len(t) >= 2: 
                        filtered_cart.append(t)
                result["updated_cart"] = filtered_cart
                
            # ✂️ [瘦身] 將耗時、Token 與壓平後的 JSON 合併成精華一行！
            logger.info(f"🔵 [ChatAgent] 輸出 | 耗時: {elapsed_time:.2f}s | Token: {input_tokens} | 解析: {json.dumps(result, ensure_ascii=False)}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Chat Agent 終極分析失敗: {e}")
            return {
                "mode": "search", "keyword": user_msg, "tags": [], "cart_action": "clear",
                "updated_cart": [user_msg] if len(user_msg) < 10 else [],
                "opening": "正在搜尋中...", "closing": "希望您喜歡！"
            }