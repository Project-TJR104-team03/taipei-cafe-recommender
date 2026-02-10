import os
import requests  
import urllib3  
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Header, HTTPException
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from urllib.parse import quote  
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, 
    LocationMessage, FlexSendMessage, PostbackEvent,
    QuickReply, QuickReplyButton, PostbackAction,
    FollowEvent
)
import google.generativeai as genai
import json 

# 引入 DataClient
from data_client import DataClient

# 消除忽略 SSL 驗證帶來的警告紅字
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 初始化環境
load_dotenv()
app = FastAPI()
# 設定 Gemini 模型
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

line_bot_api = LineBotApi(os.getenv('LINE_CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.getenv('LINE_CHANNEL_SECRET'))

# --- 【全域變數】 ---
user_sessions = {}  

# --- 共用 UI ---
def get_continue_quick_reply():
    return QuickReply(items=[
        QuickReplyButton(action={"type": "location", "label": "📍 換個地點找"}),
        QuickReplyButton(action=PostbackAction(label="🍰 找甜點好吃的", data="action=quick_tag&tag=甜點")), 
        QuickReplyButton(action=PostbackAction(label="💻 找有插座的", data="action=quick_tag&tag=插座"))
    ])

# --- AI 意圖分析核心 (雙模式版) ---
def analyze_user_intent(user_msg):
    """
    使用 Gemini 判斷是「閒聊」還是「搜尋」，並提取關鍵字
    """
    # 請依據你的帳號權限調整型號 (gemini-2.0-flash 或 gemini-2.5-flash)
    model = genai.GenerativeModel('gemini-2.0-flash')
    
    valid_tags = "不限時, 安靜, 甜點, 插座, wifi, 景觀, 復古, 寵物, 深夜, 舒適, 商業, 約會, 讀書"

    # 設定咒語 (Prompt)
    prompt = f"""
    【角色設定】
    你是一個幽默、溫暖的 AI 咖啡廳助手。
    你的任務是判斷使用者的輸入是「想要找店」還是「純粹閒聊」。

    【可用標籤清單】
    {valid_tags}

    【判斷邏輯】
    請分析使用者的輸入，並回傳對應的 JSON 格式：

    的情況 A：使用者想找咖啡廳 (Search Mode)
    - 判斷依據：提到地點、食物、氛圍、插座、找店等需求。
    - 回傳格式：
      {{
        "mode": "search",
        "tags": ["從清單選出的標籤"],
        "keyword": "提取的關鍵字(如燕麥奶、肉桂捲)，若無則留空",
        "reply": "好的！幫您尋找...(簡短的過場詞)"
      }}

    的情況 B：使用者純粹閒聊 (Chat Mode)
    - 判斷依據：打招呼、問你的名字、心情分享、講笑話、與找店無關的話題。
    - 回傳格式：
      {{
        "mode": "chat",
        "tags": [],
        "keyword": "",
        "reply": "請用繁體中文，針對使用者的話給予幽默或溫暖的回覆 (50字以內)。"
      }}

    【範例】
    User: "找個安靜的地方"
    JSON: {{"mode": "search", "tags": ["安靜"], "keyword": "", "reply": "沒問題，幫您找找安靜的角落！"}}

    User: "嗨你好，你是誰？"
    JSON: {{"mode": "chat", "tags": [], "keyword": "", "reply": "嗨！我是你的咖啡廳小助手，專門幫你找好店，順便陪你聊聊咖啡！☕"}}

    User: "有燕麥奶拿鐵嗎"
    JSON: {{"mode": "search", "tags": ["咖啡"], "keyword": "燕麥奶", "reply": "收到，幫您找有燕麥奶的店！"}}

    【真實輸入】
    "{user_msg}"
    """

    try:
        response = model.generate_content(prompt)
        clean_text = response.text.replace("```json", "").replace("```", "").strip()
        result = json.loads(clean_text)
        return result 
    except Exception as e:
        print(f"❌ AI 分析失敗: {e}")
        # 失敗備案：預設為搜尋模式，直接把整句當查詢詞
        return {"mode": "search", "tags": [], "keyword": user_msg, "reply": ""}

# 2. Webhook 進入點
@app.post("/callback")
async def callback(request: Request, x_line_signature: str = Header(None)):
    body = await request.body()
    try:
        handler.handle(body.decode("utf-8"), x_line_signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")
    return 'OK'

# 3. 處理文字訊息
@handler.add(MessageEvent, message=TextMessage)
def handle_text(event):
    user_msg = event.message.text
    user_id = event.source.user_id

    # --- (A) 重置指令 ---
    if user_msg == "重置":
        if user_id in user_sessions: 
            del user_sessions[user_id]
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="🔄 對話狀態已重置。"))
        return

    # --- (B) 負評原因處理 ---
    if user_id in user_sessions:
        target_place_id = user_sessions[user_id]
        DataClient.save_feedback(user_id, "NO", target_place_id, reason=user_msg)
        del user_sessions[user_id]
        line_bot_api.push_message(user_id, TextSendMessage(text=f"了解，因為「{user_msg}」... 正在為您尋找更適合的店家 🔍"))
        loc = DataClient.get_user_location(user_id)
        if loc:
            call_search_api(event.reply_token, loc['lat'], loc['lng'], user_id=user_id, limit=3)
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="無法讀取位置，請重新傳送 📍"))
        return 

    # --- (C) 一般流程 ---
    loc = DataClient.get_user_location(user_id)
    is_old_user = DataClient.check_user_exists(user_id)
    
    # 情況 1: 新手剛傳位置，正在打字 (直接當作 user_query 搜尋)
    if not is_old_user and loc:
        DataClient.save_feedback(user_id, "INIT_PREF", "SYSTEM_INIT", reason=user_msg)
        # ⚠️ 修改：使用者手動打字 -> user_query
        call_search_api(event.reply_token, loc['lat'], loc['lng'], user_id=user_id, user_query=user_msg)
        return

    # 情況 2: 老手搜尋 (AI 介入)
    if loc:
        # A. 呼叫 AI
        ai_result = analyze_user_intent(user_msg)
        
        mode = ai_result.get("mode", "search")
        reply_text = ai_result.get("reply", "")
        extracted_tags = ai_result.get("tags", [])
        extracted_keyword = ai_result.get("keyword", "")
        
        print(f"🧠 AI 分析: Mode={mode}, Keyword={extracted_keyword}, Tags={extracted_tags}")

        # --- 分歧點：如果是閒聊模式 ---
        if mode == "chat":
            chat_quick_reply = QuickReply(items=[
                QuickReplyButton(action=PostbackAction(label="還是幫我找店好了", data="action=onboarding&tag=熱門"))
            ])
            line_bot_api.reply_message(
                event.reply_token, 
                TextSendMessage(text=reply_text, quick_reply=chat_quick_reply)
            )
            return

        # --- 分歧點：如果是搜尋模式 ---
        # 決定要傳給 user_query 的內容
        # 策略：如果有 AI 提取的關鍵字，用關鍵字；否則用原始語句 (讓後端做 embedding)
        if extracted_keyword:
            search_term = extracted_keyword
        elif extracted_tags:
             # 如果只有標籤，也轉成字串當查詢
            search_term = extracted_tags[0]
        else:
            # AI 沒抓到重點，就直接傳整句原始話語，讓後端去煩惱
            search_term = user_msg

        # ⚠️ 修改：這是搜尋意圖 -> user_query
        call_search_api(event.reply_token, loc['lat'], loc['lng'], user_id=user_id, user_query=search_term)
        return

    # 情況 3: 無位置
    quick_reply = QuickReply(items=[QuickReplyButton(action={"type": "location", "label": "📍 傳送目前位置"})])
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="請先分享位置，我才能幫您找附近的店喔！", quick_reply=quick_reply))

# 4. 處理位置訊息
@handler.add(MessageEvent, message=LocationMessage)
def handle_location(event):
    lat, lng = event.message.latitude, event.message.longitude
    user_id = event.source.user_id
    
    DataClient.save_user_location(user_id, lat, lng)

    if not DataClient.check_user_exists(user_id):
        quick_reply = QuickReply(items=[
            QuickReplyButton(action=PostbackAction(label="📖 安靜讀書", data="action=onboarding&tag=安靜")),
            QuickReplyButton(action=PostbackAction(label="🗣️ 朋友聚會", data="action=onboarding&tag=熱鬧")),
            QuickReplyButton(action=PostbackAction(label="☕ 復古文青", data="action=onboarding&tag=復古")),
        ])
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text="👋 初次見面！請問想找哪類咖啡廳？", quick_reply=quick_reply)
        )
        return 

    call_search_api(event.reply_token, lat, lng, user_id=user_id)

# 5. 搜尋函式 (⚠️ 重要修改：content 改為 user_query，cafe_tag 改為 tag)
def call_search_api(reply_token, lat, lng, user_id=None, tag=None, user_query=None, limit=10):
    BASE_URL = os.getenv('BACKEND_API_URL', "https://ossicular-gustily-elyse.ngrok-free.dev")
    
    # 建立基礎 URL
    api_url = f"{BASE_URL}/recommend?lat={lat}&lng={lng}&user_id={user_id}"
    
    # 判斷是 Tag (按鈕) 還是 User Query (打字)
    if tag:
        # 情況 A: 按鈕點擊 -> 傳送 tag (後端做 SQL 篩選)
        api_url += f"&tag={tag}"
    elif user_query:
        # 情況 B: 手動打字/AI 搜尋 -> 傳送 user_query (後端做向量搜尋)
        api_url += f"&user_query={user_query}"
    
    print(f"📡 呼叫後端 API: {api_url}")

    cafe_list = []
    try:
        response = requests.get(api_url, timeout=10, verify=False)
        if response.status_code == 200:
            api_res = response.json()
            cafe_list = api_res.get("data", [])
        else:
            print(f"⚠️ API 回傳非 200: {response.status_code}")
    except Exception as e:
        print(f"❌ API 連線錯誤：{e}")
        
    # --- Mock 資料備援 (開發用) ---
    if not cafe_list:
        print("💡 啟動備援模式：使用 Mock 資料")
        cafe_list = [
            {"original_name": "測試用咖啡 (Mock)", "place_id": "mock_001", "attributes": {"types": ["cafe"], "rating": 4.8}, "dist_meters": 150},
            {"original_name": "路易莎 (備援)", "place_id": "mock_002", "attributes": {"types": ["chain"], "rating": 4.2}, "dist_meters": 300}
        ]

    if not cafe_list:
        line_bot_api.reply_message(reply_token, TextSendMessage(text="附近暫無推薦店家 😢"))
        return

    # 產出 Flex Message 卡片
    bubbles = []
    for cafe in cafe_list[:limit]:
        shop_name = cafe.get("original_name", cafe.get("name", "咖啡廳"))
        place_id = cafe.get('place_id', '')
        
        tags = []
        if 'ai_tags' in cafe and isinstance(cafe['ai_tags'], list):
            tags = [t.get('tag', '') for t in cafe['ai_tags'] if isinstance(t, dict)]
        if not tags and 'attributes' in cafe and 'types' in cafe['attributes']:
            tags = cafe['attributes']['types']
        
        dist_m = cafe.get('dist_meters', 0)
        dist_str = f"{dist_m / 1000:.1f} km" if dist_m >= 1000 else f"{int(dist_m)} m"
        
        rating = cafe.get('rating', cafe.get('attributes', {}).get('rating', 0.0))
        total_reviews = cafe.get('total_ratings', cafe.get('user_ratings_total', 0))
        
        map_url = f"https://www.google.com/maps/search/?api=1&query={quote(shop_name)}"
        
        bubbles.append({
            "type": "bubble",
            "body": {
                "type": "box", "layout": "vertical", "spacing": "sm",
                "contents": [
                    {"type": "text", "text": shop_name, "weight": "bold", "size": "xl", "wrap": True},
                    {
                        "type": "box", "layout": "baseline", "spacing": "sm", "margin": "md",
                        "contents": [
                            {"type": "text", "text": "⭐", "size": "sm", "color": "#FFC107", "flex": 0},
                            {"type": "text", "text": f"{rating}", "size": "sm", "weight": "bold", "color": "#666666", "flex": 0},
                            {"type": "text", "text": f"({total_reviews} 評論)", "size": "xs", "color": "#999999", "margin": "sm", "flex": 1}
                        ]
                    },
                    {
                        "type": "box", "layout": "vertical", "spacing": "xs", "margin": "md",
                        "contents": [
                            {"type": "text", "text": f"📍 距離 {dist_str}", "size": "sm", "color": "#666666"},
                            {"type": "text", "text": f"🏷️ {' '.join(tags[:3])}", "size": "xs", "color": "#aaaaaa", "wrap": True}
                        ]
                    }
                ]
            },
            "footer": {
                "type": "box", "layout": "vertical", "spacing": "sm",
                "contents": [
                    {"type": "button", "style": "link", "height": "sm", "action": {"type": "uri", "label": "🌏 Google Maps", "uri": map_url}},
                    {
                        "type": "box", "layout": "horizontal", "spacing": "sm",
                        "contents": [
                            {"type": "button", "style": "primary", "color": "#48bb78", "action": {"type": "postback", "label": "YES", "data": f"action=yes&id={place_id}"}},
                            {"type": "button", "style": "primary", "color": "#f56565", "action": {"type": "postback", "label": "NO", "data": f"action=no&id={place_id}"}}
                        ]
                    },
                    {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "KEEP (收藏)", "data": f"action=keep&id={place_id}"}}
                ]
            }
        })
    line_bot_api.reply_message(reply_token, FlexSendMessage(alt_text="推薦結果", contents={"type": "carousel", "contents": bubbles}))

# 6. Postback 處理
@handler.add(PostbackEvent)
def handle_postback(event):
    user_id = event.source.user_id
    params = dict(item.split('=') for item in event.postback.data.split('&'))
    action = params.get('action')
    
    # 快捷 Tag
    if action == "quick_tag":
        tag = params.get('tag')
        loc = DataClient.get_user_location(user_id)
        if loc:
            # ⚠️ 修改：按鈕點擊 -> 傳送 tag
            call_search_api(event.reply_token, loc['lat'], loc['lng'], user_id=user_id, tag=tag)
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="請先傳送您的位置📍"))
        return

    # 冷啟動
    if action == "onboarding":
        tag = params.get('tag')
        DataClient.save_feedback(user_id, "INIT_PREF", "SYSTEM_INIT", reason=tag)
        loc = DataClient.get_user_location(user_id) 
        if not loc:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="📍 定位過期，請重新發送！"))
            return
        # ⚠️ 修改：按鈕點擊 -> 傳送 tag
        call_search_api(event.reply_token, loc['lat'], loc['lng'], user_id=user_id, tag=tag)
        return

    place_id = params.get('id')
    
    if action == "yes":
        DataClient.save_feedback(user_id, "YES", place_id)
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text="已記住您的喜好 ✨\n還想找其他的嗎？", quick_reply=get_continue_quick_reply())
        )
    elif action == "no":
        user_sessions[user_id] = place_id
        quick_reply = QuickReply(items=[
            QuickReplyButton(action=PostbackAction(label="太貴了", data=f"reason=expensive&id={place_id}")),
            QuickReplyButton(action=PostbackAction(label="環境太吵", data=f"reason=noisy&id={place_id}")),
            QuickReplyButton(action=PostbackAction(label="沒有插座", data=f"reason=no_plug&id={place_id}")),
        ])
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text="請問不喜歡的原因是？\n(可直接打字或選按鈕)", quick_reply=quick_reply)
        )
    elif action == "keep":
        DataClient.save_feedback(user_id, "KEEP", place_id)
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text="已加入收藏 ❤️\n要繼續找其他店家嗎？", quick_reply=get_continue_quick_reply())
        )
    elif params.get('reason'):
        if user_id in user_sessions: del user_sessions[user_id]
        reason = params.get('reason')
        DataClient.save_feedback(user_id, "NO", place_id, reason=reason)
        line_bot_api.push_message(user_id, TextSendMessage(text=f"收到！正在重新篩選... 🔄"))
        loc = DataClient.get_user_location(user_id)
        if loc:
            call_search_api(event.reply_token, loc['lat'], loc['lng'], user_id=user_id, limit=3)
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="請重新傳送位置📍"))

# 7. 加入好友
@handler.add(FollowEvent)
def handle_follow(event):
    user_id = event.source.user_id
    welcome_text = "嗨！我是 AI 咖啡助手 ☕\n請傳送您的位置，讓我為您推薦！"
    quick_reply = QuickReply(items=[QuickReplyButton(action={"type": "location", "label": "📍 傳送目前位置"})])
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=welcome_text, quick_reply=quick_reply))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)