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

# 引入 DataClient
from data_client import DataClient

# 消除忽略 SSL 驗證帶來的警告紅字
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. 初始化環境
load_dotenv()
app = FastAPI()

line_bot_api = LineBotApi(os.getenv('LINE_CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.getenv('LINE_CHANNEL_SECRET'))

# --- 【全域變數】 ---
# 僅保留對話暫存 (RAM)，Cloud Run 重啟會消失 (符合 Stateless 原則)
user_sessions = {}  

# --- 共用 UI ---
def get_continue_quick_reply():
    return QuickReply(items=[
        QuickReplyButton(action={"type": "location", "label": "📍 換個地點找"}),
        QuickReplyButton(action=PostbackAction(label="🍰 找甜點好吃的", data="action=quick_tag&tag=甜點")), 
        QuickReplyButton(action=PostbackAction(label="💻 找有插座的", data="action=quick_tag&tag=插座"))
    ])

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

    # --- (B) 負評原因處理 (NO Flow) ---
    if user_id in user_sessions:
        target_cafe_id = user_sessions[user_id]
        
        DataClient.save_feedback(user_id, "NO", target_cafe_id, reason=user_msg)
        del user_sessions[user_id]
        
        line_bot_api.push_message(user_id, TextSendMessage(text=f"了解，因為「{user_msg}」... 正在為您尋找更適合的店家 🔍"))
        
        loc = DataClient.get_user_location(user_id)
        if loc:
            call_search_api(event.reply_token, loc['lat'], loc['lng'], user_id=user_id, limit=3)
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="無法讀取位置，請重新傳送 📍"))
        return 

    # --- (C) 一般搜尋 ---
    loc = DataClient.get_user_location(user_id)
    is_old_user = DataClient.check_user_exists(user_id)
    
    # 情況 1: 新手剛傳位置，正在打字
    if not is_old_user and loc:
        DataClient.save_feedback(user_id, "INIT_PREF", "SYSTEM_INIT", reason=user_msg)
        call_search_api(event.reply_token, loc['lat'], loc['lng'], user_id=user_id, tag=user_msg)
        return

    # 情況 2: 老手搜尋
    if loc:
        call_search_api(event.reply_token, loc['lat'], loc['lng'], user_id=user_id, tag=user_msg)
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

# 5. 搜尋函式 (修正版：對應新欄位)
def call_search_api(reply_token, lat, lng, user_id=None, tag=None, limit=10):
    BASE_URL = os.getenv('BACKEND_API_URL', "https://ossicular-gustily-elyse.ngrok-free.dev")
    
    if tag:
        SEARCH_API = f"{BASE_URL}/recommend?lat={lat}&lng={lng}&tag={tag}"
    else:
        SEARCH_API = f"{BASE_URL}/recommend?lat={lat}&lng={lng}&user_id={user_id}"

    cafe_list = []

    try:
        response = requests.get(SEARCH_API, timeout=10, verify=False)
        if response.status_code == 200:
            api_res = response.json()
            cafe_list = api_res.get("data", [])
        else:
            print(f"⚠️ API 回傳非 200: {response.status_code}")
    except Exception as e:
        print(f"❌ API 連線錯誤：{e}")
        
    # --- 【新增】保險機制：Mock 資料也更新為新格式 ---
    if not cafe_list:
        print("💡 啟動備援模式：使用 Mock 資料")
        cafe_list = [
            {
                "original_name": "測試用咖啡 (Mock)",
                "place_id": "mock_001",
                "attributes": {
                    "types": ["cafe", "food"],
                    "rating": 4.8
                },
                "dist_meters": 150
            },
            {
                "original_name": "路易莎 (備援)",
                "place_id": "mock_002",
                "attributes": {
                    "types": ["cafe", "chain"],
                    "rating": 4.2
                },
                "dist_meters": 300
            }
        ]
    # -------------------------------------------------------

    if not cafe_list:
        line_bot_api.reply_message(reply_token, TextSendMessage(text="附近暫無推薦店家 😢"))
        return

    # 產出 Flex Message 卡片
    bubbles = []
    for cafe in cafe_list[:limit]:
        # 1. 店名：優先找 original_name，沒有才找 name
        shop_name = cafe.get("original_name", cafe.get("name", "咖啡廳"))
        
        # 2. ID
        place_id = cafe.get('place_id', '')
        
        # 3. 標籤：優先找 ai_tags (AI 處理過)，如果沒有則找 attributes.types (Google 原生)
        tags = []
        if 'ai_tags' in cafe and isinstance(cafe['ai_tags'], list):
            # 格式若是 [{"tag": "安靜"}]
            tags = [t.get('tag', '') for t in cafe['ai_tags'] if isinstance(t, dict)]
        
        # 如果上方找不到 tags，就去 attributes 找 types
        if not tags and 'attributes' in cafe and 'types' in cafe['attributes']:
            tags = cafe['attributes']['types']
        
        # 4. 距離：後端通常會算出 dist_meters，如果沒有則顯示 0
        dist_m = cafe.get('dist_meters', 0)
        dist_str = f"{dist_m / 1000:.1f} km" if dist_m >= 1000 else f"{int(dist_m)} m"
        
        # 5. 評分與評論數：嘗試從 attributes 或根目錄找，找不到就給 0
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
            
        call_search_api(event.reply_token, loc['lat'], loc['lng'], user_id=user_id, tag=tag)
        return

    cafe_id = params.get('id')
    
    if action == "yes":
        DataClient.save_feedback(user_id, "YES", cafe_id)
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text="已記住您的喜好 ✨\n還想找其他的嗎？", quick_reply=get_continue_quick_reply())
        )
        
    elif action == "no":
        user_sessions[user_id] = cafe_id
        quick_reply = QuickReply(items=[
            QuickReplyButton(action=PostbackAction(label="太貴了", data=f"reason=expensive&id={cafe_id}")),
            QuickReplyButton(action=PostbackAction(label="環境太吵", data=f"reason=noisy&id={cafe_id}")),
            QuickReplyButton(action=PostbackAction(label="沒有插座", data=f"reason=no_plug&id={cafe_id}")),
        ])
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text="請問不喜歡的原因是？\n(可直接打字或選按鈕)", quick_reply=quick_reply)
        )
        
    elif action == "keep":
        DataClient.save_feedback(user_id, "KEEP", cafe_id)
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text="已加入收藏 ❤️\n要繼續找其他店家嗎？", quick_reply=get_continue_quick_reply())
        )
        
    elif params.get('reason'):
        if user_id in user_sessions: del user_sessions[user_id]
        reason = params.get('reason')
        DataClient.save_feedback(user_id, "NO", cafe_id, reason=reason)
        
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