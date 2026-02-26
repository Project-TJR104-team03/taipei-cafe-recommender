# app/main.py
import os
import random
import logging
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager
from urllib.parse import quote

from fastapi import FastAPI, Request, Header, HTTPException
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, 
    LocationMessage, FlexSendMessage, PostbackEvent,
    QuickReply, QuickReplyButton, PostbackAction,
    FollowEvent
)
from dotenv import load_dotenv

# 🔥 處理時間狀態
from datetime import datetime, timedelta

# 引入自定義模組
from database import db_client
from services.recommend_service import RecommendService
from services.user_service import UserService
from agents.chat_agent import ChatAgent

# --- 強制抓取 .env ---
current_file_path = Path(__file__).resolve()
env_path = current_file_path.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# --- 初始化 ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Coffee_Recommender")

@asynccontextmanager
async def lifespan(app: FastAPI):
    db_client.connect()
    yield
    db_client.close()

app = FastAPI(lifespan=lifespan)

line_bot_api = LineBotApi(os.getenv('LINE_CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.getenv('LINE_CHANNEL_SECRET'))

recommend_service = RecommendService(api_key=os.getenv("GEMINI_API_KEY"))
user_service = UserService()
chat_agent = ChatAgent()

user_sessions = {}
blacklist_sessions = {} 

# --- 輔助函式 ---
def get_standard_quick_reply():
    return QuickReply(items=[
        QuickReplyButton(action={"type": "location", "label": "📍 點我找附近的店"}),
        QuickReplyButton(action=PostbackAction(label="📂 我的收藏清單", data="action=view_keep")),
        QuickReplyButton(action=PostbackAction(label="🚫 我的黑名單", data="action=view_blacklist")),
        QuickReplyButton(action=PostbackAction(label="🍰 附近哪裡有甜點", data="action=quick_tag&tag=甜點")), 
        QuickReplyButton(action=PostbackAction(label="💻 找有插座的店", data="action=quick_tag&tag=插座")),
        QuickReplyButton(action=PostbackAction(label="🌙 開到深夜", data="action=quick_tag&tag=深夜"))
    ])

# ✨ 新增：查看清單時「專用」的快捷按鈕 (多了一顆看完了)
def get_list_view_quick_reply():
    return QuickReply(items=[
        QuickReplyButton(action=PostbackAction(label="👀 看完了，繼續找店", data="action=close_list")),
        QuickReplyButton(action={"type": "location", "label": "📍 點我找附近的店"}),
        QuickReplyButton(action=PostbackAction(label="📂 我的收藏清單", data="action=view_keep")),
        QuickReplyButton(action=PostbackAction(label="🚫 我的黑名單", data="action=view_blacklist")),
        QuickReplyButton(action=PostbackAction(label="🍰 附近哪裡有甜點", data="action=quick_tag&tag=甜點")), 
        QuickReplyButton(action=PostbackAction(label="💻 找有插座的店", data="action=quick_tag&tag=插座")),
        QuickReplyButton(action=PostbackAction(label="🌙 開到深夜", data="action=quick_tag&tag=深夜"))
    ])

def get_button_reaction(tag):
    openings = [
        f"沒問題！馬上幫您找「{tag}」的店... 🔍",
        f"想找「{tag}」是嗎？交給我！⚡",
        f"收到！正在搜尋附近的「{tag}」好去處...",
        f"OK，來看看附近有哪些「{tag}」的選擇 ☕"
    ]
    closings = [
        "這幾家感覺都很棒，您覺得呢？",
        "希望您會喜歡這些推薦！✨",
        "如果不滿意，可以點選下方「📍」按鈕換個地點找喔！",
        "這幾家評價都不錯，快去看看吧！🚀"
    ]
    return random.choice(openings), random.choice(closings)

# --- ⭐ 星星評分組件產生器 ---
def create_star_rating_box(rating, total_reviews):
    GOLD_STAR_URL = "https://scdn.line-apps.com/n/channel_devcenter/img/fx/review_gold_star_28.png"
    GREY_STAR_URL = "https://scdn.line-apps.com/n/channel_devcenter/img/fx/review_gray_star_28.png"

    try:
        rating_float = float(rating)
        num_gold = int(round(rating_float))
        num_gold = max(0, min(5, num_gold))
    except (ValueError, TypeError):
        num_gold = 0
        rating = 0.0 
        
    num_grey = 5 - num_gold
    contents = []
    for _ in range(num_gold):
        contents.append({"type": "icon", "size": "sm", "url": GOLD_STAR_URL})
    for _ in range(num_grey):
        contents.append({"type": "icon", "size": "sm", "url": GREY_STAR_URL})

    review_text = {
        "type": "text", "text": f"{rating} ({total_reviews} 評論)",
        "size": "sm", "color": "#999999", "margin": "sm", "flex": 0       
    }
    contents.append(review_text)

    return {
        "type": "box", "layout": "baseline", "spacing": "xs", "contents": contents
    }

# --- 營業時間狀態產生器 ---
def get_opening_status(cafe_data):
    opening_hours = cafe_data.get("opening_hours")
    
    if not opening_hours or "periods" not in opening_hours:
        return "", ""

    periods = opening_hours.get("periods", [])
    if not periods:
        return "", ""

    tw_now = datetime.utcnow() + timedelta(hours=8)
    current_iso = tw_now.isoweekday()
    current_day = 0 if current_iso == 7 else current_iso
    
    current_mins = current_day * 24 * 60 + tw_now.hour * 60 + tw_now.minute

    min_diff = float('inf')
    next_open_info = None
    day_map = {0: "週日", 1: "週一", 2: "週二", 3: "週三", 4: "週四", 5: "週五", 6: "週六"}

    for p in periods:
        open_day = p.get("day", 0)
        open_val = int(p.get("open", 0))
        open_hour, open_min = open_val // 60, open_val % 60
        start_mins = open_day * 24 * 60 + open_hour * 60 + open_min

        close_val = p.get("close")
        if close_val is None:
            if open_val == 0:
                return "24 小時營業", "#00B900"
            continue
            
        close_val = int(close_val)
        close_hour, close_min = close_val // 60, close_val % 60
        
        close_day = open_day
        if close_val < open_val:
            close_day = (open_day + 1) % 7
            
        end_mins = close_day * 24 * 60 + close_hour * 60 + close_min
        
        if end_mins < start_mins:
            end_mins += 7 * 24 * 60

        check_mins = current_mins
        if current_mins < start_mins and (current_mins + 7 * 24 * 60) < end_mins:
            check_mins += 7 * 24 * 60

        if start_mins <= check_mins < end_mins:
            close_str = f"{close_hour:02d}:{close_min:02d}"
            return f"營業中 · 營業至 {close_str}", "#00B900"

        diff = start_mins - current_mins
        if diff <= 0:
            diff += 7 * 24 * 60
            
        if diff < min_diff:
            min_diff = diff
            next_open_info = f"下次營業 {day_map[open_day]} {open_hour:02d}:{open_min:02d}"

    if next_open_info:
        return next_open_info, "#f56565"
        
    return "今日未營業", "#999999"

# ✨ 顯示「我的收藏」或「我的黑名單」卡片
def show_user_list(reply_token, user_id, list_type):
    cafes = user_service.get_user_places(user_id, list_type)
    list_name = "收藏清單 ❤️" if list_type == "bookmarks" else "黑名單 🚫"
    
    if not cafes:
        line_bot_api.reply_message(
            reply_token, 
            TextSendMessage(text=f"您的{list_name}目前是空的喔！", quick_reply=get_standard_quick_reply())
        )
        return

    bubbles = []
    for cafe in cafes[:10]: # 最多顯示 10 筆
        shop_name = cafe.get("final_name", "未知店家")
        original_name = cafe.get("original_name", shop_name)
        place_id = cafe.get('place_id', '')
        
        # 🔥 修改這裡：對齊 MongoDB 的巢狀欄位結構，正確抓出星星與評論數
        db_ratings = cafe.get("ratings", {})
        rating = db_ratings.get("rating", cafe.get("rating", 0.0))
        total_reviews = db_ratings.get("review_amount", cafe.get("total_ratings", 0))
        
        contact_info = cafe.get("contact", {})
        db_map_url = contact_info.get("google_maps_url")
        map_url = db_map_url if db_map_url else f"https://www.google.com/maps/search/?api=1&query={quote(original_name)}"
        
        if list_type == "bookmarks":
            action_buttons = [
                {"type": "button", "style": "primary", "color": "#48bb78", "action": {"type": "postback", "label": "導航 😍", "data": f"action=yes&id={place_id}&name={quote(shop_name)}"}},
                {"type": "button", "style": "secondary", "color": "#e53e3e", "action": {"type": "postback", "label": "💔 移除收藏", "data": f"action=remove_list&list=bookmarks&id={place_id}"}}
            ]
        else: # blacklist
            action_buttons = [
                {"type": "button", "style": "secondary", "color": "#4299e1", "action": {"type": "postback", "label": "🔄 移出黑名單", "data": f"action=remove_list&list=blacklist&id={place_id}"}}
            ]

        bubbles.append({
            "type": "bubble",
            "body": {
                "type": "box", "layout": "vertical", "spacing": "sm",
                "contents": [
                    {"type": "text", "text": f"🏷️ {list_name}", "size": "xs", "color": "#ff6b6b" if list_type == "bookmarks" else "#718096", "weight": "bold"},
                    {"type": "text", "text": shop_name, "weight": "bold", "size": "xl", "wrap": True},
                    # ✨ 成功把正確的星星和評論數放進卡片裡！
                    create_star_rating_box(rating, total_reviews)
                ]
            },
            "footer": {
                "type": "box", "layout": "vertical", "spacing": "sm",
                "contents": [
                    {"type": "button", "style": "link", "height": "sm", "action": {"type": "uri", "label": "🌏 查看地圖", "uri": map_url}},
                    {
                        "type": "box", "layout": "horizontal", "spacing": "sm",
                        "contents": action_buttons
                    }
                ]
            }
        })

    # 這裡綁定了上一動我們做好的「專屬快捷按鈕 (看完了)」
    flex_message = FlexSendMessage(
        alt_text=f"您的{list_name}", 
        contents={"type": "carousel", "contents": bubbles},
        quick_reply=get_list_view_quick_reply()
    )
    line_bot_api.reply_message(reply_token, flex_message)

# --- 核心搜尋流程 ---
async def process_recommendation(reply_token, lat, lng, user_id, tag=None, user_query=None, opening=None, closing=None, rejected_place_id=None, negative_reason=None):
   result = await recommend_service.recommend(
        lat=lat, lng=lng, user_id=user_id, 
        user_query=user_query, 
        cafe_tag=tag,
        rejected_place_id=rejected_place_id,
        negative_reason=negative_reason
    )
   cafe_list = result.get("data", [])

   # 如果 recommend_service 有回傳它實際使用的中心點座標，我們就更新使用者的定位！
   new_lat = result.get("center_lat")
   new_lng = result.get("center_lng")
   if new_lat and new_lng and (new_lat != lat or new_lng != lng):
        # 將使用者的定位更新為龍山寺 (或其他地點)，這樣下一回合就會從這裡開始搜！
        user_service.update_user_location(user_id, new_lat, new_lng)
        print(f"📍 [狀態更新] 已將使用者 {user_id} 的錨點固定至 ({new_lat}, {new_lng})")

   if not cafe_list:
        print("💡 查無資料，啟動備援模式")
        cafe_list = [
            {"final_name": "測試用咖啡 (Mock)", "place_id": "mock_001", "rating": 4.8, "dist_meters": 150, "ai_tags": [{"tag": "測試"}]},
            {"final_name": "路易莎 (備援)", "place_id": "mock_002", "rating": 4.2, "dist_meters": 300, "attributes": {"types": ["chain"]}}
        ]
    

   bubbles = []
   for cafe in cafe_list:
        shop_name = cafe.get("final_name", "咖啡廳")
        original_name = cafe.get("original_name", shop_name)
        place_id = cafe.get('place_id', '')
        
        display_tags = cafe.get('display_tags', [])
        
        dist_m = cafe.get('dist_meters', 0)
        dist_str = f"{dist_m / 1000:.1f} km" if dist_m >= 1000 else f"{int(dist_m)} m"
        
        rating = cafe.get('rating', 0.0) 
        total_reviews = cafe.get('total_ratings', 0)
        
        contact_info = cafe.get("contact", {})
        db_map_url = contact_info.get("google_maps_url")
        map_url = db_map_url if db_map_url else f"https://www.google.com/maps/search/?api=1&query={quote(original_name)}"
        
        open_text, open_color = get_opening_status(cafe)
        
        dist_time_contents = [
            {"type": "text", "text": f"📍 距離 {dist_str}", "size": "sm", "color": "#666666", "flex": 0}
        ]
        if open_text:
            dist_time_contents.append(
                {"type": "text", "text": f" · {open_text}", "size": "sm", "color": open_color, "flex": 1, "wrap": True}
            )

        info_box_contents = [
            {"type": "box", "layout": "baseline", "spacing": "none", "contents": dist_time_contents}
        ]
        
        if display_tags:
            info_box_contents.append(
                {
                    "type": "text", 
                    "text": " · ".join(display_tags), # 用間隔號串接 (如: 🔌 插座 · 🌙 深夜)
                    "size": "xs", 
                    "color": "#888888", 
                    "wrap": True, # 開啟換行，維持排版穩定
                    "margin": "sm"
                }
            )
        
        safe_name = shop_name.replace('&', '及').replace('=', '-')[:20]

        bubbles.append({
            "type": "bubble",
            "body": {
                "type": "box", "layout": "vertical", "spacing": "sm",
                "contents": [
                    {"type": "text", "text": shop_name, "weight": "bold", "size": "xl", "wrap": True},
                    create_star_rating_box(rating, total_reviews),
                    {
                        "type": "box", "layout": "vertical", "spacing": "xs", "margin": "md",
                        "contents": info_box_contents
                    }
                ]
            },
            "footer": {
                    "type": "box", "layout": "vertical", "spacing": "sm",
                    "contents": [
                        {"type": "button", "style": "link", "height": "sm", "action": {"type": "uri", "label": "🌏 查看地圖", "uri": map_url}},
                        {
                            "type": "box", "layout": "horizontal", "spacing": "sm",
                            "contents": [
                                {"type": "button", "style": "primary", "color": "#48bb78", "action": {"type": "postback", "label": "喜歡 😍", "data": f"action=yes&id={place_id}&name={safe_name}"}},
                                {"type": "button", "style": "primary", "color": "#f56565", "action": {"type": "postback", "label": "不行 🙅", "data": f"action=no&id={place_id}&name={safe_name}"}}
                            ]
                        },
                        {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "❤️ 先收藏", "data": f"action=keep&id={place_id}&name={safe_name}"}}
                    ]
                }
        })
        
   flex_message = FlexSendMessage(alt_text="推薦結果", contents={"type": "carousel", "contents": bubbles})
    
   reply_payload = []
   if opening: reply_payload.append(TextSendMessage(text=opening))
   reply_payload.append(flex_message)
    
   if closing:
        reply_payload.append(TextSendMessage(text=closing, quick_reply=get_standard_quick_reply()))
   else:
        reply_payload.append(TextSendMessage(text="還想找其他的嗎？", quick_reply=get_standard_quick_reply()))
        
   line_bot_api.reply_message(reply_token, reply_payload)

# --- Handlers ---
@app.post("/callback")
async def callback(request: Request, x_line_signature: str = Header(None)):
    body = await request.body()
    try:
        handler.handle(body.decode("utf-8"), x_line_signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")
    return 'OK'

@handler.add(MessageEvent, message=TextMessage)
def handle_text(event):
    user_msg = event.message.text
    user_id = event.source.user_id

    if user_msg == "重置":
        if user_id in user_sessions: del user_sessions[user_id]
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="🔄 對話狀態已重置。", quick_reply=get_standard_quick_reply()))
        return

    loc = user_service.get_user_location(user_id)
    lat = loc['lat'] if loc else None
    lng = loc['lng'] if loc else None

    # 處理 NO 的回饋原因 (手動打字)
    if user_id in user_sessions:
        target_place_id = user_sessions[user_id]
        user_service.log_action(
            user_id, "NO_REASON", target_place_id, 
            reason=user_msg, user_msg=user_msg, 
            lat=lat, lng=lng
        )
        del user_sessions[user_id]
        
        blacklist_sessions[user_id] = {"place_id": target_place_id, "reason": user_msg}
        
        quick_reply = QuickReply(items=[
            QuickReplyButton(action=PostbackAction(label="要，加入黑名單", data=f"action=confirm_blacklist&id={target_place_id}&ans=yes")),
            QuickReplyButton(action=PostbackAction(label="不要，下次再看看", data=f"action=confirm_blacklist&id={target_place_id}&ans=no"))
        ])
        
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text=f"了解，因為「{user_msg}」。\n\n請問要將這家店加入黑名單（以後不再推薦）嗎？", quick_reply=quick_reply)
        )
        return
    
    # 一般流程
    is_old_user = user_service.check_user_exists(user_id)

    if loc:
        ai_result = chat_agent.analyze_chat_intent(user_msg)
        mode = ai_result.get("mode", "search")
        
        if mode == "chat":
            reply_text = ai_result.get("reply", "")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text, quick_reply=get_standard_quick_reply()))
            return

        extracted_tags = ai_result.get("tags", [])
        extracted_keyword = ai_result.get("keyword", "")
        search_term = extracted_keyword if extracted_keyword else (extracted_tags[0] if extracted_tags else "熱門")
        
        # 🔥 [修正]：把 AI 翻譯出來的第一個標準 Tag 抓出來，準備傳給後端！
        primary_tag = extracted_tags[0] if extracted_tags else None

        if not is_old_user:
            user_service.log_action(
                user_id, "INIT_PREF", "SYSTEM_INIT", 
                reason=None, user_msg=user_msg, ai_analysis=ai_result, 
                lat=lat, lng=lng,
                metadata={
                    "interaction_type": "text_message",
                    "ai_mode": ai_result.get("mode"),
                    "has_location": loc is not None
                }
            )
        else:
            user_service.log_action(
                user_id, "SEARCH", "SYSTEM_SEARCH", 
                reason=None, user_msg=user_msg, ai_analysis=ai_result, 
                lat=lat, lng=lng,
                metadata={
                    "interaction_type": "text_message",
                    "ai_mode": ai_result.get("mode"),
                    "has_location": loc is not None
                }
            )

        opening = ai_result.get("opening", "好的，正在幫您搜尋中...")
        closing = ai_result.get("closing", "希望這些店符合您的需求！")

        # 🔥 [修正]：明確加上 tag=primary_tag 參數，把 AI 的翻譯結果交給 Path B！
        asyncio.create_task(process_recommendation(
            event.reply_token, lat, lng, user_id, 
            tag=primary_tag, # 👈 關鍵修改就在這行
            user_query=search_term, 
            opening=opening, closing=closing
        ))
        return

    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="請先點擊下方按鈕分享位置，我才能幫您找附近的店喔！👇", quick_reply=get_standard_quick_reply()))

@handler.add(MessageEvent, message=LocationMessage)
def handle_location(event):
    lat, lng = event.message.latitude, event.message.longitude
    user_id = event.source.user_id
    
    user_service.update_user_location(user_id, lat, lng)

    if not user_service.check_user_exists(user_id):
        quick_reply = QuickReply(items=[
            QuickReplyButton(action=PostbackAction(label="📖 安靜讀書", data="action=onboarding&tag=安靜")),
            QuickReplyButton(action=PostbackAction(label="🗣️ 朋友聚會", data="action=onboarding&tag=熱鬧")),
            QuickReplyButton(action=PostbackAction(label="☕ 復古文青", data="action=onboarding&tag=復古")),
        ])
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="👋 初次見面！請問想找哪類咖啡廳？", quick_reply=quick_reply))
        return 

    asyncio.create_task(process_recommendation(event.reply_token, lat, lng, user_id=user_id))

@handler.add(PostbackEvent)
def handle_postback(event):
    user_id = event.source.user_id
    params = dict(item.split('=', 1) for item in event.postback.data.split('&') if '=' in item)
    action = params.get('action')
    shop_name = params.get('name', '這家店')
    
    loc = user_service.get_user_location(user_id)
    lat = loc['lat'] if loc else None
    lng = loc['lng'] if loc else None

    # ✨ 新增：處理「看完了」收起清單的動作
    if action == "close_list":
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text="OK！隨時可以再呼叫我找店喔 👇", quick_reply=get_standard_quick_reply())
        )
        return

    if action == "quick_tag":
        tag = params.get('tag')
        if loc:
            user_service.update_user_location(user_id, lat, lng, tag=tag)
            op, cl = get_button_reaction(tag)
            asyncio.create_task(process_recommendation(event.reply_token, lat, lng, user_id=user_id, tag=tag, opening=op, closing=cl))
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="請先傳送您的位置📍", quick_reply=get_standard_quick_reply()))
        return

    if action == "onboarding":
        tag = params.get('tag')
        user_service.log_action(user_id, "INIT_PREF", "SYSTEM_INIT", reason=tag, lat=lat, lng=lng)
        
        if not loc:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="📍 定位過期，請重新發送！", quick_reply=get_standard_quick_reply()))
            return
        
        user_service.update_user_location(user_id, lat, lng, tag=tag)
        op, cl = get_button_reaction(tag)
        asyncio.create_task(process_recommendation(event.reply_token, lat, lng, user_id=user_id, tag=tag, opening=op, closing=cl))
        return

    if action == "view_keep":
        show_user_list(event.reply_token, user_id, "bookmarks")
        return

    if action == "view_blacklist":
        show_user_list(event.reply_token, user_id, "blacklist")
        return
        
    if action == "remove_list":
        list_type = params.get('list')
        place_id = params.get('id')
        user_service.remove_from_list(user_id, list_type, place_id)
        
        list_name = "收藏" if list_type == "bookmarks" else "黑名單"
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text=f"✅ 已將該店從{list_name}移除！", quick_reply=get_list_view_quick_reply()) # 移除後依然保持清單按鈕
        )
        return
    

    if action == "confirm_blacklist":
        place_id = params.get('id')
        ans = params.get('ans')

        session_data = blacklist_sessions.get(user_id, {})
        negative_reason = session_data.get("reason")
            
        if user_id in blacklist_sessions:
            del blacklist_sessions[user_id]
            
        if ans == "yes":
            user_service.log_action(user_id, "NO", place_id, lat=lat, lng=lng)
            reply_text = "🚫 已加入黑名單！正在為您尋找其他更適合的店家... 🔄"
        else:
            reply_text = "👌 沒問題！正在為您尋找其他店家... 🔄"
                
        line_bot_api.push_message(user_id, TextSendMessage(text=reply_text))
        
        if loc:
            asyncio.create_task(process_recommendation(
                event.reply_token, loc['lat'], loc['lng'], user_id=user_id,
                rejected_place_id=place_id, 
                negative_reason=negative_reason 
            ))
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="請重新傳送位置📍", quick_reply=get_standard_quick_reply()))
        return

    place_id = params.get('id')
    
    if action == "yes":
        user_service.log_action(user_id, "YES", place_id, lat=lat, lng=lng)
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text=f"已記住您喜歡【{shop_name}】✨\n還想找其他的嗎？", quick_reply=get_standard_quick_reply())
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
            TextSendMessage(text=f"請問不喜歡【{shop_name}】的原因是？\n(可直接打字或選按鈕)", quick_reply=quick_reply)
        )
    elif action == "keep":
        user_service.log_action(user_id, "KEEP", place_id, lat=lat, lng=lng)
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text=f"已將【{shop_name}】加入收藏 ❤️\n要繼續找其他店家嗎？", quick_reply=get_standard_quick_reply())
        )
    elif params.get('reason'):
        if user_id in user_sessions: del user_sessions[user_id]
        reason = params.get('reason')
        place_id = params.get('id')
        
        # 1. 先記錄動作
        user_service.log_action(user_id, "NO_REASON", place_id, reason=reason, lat=lat, lng=lng)
        
        # 2. 先處理文字對應與判斷
        if reason == "change_only":
            reason_text = None
            msg_text = "了解，單純想換一家口味！\n\n請問要將這家店加入黑名單（以後不再推薦）嗎？"
        else:
            reason_map = {"expensive": "太貴了", "noisy": "環境太吵", "no_plug": "沒有插座"}
            reason_text = reason_map.get(reason, reason)
            msg_text = f"了解，因為「{reason_text}」。\n\n請問要將這家店加入黑名單（以後不再推薦）嗎？"

        # 3. 再存入 session (這時 reason_text 才會有值)
        blacklist_sessions[user_id] = {"place_id": place_id, "reason": reason_text}
        
        quick_reply = QuickReply(items=[
            QuickReplyButton(action=PostbackAction(label="要，加入黑名單", data=f"action=confirm_blacklist&id={place_id}&ans=yes")),
            QuickReplyButton(action=PostbackAction(label="不要，下次再看看", data=f"action=confirm_blacklist&id={place_id}&ans=no"))
        ])
        
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg_text, quick_reply=quick_reply))

@handler.add(FollowEvent)
def handle_follow(event):
    welcome_text = "嗨！我是 AI 咖啡助手 ☕\n請點擊下方按鈕分享位置，讓我為您推薦！👇"
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=welcome_text, quick_reply=get_standard_quick_reply()))