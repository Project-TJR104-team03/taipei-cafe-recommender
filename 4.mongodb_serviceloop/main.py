# app/main.py
import os
import random
import logging
import asyncio
import re
from pathlib import Path
from contextlib import asynccontextmanager
from urllib.parse import quote

from fastapi import FastAPI, Request, Header, HTTPException
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError, LineBotApiError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, 
    LocationMessage, FlexSendMessage, PostbackEvent,
    QuickReply, QuickReplyButton, PostbackAction,
    FollowEvent
)
from dotenv import load_dotenv
import vertexai

# 🔥 處理時間狀態
from datetime import datetime, timedelta

# 引入自定義模組
from database import db_client
from services.recommend_service import RecommendService
from services.user_service import UserService
from agents.chat_agent import ChatAgent
from agents.preference_agent import PreferenceAgent

# --- 強制抓取 .env ---
current_file_path = Path(__file__).resolve()
env_path = current_file_path.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# --- 初始化 ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Coffee_Recommender")

# ✨ [全域初始化] 啟動 Vertex AI 企業級引擎
GCP_PROJECT = os.getenv("GCP_PROJECT_ID")
GCP_LOCATION = os.getenv("GCP_LOCATION", "us-central1")
vertexai.init(project=GCP_PROJECT, location=GCP_LOCATION)
logger.info(f"✅ 全域 Vertex AI 初始化完成 (Project: {GCP_PROJECT}, Location: {GCP_LOCATION})")

@asynccontextmanager
async def lifespan(app: FastAPI):
    db_client.connect()
    yield
    db_client.close()

app = FastAPI(lifespan=lifespan)

line_bot_api = LineBotApi(os.getenv('LINE_CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.getenv('LINE_CHANNEL_SECRET'))

recommend_service = RecommendService()
user_service = UserService()
chat_agent = ChatAgent()
preference_agent = PreferenceAgent()

user_sessions = {}
blacklist_sessions = {} 
pending_search_sessions = {}  # 新增：紀錄「尚未定位」的待辦搜尋

# --- 分類 10 標籤：按鈕翻譯字典 ---
MACRO_TAG_MAPPING = {
    "絕對不限時": "不限時",
    "插座筆電族": "插座,工作友善",
    "安靜好讀書": "安靜",
    "復古老宅風": "老宅,復古",
    "質感文青風": "文青,韓系風格",
    "個性工業風": "工業風格",
    "甜點下午茶": "甜點",
    "職人手沖店": "手沖精品,自家烘焙",
    "深夜夜貓族": "深夜",
    "有毛孩療癒": "寵物友善,店貓,店狗"
}

# --- 輔助函式 ---
def get_standard_quick_reply():
    return QuickReply(items=[
        QuickReplyButton(action={"type": "location", "label": "📍 點我找附近的店"}),
        QuickReplyButton(action=PostbackAction(label="🏷️ 依情境找店", data="action=explore")),
        QuickReplyButton(action=PostbackAction(label="📂 我的收藏清單", data="action=view_keep")),
        QuickReplyButton(action=PostbackAction(label="🚫 我的黑名單", data="action=view_blacklist"))        
    ])

# ✨ 新增：查看清單時「專用」的快捷按鈕 (多了一顆看完了)
def get_list_view_quick_reply():
    return QuickReply(items=[
        QuickReplyButton(action=PostbackAction(label="👀 看完了，繼續找店", data="action=close_list")),
        QuickReplyButton(action={"type": "location", "label": "📍 點我找附近的店"}),
        QuickReplyButton(action=PostbackAction(label="🏷️ 依情境找店", data="action=explore")),
        QuickReplyButton(action=PostbackAction(label="📂 我的收藏清單", data="action=view_keep")),
        QuickReplyButton(action=PostbackAction(label="🚫 我的黑名單", data="action=view_blacklist"))        
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

# ✨ [純淨版] 推薦理由濾水器 (只負責排版與防護，不破壞字串)
def clean_summary_text(text):
    if not text: return ""
    core = text.strip()
    
    # 1. 強制過濾掉括號註解
    core = re.sub(r"\(.*?\)|（.*?）", "", core)
    core = core.strip(" 。-")
    
    # 2. 智能斷句防護 (放寬到 36 字，完美利用 LINE 卡片兩行空間)
    if len(core) > 36:
        last_period = max(core.rfind("。", 0, 36), core.rfind("！", 0, 36))
        last_comma = max(core.rfind("，", 0, 36), core.rfind("、", 0, 36))
        
        if last_period > 5:
            core = core[:last_period]
        elif last_comma > 5: 
            core = core[:last_comma]
        else:
            core = core[:34] + "..."
            
    return core

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
    
    if opening_hours.get("is_24_hours", False):
        return "24 小時營業", "#00B900"
    
    def parse_time(val):
        if val is None: return None
        v = int(val)
        if v > 1440 and v != 2359: return (v // 100) * 60 + (v % 100)
        if v == 2359: return 1439
        return v
    
    week_periods = []
    for p in periods:
        open_day = int(p.get("day", 0))
        open_time = parse_time(p.get("open", 0))
        close_time = parse_time(p.get("close"))

        if close_time is None:
            if open_time == 0: return "24 小時營業", "#00B900"
            continue
            
        start_mins = open_day * 24 * 60 + open_time
        close_day = open_day
        if close_time < open_time:
            close_day = (open_day + 1) % 7
            
        end_mins = close_day * 24 * 60 + close_time
        if end_mins < start_mins: end_mins += 7 * 24 * 60
        week_periods.append([start_mins, end_mins])

    # 排序並合併相連的營業時間 (例如 23:59 接著 00:00)
    week_periods.sort(key=lambda x: x[0])
    merged = []
    for p in week_periods:
        if not merged:
            merged.append(p)
        else:
            last_start, last_end = merged[-1]
            if p[0] <= last_end + 1: # 容差 1 分鐘，完美接軌
                merged[-1][1] = max(last_end, p[1])
            else:
                merged.append(p)

    # 處理週末跨週一的循環
    if merged and merged[-1][1] >= 7 * 24 * 60:
        overflow = merged[-1][1] - 7 * 24 * 60
        if merged[0][0] <= overflow + 1:
            merged[0][0] = merged[-1][0] - 7 * 24 * 60
            merged[-1][1] = merged[0][1] + 7 * 24 * 60

    tw_now = datetime.utcnow() + timedelta(hours=8)
    current_iso = tw_now.isoweekday()
    current_day = 0 if current_iso == 7 else current_iso
    current_mins = current_day * 24 * 60 + tw_now.hour * 60 + tw_now.minute

    min_diff = float('inf')
    next_open_info = None
    day_map = {0: "週日", 1: "週一", 2: "週二", 3: "週三", 4: "週四", 5: "週五", 6: "週六"}

    for start_abs, end_abs in merged:
        for offset in [0, -7 * 24 * 60, 7 * 24 * 60]:
            c_mins = current_mins + offset
            if start_abs <= c_mins < end_abs:
                close_day = (end_abs // (24 * 60)) % 7
                close_time = end_abs % (24 * 60)
                close_hour, close_min = close_time // 60, close_time % 60
                
                if close_time >= 1439: close_str = "23:59"
                else: close_str = f"{close_hour:02d}:{close_min:02d}"
                    
                if close_day != current_day:
                    return f"營業至明日 {close_str}", "#00B900"
                return f"營業至 {close_str}", "#00B900"

            diff = start_abs - c_mins
            if 0 < diff < min_diff:
                min_diff = diff
                open_day = (start_abs // (24 * 60)) % 7
                open_time = start_abs % (24 * 60)
                open_hour, open_min = open_time // 60, open_time % 60
                day_str = "明日" if open_day == (current_day + 1) % 7 else day_map[open_day]
                next_open_info = f"下次營業 {day_str} {open_hour:02d}:{open_min:02d}"

    if next_open_info: return next_open_info, "#f56565"
    return "今日未營業", "#999999"

# ✨ 修改：發送 4 大情境懶人包卡片
def send_explore_categories(reply_token):
    def create_theme_card(title, img_url, theme_val):
        return {
            "type": "bubble", "size": "kilo", 
            "body": {
                "type": "box", "layout": "vertical", "paddingAll": "0px", 
                "action": {"type": "postback", "data": f"action=theme_search&theme={theme_val}"},
                "contents": [
                    {"type": "image", "url": img_url, "size": "full", "aspectMode": "cover", "aspectRatio": "4:3"},
                    {
                        "type": "box", "layout": "vertical", "position": "absolute", "backgroundColor": "#00000066", 
                        "width": "100%", "height": "100%", "alignItems": "center", "justifyContent": "center",
                        "contents": [{"type": "text", "text": title, "color": "#ffffff", "weight": "bold", "size": "lg"}]
                    }
                ]
            }
        }

    bubbles = [
        create_theme_card("💻 適合辦公", "https://images.unsplash.com/photo-1498050108023-c5249f4df085?w=500", "workspace"),
        create_theme_card("🍷 質感約會", "https://images.unsplash.com/photo-1517248135467-4c7edcad34c4?w=500", "dating"),
        create_theme_card("🐾 毛孩同樂", "https://images.unsplash.com/photo-1543852786-1cf6624b9987?w=500", "pet_friendly"),
        create_theme_card("🎧 獨處放鬆", "https://images.unsplash.com/photo-1445116572660-236099ec97a0?w=500", "relax")
    ]

    flex_message = FlexSendMessage(alt_text="四大情境探索", contents={"type": "carousel", "contents": bubbles})
    line_bot_api.reply_message(reply_token, flex_message)

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
        map_url = db_map_url if db_map_url else f"https://www.google.com/maps/search/?api=1&query={quote(original_name)}&query_place_id={place_id}"
        
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
async def process_recommendation(reply_token, lat, lng, user_id, tag=None, user_query=None, opening=None, closing=None, rejected_place_id=None, negative_reason=None, theme=None):
   result = await recommend_service.recommend(
        lat=lat, lng=lng, user_id=user_id, 
        user_query=user_query, 
        cafe_tag=tag,
        rejected_place_id=rejected_place_id,
        negative_reason=negative_reason,
        theme=theme
    )
   cafe_list = result.get("data", [])

   # 如果 recommend_service 有回傳它實際使用的中心點座標，我們就更新使用者的定位！
   new_lat = result.get("center_lat")
   new_lng = result.get("center_lng")
   if new_lat and new_lng and (new_lat != lat or new_lng != lng):
        # 將使用者的定位更新為龍山寺 (或其他地點)，這樣下一回合就會從這裡開始搜！
        user_service.update_user_location(user_id, new_lat, new_lng)
        print(f"📍 [狀態更新] 已將使用者 {user_id} 的錨點固定至 ({new_lat}, {new_lng})")

   # 🌟 [新增] 全面防呆攔截機制 (取代原本的 Mock 測試資料)
   if not cafe_list:
            from linebot.models import MessageAction
            
            if result.get("is_midnight_empty"):
                # 情境 A：深夜全關門 (22:00 - 06:00)
                logger.warning("🌙 觸發深夜防呆，回傳反問訊息")
                reply_text = (
                    "🌙 目前這個時間，附近的咖啡廳大多沒有營業歐！\n\n"
                    "還是您想先找其他時間要去的店呢？\n"
                    "(可以直接跟我說，例如：『改找明天下午』☕)\n\n"
                    "如果現在一定要去，建議您可以點擊下方『📍重新定位』，換到比較熱鬧的市區找找看深夜咖啡廳喔！"
                )

            else:
                # 情境 B：太偏僻或條件太嚴苛 (單純找不到店)
                logger.warning("🏜️ 查無店家，觸發一般防呆機制")
                # 動態抓取使用者當下的條件 (過濾掉系統預設的"熱門")
                current_condition = user_query if user_query and user_query != "熱門" else (tag if tag else "您的條件")
                reply_text = (
                    f"😅 哎呀，附近找不到完全符合【 {current_condition} 】的咖啡廳...\n\n"
                    "可能是附近剛好沒有店家，或是條件太嚴格了。您可以直接打字跟我說：\n"
                    "👉 『換去中山站』 (轉移陣地)\n"
                    "👉 『那只要不限時就好』 (減少條件)\n"
                    "👉 『重置』 (全部清空重來)\n\n"
                    "或是點擊下方按鈕換個地點找找看👇"
                )
                
            try:
                line_bot_api.reply_message(
                    reply_token,
                    TextSendMessage(text=reply_text, quick_reply=get_standard_quick_reply())
                )
            except LineBotApiError as e:
                logger.warning(f"⚠️ 傳送失敗: {e.message}")
                
            return # 🌟 找不到店就直接 return，終止後續的出菜流程！    
    
    # 🌟 [新增] 依據預設情境權重 (SCENARIO_CONFIG) 組合專屬開場白
   if theme and not opening:
        theme_names = {"workspace": "適合辦公", "dating": "質感約會", "pet_friendly": "毛孩同樂", "relax": "獨處放鬆"}
        theme_zh = theme_names.get(theme, "專屬")
        
        # 1. 寫入你的場景權重配置
        SCENARIO_CONFIG = {
            "適合辦公": {"has_plug": 0.35, "time_flexibility_score": 0.25, "has_wifi": 0.20, "is_work_friendly": 0.15, "is_quiet": 0.05},
            "質感約會": {"has_dessert": 0.30, "service_quality_score": 0.25, "hipster_style": 0.15, "retro": 0.15, "is_old_house": 0.10, "can_reserve": 0.05},
            "毛孩同樂": {"is_pet_friendly": 0.5, "has_shop_cat": 0.2, "has_shop_dog": 0.2, "has_outdoor_seating": 0.1},
            "獨處放鬆": {"is_quiet": 0.4, "service_quality_score": 0.2, "time_flexibility_score": 0.2, "is_specialty_coffee": 0.2}
        }
        
        # 2. 建立白話文翻譯字典
        TRANSLATION_MAP = {
            "has_plug": "提供插座",
            "time_flexibility_score": "不限時",
            "has_wifi": "穩定 Wi-Fi",
            "is_work_friendly": "工作友善",
            "is_quiet": "安靜舒適",
            "has_dessert": "美味甜點",
            "service_quality_score": "服務佳",
            "hipster_style": "質感文青",
            "retro": "復古氛圍",
            "is_old_house": "特色老宅",
            "can_reserve": "可預約",
            "is_pet_friendly": "寵物友善",
            "has_shop_cat": "有可愛店寵",
            "has_shop_dog": "有可愛店寵",
            "has_outdoor_seating": "戶外座位",
            "is_specialty_coffee": "職人手沖"
        }
        
        # 3. 抓取權重最高的前 2 名特徵
        if theme_zh in SCENARIO_CONFIG:
            features_dict = SCENARIO_CONFIG[theme_zh]
            # 依據權重分數 (value) 由大到小排序
            sorted_features = sorted(features_dict.items(), key=lambda x: x[1], reverse=True)
            
            unique_features = []
            for feat_key, _ in sorted_features:
                zh_name = TRANSLATION_MAP.get(feat_key, "環境舒適")
                # 確保翻譯出來的詞沒有重複，才加進去
                if zh_name not in unique_features:
                    unique_features.append(zh_name)
                # 只要收集滿 2 個不一樣的特徵就可以停止了
                if len(unique_features) == 2:
                    break
                    
            f1 = unique_features[0] if len(unique_features) > 0 else "環境舒適"
            f2 = unique_features[1] if len(unique_features) > 1 else "評價優良"
        else:
            f1, f2 = "環境舒適", "評價優良" # 防呆保底
            
        # 4. 多樣化句型隨機套用
        templates = [
            f"收到！為您尋找主打「{f1}」與「{f2}」的最高分【{theme_zh}】神店... 🚀",
            f"沒問題！馬上幫您鎖定具備「{f1}」且「{f2}」的【{theme_zh}】好去處... ☕",
            f"懂你想找【{theme_zh}】！立刻特搜幾家「{f1}、{f2}」的最高分名單... ⚡"
        ]
        opening = random.choice(templates)

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

        raw_reason = cafe.get('custom_reason', '') 
        
        summary_text = clean_summary_text(raw_reason)
        
        contact_info = cafe.get("contact", {})
        
        contact_info = cafe.get("contact", {})
        db_map_url = contact_info.get("google_maps_url")
        map_url = db_map_url if db_map_url else f"https://www.google.com/maps/search/?api=1&query={quote(original_name)}&query_place_id={place_id}"
        
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
                    "text": f"🏷️ {' · '.join(display_tags)}", # 🚀 統一在最前面加上一個俐落的標籤符號！
                    "size": "xs", 
                    "color": "#888888", 
                    "wrap": True, 
                    "margin": "sm"
                }
            )

        # ✨ 新增：如果這家店有 summary，就把它加在標籤下面
        if summary_text:
            info_box_contents.append(
                {
                    "type": "text",
                    "text": f"💡 {summary_text}",
                    "size": "sm",            # 🔼 從 xxs 放大到 sm (跟上面的距離文字一樣大)
                    "color": "#555555",      # 顏色稍微調深一點點，增加易讀性
                    "wrap": True,            
                    "maxLines": 2,           # 🔽 既然文字變精簡了，最多顯示兩行即可
                    "margin": "md"           
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
        
   
   # 🛡️ 加上防護罩：即使 Token 過期或重複，也不會讓伺服器崩潰
   try:
       line_bot_api.reply_message(reply_token, reply_payload)
   except LineBotApiError as e:
       logger.warning(f"⚠️ 傳送失敗 (Reply Token 已失效或被重複使用): {e.message}")

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
    # 🚀 秘訣：收到訊息瞬間，立刻把所有沉重的工作丟給背景執行！
    # 這樣主程式就能瞬間結束，立刻回傳 200 OK 給 LINE，徹底阻止 LINE 啟動「超時重試」機制
    asyncio.create_task(background_handle_text(event))

async def background_handle_text(event):
    user_msg = event.message.text
    user_id = event.source.user_id

    if user_msg == "重置":
        if user_id in user_sessions: del user_sessions[user_id]
        try:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="🔄 對話狀態已重置。", quick_reply=get_standard_quick_reply()))
        except: pass
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
        
        try:
            line_bot_api.reply_message(
                event.reply_token, 
                TextSendMessage(text=f"了解，因為「{user_msg}」。\n\n請問要將這家店加入黑名單（以後不再推薦）嗎？", quick_reply=quick_reply)
            )
        except: pass
        return
    
    # 一般流程
    is_old_user = user_service.check_user_exists(user_id)

    if loc:
        # 🧠 1. 喚醒記憶：獲取使用者 RAM
        user_state = user_service.get_user_state(user_id)
        chat_window = user_state.get("chat_window", [])
        current_cart = user_state.get("search_cart", [])
        last_session_cart = user_state.get("last_session_cart", [])
        last_updated = user_state.get("last_updated_at")

        # ⏳ 2. 逾時檢查 (Timeout Reset)：超過 4 小時自動移至備份車
        if last_updated:
            if datetime.now() - last_updated > timedelta(minutes=30):
                if current_cart:
                    last_session_cart = current_cart # 備份昨日條件
                current_cart = []
                chat_window = []

        # 🤖 3. 呼叫終極大腦
        ai_result = chat_agent.manage_dialogue_and_cart(
            user_msg=user_msg,
            chat_window=chat_window,
            current_cart=current_cart,
            last_session_cart=last_session_cart
        )

        mode = ai_result.get("mode", "search")
        reply_text = ai_result.get("reply", "")
        updated_cart = ai_result.get("updated_cart", [])

        # 📝 4. 更新滑動視窗 (保留最近 6 句話的輕量陣列)
        chat_window.append(f"User: {user_msg}")
        if mode == "chat" and reply_text:
            chat_window.append(f"AI: {reply_text}")
        if len(chat_window) > 6:
            chat_window = chat_window[-6:] # 把太舊的擠掉

        # 💾 5. 將最新狀態存回 RAM
        user_service.update_user_state(user_id, chat_window, updated_cart, last_session_cart)

        # 💬 6. 執行分支：如果是純聊天或隔夜反問，直接回覆並結束
        if mode == "chat":
            try:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text, quick_reply=get_standard_quick_reply()))
            except: pass
            return

        # 🔎 7. 執行分支：進入 Search 模式
        extracted_keyword = ai_result.get("keyword", "")
        search_term = extracted_keyword if extracted_keyword else " ".join(updated_cart)
        if not search_term.strip(): search_term = "熱門"

        extracted_tags = ai_result.get("tags", [])
        primary_tag = extracted_tags[0] if extracted_tags else None

        # 紀錄 Log
        log_action = "SEARCH" if is_old_user else "INIT_PREF"
        user_service.log_action(user_id, log_action, "SYSTEM_SEARCH", reason=None, user_msg=user_msg, ai_analysis=ai_result, lat=lat, lng=lng, metadata={"interaction_type": "text_message", "ai_mode": mode, "has_location": True})

        opening = ai_result.get("opening", "好的，正在為您特搜中...")
        closing = ai_result.get("closing", "希望這幾家店符合您的期待！")

        # ⚡ 執行推薦與出菜 (💡 注意：我們已經取消後端強制清空購物車，全權交給 AI 決定了！)
        await process_recommendation(
            event.reply_token, lat, lng, user_id, 
            tag=primary_tag, 
            user_query=search_term, 
            opening=opening, closing=closing
        )
        return

    try:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="請先點擊下方按鈕分享位置，我才能幫您找附近的店喔！👇", quick_reply=get_standard_quick_reply()))
    except: pass


@handler.add(MessageEvent, message=LocationMessage)
def handle_location(event):
    lat, lng = event.message.latitude, event.message.longitude
    user_id = event.source.user_id
    
    user_service.update_user_location(user_id, lat, lng)

    # 檢查是否有「暫存的搜尋需求」
    if user_id in pending_search_sessions:
        ui_tag = pending_search_sessions.pop(user_id) # 取出並清除暫存
        mapped_tag = MACRO_TAG_MAPPING.get(ui_tag, ui_tag) # 翻譯為底層標籤
        
        op, cl = get_button_reaction(ui_tag)
        asyncio.create_task(process_recommendation(event.reply_token, lat, lng, user_id=user_id, tag=mapped_tag, opening=op, closing=cl))
        return

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

    # ✨ 新增：呼叫 4 大分類探索卡片
    if action == "explore":
        send_explore_categories(event.reply_token)
        return
    
    # ✨ 新增：處理情境懶人包的點擊
    if action == "theme_search":
        theme = params.get('theme')
        theme_names = {"workspace": "適合辦公", "dating": "質感約會", "pet_friendly": "毛孩同樂", "relax": "獨處放鬆"}
        
        if loc:
            # 🔽 移除固定字串，直接呼叫核心流程，讓它自己「看完店家再來決定要說什麼」
            user_service.update_user_state(user_id, [], [theme_names.get(theme, "")], [])
            asyncio.create_task(process_recommendation(event.reply_token, lat, lng, user_id=user_id, theme=theme))
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="📍 請先分享位置，我才能幫您找附近的店喔！", quick_reply=get_standard_quick_reply()))
        return
    
    # ✨ 新增：處理情境懶人包的點擊
    if action == "theme_search":
        theme = params.get('theme')
        theme_names = {"workspace": "適合辦公", "dating": "質感約會", "pet_friendly": "毛孩同樂", "relax": "獨處放鬆"}
        
        if loc:
            op_msg = f"收到！馬上為您尋找最高分的「{theme_names.get(theme, '專屬')}」神店... 🚀"
            user_service.update_user_state(user_id, [], [theme_names.get(theme, "")], [])
            asyncio.create_task(process_recommendation(event.reply_token, lat, lng, user_id=user_id, theme=theme, opening=op_msg))
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="📍 請先分享位置，我才能幫您找附近的店喔！", quick_reply=get_standard_quick_reply()))
        return

    if action == "quick_tag":
        ui_tag = params.get('tag')
        mapped_tag = MACRO_TAG_MAPPING.get(ui_tag, ui_tag)
        if loc:
            user_service.update_user_location(user_id, lat, lng, tag=mapped_tag)
            op, cl = get_button_reaction(ui_tag)
            user_service.update_user_state(user_id, [], [ui_tag], [])
            asyncio.create_task(process_recommendation(event.reply_token, lat, lng, user_id=user_id, tag=mapped_tag, opening=op, closing=cl))
        else:
            pending_search_sessions[user_id] = ui_tag
            line_bot_api.reply_message(
                event.reply_token, 
                TextSendMessage(text=f"收到！你想找「{ui_tag}」對吧？\n請點擊下方 📍 點我找附近的店，我馬上幫你找！", quick_reply=get_standard_quick_reply())
            )
        return

    if action == "onboarding":
        tag = params.get('tag')
        user_service.log_action(user_id, "INIT_PREF", "SYSTEM_INIT", reason=tag, lat=lat, lng=lng)
        
        if not loc:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="📍 定位過期，請重新發送！", quick_reply=get_standard_quick_reply()))
            return
        
        user_service.update_user_location(user_id, lat, lng, tag=tag)
        op, cl = get_button_reaction(tag)
        user_service.update_user_state(user_id, [], [tag], [])
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
    

    elif action == "confirm_blacklist":
        place_id = params.get('id')
        ans = params.get('ans')

        session_data = blacklist_sessions.get(user_id, {})
        negative_reason = session_data.get("reason")
        if user_id in blacklist_sessions: del blacklist_sessions[user_id]
            
        if ans == "yes":
            user_service.log_action(user_id, "NO", place_id, lat=lat, lng=lng)
            user_service.add_to_user_list(user_id, "blacklist", place_id) # 寫入永久黑名單陣列
            reply_text = "🚫 已加入永久黑名單！正在為您尋找其他更適合的店家... 🔄"
            # 🌟 雙軌機制 3：確認加入永久黑名單，觸發 AI 學習地雷
            asyncio.create_task(background_update_persona(user_id))
        else:
            reply_text = "👌 沒問題！48小時後會再次解鎖。正在為您尋找其他店家... 🔄"
                
        line_bot_api.push_message(user_id, TextSendMessage(text=reply_text))
        
        if loc:
            asyncio.create_task(process_recommendation(
                event.reply_token, loc['lat'], loc['lng'], user_id=user_id,
                rejected_place_id=place_id, negative_reason=negative_reason 
            ))
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="請重新傳送位置📍", quick_reply=get_standard_quick_reply()))
        return

    place_id = params.get('id')
    
    if action == "yes":
        user_service.log_action(user_id, "YES", place_id, lat=lat, lng=lng)
        asyncio.create_task(background_update_persona(user_id))
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text=f"已記住您喜歡【{shop_name}】✨\n還想找其他的嗎？", quick_reply=get_standard_quick_reply())
        )
    elif action == "no":
        user_sessions[user_id] = place_id
        # 🌟 雙軌機制 1：即刻冷卻！使用者按「不行」，馬上寫入 COOLDOWN 讓他 48 小時內消失
        user_service.log_action(user_id, "COOLDOWN", place_id, lat=lat, lng=lng)
        
        quick_reply = QuickReply(items=[
            QuickReplyButton(action=PostbackAction(label="太貴了", data=f"reason=expensive&id={place_id}")),
            QuickReplyButton(action=PostbackAction(label="環境太吵", data=f"reason=noisy&id={place_id}")),
            QuickReplyButton(action=PostbackAction(label="沒有插座", data=f"reason=no_plug&id={place_id}")),
            QuickReplyButton(action=PostbackAction(label="單純不想去", data=f"reason=change_only&id={place_id}")),
        ])
        line_bot_api.reply_message(
            event.reply_token, 
            TextSendMessage(text=f"請問不喜歡【{shop_name}】的原因是？\n(此店已為您暫時隱藏 48 小時 🕒)", quick_reply=quick_reply)
        )
        
    elif action == "keep":
        user_service.log_action(user_id, "KEEP", place_id, lat=lat, lng=lng)
        user_service.add_to_user_list(user_id, "bookmarks", place_id) # 寫入資料庫陣列
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"已將【{shop_name}】加入收藏 ❤️\n要繼續找其他店家嗎？", quick_reply=get_standard_quick_reply()))
        # 🌟 雙軌機制 2：加入收藏，觸發 AI 分析喜好
        asyncio.create_task(background_update_persona(user_id))
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
    
    
# === 加在 main.py 的最下面 ===
async def background_update_persona(user_id: str):
    """背景執行：收集資料 -> 呼叫 PreferenceAgent -> 存入資料庫"""
    logger.info(f"🕵️ 啟動背景偏好分析任務 (User: {user_id})...")
    behavior_data = user_service.get_behavior_data_for_analysis(user_id)
    
    if behavior_data["frequently_bookmarked_tags"] or behavior_data["rejected_features_or_reasons"]:
        persona_data = await preference_agent.analyze_user_preferences(behavior_data)
        user_service.save_user_persona(user_id, persona_data)