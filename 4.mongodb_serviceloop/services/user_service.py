# app/services/user_service.py
import logging
from datetime import datetime
from database import db_client
from utils import get_taiwan_now

logger = logging.getLogger("Coffee_Recommender")

class UserService:
    
    def get_user_location(self, user_id: str):
        """讀取使用者位置"""
        db = db_client.get_db()
        user = db['users'].find_one({"user_id": user_id})
        if user:
            return {"lat": user["lat"], "lng": user["lng"]}
        return None

    def update_user_location(self, user_id: str, lat: float, lng: float, tag: str = None):
        """更新使用者位置與偏好"""
        db = db_client.get_db()
        current_time = get_taiwan_now()
        
        update_data = {"lat": lat, "lng": lng, "updated_at": current_time}
        if tag:
            update_data["current_preference"] = tag
            
        db['users'].update_one(
            {"user_id": user_id},
            {"$set": update_data},
            upsert=True
        )

        log_entry = {
            "user_id": user_id, 
            "action": "UPDATE_LOCATION",
            "lat": lat, 
            "lng": lng, 
            "created_at": current_time,
            "metadata": {"source": "line_user_send"}
        }
        if tag: 
            log_entry["tag"] = tag
            
        db['interaction_logs'].insert_one(log_entry)
        logger.info(f"📍 [User Service] 位置更新成功: User={user_id}, Lat={lat}, Lng={lng}, Tag={tag}")

    def log_action(self, user_id: str, action: str, place_id: str = None, 
                   reason: str = None, user_msg: str = None, 
                   ai_analysis: dict = None, lat: float = None, lng: float = None,
                   metadata: dict = None): 
        """記錄使用者行為，並同步更新 users 表的收藏與黑名單"""
        db = db_client.get_db()
        
        doc = {
            "user_id": user_id, 
            "action": action, 
            "place_id": place_id,
            "reason": reason, 
            "user_msg": user_msg, 
            "ai_analysis": ai_analysis,
            "lat": lat, 
            "lng": lng, 
            "metadata": metadata, 
            "created_at_server": get_taiwan_now()
        }
        
        db['interaction_logs'].insert_one(doc)
        logger.info(f"📝 [User Log] Action={action}, User={user_id}, Place={place_id}, Reason={reason}")

        # ✨ 同步將 YES、NO 和 KEEP 存入 users 表格中 (使用 $addToSet 避免重複)
        if action in ["KEEP", "YES"] and place_id:
            db['users'].update_one(
                {"user_id": user_id},
                {"$addToSet": {"bookmarks": place_id}}, 
                upsert=True
            )
        elif action == "NO" and place_id: # 確認加入黑名單的才存入
            db['users'].update_one(
                {"user_id": user_id},
                {"$addToSet": {"blacklist": place_id}},
                upsert=True
            )

    def check_user_exists(self, user_id: str):
        """檢查是否為老手"""
        db = db_client.get_db()
        return db['interaction_logs'].find_one({"user_id": user_id}) is not None

    # ✨ 新增：取得使用者的特定清單 (bookmarks 或 blacklist)，並連同店家資訊一起撈出
    def get_user_places(self, user_id: str, list_type: str):
        db = db_client.get_db()
        user = db['users'].find_one({"user_id": user_id})
        
        if not user or list_type not in user or not user[list_type]:
            return []
        
        place_ids = user[list_type]
        # 從 cafes 表中撈出這些店家的詳細資訊
        cafes = list(db['cafes'].find({"place_id": {"$in": place_ids}}))
        return cafes

    # ✨ 新增：從清單中移除店家
    def remove_from_list(self, user_id: str, list_type: str, place_id: str):
        db = db_client.get_db()
        
        # 1. 從 users 表中移除
        db['users'].update_one(
            {"user_id": user_id},
            {"$pull": {list_type: place_id}}
        )
        
        # 2. 為了讓推薦系統正常運作，也同步刪除 logs 裡的紀錄
        if list_type == "blacklist":
            db['interaction_logs'].delete_many({"user_id": user_id, "action": "NO", "place_id": place_id})
        elif list_type == "bookmarks":
            db['interaction_logs'].delete_many({"user_id": user_id, "action": "KEEP", "place_id": place_id})

    # ==========================================
    # 🧠 新增：對話狀態 (RAM) 管理管線
    # ==========================================
    def get_user_state(self, user_id: str) -> dict:
        """獲取使用者的 RAM (購物車與歷史對話)"""
        db = db_client.get_db()
        user = db['users'].find_one({"user_id": user_id}) or {}
        return {
            "chat_window": user.get("chat_window", []),
            "search_cart": user.get("search_cart", []),
            "last_session_cart": user.get("last_session_cart", []),
            "last_updated_at": user.get("last_updated_at")
        }

    def update_user_state(self, user_id: str, chat_window: list, search_cart: list, last_session_cart: list):
        """更新 RAM 並刷新最後活動時間"""
        db = db_client.get_db()
        db['users'].update_one(
            {"user_id": user_id},
            {"$set": {
                "chat_window": chat_window,
                "search_cart": search_cart,
                "last_session_cart": last_session_cart,
                "last_updated_at": get_taiwan_now()  # 更新時間戳記
            }},
            upsert=True
        )

    def clear_user_cart(self, user_id: str):
        """【結帳清空】出菜成功後，清空當下狀態，但保留 last_updated_at"""
        db = db_client.get_db()
        db['users'].update_one(
            {"user_id": user_id},
            {"$set": {
                "chat_window": [],
                "search_cart": [],
                # last_session_cart 絕對不清空，要留著給跨日反問備用！
            }}
        )
        
# === services/user_service.py (加在最下方) ===

    def add_to_user_list(self, user_id: str, list_type: str, place_id: str):
        """將店家加入使用者的收藏(bookmarks)或永久黑名單(blacklist)陣列"""
        db = db_client.get_db()
        db['users'].update_one(
            {"user_id": user_id},
            {"$addToSet": {list_type: place_id}}, # $addToSet 可避免重複加入
            upsert=True
        )

    def get_behavior_data_for_analysis(self, user_id: str) -> dict:
        """收集使用者的所有足跡，準備餵給 AI 進行分析"""
        db = db_client.get_db()
        
        # 1. 搜尋紀錄
        logs = list(db['interaction_logs'].find(
            {"user_id": user_id, "action": {"$in": ["SEARCH", "INIT_PREF", "YES"]}}
        ).sort("created_at_server", -1).limit(10))
        recent_searches = [log.get("user_msg") for log in logs if log.get("user_msg")]

        # 2. 拒絕原因
        reject_logs = list(db['interaction_logs'].find(
            {"user_id": user_id, "action": {"$in": ["NO", "NO_REASON"]}}
        ).sort("created_at_server", -1).limit(10))
        dislikes = [{"rejected_reason": rl.get("reason")} for rl in reject_logs if rl.get("reason")]

        # 3. 收藏清單特徵
        user_info = db['users'].find_one({"user_id": user_id}) or {}
        bookmarks = user_info.get("bookmarks", [])
        bookmarked_features = []
        if bookmarks:
            fav_cafes = list(db['cafes'].find({"place_id": {"$in": bookmarks[:5]}}))
            for fc in fav_cafes:
                tags = [t.get("tag", "") for t in fc.get("ai_tags", []) if isinstance(t, dict)]
                if tags: bookmarked_features.extend(tags[:3])

        return {
            "recent_search_queries": recent_searches,
            "rejected_features_or_reasons": dislikes,
            "frequently_bookmarked_tags": list(set(bookmarked_features))
        }

    def save_user_persona(self, user_id: str, persona_data: dict):
        """將 AI 分析出來的 Persona 存回 users 表格中"""
        if not persona_data: return
        db = db_client.get_db()
        db['users'].update_one(
            {"user_id": user_id},
            {"$set": {"ai_persona": persona_data}},
            upsert=True
        )