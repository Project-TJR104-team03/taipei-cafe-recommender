# app/services/user_service.py
import logging
from datetime import datetime
from app.database import db_client

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
        current_time = datetime.now()
        
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
            "created_at_server": datetime.now()
        }
        
        db['interaction_logs'].insert_one(doc)
        logger.info(f"📝 [User Log] Action={action}, User={user_id}, Place={place_id}, Reason={reason}")

        # ✨ 新增：同步將 KEEP 和 NO 存入 users 表格中 (使用 $addToSet 避免重複)
        if action == "KEEP" and place_id:
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