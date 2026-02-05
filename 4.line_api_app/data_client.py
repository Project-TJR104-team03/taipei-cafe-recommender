import os
import requests
import logging

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 從環境變數讀取後端網址
BACKEND_API_URL = os.getenv('BACKEND_API_URL', "https://ossicular-gustily-elyse.ngrok-free.dev")

class DataClient:
    """
    負責與後端資料庫通訊的客戶端。
    【功能】包含「本地暫存 (Mock)」，當後端 API 沒實作或連不上時，
    會自動切換成使用本地記憶體，確保機器人不會壞掉。
    """
    
    # 這是給開發測試用的「本地暫存區」
    _local_mock_db = {
        "locations": {}, # 存 user_id: {lat, lng}
        "profiles": []   # 存 user_id (代表老手)
    }

    @classmethod
    def get_user_location(cls, user_id):
        """嘗試從後端讀取，失敗則讀取本地暫存"""
        try:
            # 嘗試連線 Role 4 的 API
            url = f"{BACKEND_API_URL}/users/{user_id}/location"
            resp = requests.get(url, timeout=2) 
            
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.warning(f"⚠️ 連線後端失敗 (get_location): {e}")
        
        # --- 備援機制：如果 API 壞掉或沒做，讀取本地記憶體 ---
        logger.info(f"使用本地暫存讀取位置: {user_id}")
        return cls._local_mock_db["locations"].get(user_id)

    @classmethod
    def save_user_location(cls, user_id, lat, lng):
        """嘗試寫入後端，同時寫入本地暫存"""
        # 1. 無論如何，先存一份在本地 (這是你的保命符)
        cls._local_mock_db["locations"][user_id] = {"lat": lat, "lng": lng}
        
        try:
            # 2. 順便試著傳給後端 (如果他有做的話)
            url = f"{BACKEND_API_URL}/users/{user_id}/location"
            payload = {"lat": lat, "lng": lng}
            requests.post(url, json=payload, timeout=2)
            logger.info(f"嘗試同步位置到後端: {user_id}")
        except Exception as e:
            logger.warning(f"⚠️ 無法同步位置到後端 (沒關係，本地已存): {e}")

    @classmethod
    def check_user_exists(cls, user_id):
        """嘗試檢查後端，失敗則檢查本地暫存"""
        try:
            url = f"{BACKEND_API_URL}/users/{user_id}/profile"
            resp = requests.get(url, timeout=2)
            if resp.status_code == 200:
                return True
        except Exception:
            pass 
            
        # 備援：檢查本地紀錄
        return user_id in cls._local_mock_db["profiles"]

    @classmethod
    def save_feedback(cls, user_id, action, cafe_id, reason=None):
        """嘗試傳送回饋"""
        # 標記為老手
        if action == "INIT_PREF":
            if user_id not in cls._local_mock_db["profiles"]:
                cls._local_mock_db["profiles"].append(user_id)

        try:
            url = f"{BACKEND_API_URL}/log_user_action"
            payload = {
                "user_id": user_id,
                "action": action,
                "cafe_id": cafe_id,
                "reason": reason
            }
            requests.post(url, json=payload, timeout=10)
            logger.info(f"回饋已傳送 (API): {action}")
        except Exception as e:
            logger.error(f"API 連線錯誤 (save_feedback): {e}")