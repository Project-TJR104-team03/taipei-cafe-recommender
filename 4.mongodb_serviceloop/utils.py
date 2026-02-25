# app/utils.py
from datetime import datetime
import logging
# 注意這裡的引用路徑
from app.locations import ALL_LOCATIONS 

logger = logging.getLogger("Coffee_Recommender")

def is_google_period_open(periods: list, target_dt: datetime) -> bool:
    """
    依照自定義的「總分鐘數」格式檢查是否營業
    """
    if not periods or not isinstance(periods, list):
        return False

    target_minutes = target_dt.hour * 60 + target_dt.minute
    google_target_day = (target_dt.weekday() + 1) % 7

    for period in periods:
        if not isinstance(period, dict):
            continue
            
        day = period.get('day')
        open_minutes = period.get('open')
        close_minutes = period.get('close')

        if day is None or open_minutes is None or close_minutes is None:
            continue

        # 只要找今天的資料
        if day == google_target_day:
            # 排除 open:0, close:0 這種代表「半夜00:00準時打烊」的殘留資料
            if open_minutes == 0 and close_minutes == 0:
                if target_minutes == 0: # 除非你剛好在 00:00 搜尋
                    return True
                continue
                
            # 🔥 終極核心：只要當下時間落在 open 和 close 之間，就是有營業！
            if open_minutes <= target_minutes <= close_minutes:
                return True

    return False

def get_coordinates_locally(user_text: str):
    """
    從本地字典查找座標 (Role 4 功能)
    """
    if not user_text:
        return None
        
    if user_text in ALL_LOCATIONS:
        return ALL_LOCATIONS[user_text]
        
    sorted_keys = sorted(ALL_LOCATIONS.keys(), key=len, reverse=True)
    for loc_name in sorted_keys:
        if loc_name in user_text:
            logger.info(f"🎯 本地查表成功！關鍵字: {loc_name}")
            return ALL_LOCATIONS[loc_name]
    return None