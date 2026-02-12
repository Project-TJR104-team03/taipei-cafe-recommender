# tag_config.py

# 第一層：類別映射 (Category Mapping)
CAT_MAP = {
    "設施": "facilities", "氣氛": "atmosphere", "付款方式": "payments",
    "熱門原因": "popular_for", "特色": "highlights", "服務項目": "service_options",
    "停車場": "parking", "寵物": "pets", "兒童": "children_friendly",
    "產品/服務": "offerings", "用餐選擇": "dining_options", "客層族群": "crowd_types",
    "規劃": "planning", "無障礙程度": "accessibility"
}

# 第二層：語義歸一化規則 (Normalization Rules)
NORM_RULES = {
    "Wi-Fi": ["Wi-Fi", "wifi", "網路", "wi-fi", "連網"],
    "插座": ["插座", "插頭", "充電", "電源"],
    "工作友善": ["工作友善", "工作", "辦公", "筆電", "讀書", "學生", "獨自用餐"],
    "安靜": ["安靜", "不吵", "靜謐", "幽靜"],
    "甜點": ["甜點", "蛋糕", "鬆餅", "甜食", "點心"],
    "素食": ["素食", "全素", "蔬食", "蛋奶素"],
    "簡餐": ["簡餐", "午餐", "晚餐", "早午餐", "正餐", "定食"],
    "酒精飲料": ["酒精飲料", "酒", "啤酒", "雞尾酒", "葡萄酒", "烈酒"],
    "電子支付": ["行動支付", "apple pay", "google pay", "line pay", "街口", "電子支付"],
    "信用卡": ["信用卡", "nfc", "簽帳", "visa", "mastercard"],
    "現金": ["只收現金", "現金", "cash only"],
    "洗手間": ["洗手間", "廁所", "尿布台", "性別友善洗手間"],
    "性別友善": ["性別友善", "lgbtq", "跨性別", "性別友善空間"],
    "禁菸": ["禁菸", "No Smoking", "禁止吸菸"],
    "吸菸區": ["吸菸區", "吸菸室", "抽菸區"],
    "停車方便": ["停車方便", "停車位很多"],
    "停車困難": ["停車困難", "停車位不好找", "停車位很難找", "不好停車"],
    "外送服務": ["外送", "foodpanda", "uber eats", "無接觸送餐"],
    "免下車服務": ["免下車", "得來速", "drive-through", "drive-thru"],
    "現場表演": ["現場表演", "現場音樂", "演奏", "音樂演奏"],
    "戶外座位": ["戶外座位", "室外", "露天", "陽台", "室外雅座"],
    "可訂位": ["可訂位", "預訂", "訂位", "預約", "需要預訂"],
    "寵物友善": ["寵物友善", "寵物", "狗", "貓", "允許狗狗", "可帶狗"],"適合團體": ["適合團體", "聚餐", "聚會", "團體"],
    "兒童友善": ["兒童友善", "兒童", "高腳椅", "親子", "尿布", "兒童菜單", "適合兒童", "適合闔家光臨"],
    "收費停車": ["收費停車", "收費路邊", "收費室內", "收費停車場"],
    "無障礙設施": ["無障礙", "輪椅", "點字", "無障礙入口", "無障礙洗手間", "無障礙停車位"]
}

# 第三層：特徵布林定義 (Feature Definition)
FEATURE_DEFINITION = {
    "Wi-Fi": ("has_wifi", True), "插座": ("has_plug", True), "工作友善": ("is_work_friendly", True),
    "安靜": ("is_quiet", True), "甜點": ("has_dessert", True), "素食": ("is_vegetarian_friendly", True),
    "簡餐": ("has_meal", True), "酒精飲料": ("has_alcohol", True), "電子支付": ("accept_mobile_payment", True),
    "信用卡": ("accept_credit_card", True), "現金": ("is_cash_only", True), "洗手間": ("has_restroom", True),
    "性別友善": ("is_lgbtq_friendly", True), "禁菸": ("is_smoke_free", True), "吸菸區": ("has_smoking_area", True),
    "停車方便": ("parking_easy", True), "停車困難": ("parking_easy", False),"免下車服務": ("has_drive_thru", True),
    "現場表演": ("has_live_performance", True),"外送服務": ("has_delivery", True),"收費停車": ("has_paid_parking", True),
    "戶外座位": ("has_outdoor_seating", True),"寵物友善": ("is_pet_friendly", True), "兒童友善": ("is_child_friendly", True), 
    "可訂位": ("can_reserve", True),"適合團體": ("is_good_for_groups", True),"無障礙設施": ("has_accessibility", True)
}

# 例外排除與強行覆蓋
TAG_MAPPING = {
    "只收現金": ("現金", "payments", "is_cash_only", True),
    "收費停車場": ("收費停車", "parking", "parking_easy", False),
    "收費路邊停車格": ("收費停車", "parking", "has_paid_parking", True),
    "無障礙入口": ("無障礙設施", "accessibility", "has_accessibility", True)
}