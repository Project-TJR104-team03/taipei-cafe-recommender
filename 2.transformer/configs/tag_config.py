# tag_config.py

# 第一層：類別映射 (Category Mapping)
CAT_MAP = {
    "設施": "facilities", "氣氛": "atmosphere", "付款方式": "payments",
    "熱門原因": "popular_for", "特色": "highlights", "服務項目": "service_options",
    "停車場": "parking", "寵物": "pets", "兒童": "children_friendly",
    "產品/服務": "offerings", "用餐選擇": "dining_options", "客層族群": "crowd_types",
    "規劃": "planning", "無障礙程度": "accessibility", "裝潢風格": "design_style", "感官體驗": "sensory_experience",
    "座位配置": "seating_config", "消費規則": "store_policies", "聲音氛圍": "acoustic_vibe", "電力供應": "power_supply",
    "寵物互動": "pet_interaction"
}

# 第二層：語義歸一化規則 (Normalization Rules)
NORM_RULES = {
    "Wi-Fi": ["Wi-Fi", "wifi", "網路", "wi-fi", "連網"],
    "插座": ["插座", "插頭", "充電", "電源"],
    "工作友善": ["工作友善", "適合工作", "適合辦公", "筆電", "讀書"],
    "不限時" : ["不限時","待整天", "坐很久", "待很久", "不趕人", "no time limit", '久坐'],
    "深夜": ["深夜", "凌晨", "開很晚", "半夜", "睡不著", "宵夜"],
    "安靜": ["安靜", "不吵", "靜謐", "幽靜"],
    "甜點": ["甜點", "蛋糕", "甜食", "點心"], 
    "素食": ["素食", "全素", "蔬食", "蛋奶素"],
    "簡餐": ["簡餐", "午餐", "晚餐", "早午餐", "正餐", "定食", "輕食", '水餃', "鹹食"], 
    "手沖精品": ["手沖", "精品", "單品", '手沖咖啡'],
    "酒精飲料": ["酒精飲料", "酒", "啤酒", "雞尾酒", "葡萄酒", "烈酒"],
    "電子支付": ["行動支付", "apple pay", "google pay", "line pay", "街口", "電子支付"],
    "信用卡": ["信用卡", "nfc", "簽帳", "visa", "mastercard"],
    "只收現金": ["只收現金", "cash only"],
    "洗手間": ["洗手間", "廁所", "尿布台", "性別友善洗手間"],
    "性別友善": ["性別友善", "lgbtq", "跨性別", "性別友善空間"],
    "禁菸": ["禁止吸菸", "全面禁菸", "禁菸", "no smoking"],
    "吸菸區": ["吸菸區", "吸菸室", "抽菸區", "可吸菸"],
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
    "無障礙設施": ["無障礙", "輪椅", "點字", "無障礙入口", "無障礙洗手間", "無障礙停車位"],
    "氛圍舒適": ["環境舒適", "休閒", "舒適"],
    "適合單人": ["獨自用餐", "適合一個人"],
    "零售服務": ["店內購物", "來店取貨", '咖啡豆零售', '咖啡豆'],
    #update
    "低消": ["低消", "低消飲料", "飲料低消", "有低消", "低消一杯飲品", "低消一杯飲料", "最低消費"],
    "燕麥奶": ["燕麥奶", "植物奶", "oat milk"],
    "老宅": ["老宅", "古厝", "老屋", "老房子", '老建築', "古宅"],
    '復古':['復古','老舊', '古色古香', '老公寓', '復古風'],
    '自家烘焙': ['自家烘焙', '自家烘', '自烘咖啡豆', '烘豆'],
    "服務費": ["服務費", "10%", "一成服務費"],
    "限時": ["限時", "用餐時間限制", "限2小時", "限90分鐘", '時間限制', '計時'],
    "日式風格": ["日式", "昭和", "和風"],
    "店狗": ["店狗", "有店狗", "狗狗"],
    "店貓": ["店貓", "有店貓", "貓咪"],
    "服務親切":['服務親切', '親切', '友善', '熱情', '老闆熱情', '客氣', '老闆友善', '老闆人很好', '服務態度好', '闆娘親切', '服務人員親切', '服務熱情', '老闆健談', '老闆nice', '服務態度很好', '服務好', '服務周到'],
    "服務不佳":['服務不佳', '服務態度差', '不耐煩', '服務差', '態度很差', '沒禮貌', '不友善', '臉臭'],
    "文青":['文青','文藝'],
    '悶熱':['悶熱', '熱', '悶', '冷氣不涼'],
    "無內用座位":['無內用座位', '無內用空間'],
    "韓系風格":['韓系','韓風'],
    "工業風格":['工業'],
    "溫度冷" :['冷氣強', '很冷'],
    "服務效率不佳":['出餐慢', '等很久'],
    "免服務費":['不收服務費', '無服務費', '免服務費', '不用服務費']

}

# 第三層：特徵布林定義 (Feature Definition)
FEATURE_DEFINITION = {
    "Wi-Fi": ("has_wifi", True), "插座": ("has_plug", True), "工作友善": ("is_work_friendly", True),
    "不限時": ("time_limit_free", True), "深夜": ("is_midnight", True), "安靜": ("is_quiet", True), 
    "甜點": ("has_dessert", True), "素食": ("is_vegetarian_friendly", True), "簡餐": ("has_meal", True),
    "手沖精品": ("is_specialty_coffee", True), "酒精飲料": ("has_alcohol", True), 
    "電子支付": ("accept_mobile_payment", True), "信用卡": ("accept_credit_card", True), "只收現金": ("is_cash_only", True),
    "洗手間": ("has_restroom", True), "性別友善": ("is_lgbtq_friendly", True),
    "禁菸": ("is_smoke_free", True), "吸菸區": ("has_smoking_area", True),
    "停車方便": ("parking_easy", True), "停車困難": ("parking_difficult", True),"免下車服務": ("has_drive_thru", True),
    "現場表演": ("has_live_performance", True),"外送服務": ("has_delivery", True),"收費停車": ("has_paid_parking", True),
    "戶外座位": ("has_outdoor_seating", True),"寵物友善": ("is_pet_friendly", True), "兒童友善": ("is_child_friendly", True), 
    "可訂位": ("can_reserve", True),"適合團體": ("is_good_for_groups", True),"無障礙設施": ("has_accessibility", True),
    #update
    "低消": ("has_minimum_charge", True),
    "燕麥奶": ("is_oat_milk_available", True),
    "老宅": ("is_old_house", True),
    "服務費": ("has_service_fee", True),
    "限時": ("has_time_limit", True),
    "日式風格": ("is_japanese_style", True),
    "韓系風格": ("is_korean_style", True),
    "服務效率不佳": ("is_service_slow", True),
    "店狗": ("has_shop_dog", True),
    "店貓": ("has_shop_cat", True),
    '復古': ('retro', True),
    "溫度冷": ('is_cold', True),'免服務費': ('service_fee_free', True),
    '服務親切': ('good_service', True),
    '服務不佳': ('bad_service', True), "工業風格": ('has_industrial_design', True),
    '文青': ('hipster_style', True),'悶熱': ('stuffy', True)
}

# 例外排除與強行覆蓋
TAG_MAPPING = {
    "只收現金": ("只收現金", "payments", "is_cash_only", True),
    "收費停車場": ("收費停車", "parking", "parking_difficult", True),
    "收費路邊停車格": ("收費停車", "parking", "has_paid_parking", True),
    "無障礙入口": ("無障礙設施", "accessibility", "has_accessibility", True)
}

POSITIVE_TAG_RULES = [
    "Wi-Fi", "插座", "洗手間", "可訂位", "甜點", 
    "寵物友善", "兒童友善", "工作友善", 
    "電子支付", "信用卡", "戶外座位", "適合團體"
]

TERMINOLOGY_WHITE_LIST = {
    "無障礙設施": "has_accessibility",
    "禁菸": "is_no_smoking",
    "不限時": "no_time_limit",
    "不吵": "is_quiet"
}


NEGATIVE_TAG_RULES = {
    "停車困難": "parking_difficult"
}

# --- 新增區塊：評分權重配置 ---
SCORING_CONFIG = {
    "BASE_WEIGHT": 0.5,      # 基礎分：只要 AI 判定該特徵存在，起步分為 0.5
    "STEP_WEIGHT": 0.1,      # 共識增量：consensus_level 每增加一級，分數加 0.1
    "MIN_CONSENSUS": 1,      # 最小共識防禦（避免除以零）
    "DEFAULT_CONSENSUS": 3   # 預設共識等級（當 Stage A 沒給出具體等級時）
}

# --- 新增區塊：矛盾維度收斂定義 ---
# 格式： "目標指標名稱": {"positive": "你的布林特徵Key", "negative": "你的布林特徵Key"}
CONTRADICTION_PAIRS = {
    "service_quality_score": {
        "positive": "good_service", 
        "negative": "bad_service"
    },
    "time_flexibility_score": {
        "positive": "time_limit_free", 
        "negative": "has_time_limit"
    },
    "parking_convenience_score": {
        "positive": "parking_easy",  
        "negative": "parking_difficult" 
    },
    "fee_transparency_score": {
        "positive": "service_fee_free",
        "negative": "has_service_fee"
    },
    "temperature_comfort_score": {
        "positive": "is_cold",  # 冷氣強
        "negative": "stuffy"    # 悶熱
    }
}