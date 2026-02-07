import json
import random
import os

# 設定輸出路徑
OUTPUT_FILE = "raw_data/cafes_raw_1600.json"
TARGET_COUNT = 1600

# --- 擬真資料庫 (用來隨機組裝) ---
AREAS = ["信義", "大安", "中山", "松山", "內湖", "中正", "士林", "萬華", "北投"]
PREFIXES = ["TJR", "路易", "雲端", "黑金", "極致", "慵懶", "轉角", "巷弄", "老宅", "北歐", "工業", "貓咪", "深夜"]
SUFFIXES = ["咖啡", "Roasters", "Cafe", "Coffee", "實驗室", "所", "工作室", "聚場", "小館", "空間", "別寓"]
ADJECTIVES = ["安靜", "好喝", "吵雜", "明亮", "復古", "舒適", "寬敞", "擁擠", "隱密", "有質感"]

TAG_POOL = [
    "不限時", "插座多", "WIFI穩", "寵物友善", "有貓", "適合工作", "深夜咖啡", 
    "甜點好吃", "手沖專門", "義式", "早午餐", "戶外座位", "景觀", "約會", 
    "安靜", "喧鬧", "站著喝", "外帶", "自家烘焙", "老屋改建"
]

REVIEW_TEMPLATES = [
    "這裡的{item}非常{adj}，適合{activity}。",
    "雖然{con}，但是{pro}，整體來說值得推薦。",
    "位於{area}的隱藏好店，裝潢走{style}風。",
    "插座{socket_status}，網路{wifi_status}，根本是{person}的天堂。",
    "老闆很親切，{item}是必點招牌！",
    "假日人潮{crowd}，建議{advice}再來。"
]

# --- 隨機生成函式 ---
def generate_cafe(index):
    area = random.choice(AREAS)
    name = f"{random.choice(PREFIXES)}{random.choice(SUFFIXES)} ({area}店)"
    
    # 隨機取 3~5 個標籤
    tags = random.sample(TAG_POOL, k=random.randint(3, 5))
    tags.append(area) # 把地區也加進去
    
    # 隨機組裝 2~3 則評論
    reviews = []
    for _ in range(random.randint(2, 3)):
        template = random.choice(REVIEW_TEMPLATES)
        review = template.format(
            item=random.choice(["拿鐵", "手沖", "蛋糕", "布丁", "環境"]),
            adj=random.choice(ADJECTIVES),
            activity=random.choice(["看書", "發呆", "聊天", "工作", "約會"]),
            con=random.choice(["單價偏高", "位置不多", "有點吵", "交通不便"]),
            pro=random.choice(["氣氛很好", "東西好吃", "貓很可愛", "老闆很帥"]),
            area=area,
            style=random.choice(["工業", "日式", "韓系", "極簡"]),
            socket_status=random.choice(["很多", "很少", "要找一下"]),
            wifi_status=random.choice(["超快", "有點慢"]),
            person=random.choice(["工程師", "學生", "SOHO族"]),
            crowd=random.choice(["爆滿", "還好"]),
            advice=random.choice(["平日", "訂位", "早點來"])
        )
        reviews.append(review)

    return {
        "place_id": f"mock_{index:04d}", # e.g., mock_0001
        "name": name,
        "tags": tags,
        "reviews": reviews
    }

# --- 主程式 ---
def main():
    print(f"🚀 正在生成 {TARGET_COUNT} 筆擬真資料...")
    data = [generate_cafe(i) for i in range(1, TARGET_COUNT + 1)]
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        
    print(f"✅ 完成！已儲存至 {OUTPUT_FILE}")
    print("👉 現在你可以去改 Embedding 腳本的 INPUT_FILE 路徑來進行測試了！")

if __name__ == "__main__":
    main()