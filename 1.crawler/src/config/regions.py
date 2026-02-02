# 定義掃描模式
MODE_HIGH = "HIGH"  # 九宮格 (中高密度)
MODE_LOW = "LOW"    # 五點網格 (中低密度)

# 15 個搜尋點的完整配置
CAFE_REGIONS = {
    "A-1": {
        "name": "核心商務區-中山行天宮",
        "lat": 25.0623, "lng": 121.5332,
        "radius": 800, "offset": 0.006, "mode": MODE_HIGH
    },
    "A-2": {
        "name": "核心商務區-中山捷運站",
        "lat": 25.05, "lng": 121.52,
        "radius": 500, "offset": 0.004, "mode": MODE_HIGH
    },
    "A-3": {
        "name": "核心商務區-大同大稻埕",
        "lat": 25.0631, "lng": 121.5133,
        "radius": 1200, "offset": 0.012, "mode": MODE_HIGH
    },

    "B-1": {
        "name": "文青精華區-大安區",
        "lat": 25.0263, "lng": 121.5434,
        "radius": 500, "offset": 0.004, "mode": MODE_HIGH
    },
    "B-2": {
        "name": "文青精華區-東門",
        "lat": 25.0324, "lng": 121.519,
        "radius": 800, "offset": 0.006, "mode": MODE_HIGH
    },
    "B-3": {
        "name": "文青精華區-公館",
        "lat": 25.017, "lng": 121.534,
        "radius": 1200, "offset": 0.012, "mode": MODE_HIGH
    },

    "C-1": {
        "name": "東區與內湖-信義計畫區",
        "lat": 25.0333, "lng": 121.5631,
        "radius": 800, "offset": 0.006, "mode": MODE_HIGH
    },
    "C-2": {
        "name": "東區與內湖-松山區",
        "lat": 25.0592, "lng": 121.5574,
        "radius": 1200, "offset": 0.012, "mode": MODE_HIGH
    },
    "C-3": {
        "name": "東區與內湖-內湖科學園區",
        "lat": 25.0697, "lng": 121.5891,
        "radius": 2500, "offset": 0.025, "mode": MODE_HIGH
    },

    "D-1": {
        "name": "北區觀光區-天母芝山",
        "lat": 25.0903, "lng": 121.5245,
        "radius": 1200, "offset": 0.012, "mode": MODE_LOW
    },
    "D-2": {
        "name": "北區觀光區-北投石牌",
        "lat": 25.1321, "lng": 121.4987,
        "radius": 2500, "offset": 0.025, "mode": MODE_LOW
    },
    "D-3": {
        "name": "北區觀光區-劍潭士林",
        "lat": 25.083, "lng": 121.5285,
        "radius": 1200, "offset": 0.012, "mode": MODE_LOW
    },

    "E-1": {
        "name": "西南與南港-萬華西門",
        "lat": 25.0354, "lng": 121.4997,
        "radius": 800, "offset": 0.006, "mode": MODE_LOW
    },
    
    "E-2": {
        "name": "西南與南港-文山景美",
        "lat": 24.9892, "lng": 121.5701,
        "radius": 2500, "offset": 0.025, "mode": MODE_LOW
    },
    "E-3": {
        "name": "西南與南港-南港中信",
        "lat": 25.0546, "lng": 121.6071,
        "radius": 2500, "offset": 0.025, "mode": MODE_LOW
    }
}