# 使用官方 Python 輕量版作為基礎鏡像
FROM python:3.10-slim

# 2. 設定環境變數
# PYTHONUNBUFFERED=1: 讓 Log 直接輸出 (你原本的設定)
# CHROME_BIN: 明確告訴系統 Chrome 在哪 (新增的安全設定)
ENV PYTHONUNBUFFERED=1 \
    CHROME_BIN=/usr/bin/google-chrome-stable

# 安裝 Chrome 瀏覽器及其依賴套件 (Selenium 運行必備)
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    --no-install-recommends && \
    # 下載最新版 Chrome
    wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    # 安裝 Chrome (這行會自動補齊所有缺少的 lib，不會再報錯)
    apt-get install -y ./google-chrome-stable_current_amd64.deb && \
    # 清理安裝檔與暫存，減小 Image 大小
    rm google-chrome-stable_current_amd64.deb && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 設定工作目錄
WORKDIR /app

# 複製依賴清單並安裝套件
# 註：這裡使用 requirements.txt 較符合 Docker 標準流程
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製整個專案內容到容器中
COPY . .

# 執行主控程式，依序跑完 01, 02, 03
CMD ["python", "src/main.py"]

