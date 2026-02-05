# 使用官方 Python 3.11 輕量版
FROM python:3.11-slim

# 1. 設定環境變數
# PYTHONUNBUFFERED=1: 讓 Log 直接輸出
# CHROME_BIN: 告訴系統 Chrome 在哪
# DEBIAN_FRONTEND=noninteractive: 避免安裝套件時跳出互動視窗
ENV PYTHONUNBUFFERED=1 \
    CHROME_BIN=/usr/bin/google-chrome-stable \
    DEBIAN_FRONTEND=noninteractive

# 2. 安裝 Chrome、中文字型與必要套件
# fonts-noto-cjk: 解決中文變方塊字的問題
# --no-install-recommends: 避免安裝不必要的延伸套件，減小體積
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    fonts-noto-cjk \
    --no-install-recommends && \
    # 下載最新版 Chrome
    wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    # 安裝 Chrome
    apt-get install -y ./google-chrome-stable_current_amd64.deb && \
    # 清理暫存
    rm google-chrome-stable_current_amd64.deb && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 3. 設定工作目錄
WORKDIR /app

# 4. 複製依賴清單並安裝
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. 複製專案程式碼
COPY . .

# 6. 設定入口點 (關鍵修改！)
# 這樣做之後，你在 Cloud Run 的 "Arguments" 欄位只需要填寫 "--task reviews" 即可
ENTRYPOINT ["python", "main.py"]