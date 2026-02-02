# 使用官方 Python 輕量版作為基礎鏡像
FROM python:3.10-slim

# 設定環境變數，確保 Python 輸出直接印在終端機上
ENV PYTHONUNBUFFERED=1

# 安裝 Chrome 瀏覽器及其依賴套件 (Selenium 運行必備)
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    curl \
    unzip \
    libgconf-2-4 \
    libnss3 \
    libxss1 \
    libasound2 \
    fonts-liberation \
    libappindicator3-1 \
    xdg-utils \
    --no-install-recommends && \
    curl -sSL https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && apt-get install -y google-chrome-stable && \
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