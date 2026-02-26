# 使用官方輕量級 Python 3.10 映像檔
# slim 版本體積小，能加快 Cloud Run 的冷啟動 (Cold Start) 速度
FROM python:3.10-slim

# 設定工作目錄
WORKDIR /app

# 設定環境變數
# 1. 防止 Python 產生 .pyc 檔案
# 2. PYTHONUNBUFFERED=1 非常重要！確保 print() 的 Log 會立即輸出到 Cloud Logging，不會被緩衝卡住
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1


# 安裝系統依賴 (如果未來有用到 Selenium 才需要裝更多，目前這些夠用)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 複製 requirements.txt 並安裝相依套件
# --no-cache-dir 可以減少映像檔體積
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製主程式碼
# 假設你的主程式檔名是 main.py (如果不是，請修改這裡)
COPY . .

# 設定容器啟動的進入點 (ENTRYPOINT)
# 這樣 Airflow 傳遞的參數 (如 --task stage0_gemini) 才會正確附加在後面
ENTRYPOINT ["python", "main.py"]
