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

# 複製 requirements.txt 並安裝相依套件
# --no-cache-dir 可以減少映像檔體積
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製主程式碼
# 假設你的主程式檔名是 main.py (如果不是，請修改這裡)
COPY store_to_DB.py .

# 設定容器啟動時執行的指令
# Cloud Run Job 執行完畢後容器會自動關閉，這符合我們 ETL 的需求
CMD ["python", "store_to_DB.py"]