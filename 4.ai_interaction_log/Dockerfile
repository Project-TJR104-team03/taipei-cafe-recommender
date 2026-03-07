# 使用官方 Python 輕量化映像檔
FROM python:3.10-slim

# 避免 Python 寫入 .pyc 檔案，並強制印出 stdout 日誌 (對 Cloud Logging 很重要)
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# 設定工作目錄
WORKDIR /app

# 複製 requirements 並安裝
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製主程式碼
COPY ai_interaction_log.py .

# 啟動命令 (執行腳本)
CMD ["python", "ai_interaction_log.py"]