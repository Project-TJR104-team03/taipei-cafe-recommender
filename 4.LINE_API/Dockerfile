# 使用輕量級的 Python 3.9
FROM python:3.9-slim

# 設定工作目錄
WORKDIR /app

# 安裝系統基本工具
RUN apt-get update && apt-get install -y --no-install-recommends gcc libffi-dev g++ && rm -rf /var/lib/apt/lists/*

# 複製 requirements.txt 並安裝套件
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製所有程式碼到容器內
COPY . .

# 設定環境變數
ENV PYTHONUNBUFFERED=1

# 啟動指令 (Cloud Run 預設 Port 8080)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]