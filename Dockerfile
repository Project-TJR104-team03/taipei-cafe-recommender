# 使用 Python 官方輕量版環境
FROM python:3.10-slim

# 設定貨櫃內的工作目錄
WORKDIR /app

# 先複製清單並安裝套件
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製當前目錄下的所有程式碼進入貨櫃
COPY . .

# 告訴 Cloud Run 啟動時要執行哪一個檔案
CMD ["python", "main.py"]
