Các bước chạy các module

.\venv\Scripts\activate

1. Chạy FastAPI chatapp
 - uvicorn src.main:app --reload
 - gọi localhost:8000/chat {message: ""}

2. Chạy service tạo bài phân tích
 - python src/analysis/analyze_stock_main.py
 