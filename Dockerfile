# Dockerfile
# Sử dụng một image Python nhẹ
FROM python:3.9-slim-buster

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Copy requirements.txt và cài đặt các phụ thuộc
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy tất cả các file Python của bạn vào thư mục làm việc (script.py và thư mục utils)
COPY . /app

# Lệnh sẽ được chạy khi container khởi động.
# Lệnh này sẽ thực thi script.py một lần duy nhất.
CMD ["python", "script.py"]
