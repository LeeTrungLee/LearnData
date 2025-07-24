# Dockerfile

FROM bitnami/spark:3.4.1

# Sử dụng đúng thư mục app
WORKDIR /app

# Copy requirements trước để cache
COPY requirements.txt .

# Dùng pip đã cài sẵn trong Bitnami (nằm ở /opt/bitnami/python)
RUN /opt/bitnami/python/bin/pip install --no-cache-dir -r requirements.txt

# Copy mã nguồn
COPY . .

# Gọi main bằng Python đúng của Bitnami
CMD ["/opt/bitnami/python/bin/python", "src/main.py"]





