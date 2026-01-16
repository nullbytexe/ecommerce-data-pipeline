
# E-commerce Data Engineering Pipeline

Dự án Data Engineering hoàn chỉnh với PostgreSQL, Kafka (KRaft mode), Redis và Flask API.

## 🏗️ Kiến trúc
```md
Producer → Kafka → Consumer ────────┬───────→ Redis (Primary/Warm Cache)
                                    ↓
                             PostgreSQL (Raw)
                                    ↓
                           Stream Processor
                                    ↓
                        PostgreSQL (Analytics)
                                    ↓
                               Flask API
                                   |
                        ┌──────────┴──────────┐
                        |                     |
            [Cache Hit] ↓           [Cache Miss] ↓
                        |                     |
                Redis (Cache)           PostgreSQL
                  (Fast Path)          (Analytics)
                        ↑                     ↓
                        └────── Cache Write ──┘
```
## 🚀 Cài đặt và Chạy

### Yêu cầu
- Docker & Docker Compose
- 8GB RAM trở lên
- 10GB disk space

### Khởi động

```bash
# Clone project
git clone https://github.com/nullbytexe/ecommerce-data-pipeline
cd ecommerce-data-pipeline

# Khởi động tất cả services
docker-compose up -d

# Xem logs
docker-compose logs -f

# Kiểm tra status
docker-compose ps
```

### Truy cập Services
- **Kafka UI:** http://localhost:8080
- **API:** http://localhost:5000
- **PostgreSQL:** localhost:5432
- **Redis:** localhost:6379

## 📊 API Endpoints

### Dashboard Metrics
```bash
curl http://localhost:5000/api/dashboard
```

### Daily Sales
```bash
curl http://localhost:5000/api/sales/daily?days=7
```

### Top Products
```bash
curl http://localhost:5000/api/products/top?limit=10
```

### Customer Segments
```bash
curl http://localhost:5000/api/customers/segments
```

### Recent Orders
```bash
curl http://localhost:5000/api/orders/recent?limit=20
```

### Real-time Metrics
```bash
curl http://localhost:5000/api/metrics/realtime
```

## 🗄️ Database Schema

### Raw Schema
- `raw.orders` - Đơn hàng
- `raw.order_items` - Chi tiết đơn hàng
- `raw.customers` - Khách hàng
- `raw.products` - Sản phẩm

### Processed Schema
- `processed.daily_sales` - Doanh số theo ngày
- `processed.product_performance` - Hiệu suất sản phẩm
- `processed.customer_metrics` - Metrics khách hàng

### Analytics Schema
- `analytics.hourly_metrics` - Metrics theo giờ
- `analytics.sales_overview` - Tổng quan doanh số
- `analytics.top_products` - Top sản phẩm
- `analytics.customer_segments` - Phân khúc khách hàng

## 🔧 Kafka Topics
- `orders` - Đơn hàng
- `order_items` - Chi tiết đơn hàng
- `customers` - Khách hàng
- `products` - Sản phẩm

## 📈 Monitoring

### Kiểm tra Kafka
```bash
docker exec -it kafka_broker kafka-topics --list --bootstrap-server localhost:9092
```

### Kiểm tra PostgreSQL
```bash
docker exec -it postgres_db psql -U dataeng -d ecommerce
```

### Kiểm tra Redis
```bash
docker exec -it redis_cache redis-cli
```

## 🛠️ Troubleshooting

### Reset toàn bộ
```bash
docker-compose down -v
docker-compose up -d
```

### Xem logs của service cụ thể
```bash
docker-compose logs -f producer
docker-compose logs -f consumer
docker-compose logs -f stream-processor
```

## 📝 Notes
- Producer tạo dữ liệu fake mỗi 2 giây
- Consumer xử lý real-time và lưu vào PostgreSQL
- Stream Processor chạy mỗi 60 giây để tính toán metrics
- API cache kết quả trong Redis 30 giây

## 🎯 Mở rộng
- [ ] Thêm Apache Airflow cho scheduling
- [ ] Thêm Grafana cho visualization
- [ ] Thêm Elasticsearch cho full-text search
- [ ] Thêm Apache Spark cho batch processing
- [ ] Thêm dbt cho data transformation

## 📄 License
MIT License

---

## 🎯 Hướng dẫn sử dụng

1. **Tạo thư mục dự án và copy tất cả files**
2. **Chạy lệnh**: `docker-compose up -d`
3. **Đợi 1-2 phút** để tất cả services khởi động
4. **Truy cập Kafka UI** tại http://localhost:8080 để xem messages
5. **Test API** tại http://localhost:5000

Dự án này bao gồm đầy đủ:
- ✅ PostgreSQL với schema hoàn chỉnh
- ✅ Kafka với KRaft mode (không cần Zookeeper)
- ✅ Producer tạo dữ liệu fake
- ✅ Consumer xử lý real-time
- ✅ Stream Processor tính toán metrics
- ✅ REST API với Flask
- ✅ Redis cho caching
- ✅ Kafka UI để monitor

