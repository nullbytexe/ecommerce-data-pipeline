import json 
import logging
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values
import redis 
from config import Config
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataConsumer:
    def __init__(self):
        self.consumer = None
        self.db_conn = None
        self.redis_client = None
        self.connect_postgres()
        self.connect_kafka()
        self.connect_redis()
    
    def connect_kafka(self):
        """Connect to Kafka"""
        max_retries = 10
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.consumer = KafkaConsumer(
                    Config.ORDERS_TOPIC,
                    Config.ORDER_ITEMS_TOPIC,
                    Config.CUSTOMERS_TOPIC,
                    Config.PRODUCTS_TOPIC,
                    bootstrap_servers = Config.KAFKA_BOOTSTRAP_SERVERS,
                    group_id = Config.GROUP_ID,
                    auto_offset_reset = Config.AUTO_OFFSET_RESET,
                    enable_auto_commit = True, #commit offset định kỳ, trước khi xử lý dữ liệu -> mất data, nếu crash (xài trong demo do postgre auto commit = false nên vẫn đảm bảo tính toàn vẹn)
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                logger.info("Connected to Kafka successfully")
                return
            except Exception as e:
                retry_count += 1
                logger.warning(f"Failed to connect to Kafka (attempt {retry_count}/{max_retries}: {e})")
                time.sleep(5)

        raise Exception("Could not connect to Kafka after maximum retries")

    def connect_postgres(self):
        """Connect to PostgreSQL"""
        max_retries = 10
        retry_count = 0 
        while retry_count< 10:
            try:
                self.db_conn = psycopg2.connect(
                    host=Config.POSTGRES_HOST,
                    user=Config.POSTGRES_USER,
                    password=Config.POSTGRES_PASSWORD,
                    database=Config.POSTGRES_DB
                )
                self.db_conn.autocommit = False
                logger.info("Connected to PostgreSQL successfully")
                return
            except Exception as e:
                retry_count += 1
                logger.warning(f"Failed to connect to PostgreSQL (attempt {retry_count}/{max_retries}: {e})")
                time.sleep(5)

        raise Exception("Could not connect to Postgres after maximum retries")

    def connect_redis(self):
        """Connect to Redis"""
        max_retries = 10
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.redis_client = redis.Redis(
                    host=Config.REDIS_HOST,
                    port=Config.REDIS_PORT,
                    decode_responses= True

                )
                self.redis_client.ping()
                logger.info("Connected to Redis successfully")
                return
            except Exception as e:
                retry_count+=1
                logger.warning(f"Failed to connect to Redis (attempt {retry_count}/{max_retries}: {e})")
                time.sleep(5)
            
        raise Exception("Could not connect to Redis after maximum retries")

    def insert_customer(self, data):
        """Insert customer data into PostgreSQL"""
        try: 
            with self.db_conn.cursor() as cursor:
                query = """
                    INSERT INTO raw.customers
                    (customer_id, email, full_name, phone, city, country, registration_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) DO UPDATE SET
                        email = EXCLUDED.email,
                        full_name = EXCLUDED.full_name,
                        phone = EXCLUDED.phone,
                        city = EXCLUDED.city,
                        country = EXCLUDED.country,
                        last_updated = NOW()
                """
                cursor.execute(query, (
                    data['customer_id'],# cú pháp [] cứng bắt buộc phải có giá trị
                    data['email'],
                    data['full_name'],
                    data.get('phone'),# cú pháp get() mềm hơn, không có giá trị thì trả về None
                    data.get('city'),
                    data.get('country'),
                    data['registration_date']
                ))
                self.db_conn.commit()

                # Cache in Redis
                self.redis_client.setex(
                    f"customer:{data['customer_id']}",
                    3600,
                    json.dumps(data)
                )
                logger.info(f"Inserted customer: {data['customer_id']}")
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error inserting customer: {e}")

    def insert_product(self, data):
        """Insert product data into PostgreSQL"""
        try:
            with self.db_conn.cursor() as cursor:
                query = """
                    INSERT INTO raw.products
                    (product_id, product_name, category, price, stock_quantity)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (product_id) DO UPDATE SET
                        product_name = EXCLUDED.product_name,
                        category = EXCLUDED.category,
                        price = EXCLUDED.price,
                        stock_quantity = EXCLUDED.stock_quantity,
                        updated_at = NOW()
                    """
                
                cursor.execute(query, (
                    data['product_id'],
                    data['product_name'],
                    data['category'],
                    data['price'],
                    data['stock_quantity']
                ))
                self.db_conn.commit()

                # Cache in Redis
                self.redis_client.setex(
                    f"product:{data['product_id']}",
                    3600,
                    json.dumps(data)
                )

                logger.info(f"Inserted product: {data['product_id']}")
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error inserting product: {e}")
        
    def insert_order(self,data):
        """insert order data into PostgreSQL"""
        try:
            with self.db_conn.cursor() as cursor:
                query = """
                    INSERT INTO raw.orders
                    (order_id, customer_id, order_date, total_amount, status, payment_method, shipping_address)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO NOTHING
                """
                cursor.execute(query, (
                    data['order_id'],
                    data['customer_id'],
                    data['order_date'],
                    data['total_amount'],
                    data['status'], 
                    data.get('payment_method'), 
                    data.get('shipping_address')
                ))
                self.db_conn.commit()

                # Update Redis metrics
                self.redis_client.incr('total_orders') #cộng thêm 1 mỗi lần không cần khai báo trước nên giá trị mặc định là 0, ưu điểm là siêu nhanh
                self.redis_client.incrbyfloat('total_revenue', float(data['total_amount']))

                logger.info(f"Inserted order: {data['order_id']}")
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error inserting order: {e}")
    
    def insert_order_item(self, data):
        """Insert order item data into PostgreSQL"""
        try:
            with self.db_conn.cursor() as cursor:
                query = """
                INSERT INTO raw.order_items
                (item_id, order_id, product_id, product_name, quantity, unit_price, discount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (item_id) DO NOTHING
                """
                cursor.execute(query, (
                    data['item_id'],
                    data['order_id'],
                    data['product_id'],
                    data['product_name'],
                    data['quantity'],
                    data['unit_price'],
                    data.get('discount',0)
                ))
                self.db_conn.commit()
                logger.info(f"Inserted order item: {data['item_id']}")
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error inserting order item: {e}")

    def process_message(self, message):
        """Process incoming Kafka message"""
        topic = message.topic
        data = message.value 

        if topic == Config.CUSTOMERS_TOPIC:
            self.insert_customer(data)
        elif topic == Config.PRODUCTS_TOPIC:
            self.insert_product(data)
        elif topic == Config.ORDERS_TOPIC:
            self.insert_order(data)
        elif topic == Config.ORDER_ITEMS_TOPIC:
            self.insert_order_item(data)

    def run(self):
        """Main consumer loop"""
        logger.info("Starting data consumer...")

        try:
            for message in self.consumer:
                self.process_message(message)
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.db_conn:
                self.db_conn.close()
            if self.redis_client:
                self.redis_client.close()
if __name__ == "__main__":
    consumer = DataConsumer()
    consumer.run()

