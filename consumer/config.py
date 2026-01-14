import os

class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'dataeng')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'dataeng123')
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'ecommerce')
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    
    # Kafka Topics
    ORDERS_TOPIC = 'orders'
    ORDER_ITEMS_TOPIC = 'order_items'
    CUSTOMERS_TOPIC = 'customers'
    PRODUCTS_TOPIC = 'products'
    
    # Consumer settings
    GROUP_ID = 'ecommerce-consumer-group'
    AUTO_OFFSET_RESET = 'earliest'