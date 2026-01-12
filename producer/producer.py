import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker 
import psycopg2
from config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

class DataProducer:
    def __init__(self):
        self.producer = None
        self.db_conn = None 
        self.connect_kafka()
        self.connect_postgres()

        # Pre-generate some data
        self.customers = self.generate_customers(100)
        self.products = self.generate_products(50)

    def connect_kafka(self):
        """Connect to Kafka"""
        max_retries = 10
        retry_count = 0

        while retry_count < max_retries:
            try: 
                self.producer = KafkaProducer(
                    bootstrap_servers = Config.KAFKA_BOOSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all', 
                    retries=3
                )
                logger.info("Connected to Kafka successfully")
                return 
            except Exception as e:
                retry_count += 1
                logger.warning(f"Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {e}")
                time.sleep(5)
        raise Exception("Could not connect to Kafka after maximum retries")
    def connect_postgres(self):
        """Connect to PostgreSQL"""
        max_retries = 10
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.db_conn = psycopg2.connect(
                    host=Config.POSTGRES_HOST,
                    user=Config.POSTGRES_USER,
                    password=Config.POSTGRES_PASSWORD,
                    database=Config.POSTGRES_DB
                )
                logger.info("Connected to PostgreSQL successfully")
                return
            except Exception as e:
                retry_count += 1
                logger.warning(f"Failed to connect to PostgreSQL (attempt {retry_count}/{max_retries}): {e}")
                time.sleep(5)
        raise Exception("Could not connect to PostgreSQL after maximum retries")
    
    def generate_customers(self,count):
        """Generate fake customer data"""
        customers=[]
        for _ in range(count):
            customer={
                'customer_id': fake.uuid4(),
                'email': fake.email(),
                'full_name': fake.name(),
                'phone': fake.phone_number(),
                'city': fake.city(),
                'country': fake.country(),
                'registration_date': fake.date_time_between(start_date='-2y', end_date='now').isoformat()
            }
            customers.append(customer)
        return customers
    def generate_products(self,count):
        """Generate fake product data"""
        categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys', 'Food']
        products = []

        for _ in range(count):
            product = {
                'product_id': fake.uuid4(),
                'product_name': fake.catch_phrase(),
                'category': random.choice(categories),
                'price': round(random.uniform(10,500), 2),
                'stock_quantity': random.randint(0, 1000)
            }
            products.append(product)
        return products
    
    def generate_order(self):
        """Generate a fake order with items"""
        customer = random.choice(self.customers)
        num_items = random.randint(1,5)
        order_items  random.sample(self.products, num_items)

        order = {
            'order_id': fake.uuid4(),
            'customer_id': customer['customer_id'],
            'order_date': datetime.now().isoformat(),
            'status': random.choice(['pending', 'processing', 'shipped', 'delivered']),
            'payment_method': random.choice(['credit_card','paypal','bank_transfer']),
            'shipping_address': fake.address()
        }

        items = []
        total_amount = 0

        for product in order_items:
            quantity = random.randint(1,3)
            unit_price = product['price']
            discount = round(random.uniform(0, unit_price * 0.2), 2)

            item = {
                'item_id': fake.uuid4(),
                'order_id': order['order_id'],
                'product_id': product['product_id'],
                'product_name': product['product_name'],
                'quantity': quantity,
                'unit_price': unit_price,
                'discount': discount
            }
            items.append(item)
            total_amount += (unit_price - discount) * quantity
        
        order['total_amount']= round(total_amount,2)

        return order, items

    def send_to_kafka(self,topic, data):
        """Send data to Kafka topic"""
        try: 
            future = self.producer.send(topic,data)
            future.get(timeout=10)
            return True
        except Exception as e:
            logger.error(f"Error sending to Kafka topic {topic}: {e}")
            return False

    def run(self):
        """Main producer loop"""

        # Send initial customers and products
        logger.info("Sending initial customers...")
        for customer in self.customers:
            self.send_to_kafka(Config.CUSTOMERS_TOPIC, customer)

        logger.info("Sending initial products...")
        for product in self.products:
            self.send_to_kafka(Config.PRODUCTS_TOPIC,product)

        # Continuously generate and send orders
        logger.info("Starting order generation...")
        order_count = 0

        try:
            while True:
                order, items = self.generate_order()

                # Send order
                if self.send_to_kafka(Config.ORDERS_TOPIC, order):
                    order_count += 1
                    logger.info(f"Sent order {order_count}: {order['order_id']}")

                    # Send item in items:
                    for item in items:
                        self.send_to_kafka(Config.ORDER_ITEMS_TOPIC, item)

                time.sleep(Config.SLEEP_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Shutting down producer...")
        finally:
            if self.producer:
                self.producer.close()
            if self.db_conn:
                self.db_conn.close()

if __name__ == "__main__":
    producer = DataProducer()
    producer.run()    