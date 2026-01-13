import json
import logging
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import time 
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamProcessor:
    def __init__(self):
        self.db_conn = None
        self.connect_postgres()

    def connect_postgres(self):
        """Connect to PostgreSQL"""
        max_retries = 10
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.db_conn = psycopg2.connect(
                    host=os.getenv('POSTGRES_HOST', 'localhost'),
                    user=os.getenv('POSTGRES_USER', 'dataeng'),
                    password=os.getenv('POSTGRES_PASSWORD', 'dataeng123'),
                    database=os.getenv('POSTGRES_DB','ecommerce')
                )
                self.db_conn.autocommit =False
                logger.info("Connected to PostgreSQL successfully")
                return 
            except Exception as e:
                retry_count += 1
                logger.warning(f"Failed to connect to PostgreSQL (attempt {retry_count}/{max_retries}: {e})")
                time.sleep(5)
        
        raise Exception("Could not connect to PostgreSQL after maximum retries")

    def process_daily_sales(self):
        """Calculate daily sales metrics"""
        try:
            with self.db_conn.cursor() as cursor:
                query = """
                    INSERT INTO processed.daily_sales (date, total_orders, total_revenue, avg_order_value, unique_customers)
                    SELECT
                        DATE(order_date) as date,
                        COUNT(*) as total_orders,
                        SUM(total_amount) as total_revenue,
                        AVG(total_amount) as avg_order_value,
                        COUNT(DISTINCT customer_id) as unique_customers
                    FROM raw.orders
                    WHERE DATE(order_date) = CURRENT_DATE
                    GROUP BY DATE(order_date)
                    ON CONFLICT (date) DO UPDATE SET
                        total_orders = EXCLUDED.total_orders,
                        total_revenue = EXCLUDED.total_revenue,
                        avg_order_value = EXCLUDED.avg_order_value,
                        unique_customers = EXCLUDED.unique_customers,
                        processed_at = NOW()
                """
                cursor.execute(query)
                self.db_conn.commit()
                logger.info("Processed daily sales metrics")
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error processing daily sales: {e}")
    
    def process_product_performance(self):
        """Calculate product performace metrics"""
        try:
            with self.db_conn.cursor() as cursor:
                query = """
                    INSERT INTO processed.product_performance (product_id, date, total_quantity_sold, total_revenue, order_count)
                    SELECT
                        oi.product_id,
                        DATE(o.order_date) as date,
                        SUM(oi.quantity) as total_quantity_sold,
                        SUM(oi.quantity * oi.unit_price - oi.discount) as total_revenue,
                        COUNT(DISTINCT o.order_id) as order_count
                    FROM raw.order_items oi
                    JOIN raw.orders o ON oi.order_id = o.order_id
                    WHERE DATE(o.order_date) = CURRENT_DATE
                    GROUP BY oi.product_id, DATE(o.order_date)
                    ON CONFLICT (product_id, date) DO UPDATE SET
                        total_quantity_sold = EXCLUDED.total_quantity_sold,
                        total_revenue = EXCLUDED.total_revenue,
                        order_count = EXCLUDED.order_count
                """
                cursor.execute(query)
                self.db_conn.commit()
                logger.info("Processed product performance metrics")
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error processing product performance: {e}")
    
    def process_customer_metrics(self):
        """Calculate customer lifetime metrics"""
        try:
            with self.db_conn.cursor() as cursor:
                query = """
                    INSERT INTO processed.customer_metrics
                    (customer_id, total_orders, total_spent, avg_order_value, first_order_date, last_order_date, customer_lifetime_days)
                    SELECT
                        customer_id,
                        COUNT(*) as total_orders,
                        SUM(total_amount) as total_spent,
                        AVG(total_amount) as avg_order_value,
                        MIN(order_date) as first_order_date,
                        MAX(order_date) as last_order_date,
                        EXTRACT (DAY FROM (MAX(order_date) - MIN(order_date))) as customer_lifetime_days
                    FROM raw.orders
                    GROUP BY customer_id
                    ON CONFLICT (customer_id) DO UPDATE SET
                         total_orders = EXCLUDED.total_orders,
                         total_spent = EXCLUDED.total_spent,
                         avg_order_value = EXCLUDED.avg_order_value,
                         first_order_date = EXCLUDED.first_order_date,
                         last_order_date = EXCLUDED.last_order_date,
                         customer_lifetime_days = EXCLUDED.customer_lifetime_days,
                         updated_at = NOW()
                """
                cursor.execute(query)
                self.db_conn.commit()
                logger.info("Processed customer metrics")
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error processing customer metrics: {e}")
    
    def process_hourly_metrics(self):
        """Calculate hourly metrics"""
        try:
            with self.db_conn.cursor() as cursor:
                query = """
                    INSERT INTO analytics.hourly_metrics (metric_hour, orders_count, revenue, avg_order_value, top_product_id, top_category)
                    WITH hourly_data AS (
                        SELECT
                            DATE_TRUNC('hour', o.order_date) as metric_hour,
                            COUNT(*) as orders_count,
                            SUM(o.total_amount) as revenue,
                            AVG(o.total_amount) as avg_order_value
                        FROM raw.orders o
                        WHERE o.order_date >= NOW() - INTERVAL '1 hour'
                        GROUP BY DATE_TRUNC('hour', o.order_date)
                    ),
                    top_product AS (
                        SELECT DISTINCT ON (DATE_TRUNC('hour', o.order_date))
                            DATE_TRUNC('hour', o.order_date) as metric_hour,
                            oi.product_id,
                            p.category
                        FROM raw.orders o
                        JOIN raw.order_items oi ON o.order_id = oi.order_id
                        JOIN raw.products p ON oi.product_id = p.product_id
                        WHERE o.order_date >= NOW() - INTERVAL '1 hour'
                        GROUP BY DATE_TRUNC('hour', o.order_date), oi.product_id, p.category
                        ORDER BY DATE_TRUNC('hour', o.order_date), SUM(oi.quantity) DESC
                    )
                    SELECT 
                        hd.metric_hour,
                        hd.orders_count,
                        hd.revenue,
                        hd.avg_order_value,
                        tp.product_id,
                        tp.category
                    FROM hourly_data hd
                    LEFT JOIN top_product tp ON hd.metric_hour = tp.metric_hour
                    ON CONFLICT (metric_hour) DO UPDATE SET
                        orders_count = EXCLUDED.orders_count,
                        revenue = EXCLUDED.revenue,
                        avg_order_value = EXCLUDED.avg_order_value,
                        top_product_id = EXCLUDED.top_product_id,
                        top_category = EXCLUDED.top_category
                """
                cursor.execute(query)
                self.db_conn.commit()
                logger.info("Processed hourly metrics")
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error processing hourly metrics: {e}")
    
    def run(self):
        """Main processing loop"""
        logger.info("Starting stream processor...")

        try:
            while True:
                logger.info("Running processing cycle...")

                self.process_daily_sales()
                time.sleep(2)
                
                self.process_product_performance()
                time.sleep(2)

                self.process_customer_metrics()
                time.sleep(2)

                self.process_hourly_metrics()

                # Wait before next cycle 
                logger.info("Processing cycle complete. Waiting 60 seconds...")
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Shutting down processor...")
        finally:
            if self.db_conn:
                self.db_conn.close()

if __name__ == "__main__":
    processor = StreamProcessor()
    processor.run()

