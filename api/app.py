from flask import Flask, jsonify, request
from flask_cors import CORS 
import psycopg2
from psycopg2.extras import RealDictCursor
import redis 
import json 
import os
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST','localhost'),
        user=os.getenv('POSTGRES_USER', 'dataeng'),
        password=os.getenv('POSTGRES_PASSWORD','dataeng123'),
        database=os.getenv('POSTGRES_DB','ecommerce'),
        cursor_factory=RealDictCursor # chuyển trực tiếp dữ liệu lấy từ query thành dict tiện cho jsonify 
    )

# Redis connection
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST','localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    decode_responses=True # lấy dữ liệu từ redis là str, thay vì là byte theo giao thức của redis
)

@app.route('/')
def home():
    return jsonify({
        'meassage': 'E-commerce Analytics API',
        'version': '1.0',
        'endpoints': {
            '/api/dashboard': 'Get dashboard metrics',
            '/api/sales/daily': 'Get daily sales',
            '/api/products/top': 'Get top products',
            '/api/customers/segments': "Get customer segments",
            '/api/orders/recent': 'Get recent orders',
            '/api/metrics/realtime': 'Get real-time metrics'
        }
    })

@app.route('/api/dashboard')
def dashboard():
    """Get overall dashboard metrics"""
    try:
        # Try to get from cache first
        cached = redis_client.get('dashboard_metrics')
        if cached:
            return jsonify(json.loads(cached))
        
        conn = get_db_connection()
        cursor = conn.cursor()

        # Total orders
        cursor.execute("SELECT COUNT(*) as total FROM raw.orders")
        total_orders = cursor.fetchone()['total']

        # Total revenue
        cursor.execute("SELECT COALESCE(SUM(total_amount),0) as total FROM raw.orders")
        total_revenue = float(cursor.fetchone()['total'])

        # Total customers
        cursor.execute("SELECT COUNT(*) as total FROM raw.customers")
        total_customers = cursor.fetchone()['total']

        # Total products
        cursor.execute("SELECT COUNT(*) as total FROM raw.products")
        total_products = cursor.fetchone()['total']

        # Today's orders
        cursor.execute("SELECT COUNT(*) as total FROM raw.orders WHERE DATE(order_date) = CURRENT_DATE")
        today_orders = cursor.fetchone()['total']

        # Today's revenue
        cursor.execute("SELECT COALESCE(SUM(total_amount),0) as total FROM raw.orders WHERE DATE(order_date) = CURRENT_DATE")
        today_revenue = float(cursor.fetchone()['total'])

        cursor.close()
        conn.close()

        result = {
            'total_orders': total_orders,
            'total_revenue': total_revenue,
            'total_customers': total_customers,
            'total_products': total_products,
            'today_orders': today_orders,
            'today_revenue': today_revenue,
            'timestamp': datetime.now().isoformat()
        }

        # Cache for 30 seconds
        redis_client.setex('dashboard_metrics', 30, json.dumps(result))

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sales/daily')
def daily_sales():
    """Get daily sales data"""
    try:
        days = request.args.get('days', 7, type=int)

        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            SELECT * FROM processed.daily_sales
            ORDER BY date DESC
            LIMIT %s
        """
        cursor.execute(query, (days,))
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/products/top')
def top_products():
    """Get top selling products"""
    try:
        limit = request.args.get('limit', 10, type=int)

        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            SELECT * FROM analytics.top_products
            LIMIT %s
        """
        cursor.execute(query,(limit,))
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/customers/segments')
def customer_segments():
    """Get customer segmentation"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            SELECT
                segment,
                COUNT(*) as customer_count,
                SUM(total_spent) as total_revenue,
                AVG(total_orders) as avg_orders
            FROM analytics.customer_segments
            GROUP BY segment
            ORDER BY total_revenue DESC
        """
        cursor.execute(query)
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/order/recent')
def recent_orders():
    """Get recent orders"""
    try:
        limit = request.args.get('limit', 20, type=int)

        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            SELECT
                o.order_id,
                o.customer_id,
                c.full_name as customer_name,
                o.order_date,
                o.total_amount,
                o.status,
                o.payment_method
            FROM raw.orders o
            LEFT JOIN raw.customers c ON o.customer_id = c.customer_id
            ORDER BY o.order_date DESC
            LIMIT %s
        """
        cursor.execute(query, (limit,))
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/metrics/realtime')
def realtime_metrics():
    """Get real-time metrics from Redis"""
    try:
        total_orders = redis_client.get('total_orders') or 0
        total_revenue = redis_client.get('total_revenue') or 0

        return jsonify({
            'total_orders': int(total_orders),
            'total_revenue': float(total_revenue),
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sales/overview')
def sales_overview():
    """Get sales overview from view"""
    try:
        days = request.args.get('days', 30, type=int)

        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            SELECT * FROM analytics.sales_overview
            ORDER BY sale_date DESC
            LIMIT %s
        """
        cursor.execute(query, (days,))
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify([dict(row) for row in results])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)