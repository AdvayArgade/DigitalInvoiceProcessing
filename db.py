import mysql.connector
import random
from datetime import datetime, timedelta
import json

# Database connection configuration
config = {
    'user': 'root',
    'password': 'mcks0963',
    'host': 'localhost',
    'database': 'orders',
    'raise_on_warnings': True
}

# Pools of choices for random data generation
products = ['Laptop', 'Smartphone', 'Tablet', 'Monitor', 'Keyboard', 'Mouse', 'Printer', 'Scanner']
vendors = ['VendorA', 'VendorB', 'VendorC', 'VendorD', 'VendorE']
specifics_options = [
    {"color": "black", "weight": 2.5},
    {"color": "silver", "weight": 1.8},
    {"color": "white", "weight": 2.0},
    {"color": "blue", "weight": 1.5}
]


# Function to generate random data
def generate_random_data():
    product = random.choice(products)
    quantity = random.randint(1, 100)
    vendor = random.choice(vendors)
    price_per_item = round(random.uniform(10.0, 1000.0), 2)
    order_time = datetime.now() - timedelta(days=random.randint(0, 365))
    specifics = json.dumps(random.choice(specifics_options))
    return product, quantity, vendor, price_per_item, order_time, specifics


# Connect to the MySQL database
try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    # Insert 1000 rows
    for _ in range(1000):
        data = generate_random_data()
        insert_query = """
        INSERT INTO order_details (Product, Quantity, Vendor, Price_per_item, Order_time, specifics)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, data)

    # Commit the transaction
    conn.commit()
    print("1000 rows inserted successfully.")
    cursor.close()
    conn.close()
    print("MySQL connection is closed.")

except mysql.connector.Error as err:
    print(f"Error: {err}")

