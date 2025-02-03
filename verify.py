import difflib
import mysql.connector
import hashlib
from datetime import datetime
import json
from datetime import timedelta

# Database connection configuration
config = {
    'user': 'root',
    'password': 'mcks0963',
    'host': 'localhost',
    'database': 'orders',
    'raise_on_warnings': True
}


def truncate_to_hours(timestamp):
    return timestamp.replace(minute=0, second=0, microsecond=0)


def generate_hash(product, quantity, vendor, price_per_item, order_time):
    truncated_time = truncate_to_hours(order_time)
    data_to_hash = f"{product}|{quantity}|{vendor}|{price_per_item}|{truncated_time}"
    return hashlib.sha256(data_to_hash.encode()).hexdigest()


def is_similar(str1, str2, threshold=0.8):
    return difflib.SequenceMatcher(None, str1.lower().strip(), str2.lower().strip()).ratio() >= threshold


# Allow for rounded off total price
# Compare the quantity and price per item manually if the hash does not match
def compare(cursor, data, hash_value):
    # Step 1: Fetch all rows that match the hash
    query = "SELECT * FROM order_details WHERE Hash = %s"
    cursor.execute(query, (hash_value,))
    matching_rows = cursor.fetchall()

    if not matching_rows:
        print("No rows found with the given hash.")
        return []

    # Step 2: Filter rows based on timestamp (allowing a 2-minute difference)
    valid_rows = []
    for row in matching_rows:
        db_timestamp = row[5]  # Assuming the timestamp column is the 6th column (index 5)
        if abs((db_timestamp - data['Order_time']).total_seconds()) <= 120:  # Allowing ±2 minutes
            valid_rows.append(row)
        else:
            print(f"Timestamp mismatch: Expected around {data['Order_time']}, but found {db_timestamp}")

    if not valid_rows:
        print("No rows matched within the 2-minute timestamp range.")
        return []

    # Step 3: Compute total value and match it
    actual_total_value = data['Total_price']
    total_value_matched_rows = []

    for row in valid_rows:
        db_quantity = row[2]  # Assuming Quantity is at index 2
        db_price_per_item = row[4]  # Assuming Price_per_item is at index 4
        db_total_value = db_quantity * db_price_per_item

        if abs(db_total_value - actual_total_value) < 1e-6:  # Allow for floating-point precision errors
            total_value_matched_rows.append(row)
        else:
            print(f"Total value mismatch: Expected {db_total_value}, but found {actual_total_value}")

    if not total_value_matched_rows:
        print("No rows matched the total value condition.")
        return []

    # Step 4: Check specifics
    final_matched_rows = []
    for row in total_value_matched_rows:
        print("specifics in row: ", row[7])
        db_specifics = json.loads(row[7])  # Convert DB JSON string to dictionary
        input_specifics = json.loads(data['specifics'])  # Ensure input is also a dictionary

        if db_specifics == input_specifics:
            print("Specifics match!")
            final_matched_rows.append(row)

        else:
            print(f"Specifics mismatch: Expected {data['specifics']}, but found {db_specifics}")

    if final_matched_rows:
        print(f"Found {len(final_matched_rows)} matching row(s) after all checks.")
    else:
        print("No rows fully matched all conditions.")

    return final_matched_rows


# Add the robust string matching
# Fix rounding off total price
def retrieve_matching_rows(cursor, conn, new_data):
    product = new_data["Product"]
    quantity = new_data["Quantity"]
    vendor = new_data["Vendor"]
    price_per_item = new_data["Price_per_item"]
    order_time = new_data["Order_time"]
    specifics = new_data["specifics"]
    total_price = new_data["Total_price"]

    # Step 1: Fetch rows matching the hash where cleared = false
    query = "SELECT * FROM order_details WHERE Hash = %s AND cleared = false"
    cursor.execute(query, (generate_hash(product, quantity, vendor, price_per_item, order_time),))
    matching_rows = cursor.fetchall()

    if not matching_rows:
        print("No rows found with matching hash. Checking based on vendor, product, and timestamp.")

        # Step 2: Find rows where vendor, product, and timestamp (5 min) match
        time_lower = order_time - timedelta(minutes=5)
        time_upper = order_time
        query = """
                        SELECT * FROM order_details
                        WHERE Order_time BETWEEN %s AND %s
                        AND cleared = false
                        """
        cursor.execute(query, (time_lower, time_upper))
        rows = cursor.fetchall()

        matching_rows = [
            row for row in rows
            if is_similar(row[3], new_data["Vendor"]) and is_similar(row[1], new_data["Product"])
        ]

        print("Possible matches after string matching:")
        for row in matching_rows:
            print(row)

        if not matching_rows:
            print("No approximate matches found.")
            return []

        # query = """
        # SELECT * FROM order_details
        # WHERE Vendor = %s AND Product = %s
        # AND Order_time BETWEEN %s AND %s
        # AND cleared = false
        # """
        #
        # cursor.execute(query, (vendor, product, time_lower, time_upper))
        # matching_rows = cursor.fetchall()

        # Step 3: Check where the mismatch is (Quantity or Price)
        for row in matching_rows:
            db_quantity = row[2]  # Assuming Quantity is at index 2
            db_price_per_item = row[4]  # Assuming Price_per_item is at index 4

            quantity_mismatch = db_quantity != quantity
            price_mismatch = db_price_per_item != price_per_item

            if quantity_mismatch:
                print(f"Quantity mismatch: Expected {quantity}, but found {db_quantity}\n")

            if price_mismatch:
                print(f"Price per item mismatch: Expected {price_per_item}, but found {db_price_per_item}\n")

            if price_mismatch or quantity_mismatch:
                matching_rows.remove(row)

        if not matching_rows:
            return []

    # Step 4: Timestamp Check (Allowing ±2 Minutes)
    valid_rows = []
    for row in matching_rows:
        db_timestamp = row[5]  # Assuming Order_time is at index 5
        if abs((db_timestamp - order_time).total_seconds()) <= 300:
            valid_rows.append(row)
        else:
            print(f"Timestamp mismatch: Expected around {order_time}, but found {db_timestamp}\n")

    if not valid_rows:
        print("No rows matched within the 5-minute timestamp range.\n")
        return []

    # Step 5: Compute and Match Total Price (Allowing for Rounding)
    total_price_matched_rows = []
    for row in valid_rows:
        db_quantity = row[2]
        db_price_per_item = row[4]
        db_total_price = db_quantity * db_price_per_item

        # Allow small rounding errors and also allow DB price to be higher
        if round(db_total_price, 0) == round(total_price, 0):
            total_price_matched_rows.append(row)
            print("Total price rounded off.\n")

        if db_total_price >= total_price:
            total_price_matched_rows.append(row)
            print("Invoice price less than expected total price.\n")

        else:
            print(f"Total price mismatch: Expected ~{db_total_price}, but found {total_price}\n")

    if not total_price_matched_rows:
        print("No rows matched the total price condition.\n")
        return []

    # Step 6: Check Specifics
    final_matched_rows = []
    for row in total_price_matched_rows:
        db_specifics = json.loads(row[7])  # Convert JSON string to dictionary
        input_specifics = json.loads(specifics)

        if db_specifics == input_specifics:
            final_matched_rows.append(row)
        else:
            print(f"Specifics mismatch: Expected {input_specifics}, but found {db_specifics}\n")

    if len(final_matched_rows) == 1:
        # Step 7: If exactly 1 row matches, mark it as cleared
        matching_id = final_matched_rows[0][0]  # Assuming ID is at index 0
        update_query = "UPDATE order_details SET cleared = true WHERE ID = %s"
        cursor.execute(update_query, (matching_id,))
        conn.commit()
        print(f"Marked row ID {matching_id} as cleared.")

    if final_matched_rows:
        print(f"Found {len(final_matched_rows)} matching row(s) after all checks.\n")
    else:
        print("No rows fully matched all conditions.\n")

    return final_matched_rows


# Example new data point
new_data = {
    "Product": "Printers",
    "Quantity": 96,
    "Vendor": "Vendor D",
    "Price_per_item": 263.76,
    "Order_time": datetime.strptime("2024-03-15 20:27:26", "%Y-%m-%d %H:%M:%S"),  # Specific timestamp
    "specifics": json.dumps({"weight": 1.8, "color": "silver"}, separators=(',', ':')),  # Ensure consistent formatting
    "Total_price": 25321
}

# Generate hash for the new data point
new_hash = generate_hash(
    new_data["Product"],
    new_data["Quantity"],
    new_data["Vendor"],
    new_data["Price_per_item"],
    new_data["Order_time"]
)

print(f"Generated Hash: {new_hash}")

# Connect to the MySQL database
try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    # Retrieve rows matching the hash and specifics
    matching_rows = retrieve_matching_rows(cursor, conn, new_data)

    if matching_rows:
        for row in matching_rows:
            print(row)

    else:
        print("No matching rows")

    cursor.close()
    conn.close()
    print("MySQL connection is closed.")

except mysql.connector.Error as err:
    print(f"Error: {err}")
