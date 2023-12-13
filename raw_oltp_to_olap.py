import psycopg2
from psycopg2 import sql


def fill_time_dimension(warehouse_conn):
    """
    Creates time dimension records from the orders in staging table.

    Args:
        warehouse_conn (connection): Connection to the warehouse database.

    Returns:
        None
    """
    try:
        with warehouse_conn.cursor() as cursor:
            # Create Time Dimension Records from Orders with Hierarchy-based time_id
            query = sql.SQL("""
                INSERT INTO core.time_dimension (timeid, date, day, month, year, day_name, is_weekend, month_name, quarter, hour, minutes, seconds)
                SELECT
                    EXTRACT(EPOCH FROM order_timestamp)::BIGINT AS timeid,
                    date_trunc('day', order_timestamp) AS date,
                    EXTRACT(day FROM order_timestamp) AS day,
                    EXTRACT(month FROM order_timestamp) AS month,
                    EXTRACT(year FROM order_timestamp) AS year,
                    TO_CHAR(order_timestamp, 'Day') AS day_name,
                    CASE WHEN EXTRACT(ISODOW FROM order_timestamp) IN (6,7) THEN 1 ELSE 0 END AS is_weekend,
                    TO_CHAR(order_timestamp, 'Month') AS month_name,
                    EXTRACT(quarter FROM order_timestamp) AS quarter,
                    EXTRACT(hour FROM order_timestamp) AS hour,
                    EXTRACT(minute FROM order_timestamp) AS minutes,
                    EXTRACT(second FROM order_timestamp) AS seconds
                FROM staging.orders;
            """)
            cursor.execute(query)

        # Commit the changes
        warehouse_conn.commit()

        print("Time Dimension records created successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        warehouse_conn.close()


def fill_customer_dimension(warehouse_conn):
    """
    Fill the customer_dimension table in the warehouse database with customer information.

    Args:
        warehouse_conn: Connection object for the warehouse database.

    Returns:
        None
    """
    try:
        with warehouse_conn.cursor() as cursor:

            # Insert records into core.customer_dimension by combining information from customer and location
            query = sql.SQL("""
                INSERT INTO core.customer_dimension (customer_id, first_name, last_name, email, country, state, city, latitude, longitude)
                SELECT
                    c.customer_id,
                    c.first_name,
                    c.last_name,
                    c.email,
                    l.country,
                    l.state,
                    l.city,
                    l.latitude,
                    l.longitude
                FROM staging.customer c
                JOIN staging.location l ON c.location_id = l.location_id;
            """)
            cursor.execute(query)

        # Commit the changes
        warehouse_conn.commit()

        print("Customer Dimension records filled successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        warehouse_conn.close()


def fill_order_dimension(warehouse_conn):
    try:
        with warehouse_conn.cursor() as cursor:
            # Insert records into core.order_dimension by combining information from orders and payment_method
            query = sql.SQL("""
                INSERT INTO core.order_dimension (order_id, payment_method)
                SELECT
                    o.order_id,
                    pm.payment_method
                FROM staging.orders o
                JOIN staging.payment_method pm ON o.payment_method_id = pm.payment_method_id;
            """)
            cursor.execute(query)

        # Commit the changes
        warehouse_conn.commit()

        print("Order Dimension records filled successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        warehouse_conn.close()


def fill_product_dimension(warehouse_conn):
    try:
        with warehouse_conn.cursor() as cursor:
            # Insert records into core.product_dimension by combining information from product, category,
            # and subcategory
            query = sql.SQL("""
                INSERT INTO core.product_dimension (product_id, name, price, description, category, sub_category)
                SELECT
                    p.product_id,
                    p.name,
                    p.price,
                    p.description,
                    c.category_name,
                    s.subcategory_name
                FROM staging.product p
                JOIN staging.subcategory s ON p.subcategory_id = s.subcategory_id
                JOIN staging.category c ON s.category_id = c.category_id;
            """)
            cursor.execute(query)

        # Commit the changes
        warehouse_conn.commit()

        print("Product Dimension records filled successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        warehouse_conn.close()


def fill_campaign_dimension(warehouse_conn):
    try:
        with warehouse_conn.cursor() as cursor:
            # Insert records into core.campaign_dimension with mapped start_date and end_date
            query = sql.SQL("""
                INSERT INTO core.campaign_dimension (campaign_id, campaign_name, start_date, end_date)
                SELECT
                    campaign_id,
                    campaign_name,
                    start_date,
                    end_date
                FROM (
                    SELECT
                        campaign_id,
                        campaign_name,
                        (DATE '2022-01-01' + (offer_week - 1) * 7) AS start_date,
                        (DATE '2022-01-01' + (offer_week) * 7 - 1) AS end_date
                    FROM staging.marketing_campaigns
                ) AS mapped_campaigns;
            """)
            cursor.execute(query)

        # Commit the changes
        warehouse_conn.commit()

        print("Campaign Dimension records filled successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        warehouse_conn.close()









warehouse_conn = psycopg2.connect(
    database="warehouse",
    user="postgres",
    password="swati",
    host="localhost",
    port="5432"
)

fill_customer_dimension(warehouse_conn)

fill_time_dimension(warehouse_conn)

fill_product_dimension(warehouse_conn)

fill_campaign_dimension(warehouse_conn)
