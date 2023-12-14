# *******                           # *******
#  *****                            #  *****
#   ***                             #   ***
#    *     REFER SCHEMA DIAGRAM     #    *
#   ***                             #   ***
#  *****                            #  *****
# *******                           # *******
import psycopg2


def create_customer_product_ratings_table(conn):
    """
    Creates a new table called `customer_product_ratings` in the `public` schema of the specified database connection.

    Parameters:
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        None

    The `customer_product_ratings` table has the following fields:
    - `customerproductrating_id`: The primary key for the table and is of type `SERIAL`.
    - `customer_id`: A foreign key referencing the `customer_id` field in the `customer` table and is of type `INTEGER`.
    - `product_id`: A foreign key referencing the `product_id` field in the `product` table and is of type `INTEGER`.
    - `ratings`: A field to store the product ratings and is of type `NUMERIC(2,1)`.
    - `review`: A field to store the customer's review of the product and is of type `VARCHAR(255)`.
    - `sentiment`: A field to store the sentiment of the review and is of type `VARCHAR(10)`.

    The `customerproductrating_ratings_check` constraint ensures that the `ratings` field has a value between 1 and 5
    (inclusive).
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.customer_product_ratings (
                customerproductrating_id SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES public.customer(customer_id),
                product_id INTEGER REFERENCES public.product(product_id),
                ratings NUMERIC(2,1),
                review VARCHAR(255),
                sentiment VARCHAR(10),
                CONSTRAINT customerproductrating_ratings_check CHECK (ratings >= 1 AND ratings <= 5)
            );
        """)


def create_location_table(conn):
    """
    Create a table called `location` in the `public` schema of the database if it does not already exist.
    The table has the following columns:
    - `location_id` (serial primary key)
    - `latitude` (double precision)
    - `longitude` (double precision)
    - `country` (varchar)
    - `state` (varchar with a maximum length of 100)
    - `city` (varchar with a maximum length of 100)

    Parameters:
    - conn: a connection object to the database

    Returns:
    - None
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.location (
                location_id SERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                country VARCHAR,
                state VARCHAR(100),
                city VARCHAR(100)
            );
        """)


def create_marketing_campaigns_table(conn):
    """
    Creates a table named "marketing_campaigns" in the "public" schema if it doesn't already exist.

    Parameters:
        conn (connection): The database connection object.

    Returns:
        None

    Fields:
        - campaign_id (SERIAL): A unique identifier for each campaign.
        - campaign_name (VARCHAR(255)): The name of the campaign, with a maximum length of 255 characters.
        - offer_week (INTEGER): The week number of the campaign offer.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.marketing_campaigns (
                campaign_id SERIAL PRIMARY KEY,
                campaign_name VARCHAR(255),
                offer_week INTEGER
            );
        """)


def create_orderitem_table(conn):
    """
    Creates a new table called `orderitem` in the `public` schema of the database if it doesn't already exist. The table has the following columns:

    - `orderitem_id`: SERIAL PRIMARY KEY
    - `order_id`: INTEGER REFERENCES public.orders(order_id)
    - `product_id`: INTEGER REFERENCES public.product(product_id)
    - `quantity`: INTEGER
    - `supplier_id`: INTEGER REFERENCES public.supplier(supplier_id)
    - `subtotal`: NUMERIC(10,2)
    - `discount`: NUMERIC(5,2)

    Parameters:
        conn (connection): A database connection object.

    Returns:
        None
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.orderitem (
                orderitem_id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES public.orders(order_id),
                product_id INTEGER REFERENCES public.product(product_id),
                quantity INTEGER,
                supplier_id INTEGER REFERENCES public.supplier(supplier_id),
                subtotal NUMERIC(10,2),
                discount NUMERIC(5,2)
            );
        """)


def create_orders_table(conn):
    """
    Creates the 'orders' table in the database if it does not already exist.

    Parameters:
        conn (object): The connection object to the database.

    Returns:
        None

    Fields:
        - order_id_surrogate (SERIAL): The surrogate primary key for the orders table.
        - order_id (INTEGER): The foreign key from the orderitem table.
        - customer_id (INTEGER): The foreign key from the customer table.
        - order_timestamp (TIMESTAMP): The timestamp of the order.
        - campaign_id (INTEGER): The foreign key from the marketing_campaigns table.
        - amount (INTEGER): The order amount.
        - payment_method_id (INTEGER): The foreign key from the payment_method table.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.orders (
                order_id_surrogate SERIAL PRIMARY KEY,
                order_id INTEGER UNIQUE REFERENCES public.orderitem(order_id),
                customer_id INTEGER REFERENCES public.customer(customer_id),
                order_timestamp TIMESTAMP,
                campaign_id INTEGER REFERENCES public.marketing_campaigns(campaign_id),
                amount INTEGER,
                payment_method_id INTEGER REFERENCES public.payment_method(payment_method_id)
            );
        """)


def create_payment_method_table(conn):
    """
    Creates a payment_method table in the public schema if it does not already exist.

    Parameters:
    - conn: A database connection object.

    Returns:
    None

    Fields:
    - payment_method_id: A serial primary key field for the payment method.
    - payment_method: A string field representing the payment method.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.payment_method (
                payment_method_id SERIAL PRIMARY KEY,
                payment_method VARCHAR(50)
            );
        """)


def create_product_table(conn):
    """
    Create a product table in the public schema of the database if it does not already exist.

    Parameters:
        conn (connection): The database connection object.

    Returns:
        None

    Fields:
        - product_id (SERIAL): The unique identifier for the product.
        - name (VARCHAR): The name of the product.
        - price (NUMERIC): The price of the product.
        - description (TEXT): The description of the product.
        - subcategory_id (INTEGER): The foreign key referencing the subcategory table.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.product (
                product_id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                price NUMERIC(10,2),
                description TEXT,
                subcategory_id INTEGER REFERENCES public.subcategory(subcategory_id)
            );
        """)


def create_returns_table(conn):
    """
    Creates a returns table in the public schema of the specified database connection.

    Args:
        conn: A database connection object.

    Returns:
        None

    Fields:
        return_id: The primary key of the returns table.
        order_id: The foreign key referencing the order_id from the orders table.
        product_id: The foreign key referencing the product_id from the product table.
        return_date: The date of the return.
        reason: The reason for the return.
        amount_refunded: The amount refunded for the return.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.returns (
                return_id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES public.orders(order_id),
                product_id INTEGER REFERENCES public.product(product_id),
                return_date DATE,
                reason TEXT,
                amount_refunded NUMERIC(10,2)
            );
        """)


def create_subcategory_table(conn):
    """
    Creates a new table called "subcategory" in the public schema of the database, if it does not already exist.

    Parameters:
    - conn: A psycopg2 connection object to the database.

    Returns:
    None

    Fields:
    - subcategory_id: The unique identifier for each subcategory. It is an auto-incrementing serial field.
    - subcategory_name: The name of the subcategory. It is a string with a maximum length of 50 characters.
    - category_id: The foreign key reference to the category table. It is an integer field.

    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.subcategory (
                subcategory_id SERIAL PRIMARY KEY,
                subcategory_name VARCHAR(50),
                category_id INTEGER REFERENCES public.category(category_id)
            );
        """)


def create_supplier_table(conn):
    """
    Creates a supplier table in the public schema if it does not already exist.

    Parameters:
        conn (Connection): A database connection object.

    Returns:
        None

    Table Fields:
        - supplier_id (SERIAL PRIMARY KEY): The unique identifier for each supplier.
        - supplier_name (VARCHAR(255)): The name of the supplier.
        - email (VARCHAR(255)): The email address of the supplier.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.supplier (
                supplier_id SERIAL PRIMARY KEY,
                supplier_name VARCHAR(255),
                email VARCHAR(255)
            );
        """)


def create_customer_table(conn):
    """
    Creates a new table in the database called 'customer' if it does not already exist.

    Parameters:
        conn (psycopg2.extensions.connection): The database connection.

    Returns:
        None

    Fields:
        - customer_id (int): The unique identifier for a customer.
        - first_name (str): The first name of the customer.
        - last_name (str): The last name of the customer.
        - email (str): The email address of the customer (must be unique).
        - location_id (int): The reference to the location of the customer.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.customer (
                customer_id SERIAL PRIMARY KEY,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                email VARCHAR(255) UNIQUE,
                location_id INTEGER REFERENCES public.location(location_id)
            );
        """)


def create_category_table(conn):
    """
    Create a category table if it does not already exist in the public schema.

    Parameters:
        conn (object): The connection object to the database.

    Returns:
        None

    Fields:
        - category_id (int): The unique identifier for a category.
        - category_name (str): The name of the category (must be unique).
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.category (
                category_id SERIAL PRIMARY KEY,
                category_name VARCHAR(50) UNIQUE
            );
        """)


def create_campaign_product_subcategory_table(conn):
    """
    Creates a table called `campaign_product_subcategory` in the `public` schema if it does not already exist.
    This table stores the discount values for each campaign and subcategory combination.
    This table will be put in use for populating the discounts for order items.
    Parameters:
        conn (psycopg2.extensions.connection): A connection object to the PostgreSQL database.

    Returns:
        None

    Raises:
        None

    Fields:
        - campaign_product_subcategory_id (SERIAL PRIMARY KEY): The unique identifier for each row in the table.
        - campaign_id (INTEGER): The ID of the campaign associated with the product subcategory.
        - subcategory_id (INTEGER): The ID of the subcategory associated with the campaign.
        - discount (NUMERIC(3, 2)): The discount value for the campaign and subcategory combination.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.campaign_product_subcategory (
                campaign_product_subcategory_id SERIAL PRIMARY KEY,
                campaign_id INTEGER,
                subcategory_id INTEGER,
                discount NUMERIC(3, 2),
                FOREIGN KEY (campaign_id) REFERENCES public.marketing_campaigns(campaign_id),
                FOREIGN KEY (subcategory_id) REFERENCES public.subcategory(subcategory_id)
            );
        """)


# Update the main function to include new table functions
def main():
    conn = None
    try:
        conn = psycopg2.connect(
            host="your_host",
            database="your_database",
            user="your_user",
            password="your_password"
        )

        create_customer_product_ratings_table(conn)
        create_location_table(conn)
        create_marketing_campaigns_table(conn)
        create_orderitem_table(conn)
        create_orders_table(conn)
        create_payment_method_table(conn)
        create_product_table(conn)
        create_returns_table(conn)
        create_subcategory_table(conn)
        create_supplier_table(conn)
        create_customer_table(conn)
        create_category_table(conn)
        create_campaign_product_subcategory_table(conn)

        conn.commit()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
