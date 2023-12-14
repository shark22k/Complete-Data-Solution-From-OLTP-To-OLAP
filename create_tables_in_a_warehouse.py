import logging
import psycopg2


def intialize_logger():
    """
    Initializes and configures a logger with two handlers for error and info messages.

    Returns:
        logger (logging.Logger): The configured logger object.

    Raises:
        None
    """

    # Configure logging with two handlers
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # Handler for errors
    error_handler = logging.FileHandler('create_table_warehouse.log')
    error_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
    error_handler.setFormatter(error_formatter)
    logger.addHandler(error_handler)

    # Handler for info messages
    info_handler = logging.FileHandler('create_table_warehouse.log')
    info_handler.setLevel(logging.INFO)
    info_formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
    info_handler.setFormatter(info_formatter)
    logger.addHandler(info_handler)

    return logger


def create_customer_product_ratings_fact_table(conn, logger):
    sql_query = """
        CREATE TABLE IF NOT EXISTS core.customer_product_ratings_fact (
            customerproductrating_id SERIAL PRIMARY KEY,
            customer_id INTEGER REFERENCES core.customer_dimension(customer_id),
            product_id INTEGER REFERENCES core.product_dimension(product_id),
            ratings NUMERIC(2,1),
            review VARCHAR(255),
            sentiment VARCHAR(10),
            CONSTRAINT customerproductrating_ratings_check CHECK (ratings >= 1 AND ratings <= 5)
        );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()
            logger.info("customer_product_ratings_fact table created successfully.")
    except Exception as e:
        logger.error(f"Failed to create customer_product_ratings_fact table. Error: {e}")


def create_sales_fact_table(conn, logger):
    sql_query = """
        CREATE TABLE IF NOT EXISTS core.Sales_fact (
            id SERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL,
            time_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            campaign_id INTEGER NOT NULL,
            supplier_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            subtotal INTEGER NOT NULL,
            discount_percentage NUMERIC(3,2) NOT NULL,
            sales_price INTEGER NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES core.customer_dimension(customer_id),
            FOREIGN KEY (supplier_id) REFERENCES core.supplier_dimension(supplier_id),
            FOREIGN KEY (product_id) REFERENCES core.product_dimension(product_id),
            FOREIGN KEY (campaign_id) REFERENCES core.campaign_dimension(campaign_id),
            FOREIGN KEY (order_id) REFERENCES core.Order_dimension(order_id)
        );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()
            logger.info("Sales_fact table created successfully.")
    except Exception as e:
        logger.error(f"Failed to create Sales_fact table.  Error: {e}")


def create_supplier_dimension_table(conn, logger):
    sql_query = """
        CREATE TABLE IF NOT EXISTS core.supplier_dimension (
            supplier_id INTEGER PRIMARY KEY,
            supplier_name CHARACTER VARYING(50),
            email CHARACTER VARYING(50),
            country CHARACTER VARYING(50),
            state CHARACTER VARYING(50),
            city CHARACTER VARYING(50)
        );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()
            logger.info("Supplier_dimension table created successfully.")
    except Exception as e:
        logger.error(f"Failed to create Supplier_dimension table. Error: {e}")


def create_order_dimension_table(conn, logger):
    sql_query = """
        CREATE TABLE IF NOT EXISTS core.Order_dimension (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER REFERENCES core.product_dimension(product_id),
            payment_method CHARACTER VARYING(50)
        );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()
            logger.info("Order_dimension table created successfully.")
    except Exception as e:
        logger.error(f"Failed to create Order_dimension table. Error: {e}")


def create_campaign_dimension_table(conn, logger):
    sql_query = """
        CREATE TABLE IF NOT EXISTS core.campaign_dimension (
            campaign_id INTEGER PRIMARY KEY,
            campaign_name CHARACTER VARYING(100) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL
        );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()
            logger.info("Campaign_dimension table created successfully.")
    except Exception as e:
        logger.error(f"Failed to create Campaign_dimension table. Error: {e}")


def create_customer_dimension_table(conn, logger):
    sql_query = """
        CREATE TABLE IF NOT EXISTS core.customer_dimension (
            customer_id INTEGER PRIMARY KEY,
            first_name CHARACTER VARYING(50) NOT NULL,
            last_name CHARACTER VARYING(50) NOT NULL,
            email CHARACTER VARYING(255) NOT NULL,
            country CHARACTER VARYING(50),
            state CHARACTER VARYING(50),
            city CHARACTER VARYING(50),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION
        );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()
            logger.info("Customer_dimension table created successfully.")
    except Exception as e:
        logger.error(f"Failed to create Customer_dimension table. Error: {e}")


def create_product_dimension_table(conn, logger):
    sql_query = """
        CREATE TABLE IF NOT EXISTS core.product_dimension (
            product_id INTEGER PRIMARY KEY,
            name CHARACTER VARYING(255) NOT NULL,
            price NUMERIC(10,2) NOT NULL,
            description TEXT NOT NULL,
            category CHARACTER VARYING(100) NOT NULL,
            sub_category CHARACTER VARYING(100) NOT NULL
        );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()
            logger.info("Product_dimension table created successfully.")
    except Exception as e:
        logger.error(f"Failed to create Product_dimension table. Error: {e}")


def create_time_dimension_table(conn, logger):
    sql_query = """
        CREATE TABLE IF NOT EXISTS core.time_dimension (
            id INTEGER PRIMARY KEY,
            timeid INTEGER NOT NULL,
            date DATE NOT NULL,
            day SMALLINT NOT NULL,
            month SMALLINT NOT NULL,
            year INTEGER NOT NULL,
            day_name CHARACTER VARYING(10) NOT NULL,
            is_weekend SMALLINT NOT NULL,
            month_name CHARACTER VARYING(10) NOT NULL,
            quarter SMALLINT NOT NULL,
            hour SMALLINT NOT NULL,
            minutes SMALLINT NOT NULL,
            seconds SMALLINT NOT NULL
        );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()
            logger.info("Time_dimension table created successfully.")
    except Exception as e:
        logger.error(f"Failed to create Time_dimension table. Error: {e}")


def create_returns_fact_table(conn, logger):
    sql_query = """
        CREATE TABLE IF NOT EXISTS core.returns_fact (
            return_id SERIAL PRIMARY KEY,
            order_id INTEGER REFERENCES core.Order_dimension(order_id),
            product_id INTEGER REFERENCES core.product_dimension(product_id),
            return_date DATE,
            reason TEXT,
            amount_refunded NUMERIC(10,2)
        );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()
            logger.info("returns_fact table created successfully.")
    except Exception as e:
        logger.error(f"Failed to create returns_fact table. Error: {e}")


def main():
    conn = psycopg2.connect(
        database="warehouse",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )
    logger = intialize_logger()
    create_supplier_dimension_table(conn, logger)
    create_customer_dimension_table(conn, logger)
    create_product_dimension_table(conn, logger)
    create_order_dimension_table(conn, logger)
    create_campaign_dimension_table(conn, logger)
    create_time_dimension_table(conn, logger)
    create_customer_product_ratings_fact_table(conn, logger)
    create_returns_fact_table(conn, logger)
    create_sales_fact_table(conn, logger)
    conn.close()


if __name__ == "__main__":
    main()
