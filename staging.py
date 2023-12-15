import psycopg2
import json
import logging
import os
from psycopg2 import OperationalError, sql
import psycopg2
from psycopg2 import sql
from psycopg2 import IntegrityError, DataError


# setup a etl path
def load_etl_path():
    """
    Load the ETL path from the environment variable 'ETL_LOAD_PATH'.

    Returns:
        str: The value of the 'ETL_LOAD_PATH' environment variable.
    """
    variable_name = 'ETL_LOAD_PATH'
    value = os.environ.get(variable_name)
    return value


# set up a logger
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
    error_handler = logging.FileHandler('delta_load.log')
    error_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
    error_handler.setFormatter(error_formatter)
    logger.addHandler(error_handler)

    # Handler for info messages
    info_handler = logging.FileHandler('delta_load.log')
    info_handler.setLevel(logging.INFO)
    info_formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
    info_handler.setFormatter(info_formatter)
    logger.addHandler(info_handler)
    return logger


def cascade_truncate_tables_staging(logger):
    warehouse_conn, warehouse_cursor = None, None
    try:
        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()

        # Truncate tables in a specific order to avoid foreign key constraint errors
        tables = [
            "staging.customer_product_ratings",
            "staging.orderitem",
            "staging.location",
            "staging.category",
            "staging.supplier",
            "staging.payment_method",
            "staging.subcategory",
            "staging.product",
            "staging.customer",
            "staging.marketing_campaigns"
        ]

        for table in tables:
            warehouse_cursor.execute(f"TRUNCATE TABLE {table} CASCADE")

        # Commit the changes to the data warehouse
        warehouse_conn.commit()

    except OperationalError as e:
        logger.error(f"Error connecting to the data warehouse: {e}")

    finally:
        # Close cursor and connection
        warehouse_cursor.close()
        warehouse_conn.close()
        logger.info("Tables truncated successfully")


def cascade_truncate_tables_core(logger):
    warehouse_conn, warehouse_cursor = None, None
    try:
        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()

        # Truncate tables in a specific order to avoid foreign key constraint errors
        tables = [
            "core.Sales_fact",
            "core.supplier_dimension",
            "core.Order_dimension",
            "core.campaign_dimension",
            "core.customer_dimension",
            "core.product_dimension",
            "core.time_dimension",
            "core.returns_fact"
        ]

        for table in tables:
            warehouse_cursor.execute(f"TRUNCATE TABLE {table} CASCADE")

        # Commit the changes to the data warehouse
        warehouse_conn.commit()

    except OperationalError as e:
        logger.error(f"Error connecting to the data warehouse: {e}")

    finally:
        # Close cursor and connection
        warehouse_cursor.close()
        warehouse_conn.close()
        logger.info("Tables truncated successfully")


# delta load location table
def perform_delta_load_location(ETL_LOAD_FOLDER, logger):
    """
    Performs a delta load of the location data from the production database to the data warehouse.

    Parameters:
    - ETL_LOAD_FOLDER (str): The folder path where the last_location_id.json file is located.
    - logger (logging.Logger): The logger object used for logging.

    Returns:
    - None
    """
    production_conn, warehouse_conn = None, None

    def save_last_location_id(last_location_id):
        with open(os.path.join(ETL_LOAD_FOLDER, 'last_location_id.json'), 'w') as file:
            json.dump({'last_location_id': last_location_id}, file)

    def load_last_location_id():
        try:
            with open(os.path.join(ETL_LOAD_FOLDER, 'last_location_id.json'), 'r') as file:
                data = json.load(file)
                return data.get('last_location_id', 0)
        except FileNotFoundError:
            return 0

    try:
        # Connect to the production database (location)
        production_conn = psycopg2.connect(
            database="production",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the production database
        production_cursor = production_conn.cursor()

        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()
        try:
            # Capture the last extracted location_id
            last_extracted_location_id = load_last_location_id()

            production_cursor.execute("""
                SELECT
                    location_id,
                    latitude,
                    longitude,
                    country,
                    state,
                    city
                FROM public.location
                WHERE location_id > %s
            """, (last_extracted_location_id,))

            rows = production_cursor.fetchall()

            if rows:
                warehouse_cursor.executemany("""
                    INSERT INTO staging.location (location_id, latitude, longitude, country, state, city)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in rows])

                # Get the maximum location_id from the production data
                max_location_id = max(row[0] for row in rows)

                # Save the last extracted location_id to the file
                save_last_location_id(max_location_id)

                # Commit the changes to the data warehouse
                warehouse_conn.commit()

                # Log success
                logger.info(f"Delta load for location completed successfully. rows inserted {len(rows)}")

        except Exception as e:
            # Log the error
            logger.error(f"Error performing delta load for location: {e}")

            # Rollback changes
            warehouse_conn.rollback()

        finally:
            # Close cursors
            production_cursor.close()
            warehouse_cursor.close()

    except OperationalError as e:
        # Log the error
        logger.error(f"Error connecting to the databases: {e}")

    finally:
        # Close connections
        if production_conn is not None:
            production_conn.close()
        if warehouse_conn is not None:
            warehouse_conn.close()


# delta load category table
def perform_delta_load_category(ETL_LOAD_FOLDER, logger):
    """
    Performs a delta load for the category table.

    Args:
        ETL_LOAD_FOLDER (str): The folder path where the ETL load files are stored.
        logger (Logger): The logger object used for logging.

    Returns:
        None
    """
    production_conn, warehouse_conn = None, None

    def read_last_extracted_category_id():
        try:
            with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_category_id.json'), 'r') as file:
                data = json.load(file)
                return data.get('last_extracted_category_id', 0)
        except FileNotFoundError:
            return 0

    def write_last_extracted_category_id(last_extracted_category_id):
        data = {'last_extracted_category_id': last_extracted_category_id}
        with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_category_id.json'), 'w') as file:
            json.dump(data, file)

    try:
        # Connect to the production database
        production_conn = psycopg2.connect(
            database="production",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the production database
        production_cursor = production_conn.cursor()

        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()

        try:
            # Read the last extracted category_id from the JSON file
            last_extracted_category_id = read_last_extracted_category_id()

            production_cursor.execute("""
                SELECT
                    category_id,
                    category_name
                FROM public.category
                WHERE category_id > %s
            """, (last_extracted_category_id,))

            # Fetch all records
            records = production_cursor.fetchall()

            if not records:
                # Log a message indicating no new records
                logger.info("No new records to load for category.")
            else:
                # Insert all records into the data warehouse
                warehouse_cursor.executemany("""
                    INSERT INTO staging.category (category_id, category_name)
                    VALUES (%s, %s)
                """, records)

                # Update the last extracted category_id
                last_extracted_category_id = max(record[0] for record in records)

                # Commit changes
                warehouse_conn.commit()

                # Log the number of new records inserted
                logger.info(f"Delta load for category completed successfully. {len(records)} new records inserted.")

            # Write the updated last extracted category_id to the JSON file
            write_last_extracted_category_id(last_extracted_category_id)

        except Exception as e:
            # Log the error
            logger.error(f"Error performing delta load for category: {e}")

            # Rollback changes
            warehouse_conn.rollback()

        finally:
            # Close cursors
            production_cursor.close()
            warehouse_cursor.close()

    except OperationalError as e:
        # Log the error
        logger.error(f"Error connecting to the databases: {e}")

    finally:
        # Close connections
        if production_conn is not None:
            production_conn.close()
        if warehouse_conn is not None:
            warehouse_conn.close()


#  delta load supplier table
def perform_delta_load_supplier(ETL_LOAD_FOLDER, logger):
    """
    Performs a delta load for the supplier data from the production database to the data warehouse(staging).

    Args:
        ETL_LOAD_FOLDER (str): The path to the folder where ETL data is stored.
        logger: The logger object for logging messages.

    Returns:
        None
    """
    production_conn, warehouse_conn = None, None

    def read_last_extracted_supplier_id():
        try:
            with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_supplier_id.json'), 'r') as file:
                data = json.load(file)
                return data.get('last_extracted_supplier_id', 0)
        except FileNotFoundError:
            return 0

    def write_last_extracted_supplier_id(last_extracted_supplier_id):
        data = {'last_extracted_supplier_id': last_extracted_supplier_id}
        with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_supplier_id.json'), 'w') as file:
            json.dump(data, file)

    try:
        # Connect to the production database
        production_conn = psycopg2.connect(
            database="production",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the production database
        production_cursor = production_conn.cursor()

        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()

        try:
            # Read the last extracted supplier_id from the JSON file
            last_extracted_supplier_id = read_last_extracted_supplier_id()

            production_cursor.execute("""
                SELECT
                    supplier_id,
                    supplier_name,
                    email
                FROM public.supplier
                WHERE supplier_id > %s
            """, (last_extracted_supplier_id,))

            # Fetch all records
            records = production_cursor.fetchall()

            if not records:
                # Log a message indicating no new records
                logger.info("No new records to load for supplier.")
            else:
                # Insert all records into the data warehouse
                warehouse_cursor.executemany("""
                    INSERT INTO staging.supplier (supplier_id, supplier_name, email)
                    VALUES (%s, %s, %s)
                """, records)

                # Update the last extracted supplier_id
                last_extracted_supplier_id = max(record[0] for record in records)

                # Commit changes
                warehouse_conn.commit()

                # Log the number of new records inserted
                logger.info(f"Delta load for supplier completed successfully. {len(records)} new records inserted.")

            # Write the updated last extracted supplier_id to the JSON file
            write_last_extracted_supplier_id(last_extracted_supplier_id)

        except Exception as e:
            # Log the error
            logger.error(f"Error performing delta load for supplier: {e}")

            # Rollback changes
            warehouse_conn.rollback()

        finally:
            # Close cursors
            production_cursor.close()
            warehouse_cursor.close()

    except OperationalError as e:
        # Log the error
        logger.error(f"Error connecting to the databases: {e}")

    finally:
        # Close connections
        if production_conn is not None:
            production_conn.close()
        if warehouse_conn is not None:
            warehouse_conn.close()


#  delta load payment_method table
def perform_delta_load_payment_method(ETL_LOAD_FOLDER, logger):
    """
    Perform a delta load for the payment_method table.

    Args:
        ETL_LOAD_FOLDER (str): The path to the ETL load folder.
        logger (Logger): The logger object for logging.

    Returns:
        None
    """
    production_conn, warehouse_conn = None, None

    def read_last_extracted_payment_method_id():
        try:
            with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_payment_method_id.json'), 'r') as file:
                data = json.load(file)
                return data.get('last_extracted_payment_method_id', 0)
        except FileNotFoundError:
            return 0

    def write_last_extracted_payment_method_id(last_extracted_payment_method_id):
        data = {'last_extracted_payment_method_id': last_extracted_payment_method_id}
        with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_payment_method_id.json'), 'w') as file:
            json.dump(data, file)

    try:
        # Connect to the production database
        production_conn = psycopg2.connect(
            database="production",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the production database
        production_cursor = production_conn.cursor()

        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()

        try:
            # Read the last extracted payment_method_id from the JSON file
            last_extracted_payment_method_id = read_last_extracted_payment_method_id()

            production_cursor.execute("""
                SELECT
                    payment_method_id,
                    payment_method
                FROM public.payment_method
                WHERE payment_method_id > %s
            """, (last_extracted_payment_method_id,))

            # Fetch all records
            records = production_cursor.fetchall()

            if not records:
                # Log a message indicating no new records
                logger.info("No new records to load for payment_method.")
            else:
                # Insert all records into the data warehouse
                warehouse_cursor.executemany("""
                    INSERT INTO staging.payment_method (payment_method_id, payment_method)
                    VALUES (%s, %s)
                """, records)

                # Update the last extracted payment_method_id
                last_extracted_payment_method_id = max(record[0] for record in records)

                # Commit changes
                warehouse_conn.commit()

                # Log the number of new records inserted
                logger.info(
                    f"Delta load for payment_method completed successfully. {len(records)} new records inserted.")

            # Write the updated last extracted payment_method_id to the JSON file
            write_last_extracted_payment_method_id(last_extracted_payment_method_id)

        except Exception as e:
            # Log the error
            logger.error(f"Error performing delta load for payment_method: {e}")

            # Rollback changes
            warehouse_conn.rollback()

        finally:
            # Close cursors
            production_cursor.close()
            warehouse_cursor.close()

    except OperationalError as e:
        # Log the error
        logger.error(f"Error connecting to the databases: {e}")

    finally:
        # Close connections
        if production_conn is not None:
            production_conn.close()
        if warehouse_conn is not None:
            warehouse_conn.close()


# delta load subcategory table
def perform_delta_load_subcategory(ETL_LOAD_FOLDER, logger):
    """
    Perform a delta load for the subcategory table.

    Args:
        ETL_LOAD_FOLDER (str): The path to the ETL load folder.
        logger (Logger): The logger object for logging.

    Returns:
        None
    """
    production_conn, warehouse_conn = None, None

    def read_last_extracted_subcategory_id():
        try:
            with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_subcategory_id.json'), 'r') as file:
                data = json.load(file)
                return data.get('last_extracted_subcategory_id', 0)
        except FileNotFoundError:
            return 0

    def write_last_extracted_subcategory_id(last_extracted_subcategory_id):
        data = {'last_extracted_subcategory_id': last_extracted_subcategory_id}
        with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_subcategory_id.json'), 'w') as file:
            json.dump(data, file)

    try:
        # Connect to the production database
        production_conn = psycopg2.connect(
            database="production",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the production database
        production_cursor = production_conn.cursor()

        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()

        try:
            # Read the last extracted subcategory_id from the JSON file
            last_extracted_subcategory_id = read_last_extracted_subcategory_id()

            production_cursor.execute("""
                SELECT
                    subcategory_id,
                    subcategory_name,
                    category_id
                FROM public.subcategory
                WHERE subcategory_id > %s
            """, (last_extracted_subcategory_id,))

            # Fetch all records
            records = production_cursor.fetchall()

            if not records:
                # Log a message indicating no new records
                logger.info("No new records to load for subcategory.")
            else:
                # Insert all records into the data warehouse
                warehouse_cursor.executemany("""
                    INSERT INTO staging.subcategory (subcategory_id, subcategory_name, category_id)
                    VALUES (%s, %s, %s)
                """, records)

                # Update the last extracted subcategory_id
                last_extracted_subcategory_id = max(record[0] for record in records)

                # Commit changes
                warehouse_conn.commit()

                # Log the number of new records inserted
                logger.info(f"Delta load for subcategory completed successfully. {len(records)} new records inserted.")

            # Write the updated last extracted subcategory_id to the JSON file
            write_last_extracted_subcategory_id(last_extracted_subcategory_id)

        except Exception as e:
            # Log the error
            logger.error(f"Error performing delta load for subcategory: {e}")

            # Rollback changes
            warehouse_conn.rollback()

        finally:
            # Close cursors
            production_cursor.close()
            warehouse_cursor.close()

    except OperationalError as e:
        # Log the error
        logger.error(f"Error connecting to the databases: {e}")

    finally:
        # Close connections
        if production_conn is not None:
            production_conn.close()
        if warehouse_conn is not None:
            warehouse_conn.close()


# delta load product table
def perform_delta_load_product(ETL_LOAD_FOLDER, logger):
    """
    Perform a delta load for the product table.

    Args:
        ETL_LOAD_FOLDER (str): The path to the ETL load folder.
        logger (Logger): The logger object for logging.

    Returns:
        None
    """
    production_conn, warehouse_conn = None, None

    def read_last_extracted_product_id():
        try:
            with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_product_id.json'), 'r') as file:
                data = json.load(file)
                return data.get('last_extracted_product_id', 0)
        except FileNotFoundError:
            return 0

    def write_last_extracted_product_id(last_extracted_product_id):
        data = {'last_extracted_product_id': last_extracted_product_id}
        with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_product_id.json'), 'w') as file:
            json.dump(data, file)

    try:
        # Connect to the production database
        production_conn = psycopg2.connect(
            database="production",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the production database
        production_cursor = production_conn.cursor()

        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()

        try:
            # Read the last extracted product_id from the JSON file
            last_extracted_product_id = read_last_extracted_product_id()

            production_cursor.execute("""
                SELECT
                    product_id,
                    name,
                    price,
                    description,
                    subcategory_id
                FROM public.product
                WHERE product_id > %s
            """, (last_extracted_product_id,))

            # Fetch all records
            records = production_cursor.fetchall()

            if not records:
                # Log a message indicating no new records
                logger.info("No new records to load for product.")
            else:
                # Insert all records into the data warehouse
                warehouse_cursor.executemany("""
                    INSERT INTO staging.product (product_id, name, price, description, subcategory_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, records)

                # Update the last extracted product_id
                last_extracted_product_id = max(record[0] for record in records)

                # Commit changes
                warehouse_conn.commit()

                # Log the number of new records inserted
                logger.info(f"Delta load for product completed successfully. {len(records)} new records inserted.")

            # Write the updated last extracted product_id to the JSON file
            write_last_extracted_product_id(last_extracted_product_id)

        except Exception as e:
            # Log the error
            logger.error(f"Error performing delta load for product: {e}")

            # Rollback changes
            warehouse_conn.rollback()

        finally:
            # Close cursors
            production_cursor.close()
            warehouse_cursor.close()

    except OperationalError as e:
        # Log the error
        logger.error(f"Error connecting to the databases: {e}")

    finally:
        # Close connections
        if production_conn is not None:
            production_conn.close()
        if warehouse_conn is not None:
            warehouse_conn.close()


# delta load customer table
def perform_delta_load_customer(ETL_LOAD_FOLDER, logger):
    """
    Perform a delta load for the customer table.

    Args:
        ETL_LOAD_FOLDER (str): The path to the ETL load folder.
        logger (Logger): The logger object for logging.

    Returns:
        None
    """
    production_conn, warehouse_conn = None, None

    def read_last_extracted_customer_id():
        try:
            with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_customer_id.json'), 'r') as file:
                data = json.load(file)
                return data.get('last_extracted_customer_id', 0)
        except FileNotFoundError:
            return 0

    def write_last_extracted_customer_id(last_extracted_customer_id):
        data = {'last_extracted_customer_id': last_extracted_customer_id}
        with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_customer_id.json'), 'w') as file:
            json.dump(data, file)

    try:
        # Connect to the production database
        production_conn = psycopg2.connect(
            database="production",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the production database
        production_cursor = production_conn.cursor()

        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()

        try:
            # Read the last extracted customer_id from the JSON file
            last_extracted_customer_id = read_last_extracted_customer_id()

            production_cursor.execute("""
                SELECT
                    customer_id,
                    first_name,
                    last_name,
                    email,
                    location_id
                FROM public.customer
                WHERE customer_id > %s
            """, (last_extracted_customer_id,))

            # Fetch all records
            records = production_cursor.fetchall()

            if not records:
                # Log a message indicating no new records
                logger.info("No new records to load for customer.")
            else:
                # Insert all records into the data warehouse
                warehouse_cursor.executemany("""
                    INSERT INTO staging.customer (customer_id, first_name, last_name, email, location_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, records)

                # Update the last extracted customer_id
                last_extracted_customer_id = max(record[0] for record in records)

                # Commit changes
                warehouse_conn.commit()

                # Log the number of new records inserted
                logger.info(f"Delta load for customer completed successfully. {len(records)} new records inserted.")

            # Write the updated last extracted customer_id to the JSON file
            write_last_extracted_customer_id(last_extracted_customer_id)

        except Exception as e:
            # Log the error
            logger.error(f"Error performing delta load for customer: {e}")

            # Rollback changes
            warehouse_conn.rollback()

        finally:
            # Close cursors
            production_cursor.close()
            warehouse_cursor.close()

    except OperationalError as e:
        # Log the error
        logger.error(f"Error connecting to the databases: {e}")

    finally:
        # Close connections
        if production_conn is not None:
            production_conn.close()
        if warehouse_conn is not None:
            warehouse_conn.close()


# delta load marketing campaign table
def perform_delta_load_marketing_campaigns(ETL_LOAD_FOLDER, logger):
    """
    Perform a delta load of marketing campaigns from the production database to the data warehouse.

    Args:
        ETL_LOAD_FOLDER (str): The folder path where the ETL load files are stored.
        logger (Logger): The logger object used for logging messages.

    Returns:
        None
    """
    production_conn, warehouse_conn = None, None

    def read_last_extracted_campaign_id():
        try:
            with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_campaign_id.json'), 'r') as file:
                data = json.load(file)
                return data.get('last_extracted_campaign_id', 0)
        except FileNotFoundError:
            return 0

    def write_last_extracted_campaign_id(last_extracted_campaign_id):
        data = {'last_extracted_campaign_id': last_extracted_campaign_id}
        with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_campaign_id.json'), 'w') as file:
            json.dump(data, file)

    try:
        # Connect to the production database
        production_conn = psycopg2.connect(
            database="production",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the production database
        production_cursor = production_conn.cursor()

        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()

        try:
            # Read the last extracted campaign_id from the JSON file
            last_extracted_campaign_id = read_last_extracted_campaign_id()

            production_cursor.execute("""
                SELECT
                    campaign_id,
                    campaign_name,
                    offer_week
                FROM public.marketing_campaigns
                WHERE campaign_id > %s
            """, (last_extracted_campaign_id,))

            # Fetch all records
            records = production_cursor.fetchall()

            # Insert all records into the data warehouse
            warehouse_cursor.executemany("""
                INSERT INTO staging.marketing_campaigns (campaign_id, campaign_name, offer_week)
                VALUES (%s, %s, %s)
            """, records)

            # Update the last extracted campaign_id
            if records:
                last_extracted_campaign_id = max(record[0] for record in records)
                logger.info(
                    f"Delta load for marketing_campaigns completed successfully. Records inserted: {len(records)}")
            else:
                logger.info("No new records to load for marketing_campaigns")
            # Commit changes
            warehouse_conn.commit()

            # Write the updated last extracted campaign_id to the JSON file
            write_last_extracted_campaign_id(last_extracted_campaign_id)

            # Log success

        except Exception as e:
            # Log the error
            logger.error(f"Error performing delta load for marketing_campaigns: {e}")

            # Rollback changes
            warehouse_conn.rollback()

        finally:
            # Close cursors
            production_cursor.close()
            warehouse_cursor.close()

    except OperationalError as e:
        # Log the error
        logger.error(f"Error connecting to the databases: {e}")

    finally:
        # Close connections
        if production_conn is not None:
            production_conn.close()
        if warehouse_conn is not None:
            warehouse_conn.close()


def perform_delta_load_customer_product_ratings(ETL_LOAD_FOLDER, logger):
    """
    Perform a delta load of customer product ratings from the production database to the data warehouse.

    Args:
        ETL_LOAD_FOLDER (str): The folder path where the ETL load files are stored.
        logger (Logger): The logger object used for logging messages.

    Returns:
        None
    """
    production_conn, warehouse_conn = None, None

    def read_last_extracted_rating_id():
        try:
            with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_rating_id.json'), 'r') as file:
                data = json.load(file)
                return data.get('last_extracted_rating_id', 0)
        except FileNotFoundError:
            return 0

    def write_last_extracted_rating_id(last_extracted_rating_id):
        data = {'last_extracted_rating_id': last_extracted_rating_id}
        with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_rating_id.json'), 'w') as file:
            json.dump(data, file)

    try:
        # Connect to the production database
        production_conn = psycopg2.connect(
            database="production",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the production database
        production_cursor = production_conn.cursor()

        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()

        try:
            # Read the last extracted rating_id from the JSON file
            last_extracted_rating_id = read_last_extracted_rating_id()

            production_cursor.execute("""
                SELECT
                    customerproductrating_id,
                    customer_id,
                    product_id,
                    ratings,
                    review,
                    sentiment
                FROM public.customer_product_ratings
                WHERE customerproductrating_id > %s
            """, (last_extracted_rating_id,))

            # Fetch all records
            records = production_cursor.fetchall()

            # Insert all records into the data warehouse
            warehouse_cursor.executemany("""
                INSERT INTO staging.customer_product_ratings (customerproductrating_id, customer_id, product_id, ratings, review, sentiment)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, records)

            # Update the last extracted rating_id
            if records:
                last_extracted_rating_id = max(record[0] for record in records)

                # Log success
                logger.info(
                    f"Delta load for customer_product_ratings completed successfully. Records inserted: {len(records)}")
            else:
                logger.info("No new records to load for customer_product_ratings")
            # Commit changes
            warehouse_conn.commit()

            # Write the updated last extracted rating_id to the JSON file
            write_last_extracted_rating_id(last_extracted_rating_id)

        except Exception as e:
            # Log the error
            logger.error(f"Error performing delta load for customer_product_ratings: {e}")

            # Rollback changes
            warehouse_conn.rollback()

        finally:
            # Close cursors
            production_cursor.close()
            warehouse_cursor.close()

    except OperationalError as e:
        # Log the error
        logger.error(f"Error connecting to the databases: {e}")

    finally:
        # Close connections
        if production_conn is not None:
            production_conn.close()
        if warehouse_conn is not None:
            warehouse_conn.close()


def perform_delta_load_orders(ETL_LOAD_FOLDER, logger):
    """
    Perform a delta load for the orders table.

    Args:
        ETL_LOAD_FOLDER (str): The path to the ETL load folder.
        logger (Logger): The logger object for logging.

    Returns:
        None
    """
    production_conn, warehouse_conn = None, None

    def read_last_extracted_order_id():
        try:
            with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_order_id.json'), 'r') as file:
                data = json.load(file)
                return data.get('last_extracted_order_id', 0)
        except FileNotFoundError:
            return 0

    def write_last_extracted_order_id(last_extracted_order_id):
        data = {'last_extracted_order_id': last_extracted_order_id}
        with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_order_id.json'), 'w') as file:
            json.dump(data, file)

    try:
        # Connect to the production database
        production_conn = psycopg2.connect(
            database="production",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the production database
        production_cursor = production_conn.cursor()

        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()

        try:
            # Read the last extracted order_id from the JSON file
            last_extracted_order_id = read_last_extracted_order_id()

            production_cursor.execute("""
                SELECT
                    order_id_surrogate,
                    order_id,
                    customer_id,
                    order_timestamp,
                    campaign_id,
                    amount,
                    payment_method_id
                FROM public.orders
                WHERE order_id_surrogate > %s
            """, (last_extracted_order_id,))

            # Fetch all records
            records = production_cursor.fetchall()

            if not records:
                # Log a message indicating no new records
                logger.info("No new records to load for orders.")
            else:
                # Insert all records into the data warehouse
                warehouse_cursor.executemany("""
                    INSERT INTO staging.orders (order_id_surrogate, order_id, customer_id, order_timestamp, campaign_id,
                    amount, payment_method_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, records)

                # Update the last extracted order_id
                last_extracted_order_id = max(record[1] for record in records)

                # Commit changes
                warehouse_conn.commit()

                # Log the number of new records inserted
                logger.info(f"Delta load for orders completed successfully. {len(records)} new records inserted.")

            # Write the updated last extracted order_id to the JSON file
            write_last_extracted_order_id(last_extracted_order_id)

        except Exception as e:
            # Log the error
            logger.error(f"Error performing delta load for orders: {e}")

            # Rollback changes
            warehouse_conn.rollback()

        finally:
            # Close cursors
            production_cursor.close()
            warehouse_cursor.close()

    except OperationalError as e:
        # Log the error
        logger.error(f"Error connecting to the databases: {e}")

    finally:
        # Close connections
        if production_conn is not None:
            production_conn.close()
        if warehouse_conn is not None:
            warehouse_conn.close()


def perform_delta_load_orderitem(ETL_LOAD_FOLDER, logger):
    """
    Perform a delta load for the orderitem table.

    Args:
        ETL_LOAD_FOLDER (str): The path to the ETL load folder.
        logger (Logger): The logger object for logging.

    Returns:
        None
    """
    production_conn, warehouse_conn = None, None

    def read_last_extracted_orderitem_id():
        try:
            with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_orderitem_id.json'), 'r') as file:
                data = json.load(file)
                return data.get('last_extracted_orderitem_id', 0)
        except FileNotFoundError:
            return 0

    def write_last_extracted_orderitem_id(last_extracted_orderitem_id):
        data = {'last_extracted_orderitem_id': last_extracted_orderitem_id}
        with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_orderitem_id.json'), 'w') as file:
            json.dump(data, file)

    try:
        # Connect to the production database (orderitem)
        production_conn = psycopg2.connect(
            database="production",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the production database
        production_cursor = production_conn.cursor()

        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()

        try:
            # Read the last extracted orderitem_id from the JSON file
            last_extracted_orderitem_id = read_last_extracted_orderitem_id()

            production_cursor.execute("""
                SELECT
                    orderitem_id,
                    order_id,
                    product_id,
                    quantity,
                    supplier_id,
                    subtotal,
                    discount
                FROM public.orderitem
                WHERE orderitem_id > %s
            """, (last_extracted_orderitem_id,))

            # Fetch all records
            records = production_cursor.fetchall()

            if not records:
                # Log a message indicating no new records
                logger.info("No new records to load for orderitem.")
            else:
                # Insert all records into the data warehouse
                warehouse_cursor.executemany("""
                    INSERT INTO staging.orderitem (orderitem_id, order_id, product_id, quantity, supplier_id, subtotal, discount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, records)

                # Update the last extracted orderitem_id
                last_extracted_orderitem_id = max(record[0] for record in records)

                # Commit changes
                warehouse_conn.commit()

                # Log the number of new records inserted
                logger.info(f"Delta load for orderitem completed successfully. {len(records)} new records inserted.")

            # Write the updated last extracted orderitem_id to the JSON file
            write_last_extracted_orderitem_id(last_extracted_orderitem_id)

        except Exception as e:
            # Log the error
            logger.error(f"Error performing delta load for orderitem: {e}")

            # Rollback changes
            warehouse_conn.rollback()

        finally:
            # Close cursors
            production_cursor.close()
            warehouse_cursor.close()

    except OperationalError as e:
        # Log the error
        logger.error(f"Error connecting to the databases: {e}")

    finally:
        # Close connections
        if production_conn is not None:
            production_conn.close()
        if warehouse_conn is not None:
            warehouse_conn.close()


def perform_delta_load_returns(ETL_LOAD_FOLDER, logger):
    """
    Perform a delta load for the returns table.

    Args:
        ETL_LOAD_FOLDER (str): The path to the ETL load folder.
        logger (Logger): The logger object for logging.

    Returns:
        None
    """
    production_conn, warehouse_conn = None, None

    def read_last_extracted_return_id():
        try:
            with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_return_id.json'), 'r') as file:
                data = json.load(file)
                return data.get('last_extracted_return_id', 0)
        except FileNotFoundError:
            return 0

    def write_last_extracted_return_id(last_extracted_return_id):
        data = {'last_extracted_return_id': last_extracted_return_id}
        with open(os.path.join(ETL_LOAD_FOLDER, 'last_extracted_return_id.json'), 'w') as file:
            json.dump(data, file)

    try:
        # Connect to the production database (returns)
        production_conn = psycopg2.connect(
            database="production",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the production database
        production_cursor = production_conn.cursor()

        # Connect to the data warehouse
        warehouse_conn = psycopg2.connect(
            database="warehouse",
            user="postgres",
            password="swati",
            host="localhost",
            port="5432"
        )

        # Create a cursor for the data warehouse
        warehouse_cursor = warehouse_conn.cursor()

        try:
            # Read the last extracted return_id from the JSON file
            last_extracted_return_id = read_last_extracted_return_id()

            production_cursor.execute("""
                SELECT
                    return_id,
                    order_id,
                    product_id,
                    return_date,
                    reason,
                    amount_refunded
                FROM public.returns
                WHERE return_id > %s
            """, (last_extracted_return_id,))

            # Fetch all records
            records = production_cursor.fetchall()

            if not records:
                # Log a message indicating no new records
                logger.info("No new records to load for returns.")
            else:
                # Insert all records into the data warehouse
                warehouse_cursor.executemany("""
                    INSERT INTO staging.returns (return_id, order_id, product_id, return_date, reason, amount_refunded)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, records)

                # Update the last extracted return_id
                last_extracted_return_id = max(record[0] for record in records)

                # Commit changes
                warehouse_conn.commit()

                # Log the number of new records inserted
                logger.info(f"Delta load for returns completed successfully. {len(records)} new records inserted.")

            # Write the updated last extracted return_id to the JSON file
            write_last_extracted_return_id(last_extracted_return_id)

        except Exception as e:
            # Log the error
            logger.error(f"Error performing delta load for returns: {e}")

            # Rollback changes
            warehouse_conn.rollback()

        finally:
            # Close cursors
            production_cursor.close()
            warehouse_cursor.close()

    except OperationalError as e:
        # Log the error
        logger.error(f"Error connecting to the databases: {e}")

    finally:
        # Close connections
        if production_conn is not None:
            production_conn.close()
        if warehouse_conn is not None:
            warehouse_conn.close()


def perform_delta_load_staging(ETL_LOAD_FOLDER, logger):
    # load location table
    perform_delta_load_location(ETL_LOAD_FOLDER, logger)
    # load category table
    perform_delta_load_category(ETL_LOAD_FOLDER, logger)
    # load supplier table
    perform_delta_load_supplier(ETL_LOAD_FOLDER, logger)
    # load payment method table
    perform_delta_load_payment_method(ETL_LOAD_FOLDER, logger)
    # load subcategory table
    perform_delta_load_subcategory(ETL_LOAD_FOLDER, logger)
    # load product table
    perform_delta_load_product(ETL_LOAD_FOLDER, logger)
    # load customer table
    perform_delta_load_customer(ETL_LOAD_FOLDER, logger)
    # load marketing campaign table
    perform_delta_load_marketing_campaigns(ETL_LOAD_FOLDER, logger)
    # load customer_product_ratings table
    perform_delta_load_customer_product_ratings(ETL_LOAD_FOLDER, logger)
    # load orders table
    perform_delta_load_orders(ETL_LOAD_FOLDER, logger)


#####################################################################################################
def delta_core_load_time_dimension(logger):
    """
    Creates time dimension records from the orders in staging table.

    Args:
        conn (connection): Connection to the warehouse database.

    Returns:
        None
    """
    conn = psycopg2.connect(
        database="warehouse",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )
    try:
        with conn.cursor() as cursor:
            # Create Time Dimension Records from Orders with Hierarchy-based time_id
            query = sql.SQL("""
                INSERT INTO core.time_dimension (timeid, date, day, month, year, day_name, is_weekend, month_name, quarter, hour, minutes, seconds)
                SELECT DISTINCT 
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
        conn.commit()
        logger.info("Time Dimension records created successfully")

    except Exception as e:
        logger.error(f"Error connecting to the data warehouse: {e}")

    finally:
        # Close the connection
        conn.close()


def delta_core_load_customer_dimension(logger):
    """
    Fill the customer_dimension table in the warehouse database with customer information.

    Args:
        warehouse_conn: Connection object for the warehouse database.

    Returns:
        None
    """
    warehouse_conn = psycopg2.connect(
        database="warehouse",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )
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

        logger.info("Customer Dimension records filled successfully.")

    except Exception as e:
        logger.error(f"Error connecting to the data warehouse: {e}")

    finally:
        # Close the connection
        warehouse_conn.close()


def delta_core_load_product_dimension(logger):
    """
    Load product dimension data from staging tables into the core.product_dimension table.

    Parameters:
    - logger: The logger object used for logging.

    Returns:
    None
    """
    warehouse_conn = psycopg2.connect(
        database="warehouse",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )

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
        logger.info("Product Dimension records filled successfully.")

    except Exception as e:
        logger.error("Product Dimension not created", e)

    finally:
        # Close the connection
        warehouse_conn.close()


def delta_core_load_campaign_dimension(logger):
    warehouse_conn = psycopg2.connect(
        database="warehouse",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )

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
        logger.info("Campaign Dimension records filled successfully.")

    except Exception as e:
        logger.error("Campaign Dimension, not created", e)

    finally:
        # Close the connection
        warehouse_conn.close()


def delta_core_load_order_dimension(logger):
    warehouse_conn = psycopg2.connect(
        database="warehouse",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )

    try:
        with warehouse_conn.cursor() as cursor:
            # Insert records into core.order_dimension by combining information from orders, payment_method,
            # and customers
            query = sql.SQL("""
                INSERT INTO core.order_dimension (order_id, customer_id, payment_method)
                SELECT
                    o.order_id,
                    o.customer_id,
                    pm.payment_method
                FROM staging.orders o
                JOIN staging.payment_method pm ON o.payment_method_id = pm.payment_method_id
            """)
            cursor.execute(query)

        # Commit the changes
        warehouse_conn.commit()
        logger.info("Order Dimension records filled successfully.")

    except Exception as e:
        logger.error("Order Dimension, not created", e)

    finally:
        # Close the connection
        warehouse_conn.close()


def delta_core_load_supplier_dimension(logger):
    warehouse_conn = psycopg2.connect(
        database="warehouse",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )

    try:
        with warehouse_conn.cursor() as cursor:
            # Insert records into core.supplier_dimension
            query = sql.SQL("""
                INSERT INTO core.supplier_dimension (supplier_id, supplier_name, email)
                SELECT
                    supplier_id,
                    supplier_name,
                    email
                FROM staging.supplier;
            """)
            cursor.execute(query)

        # Commit the changes
        warehouse_conn.commit()
        logger.info("Supplier Dimension records filled successfully.")

    except Exception as e:
        logger.error("Supplier Dimension, not created", e)

    finally:
        # Close the connection
        warehouse_conn.close()


def delta_core_load_sales_fact(logger):
    warehouse_conn = psycopg2.connect(
        database="warehouse",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )
    try:
        with warehouse_conn.cursor() as cursor:

            # Insert records into core.sales_fact by combining information from orderitem and orders
            query = sql.SQL("""
                INSERT INTO core.sales_fact (order_id, time_id, product_id, customer_id, campaign_id, supplier_id, quantity, subtotal, discount_percentage, sales_price)
                SELECT
                    o.order_id,
                    EXTRACT(EPOCH FROM o.order_timestamp)::integer AS time_id,
                    oi.product_id,
                    o.customer_id,
                    COALESCE(o.campaign_id, 0),  -- Replace NULL with 0 using COALESCE
                    oi.supplier_id,
                    oi.quantity,
                    oi.subtotal,
                    oi.discount,
                    (1-oi.discount) * oi.subtotal AS sales_price
                FROM staging.orderitem oi
                JOIN staging.orders o ON oi.order_id = o.order_id;
            """)
            cursor.execute(query)

        # Commit the changes
        warehouse_conn.commit()

        logger.info("Sales Fact records filled successfully.")

    except Exception as e:
        logger.error("sales_fact, not created", e)

    finally:
        # Close the connection
        warehouse_conn.close()


def delta_core_load_returns_fact(logger):
    warehouse_conn = psycopg2.connect(
        database="warehouse",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )
    try:
        with warehouse_conn.cursor() as cursor:
            # Insert records into core.returns_fact
            query = sql.SQL("""
                INSERT INTO core.returns_fact (return_id, order_id, product_id, return_date, reason, amount_refunded)
                SELECT
                    return_id,
                    order_id,
                    product_id,
                    return_date,
                    reason,
                    amount_refunded
                FROM staging.returns;
            """)
            cursor.execute(query)

        # Commit the changes
        warehouse_conn.commit()

        logger.info("Returns Fact records filled successfully.")

    except IntegrityError as integrity_error:
        logger.error("Integrity error: %s", integrity_error)
    except DataError as data_error:
        logger.error("Data error: %s", data_error)
    except psycopg2.Error as e:
        logger.error("Unexpected error: %s", e)

    finally:
        # Close the connection
        warehouse_conn.close()

def delta_core_load_customer_product_ratings_fact(logger):
    warehouse_conn = psycopg2.connect(
        database="warehouse",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )
    try:
        with warehouse_conn.cursor() as cursor:
            # Insert records into core.customer_product_fact
            query = sql.SQL("""
                INSERT INTO core.customer_product_ratings_fact (customerproductrating_id, customer_id, product_id, ratings, review, sentiment)
                SELECT
                    customerproductrating_id,
                    customer_id,
                    product_id,
                    ratings,
                    review,
                    sentiment
                FROM staging.customer_product_ratings;
            """)
            cursor.execute(query)

        # Commit the changes
        warehouse_conn.commit()

        logger.info("Customer Product Fact records filled successfully.")

    except IntegrityError as integrity_error:
        logger.error("Integrity error: %s", integrity_error)
    except DataError as data_error:
        logger.error("Data error: %s", data_error)
    except psycopg2.Error as e:
        logger.error("Unexpected error: %s", e)
    finally:
        # Close the connection
        warehouse_conn.close()


def perform_delta_core_load(logger):
    testing = True
    if testing:
        cascade_truncate_tables_core(logger)
    else:
        # delta_core_load_time_dimension(logger)
        # delta_core_load_customer_dimension(logger)
        # delta_core_load_product_dimension(logger)
        # delta_core_load_campaign_dimension(logger)
        # delta_core_load_order_dimension(logger)
        # delta_core_load_supplier_dimension(logger)
        # delta_core_load_sales_fact(logger)
        # delta_core_load_returns_fact(logger)
        delta_core_load_customer_product_ratings_fact(logger)

def main():
    # load etl path
    ETL_LOAD_FOLDER = load_etl_path()
    # setting up a log
    logger = intialize_logger()
    ############################################################################################
    # perform delta load into staging
    # perform_delta_load_staging(ETL_LOAD_FOLDER, logger)
    ############################################################################################
    # perform core load from staging
    perform_delta_core_load(logger)


if __name__ == "__main__":
    main()
