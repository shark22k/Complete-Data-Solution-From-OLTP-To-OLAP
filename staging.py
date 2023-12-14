import psycopg2
import json
import logging
import os
from psycopg2 import OperationalError


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


def cascade_truncate_tables():
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
        print(f"Error connecting to the data warehouse: {e}")

    finally:
        # Close cursor and connection
        warehouse_cursor.close()
        warehouse_conn.close()


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
                logger.info("Delta load for location completed successfully. rows inserted {len(rows)}")

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
        # Connect to the production database,
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

            # Commit changes
            warehouse_conn.commit()

            # Write the updated last extracted campaign_id to the JSON file
            write_last_extracted_campaign_id(last_extracted_campaign_id)

            # Log success
            logger.info("Delta load for marketing_campaigns completed successfully. Records inserted: {len(records)}")

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

            # Commit changes
            warehouse_conn.commit()

            # Write the updated last extracted rating_id to the JSON file
            write_last_extracted_rating_id(last_extracted_rating_id)

            # Log success
            logger.info(
                f"Delta load for customer_product_ratings completed successfully. Records inserted: {len(records)}")

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


def main():
    testing_flag = False
    if testing_flag:
        cascade_truncate_tables()
    else:
        # load etl path
        ETL_LOAD_FOLDER = load_etl_path()
        # setting up a log
        logger = intialize_logger()
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


if __name__ == "__main__":
    main()
