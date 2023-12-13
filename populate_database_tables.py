from datetime import datetime, timedelta
from random import uniform, random

import psycopg2
from faker import Faker
from psycopg2.extras import execute_values

# below is code for filling mock data in production database tables, your task is to , add create if not exists for
# each table and refine code and put in its own function

fake = Faker()


def fill_customer_mock_data_in_production():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        database="production",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )

    try:
        with conn.cursor() as cursor:
            # Create Customer table if not exists
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Customer (
                    CustomerID SERIAL PRIMARY KEY,
                    FirstName VARCHAR(255),
                    LastName VARCHAR(255),
                    Email VARCHAR(255) UNIQUE,
                    Country VARCHAR(100)
                )
            ''')

            # Generate and insert fake data into Customer table
            CUST_RECORDS_TO_PUT_TABLE = 500  # Adjust the number of entries as needed
            for _ in range(CUST_RECORDS_TO_PUT_TABLE):
                cursor.execute('''
                    INSERT INTO Customer (FirstName, LastName, Email, Country)
                    VALUES (%s, %s, %s, %s)
                ''', (fake.first_name(), fake.last_name(), fake.email(), fake.country()))

        # Commit changes
        conn.commit()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        conn.close()


def fill_category_subcategory_mock_data_in_production():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        database="production",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )

    try:
        with conn.cursor() as cursor:
            # Create Category table if not exists
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Category (
                    CategoryID SERIAL PRIMARY KEY,
                    CategoryName VARCHAR(50) UNIQUE
                )
            ''')

            # Create Subcategory table if not exists
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Subcategory (
                    SubcategoryID SERIAL PRIMARY KEY,
                    SubcategoryName VARCHAR(50),
                    CategoryID INT,
                    FOREIGN KEY (CategoryID) REFERENCES Category(CategoryID)
                )
            ''')

            # List of realistic e-commerce category names
            categories = [
                "Electronics", "Clothing", "Home & Kitchen", "Books", "Sports & Outdoors",
                "Toys & Games", "Beauty & Personal Care", "Health & Household", "Automotive",
                "Tools & Home Improvement", "Office Products", "Grocery", "Pet Supplies",
                "Musical Instruments", "Movies & TV", "Video Games", "Jewelry", "Computers",
                "Shoes", "Watches"
            ]

            # Dictionary mapping categories to subcategories
            subcategory_dict = {
                "Electronics": ["Smartphones", "Laptops", "Headphones", "Cameras", "Wearables"],
                "Clothing": ["T-Shirts", "Dresses", "Jeans", "Sweaters", "Activewear"],
                "Home & Kitchen": ["Furniture", "Cookware", "Bedding", "Appliances", "Decor"],
                "Books": ["Fiction", "Non-Fiction", "Mystery", "Science Fiction", "Biography"],
                "Sports & Outdoors": ["Outdoor Clothing", "Exercise Equipment", "Camping Gear", "Sports Shoes",
                                      "Bicycles"],
                "Toys & Games": ["Board Games", "Action Figures", "Puzzles", "Dolls", "Educational Toys"],
                "Beauty & Personal Care": ["Skincare", "Haircare", "Makeup", "Fragrances", "Personal Hygiene"],
                "Health & Household": ["Vitamins", "Medical Supplies", "Cleaning Products", "Pet Care",
                                       "Health Monitors"],
                "Automotive": ["Car Parts", "Car Accessories", "Oil & Lubricants", "Tools", "Electronics"],
                "Tools & Home Improvement": ["Power Tools", "Hand Tools", "Home Security", "Lighting", "Paint"],
                "Office Products": ["Office Furniture", "Stationery", "Printers", "Computers", "Desk Accessories"],
                "Grocery": ["Fresh Produce", "Beverages", "Snacks", "Canned Goods", "Bakery"],
                "Pet Supplies": ["Dog Food", "Cat Food", "Pet Toys", "Grooming", "Pet Beds"],
                "Musical Instruments": ["Guitars", "Keyboards", "Drums", "Wind Instruments", "DJ Equipment"],
                "Movies & TV": ["Action & Adventure", "Drama", "Comedy", "Science Fiction", "Documentaries"],
                "Video Games": ["Action", "Adventure", "Role-Playing", "Sports", "Simulation"],
                "Jewelry": ["Rings", "Necklaces", "Bracelets", "Earrings", "Watches"],
                "Computers": ["Laptops", "Desktops", "Monitors", "Accessories", "Networking"],
                "Shoes": ["Running Shoes", "Casual Shoes", "Boots", "Sandals", "Athletic Shoes"],
                "Watches": ["Analog Watches", "Digital Watches", "Smartwatches", "Luxury Watches", "Sports Watches"]
            }

            # Generate and insert fake data for categories
            for category in categories:
                cursor.execute('''
                    INSERT INTO Category (CategoryName)
                    VALUES (%s)
                ''', (category,))

            # Generate and insert fake data for subcategories
            for category, subcategories in subcategory_dict.items():
                category_id_query = 'SELECT CategoryID FROM Category WHERE CategoryName = %s'
                cursor.execute(category_id_query, (category,))
                category_id = cursor.fetchone()[0]

                for subcategory in subcategories:
                    cursor.execute('''
                        INSERT INTO Subcategory (SubcategoryName, CategoryID)
                        VALUES (%s, %s)
                    ''', (subcategory, category_id))

        # Commit changes
        conn.commit()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        conn.close()


def fill_product_mock_data_in_production():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        database="production",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )

    try:
        with conn.cursor() as cursor:
            # Create Product table if not exists
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Product (
                    ProductID SERIAL PRIMARY KEY,
                    Name VARCHAR(255),
                    Price DECIMAL(10, 2),
                    Description TEXT,
                    SubCategoryID INT,
                    FOREIGN KEY (SubCategoryID) REFERENCES Subcategory(SubcategoryID)
                )
            ''')

            # Dictionary mapping categories to subcategories
            subcategory_dict = {
                "Electronics": ["Smartphones", "Laptops", "Headphones", "Cameras", "Wearables"],
                "Clothing": ["T-Shirts", "Dresses", "Jeans", "Sweaters", "Activewear"],
                "Home & Kitchen": ["Furniture", "Cookware", "Bedding", "Appliances", "Decor"],
                "Books": ["Fiction", "Non-Fiction", "Mystery", "Science Fiction", "Biography"],
                "Sports & Outdoors": ["Outdoor Clothing", "Exercise Equipment", "Camping Gear", "Sports Shoes",
                                      "Bicycles"],
                "Toys & Games": ["Board Games", "Action Figures", "Puzzles", "Dolls", "Educational Toys"],
                "Beauty & Personal Care": ["Skincare", "Haircare", "Makeup", "Fragrances", "Personal Hygiene"],
                "Health & Household": ["Vitamins", "Medical Supplies", "Cleaning Products", "Pet Care",
                                       "Health Monitors"],
                "Automotive": ["Car Parts", "Car Accessories", "Oil & Lubricants", "Tools", "Electronics"],
                "Tools & Home Improvement": ["Power Tools", "Hand Tools", "Home Security", "Lighting", "Paint"],
                "Office Products": ["Office Furniture", "Stationery", "Printers", "Computers", "Desk Accessories"],
                "Grocery": ["Fresh Produce", "Beverages", "Snacks", "Canned Goods", "Bakery"],
                "Pet Supplies": ["Dog Food", "Cat Food", "Pet Toys", "Grooming", "Pet Beds"],
                "Musical Instruments": ["Guitars", "Keyboards", "Drums", "Wind Instruments", "DJ Equipment"],
                "Movies & TV": ["Action & Adventure", "Drama", "Comedy", "Science Fiction", "Documentaries"],
                "Video Games": ["Action", "Adventure", "Role-Playing", "Sports", "Simulation"],
                "Jewelry": ["Rings", "Necklaces", "Bracelets", "Earrings", "Watches"],
                "Computers": ["Laptops", "Desktops", "Monitors", "Accessories", "Networking"],
                "Shoes": ["Running Shoes", "Casual Shoes", "Boots", "Sandals", "Athletic Shoes"],
                "Watches": ["Analog Watches", "Digital Watches", "Smartwatches", "Luxury Watches", "Sports Watches"]
            }

            # Generate and insert category-specific data for products
            for category, sub_category_list in subcategory_dict.items():
                for sub_category in sub_category_list:
                    # Get SubcategoryID for the current subcategory
                    subcategory_id_query = 'SELECT SubcategoryID FROM Subcategory WHERE SubcategoryName = %s'
                    cursor.execute(subcategory_id_query, (sub_category,))
                    subcategory_id = cursor.fetchone()[0]

                    # Define category-specific product names and descriptions
                    product_names = [
                        f"{sub_category} - Product {i + 1}" for i in range(5)
                    ]
                    product_descriptions = [
                        f"This is a description for {sub_category} - Product {i + 1}" for i in range(5)
                    ]

                    # Insert category-specific product data into the Product table
                    for i in range(5):  # Limit to 5 products per subcategory
                        cursor.execute('''
                            INSERT INTO Product (Name, Price, Description, SubCategoryID)
                            VALUES (%s, %s, %s, %s)
                        ''', (
                            product_names[i], abs(fake.pydecimal(left_digits=3, right_digits=2)),
                            product_descriptions[i],
                            subcategory_id))

        # Commit changes
        conn.commit()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        conn.close()


def fill_marketing_campaign_mock_data_in_production():
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        database="production",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    try:
        # Create MarketingCampaign table if not exists
        create_marketing_campaign_table_query = '''
            CREATE TABLE IF NOT EXISTS MarketingCampaign (
                CampaignID SERIAL PRIMARY KEY,
                CampaignName VARCHAR(255),
                Offer_week INT
            );
        '''
        cursor.execute(create_marketing_campaign_table_query)

        # Commit the changes
        conn.commit()

        # Define holidays and their corresponding offer week numbers
        holidays = [
            {"name": "NewYear", "week_number": 1},
            {"name": "ValentinesDay", "week_number": 6},
            {"name": "Easter", "week_number": 16},
            {"name": "IndependenceDay", "week_number": 27},
            {"name": "Halloween", "week_number": 44},
            {"name": "Thanksgiving", "week_number": 48},
            {"name": "Christmas", "week_number": 52},
            {"name": "LaborDay", "week_number": 36},
            {"name": "MemorialDay", "week_number": 21},
            {"name": "BlackFriday", "week_number": 48},
            {"name": "CyberMonday", "week_number": 49},
            {"name": "Mother'sDay", "week_number": 19},
            {"name": "Father'sDay", "week_number": 24},
            {"name": "BackToSchool", "week_number": 34},
            {"name": "SummerSolstice", "week_number": 25},
            {"name": "GroundhogDay", "week_number": 5}
        ]

        # Populate MarketingCampaign entries aligned with holidays
        for holiday in holidays:
            campaign_name = f"{holiday['name']}Sale"
            offer_week = holiday['week_number']

            # Insert data into MarketingCampaign table
            insert_campaign_query = '''
                INSERT INTO MarketingCampaign (CampaignName, Offer_week)
                VALUES (%s, %s)
                RETURNING CampaignID;
            '''
            cursor.execute(insert_campaign_query, (campaign_name, offer_week))

        # Commit the changes
        conn.commit()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()


def fill_campaign_product_subcategory_mock_data_in_production():
    # SQL query to create CampaignProductCategory table
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS campaign_product_subcategory (
            campaign_product_subcategory_id SERIAL PRIMARY KEY,
            Campaign_ID INT,
            SubCategory_ID INT,
            Discount DECIMAL(3, 2),
            FOREIGN KEY (Campaign_ID) REFERENCES MarketingCampaign(Campaign_ID),
            FOREIGN KEY (SubCategory_ID) REFERENCES SubCategory(SubCategory_ID)
        );
    '''

    # SQL query to insert data with random discounts
    insert_data_query = '''
        INSERT INTO campaign_product_subcategory (Campaign_ID, SubCategory_ID, Discount) VALUES %s;
    '''

    # Connect to the database
    conn = psycopg2.connect(
        database="production",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    try:
        # Create the CampaignProductCategory table
        cursor.execute(create_table_query)
        conn.commit()

        # Generate data with random discounts
        data_values = []
        for campaign_id in range(1, 17):  # Assuming you have 10 campaigns
            for sub_category_id in range(1, 101):  # Assuming you have 20 subcategories
                discount = round(uniform(0.05, 0.25), 2)
                data_values.append((campaign_id, sub_category_id, discount))

        # Insert the generated data
        execute_values(cursor, insert_data_query, data_values)
        conn.commit()

        print("Table created and data inserted successfully!")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the connection
        cursor.close()
        conn.close()


def create_and_fill_supplier():
    # SQL query to create Supplier table
    create_supplier_table_query = '''
        CREATE TABLE IF NOT EXISTS Supplier (
            SupplierID SERIAL PRIMARY KEY,
            SupplierName VARCHAR(255),
            ContactEmail VARCHAR(255),
            Email VARCHAR(255)
        );
    '''

    # SQL query to insert data into Supplier table
    insert_supplier_query = '''
        INSERT INTO Supplier (SupplierName, ContactEmail, Email)
        VALUES %s;
    '''

    # Number of suppliers to generate
    num_suppliers = 50

    # Create Faker instance
    fake = Faker()

    # Generate sample data for suppliers using Faker
    sample_suppliers = [
        (fake.company(), fake.email(), fake.email()) for _ in range(num_suppliers)
    ]

    # Connect to the database
    conn = psycopg2.connect(
        database="production",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    try:
        # Execute the query to create the table
        cursor.execute(create_supplier_table_query)
        conn.commit()

        # Execute the query to insert data
        execute_values(cursor, insert_supplier_query, sample_suppliers, template=None, page_size=100)
        conn.commit()

        print(f"{num_suppliers} suppliers inserted successfully!")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the connection
        cursor.close()
        conn.close()


def create_orders_table(cursor):
    create_orders_table_query = '''
        CREATE TABLE IF NOT EXISTS Orders (
            Order_ID_surrogate SERIAL PRIMARY KEY,
            Order_ID INT UNIQUE,
            Customer_ID INT REFERENCES Customer(Customer_ID),
            Order_Date DATE,
            Campaign_ID INT REFERENCES MarketingCampaign(Campaign_ID),
            Amount INT,
            Payment_method_iD INT REFERENCES PaymentMethod(Payment_method_iD)
        );
    '''
    cursor.execute(create_orders_table_query)
    return cursor


def create_orderitem_table(cursor):
    create_orderitem_table_query = '''
        CREATE TABLE IF NOT EXISTS OrderItem (
            OrderItem_ID SERIAL PRIMARY KEY,
            Order_ID INT REFERENCES Orders(Order_ID),
            Product_ID INT REFERENCES Product(product_id),
            Quantity INT,
            Supplier_ID INT REFERENCES Supplier(supplier_id),
            Subtotal DECIMAL(10, 2),
            Discount DECIMAL(5, 2)
        );
    '''
    cursor.execute(create_orderitem_table_query)
    return cursor


def create_returns_table(cursor):
    create_returns_table_query = '''
        CREATE TABLE IF NOT EXISTS Returns (
            Return_ID SERIAL PRIMARY KEY,
            Order_ID INT,
            Return_Date DATE,
            Reason TEXT
        );
    '''
    cursor.execute(create_returns_table_query)
    return cursor


def insert_into_orderitem(cursor, order_id, product_id, quantity, supplier_id, subtotal, discount):
    insert_orderitem_query = '''
        INSERT INTO OrderItem (Order_ID, Product_ID, Quantity, Supplier_ID, Subtotal, Discount)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING OrderItem_ID;
    '''
    try:
        cursor.execute(insert_orderitem_query, (order_id, product_id, quantity, supplier_id, subtotal, discount))
        cursor.connection.commit()
        return cursor.fetchone()[0]
    except Exception as e:
        cursor.connection.rollback()
        print(f"Error: {e}")


def insert_into_orders(cursor, order_id, customer_id, order_timestamp, campaign_id, order_value, payment_id):
    insert_orders_query = '''
        INSERT INTO Orders (Order_ID, Customer_ID, Order_Date, Campaign_ID, Amount, Payment_method_iD)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING Order_ID_surrogate;
    '''
    try:
        cursor.execute(insert_orders_query,
                       (order_id, customer_id, order_timestamp, campaign_id, order_value, payment_id))
        cursor.connection.commit()
        return cursor.fetchone()[0]
    except Exception as e:
        cursor.connection.rollback()
        print(f"Error: {e}")


def fetch_campaign_data_from_db(cursor):
    cursor.execute("SELECT * FROM MarketingCampaign")
    marketing_campaign_data = cursor.fetchall()
    marketing_campaign_list = []
    for row in marketing_campaign_data:
        campaign_dict = {
            'CampaignID': row[0],
            'CampaignName': row[1],
            'OfferWeek': row[2]
        }
        marketing_campaign_list.append(campaign_dict)
    return marketing_campaign_list


def fetch_product_data_from_db(cursor):
    fetch_product_query = '''
        SELECT Product_ID, Name, Price, Description, SubCategory_ID
        FROM Product;
    '''
    cursor.execute(fetch_product_query)
    product_data = cursor.fetchall()
    product_dict_list = [
        {
            'product_id': row[0],
            'name': row[1],
            'price': row[2],
            'description': row[3],
            'sub_category_id': row[4]
        }
        for row in product_data
    ]
    return product_dict_list


def fetch_campaign_product_subcategory_data_from_db(cursor):
    cursor.execute("SELECT * FROM campaign_product_subcategory")
    campaign_product_subcategory_data = cursor.fetchall()
    campaign_product_subcategory_list = []
    for row in campaign_product_subcategory_data:
        campaign_product_subcategory_dict = {
            'campaign_subcategory_id': row[0],
            'campaign_id': row[1],
            'subcategory_id': row[2],
            'discount': row[3]
        }
        campaign_product_subcategory_list.append(campaign_product_subcategory_dict)
    return campaign_product_subcategory_list


def fetch_campaign_row(marketing_campaign_data, week_number):
    for row in marketing_campaign_data:
        if row['OfferWeek'] == week_number:
            return row
    return None


def fetch_product_row(product_data, product_id):
    for row in product_data:
        if row['product_id'] == product_id:
            return row
    return None


def fetch_campaign_product_subcategory_row(campaign_product_subcategory_data, campaign_id, subcategory_id):
    for row in campaign_product_subcategory_data:
        if row['campaign_id'] == campaign_id and row['subcategory_id'] == subcategory_id:
            return row
    return None


def generate_random_timestamp():
    start_date = datetime(2016, 1, 1)
    end_date = datetime(2022, 12, 31)
    time_difference = (end_date - start_date).total_seconds()
    random_seconds = random.uniform(0, time_difference)
    random_timestamp = start_date + timedelta(seconds=random_seconds)
    return random_timestamp


def insert_mock_data(cursor, marketing_campaign_data, product_data, campaign_product_subcategory_data):
    customer_ids = list(range(1, 501))
    payment_method_ids = list(range(1, 6))

    for order_id in range(1, 50001):
        order_timestamp = generate_random_timestamp()

        # Fetch campaign data
        campaign_row = fetch_campaign_row(marketing_campaign_data, order_timestamp.isocalendar()[1])
        campaign_id = campaign_row['CampaignID'] if campaign_row else None

        # Select a random customer_id
        customer_id = random.choice(customer_ids)

        # Select a random number of products in the order from the range [1, 10]
        products_count_in_order = random.randint(1, 10)
        order_value = 0
        products_insert_query = []

        # Loop through products in the order
        for _ in range(products_count_in_order):
            product_id = random.randint(1, 500)
            product_row = fetch_product_row(product_data, product_id)
            if product_row:
                price = product_row['price']
                sub_category_id = product_row['sub_category_id']
                quantity = random.randint(1, 10)
                subtotal = price * quantity
                supplier_id = random.randint(1, 50)

                if campaign_id is None:
                    discount = 0
                else:
                    campaign_product_subcategory_row = fetch_campaign_product_subcategory_row(
                        campaign_product_subcategory_data, campaign_id, sub_category_id)
                    discount = 0 if campaign_product_subcategory_row is None else campaign_product_subcategory_row[
                        'discount']

                products_insert_query.append(
                    {
                        'order_id': order_id,
                        'product_id': product_id,
                        'quantity': quantity,
                        'supplier_id': supplier_id,
                        'subtotal': subtotal,
                        'discount': discount
                    })
                order_value += subtotal

        # Select a random payment_id
        payment_id = random.choice(payment_method_ids)

        # Insert into Orders
        order_surrogate_key = insert_into_orders(cursor, order_id, customer_id, order_timestamp, campaign_id,
                                                 order_value, payment_id)

        # Insert into OrderItem
        for query in products_insert_query:
            insert_into_orderitem(cursor, **query)

    # Close the cursor and connection
    cursor.close()
    cursor.connection.close()


if __name__ == "__main__":
    conn = psycopg2.connect(
        database="production",
        user="postgres",
        password="swati",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    try:

        # Example usage
        create_and_fill_supplier()

        # Example usage
        fill_campaign_product_subcategory_mock_data_in_production()

        # Example usage
        fill_product_mock_data_in_production()

        # Example usage
        fill_customer_mock_data_in_production()

        # Example usage
        fill_category_subcategory_mock_data_in_production()

        # Example usage
        fill_marketing_campaign_mock_data_in_production()

        # Create tables if not exist
        # create_orders_table(cursor)
        # create_orderitem_table(cursor)
        # create_returns_table(cursor)

        # Fetch data from tables
        marketing_campaign_data = fetch_campaign_data_from_db(cursor)
        product_data = fetch_product_data_from_db(cursor)
        campaign_product_subcategory_data = fetch_campaign_product_subcategory_data_from_db(cursor)

        # Insert mock data
        insert_mock_data(cursor, marketing_campaign_data, product_data, campaign_product_subcategory_data)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()
# hellogfgghfghdyhdthudytrtyhvkhgfdhghfghfhgfhggkhrghrhgghgfgh