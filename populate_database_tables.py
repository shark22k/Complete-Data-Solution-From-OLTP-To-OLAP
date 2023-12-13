from random import uniform
from faker import Faker
from psycopg2.extras import execute_values
# below is code for filling mock data in production database tables, your task is to , add create if not exists for
# each table and refine code and put in its own function


import psycopg2
from faker import Faker

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
