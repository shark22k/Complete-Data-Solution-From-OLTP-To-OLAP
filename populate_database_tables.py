from random import uniform
from faker import Faker
from psycopg2.extras import execute_values


def populate_customer_table(conn):
    """
    Populates the customer table with random data.

    Parameters:
    - conn: A psycopg2 connection object to the database.

    Returns:
    None
    """
    # Create a Faker instance
    fake = Faker()
    CUST_RECORDS_TO_PUT_TABLE = 500

    # Generate and insert fake data
    for _ in range(CUST_RECORDS_TO_PUT_TABLE):  # Adjust the number of entries as needed
        conn.cursor.execute('''
            INSERT INTO Customer (FirstName, LastName, Email, Country)
            VALUES (%s, %s, %s, %s)
        ''', (fake.first_name(), fake.last_name(), fake.email(), fake.country()))

    # Commit changes and close the connection
    conn.commit()

    def insert_fake_categories(conn):
        """
        Insert fake categories into the database.

        Parameters:
            conn (Connection): The database connection.

        Returns:
            None
        """
        categories = [
            "Electronics",
            "Clothing",
            "Home & Kitchen",
            "Books",
            "Sports & Outdoors",
            "Toys & Games",
            "Beauty & Personal Care",
            "Health & Household",
            "Automotive",
            "Tools & Home Improvement",
            "Office Products",
            "Grocery",
            "Pet Supplies",
            "Musical Instruments",
            "Movies & TV",
            "Video Games",
            "Jewelry",
            "Computers",
            "Shoes",
            "Watches"
        ]

        # Generate and insert fake data for categories
        for category in categories:
            conn.cursor.execute('''
                INSERT INTO Category (CategoryName)
                VALUES (%s)
            ''', (category,))

    def insert_fake_subcategories(conn):
        """
        Inserts fake subcategories into the database.

        Parameters:
            conn (connection): The database connection object.

        Returns:
            None
        """
        # Dictionary mapping categories to subcategories
        subcategory_dict = {
            "Electronics": ["Smartphones", "Laptops", "Headphones", "Cameras", "Wearables"],
            "Clothing": ["T-Shirts", "Dresses", "Jeans", "Sweaters", "Activewear"],
            "Home & Kitchen": ["Furniture", "Cookware", "Bedding", "Appliances", "Decor"],
            "Books": ["Fiction", "Non-Fiction", "Mystery", "Science Fiction", "Biography"],
            "Sports & Outdoors": ["Outdoor Clothing", "Exercise Equipment", "Camping Gear", "Sports Shoes", "Bicycles"],
            "Toys & Games": ["Board Games", "Action Figures", "Puzzles", "Dolls", "Educational Toys"],
            "Beauty & Personal Care": ["Skincare", "Haircare", "Makeup", "Fragrances", "Personal Hygiene"],
            "Health & Household": ["Vitamins", "Medical Supplies", "Cleaning Products", "Pet Care", "Health Monitors"],
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

        # Generate and insert fake data for subcategories
        for category, subcategories in subcategory_dict.items():
            category_id_query = 'SELECT CategoryID FROM Category WHERE CategoryName = %s'
            conn.cursor.execute(category_id_query, (category,))
            category_id = conn.cursor.fetchone()[0]

            for subcategory in subcategories:
                conn.cursor.execute('''
                    INSERT INTO Subcategory (SubcategoryName, CategoryID)
                    VALUES (%s, %s)
                ''', (subcategory, category_id))


def populate_marketing_campaign_table(conn):
    """
    Populates the MarketingCampaign table with data aligned with various holidays.

    Parameters:
        cursor (Cursor): The database cursor object.

    Returns:
        None
    """
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
        {"name": "GroundhogDay", "week_number": 5},
    ]

    # Populate MarketingCampaign entries aligned with holidays
    for holiday in holidays:
        campaign_name = f"{holiday['name']}Sale"
        offer_week = holiday['week_number']
        insert_campaign_query = '''
            INSERT INTO MarketingCampaign (CampaignName, Offer_week)
            VALUES (%s, %s)
            RETURNING CampaignID;
        '''
        conn.cursor.execute(insert_campaign_query, (campaign_name, offer_week))


def insert_campaign_product_subcategory_data(conn):
    cursor = conn.cursor()

    # SQL query to insert data with random discounts
    insert_data_query = '''
        INSERT INTO campaign_product_subcategory (Campaign_ID, SubCategory_ID, Discount) VALUES %s;
    '''

    try:
        # Generate data with random discounts
        data_values = []
        for campaign_id in range(1, 17):  # Assuming you have 16 campaigns
            for sub_category_id in range(1, 101):  # Assuming you have 100 subcategories
                discount = round(uniform(0.05, 0.25), 2)
                data_values.append((campaign_id, sub_category_id, discount))

        # Insert the generated data
        execute_values(cursor, insert_data_query, data_values)
        conn.commit()

        print("Data inserted successfully!")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the cursor
        cursor.close()
