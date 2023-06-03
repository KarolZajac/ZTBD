import psycopg2
import json

connection = psycopg2.connect(
    host="localhost",
    database="yelp_database",
    user="postgres",
    password="karol123"
)

cursor = connection.cursor()

create_table_query = '''
CREATE TABLE Business (
    business_id VARCHAR(30),
    name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    latitude FLOAT,
    longitude FLOAT,
    stars FLOAT,
    review_count INTEGER,
    is_open INTEGER,
    attributes TEXT,
    categories TEXT,
    hours TEXT
);


'''
cursor.execute(create_table_query)
connection.commit()

json_data = [json.loads(line)
        for line in open('yelp_academic_dataset_business.json', 'r', encoding='utf-8')]

for item in json_data:
    business_id = item.get('business_id')
    name = item.get('name')
    address = item.get('address')
    city = item.get('city')
    state = item.get('state')
    postal_code = item.get('postal_code')
    latitude = item.get('latitude')
    longitude = item.get('longitude')
    stars = item.get('stars')
    review_count = item.get('review_count')
    is_open = item.get('is_open')
    attributes = item.get('attributes')
    categories = item.get('categories')
    hours = item.get('hours')

    insert_query = """
        INSERT INTO Business (
            business_id, name, address, city, state, postal_code, latitude,
            longitude, stars, review_count, is_open, attributes, categories, hours
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    cursor.execute(
        insert_query,
        (
            business_id, name, address, city, state, postal_code, latitude,
            longitude, stars, review_count, is_open, json.dumps(attributes),
            categories, json.dumps(hours)
        )
    )

connection.commit()

cursor.close()
connection.close()
