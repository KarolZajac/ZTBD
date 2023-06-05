import psycopg2
import json


class PostgreSQLDataImporter:
    def __init__(self, db_size):
        self.import_done = False
        self.db_size = db_size
        self.connection = psycopg2.connect(
            host="localhost",
            database="yelp_database",
            user="postgres",
            password="karol123"
        )
        self.cursor = self.connection.cursor()
        self.drop_table()
        self.init_table()
        self.import_data()
        self.cursor.close()
        self.connection.close()
        self.init_done_callback()

    def init_done_callback(self):
        self.import_done = True

    def init_table(self):
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
        self.cursor.execute(create_table_query)
        self.connection.commit()

    def drop_table(self):
        drop_table_query = "DROP TABLE IF EXISTS Business;"
        self.cursor.execute(drop_table_query)
        self.connection.commit()

    def import_data(self):

        json_data = []

        with open('yelp_academic_dataset_business.json', 'r', encoding='utf-8') as file:
            for i, line in enumerate(file):
                if i >= self.db_size:
                    break

                # Split the line into individual JSON objects
                json_objects = line.strip().split('}{')

                for json_obj in json_objects:
                    # Add necessary characters to create a valid JSON object
                    if not json_obj.startswith('{'):
                        json_obj = '{' + json_obj
                    if not json_obj.endswith('}'):
                        json_obj = json_obj + '}'

                    # Handle empty objects or fragments
                    if json_obj.strip() == '{}':
                        continue

                    json_data.append(json.loads(json_obj))

                if len(json_data) >= self.db_size:
                    break

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

            self.cursor.execute(
                insert_query,
                (
                    business_id, name, address, city, state, postal_code, latitude,
                    longitude, stars, review_count, is_open, json.dumps(attributes),
                    categories, json.dumps(hours)
                )
            )

        self.connection.commit()
