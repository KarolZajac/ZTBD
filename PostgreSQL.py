from datetime import datetime, timedelta

import psycopg2

from database_test import TestDB


class PostgreSQLDB(TestDB):
    def __init__(self, db_size):
        super().__init__(db_size)
        self.columns_string = "(business_id, name, address, city, state, postal_code, latitude, longitude, stars, " \
                              "review_count, is_open, attributes, categories, hours)"
        self.values_string = "(" + ", ".join(["%s"] * 14) + ")"
        self.connection = psycopg2.connect(
            host="localhost",
            database="yelp_database",
            user="postgres",
            password="karol123"
        )
        self.cursor = self.connection.cursor()

    def test_select_index(self, index: str) -> timedelta:
        clock_start = datetime.now()
        query = "SELECT * FROM Business WHERE business_id = '{index}';".format(index=index)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_select_where(self, key: str, value: str) -> timedelta:
        clock_start = datetime.now()
        query = "SELECT * FROM Business WHERE {column} = '{value}';".format(column=key, value=value)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_insert_1(self) -> timedelta:
        select_query = "SELECT * FROM Business ORDER BY random() LIMIT 1"
        self.cursor.execute(select_query)
        random_record = self.cursor.fetchone()
        clock_start = datetime.now()
        query = "INSERT INTO Business {columns} VALUES {values};".format(columns=self.columns_string,
                                                                         values=self.values_string)
        self.cursor.execute(query, random_record)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_insert_100(self) -> timedelta:
        select_query = "SELECT * FROM Business ORDER BY random() LIMIT 100"
        self.cursor.execute(select_query)
        random_records = self.cursor.fetchall()
        clock_start = datetime.now()
        query = "INSERT INTO Business {columns} VALUES {values};".format(columns=self.columns_string,
                                                                         values=self.values_string)
        self.cursor.executemany(query, random_records)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_delete_index(self, index: str) -> timedelta:
        clock_start = datetime.now()
        query = "DELETE FROM Business WHERE business_id = '{index}';".format(index=index)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_delete_where(self, key: str, value: str) -> timedelta:
        clock_start = datetime.now()
        query = "DELETE FROM Business WHERE {column} = '{value}';".format(column=key, value=value)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_update_index(self, search_index: str, update_key: str, update_value: str) -> timedelta:
        clock_start = datetime.now()
        query = "UPDATE Business SET {column} = '{value}' WHERE business_id = '{index}';".format(index=search_index,
                                                                                                  column=update_key,
                                                                                                  value=update_value)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_update_where(self, search_key: str, search_value: str, update_key: str, update_value: str) -> timedelta:
        clock_start = datetime.now()
        query = "UPDATE Business SET {column} = '{value}' WHERE {search_column} = '{index}';".format(
            search_column=search_key,
            index=search_value,
            column=update_key,
            value=update_value)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start
