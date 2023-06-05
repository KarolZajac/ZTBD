from datetime import datetime, timedelta

import psycopg2

from database_test import TestDB
from PSQLDataImporter import PostgreSQLDataImporter


class PostgreSQLDB(TestDB):
    def __init__(self, db_size, dataset):
        super().__init__(db_size)
        self.data_importer = PostgreSQLDataImporter(db_size, dataset)
        print("PostgreSQL data table ready!")

        if dataset == "Yelp":
            self.table_name = "Business"
            self.primary_key = "business_id"
            self.columns_string = "(business_id, name, address, city, state, postal_code, latitude, longitude, stars, " \
                                  "review_count, is_open, attributes, categories, hours)"
            self.values_string = "(" + ", ".join(["%s"] * 14) + ")"
        elif dataset == "IMDB":
            self.table_name = "Movies"
            self.primary_key = "top_id"
            self.columns_string = "(top_id, title, year, rating)"
            self.values_string = "(" + ", ".join(["%s"] * 4) + ")"

        self.connection = psycopg2.connect(
            host="localhost",
            database="yelp_database",
            user="postgres",
            password="karol123"
        )
        self.cursor = self.connection.cursor()
        print("PostgreSQL db test init done!")

    def test_select_index(self, index: str) -> timedelta:
        clock_start = datetime.now()
        query = "SELECT * FROM {table} WHERE {primary_key} = '{index}';".format(table=self.table_name,
                                                                                primary_key=self.primary_key,
                                                                                index=index)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_select_where(self, key: str, value: str) -> timedelta:
        clock_start = datetime.now()
        query = "SELECT * FROM {table} WHERE {column} = '{value}';".format(table=self.table_name, column=key,
                                                                           value=value)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_insert_1(self) -> timedelta:
        select_query = "SELECT * FROM {table} ORDER BY random() LIMIT 1;".format(table=self.table_name, )
        self.cursor.execute(select_query)
        random_record = self.cursor.fetchone()
        clock_start = datetime.now()
        query = "INSERT INTO {table} {columns} VALUES {values};".format(table=self.table_name,
                                                                        columns=self.columns_string,
                                                                        values=self.values_string)
        self.cursor.execute(query, random_record)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_insert_many(self, n=100) -> timedelta:
        select_query = "SELECT * FROM {table} ORDER BY random() LIMIT {n};".format(table=self.table_name, n=n)
        self.cursor.execute(select_query)
        random_records = self.cursor.fetchall()
        clock_start = datetime.now()
        query = "INSERT INTO {table} {columns} VALUES {values};".format(table=self.table_name,
                                                                        columns=self.columns_string,
                                                                        values=self.values_string)
        self.cursor.executemany(query, random_records)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_delete_index(self, index: str) -> timedelta:
        clock_start = datetime.now()
        query = "DELETE FROM {table} WHERE {primary_key} = '{index}';".format(table=self.table_name,
                                                                              primary_key=self.primary_key, index=index)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_delete_where(self, key: str, value: str) -> timedelta:
        clock_start = datetime.now()
        query = "DELETE FROM {table} WHERE {column} = '{value}';".format(table=self.table_name, column=key, value=value)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_update_index(self, search_index: str, update_key: str, update_value: str) -> timedelta:
        clock_start = datetime.now()
        query = "UPDATE {table} SET {column} = '{value}' WHERE {primary_key} = '{index}';".format(table=self.table_name,
                                                                                                  primary_key=self.primary_key,
                                                                                                  index=search_index,
                                                                                                  column=update_key,
                                                                                                  value=update_value)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_update_where(self, search_key: str, search_value: str, update_key: str, update_value: str) -> timedelta:
        clock_start = datetime.now()
        query = "UPDATE {table} SET {column} = '{value}' WHERE {search_column} = '{index}';".format(
            table=self.table_name,
            search_column=search_key,
            index=search_value,
            column=update_key,
            value=update_value)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_count_total(self) -> timedelta:
        clock_start = datetime.now()
        query = "SELECT COUNT(*) FROM {table};".format(table=self.table_name, )
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_column_avg(self, column: str) -> timedelta:
        clock_start = datetime.now()
        query = "SELECT AVG({column}) AS average_value FROM {table};".format(table=self.table_name, column=column)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_column_median(self, column: str) -> timedelta:
        clock_start = datetime.now()
        query = "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY {column} ) AS median FROM {table};" \
            .format(table=self.table_name, column=column)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_distribution(self, column: str) -> timedelta:
        clock_start = datetime.now()
        query = "SELECT AVG({column}) AS average_value, STDDEV({column}) AS stddev_value FROM {table};" \
            .format(table=self.table_name, column=column)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start

    def test_count_word_occurences(self, search_column: str, string: str) -> timedelta:
        clock_start = datetime.now()
        query = "SELECT COUNT(*) AS word_occurences FROM {table} WHERE {column} LIKE '%{string}%';" \
            .format(table=self.table_name, column=search_column, string=string)
        self.cursor.execute(query)
        self.connection.commit()
        return datetime.now() - clock_start
