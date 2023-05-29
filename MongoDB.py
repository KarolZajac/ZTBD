from datetime import datetime, timedelta

import pandas as pd
from bson import ObjectId
from pymongo import MongoClient
from database_test import TestDB


class MongoDB(TestDB):
    def __init__(self, db_size):
        super().__init__(db_size)
        client = MongoClient("mongodb+srv://Michni:michni@ztbd.7ciyl2n.mongodb.net/")
        db = client["ZTBD"]
        self.collection = db["yelp"]


    def test_select_index(self, index: str) -> timedelta:
        clock_start = datetime.now()
        query = {"business_id": index}
        self.collection.find_one(query)
        return datetime.now() - clock_start

    def test_select_where(self, key: str, value: str) -> timedelta:
        clock_start = datetime.now()
        self.collection.find({key: value})
        return datetime.now() - clock_start

    def test_insert_1(self) -> timedelta:
        record = self.collection.find_one()
        record['_id'] = ObjectId()
        clock_start = datetime.now()
        self.collection.insert_one(record)
        return datetime.now() - clock_start

    def test_insert_100(self) -> timedelta:
        documents = self.collection.find().limit(100)
        data = [doc for doc in documents]
        df = pd.DataFrame(data)
        df.drop('_id', axis=1, inplace=True)
        documents_to_insert = df.to_dict(orient='records')
        clock_start = datetime.now()
        self.collection.insert_many(documents_to_insert)
        return datetime.now() - clock_start

    def test_delete_index(self, index: str) -> timedelta:
        clock_start = datetime.now()
        self.collection.delete_one({"business_id": index})
        return datetime.now() - clock_start

    def test_delete_where(self, key: str, value: str) -> timedelta:
        clock_start = datetime.now()
        self.collection.delete_many({key: value})
        return datetime.now() - clock_start

    def test_update_index(self, search_index: str, update_key: str, update_value: str) -> timedelta:
        clock_start = datetime.now()
        self.collection.update_one(
            {'business_id': search_index},
            {'$set': {update_key: update_value}}
        )
        return datetime.now() - clock_start

    def test_update_where(self, search_key: str, search_value: str, update_key: str, update_value: str) -> timedelta:
        clock_start = datetime.now()
        self.collection.update_many(
            {search_key: search_value},
            {'$set': {update_key: update_value}}
        )
        return datetime.now() - clock_start


