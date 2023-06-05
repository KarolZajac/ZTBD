import json
from datetime import datetime

import pandas as pd
from bson import ObjectId
from pymongo import MongoClient
from database_test import TestDB


class MongoDB(TestDB):
    def __init__(self, db_size, scrapedData=False):
        super().__init__(db_size)
        client = MongoClient("mongodb+srv://Michni:michni@ztbd.7ciyl2n.mongodb.net/")
        db = client["ZTBD"]
        for collection_name in db.list_collection_names():
            db[collection_name].drop()

        if scrapedData is False:
            data = []
            self.collection = db["yelp"]
            with open('yelp_academic_dataset_business.json', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        obj = json.loads(line)
                        data.append(obj)
            self.collection.insert_many(data[:db_size])
        else:
            self.collection = db["imdb"]
            with open('scraped_data.json') as f:
                data = json.load(f)
            movies = []
            for movie_entry in data['movies']:
                movie = {
                    "id": movie_entry[0],
                    "title": movie_entry[1],
                    "year": movie_entry[2],
                    "rating": movie_entry[3]
                }
                movies.append(movie)
            self.collection.insert_many(movies[:db_size])




    def test_select_index(self, index: str) -> float:
        clock_start = datetime.now()
        query = {"business_id": index}
        self.collection.find_one(query)
        return datetime.now() - clock_start

    def test_select_where(self, key: str, value: str) -> float:
        clock_start = datetime.now()
        self.collection.find({key: value})
        return datetime.now() - clock_start

    def test_insert_1(self) -> float:
        record = self.collection.find_one()
        record['_id'] = ObjectId()
        clock_start = datetime.now()
        self.collection.insert_one(record)
        return datetime.now() - clock_start

    def test_insert_many(self, n=100) -> float:
        documents = self.collection.find().limit(n)
        data = [doc for doc in documents]
        df = pd.DataFrame(data)
        df.drop('_id', axis=1, inplace=True)
        documents_to_insert = df.to_dict(orient='records')
        clock_start = datetime.now()
        self.collection.insert_many(documents_to_insert)
        return datetime.now() - clock_start

    def test_delete_index(self, index: str) -> float:
        clock_start = datetime.now()
        self.collection.delete_one({"business_id": index})
        return datetime.now() - clock_start

    def test_delete_where(self, key: str, value: str) -> float:
        clock_start = datetime.now()
        self.collection.delete_many({key: value})
        return datetime.now() - clock_start

    def test_update_index(self, search_index: str, update_key: str, update_value: str) -> float:
        clock_start = datetime.now()
        self.collection.update_one(
            {'business_id': search_index},
            {'$set': {update_key: update_value}}
        )
        return datetime.now() - clock_start

    def test_update_where(self, search_key: str, search_value: str, update_key: str, update_value: str) -> float:
        clock_start = datetime.now()
        self.collection.update_many(
            {search_key: search_value},
            {'$set': {update_key: update_value}}
        )
        return datetime.now() - clock_start


    def test_column_median(self, key: str) -> float:
        clock_start = datetime.now()
        target_field = key
        values = list(self.collection.find({}, {target_field: 1}))
        data = [entry[target_field] for entry in values]
        data.sort()
        n = len(data)
        if n % 2 == 0:
            (data[n // 2 - 1] + data[n // 2]) / 2
        else:
            data[n // 2]
        return datetime.now() - clock_start


    def test_count_total(self) -> float:
        clock_start = datetime.now()
        self.collection.count_documents({})
        return datetime.now() - clock_start

    def test_column_avg(self, key: str):
        clock_start = datetime.now()
        target_field = key
        values = list(self.collection.find({}, {target_field: 1}))
        data = [int(entry[target_field]) for entry in values if entry[target_field] and entry[target_field].isdigit()]

        if len(data) > 0:
            total = sum(data)
            total / len(data)
        return datetime.now() - clock_start

    def test_count_word_occurences(self, key: str, string: str):
        clock_start = datetime.now()
        target_field = key
        target_phrase = string

        pipeline = [
            {"$project": {target_field: 1}},
            {"$unwind": f"${target_field}"},
            {"$match": {target_field: {"$regex": target_phrase, "$options": "i"}}},
            {"$group": {"_id": "$" + target_field, "count": {"$sum": 1}}}
        ]

        result = list(self.collection.aggregate(pipeline))

        if len(result) > 0:
            sum(entry["count"] for entry in result)
        return datetime.now() - clock_start




