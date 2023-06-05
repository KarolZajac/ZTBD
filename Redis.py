from database_test import TestDB
from redis import Redis
from redis.commands.json.path import Path
from redis.commands.search.query import Query
import pandas as pd
from datetime import datetime, timedelta
import json


class RedisDB(TestDB):
    def __init__(self, db_size, dataset):
        self.db_size = db_size
        r = Redis(host='redis-13804.c1.us-east1-2.gce.cloud.redislabs.com', port=13804, username='default', password='iRaOsubYgU8QoZ855RxLdKhNFhOwMx0K')

        if dataset == "Yelp":
            self.db_schema = 'business'  
            self.db_index = 'business_id'
            self.file_data = pd.read_json('yelp_academic_dataset_business.json', lines=True)
            self.file_data = self.file_data[['business_id', 'name', 'city', 'stars', 'review_count', 'is_open', 'categories']]
        
        elif dataset == "IMDB":        
            self.db_schema = 'movies'  
            self.db_index = 'top_id'
            with open('scraped_data.json') as file:
                self.file_data = json.load(file)
            self.file_data = pd.DataFrame(self.file_data['movies'])
            self.file_data.columns=['top_id', 'title', 'year', 'rating']
            
            
        for key in r.scan_iter(f"{self.db_schema}:*"):
            r.delete(key)

        for _, row in self.file_data.head(self.db_size).iterrows():
            row_inp = f"{self.db_schema}:{row[self.db_index]}", Path.root_path(), {f'{self.db_schema}': row.drop(self.db_index).to_dict()}
            r.json().set(*row_inp)
            
            
        self.db = r
    
    def test_select_index(self, index: str) -> timedelta:
        clock_start = datetime.now()
        self.db.json().get(f'{self.db_schema}:{index}')
        return datetime.now() - clock_start
    
    def test_select_where(self, key: str, value:str) -> timedelta:
        clock_start = datetime.now()
        self.db.ft(self.db_schema).search(Query(value).limit_fields(key))
        return datetime.now() - clock_start
    
    def test_insert_1(self) -> timedelta:
        row = self.file_data.iloc[0]
        row_inp = f"{self.db_schema}:{row[self.db_index]}", Path.root_path(), {self.db_schema: row.drop(self.db_index).to_dict()}
        clock_start = datetime.now()
        self.db.json().set(*row_inp)
        return datetime.now() - clock_start
    
    def test_insert_many(self, n=100) -> float:
        clock_start = datetime.now()
        for _, row in self.file_data.tail(n).iterrows():
            self.db.json().set(f"{self.db_schema}:{row[self.db_index]}", Path.root_path(), {self.db_schema: row.drop(self.db_index).to_dict()})
        return datetime.now() - clock_start
    
    def test_delete_index(self, index: str) -> timedelta:
        clock_start = datetime.now()
        self.db.delete(f'{self.db_schema}:{index}')
        return datetime.now() - clock_start
    
    def test_delete_where(self, key: str, value:str) -> timedelta:
        clock_start = datetime.now()
        for result in self.db.ft(self.db_schema).search(Query(value).limit_fields(key)).docs:
            self.db.delete(result.id)
        return datetime.now() - clock_start
    
    def test_update_index(self, search_index: str, update_key: str, update_value:str) -> timedelta:
        clock_start = datetime.now()
        val = self.db.json().get(f'{self.db_schema}:{search_index}')
        val[self.db_schema][update_key] = update_value
        self.db.json().set(f"{self.db_schema}:{search_index}", Path.root_path(), val)
        return datetime.now() - clock_start
    
    def test_update_where(self, search_key: str, search_value:str, update_key: str, update_value:str) -> timedelta:
        clock_start = datetime.now()
        for search_index in self.db.ft(self.db_schema).search(Query(search_value).limit_fields(search_key)).docs:
            val = self.db.json().get(f'{self.db_schema}:{search_index}')
            val[self.db_schema][update_key] = update_value
            self.db.json().set(f"{self.db_schema}:{search_index}", Path.root_path(), val)
        return datetime.now() - clock_start

    def test_count_total(self) -> float:
        clock_start = datetime.now()
        res_df = pd.Series(self.db.ft(self.db_schema).search(
                Query("*")
            ).docs).shape[0]
        return datetime.now() - clock_start

    def test_column_avg(self, column: str) -> float:
        clock_start = datetime.now()
        res_df = pd.Series(self.db.ft(self.db_schema).search(
                Query("*")
            ).docs).apply(lambda x: json.loads(x.json)[self.db_schema][column]).mean()
        return datetime.now() - clock_start
    
    def test_column_median(self, column: str) -> float:
        clock_start = datetime.now()
        res_df = pd.Series(self.db.ft(self.db_schema).search(
                Query("*")
            ).docs).apply(lambda x: json.loads(x.json)[self.db_schema][column]).median()
        return datetime.now() - clock_start

    def test_column_stddev(self, column: str) -> float:
        clock_start = datetime.now()
        res_df = pd.Series(self.db.ft(self.db_schema).search(
                Query("*")
            ).docs).apply(lambda x: json.loads(x.json)[self.db_schema][column]).std()
        return datetime.now() - clock_start

    def test_count_word_occurences(self, search_column: str, string: str) -> float:
        clock_start = datetime.now()
        res_df = pd.Series(self.db.ft(self.db_schema).search(
                Query("*")
            ).docs).apply(lambda x: json.loads(x.json)[self.db_schema][search_column])
        res = res_df[res_df.str.contains(string)].shape[0]
        return datetime.now() - clock_start
    
    