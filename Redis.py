from database_test import TestDB
from redis import Redis
from redis.commands.json.path import Path
from redis.commands.search.query import Query
import pandas as pd
from datetime import datetime



class RedisDB(TestDB):
    def __init__(self, db_size):
        self.db_size = db_size
        self.file_data = pd.read_json('./data/yelp_academic_dataset_business.json', lines=True)
        self.file_data = self.file_data[['business_id', 'name', 'city', 'stars', 'review_count', 'is_open']]
        
        r = Redis(host='redis-13804.c1.us-east1-2.gce.cloud.redislabs.com', port=13804, username='default', password='iRaOsubYgU8QoZ855RxLdKhNFhOwMx0K')

        for key in r.scan_iter("business:*"):
            r.delete(key)
            
        for _, row in self.file_data.head(self.db_size).iterrows():
            row_inp = f"business:{row.business_id}", Path.root_path(), {'business': row.drop('business_id').to_dict()}
            r.json().set(*row_inp)
            
        self.db = r
    
    def test_select_index(self, index: str) -> float:
        clock_start = datetime.now()
        self.db.json().get(f'business:{index}')
        return datetime.now() - clock_start
    
    def test_select_where(self, key: str, value:str) -> float:
        clock_start = datetime.now()
        self.db.ft('business').search(Query(value).limit_fields(key))
        return datetime.now() - clock_start
    
    def test_insert_1(self) -> float:
        row = self.file_data.iloc[0]
        row_inp = f"business:{row.business_id}", Path.root_path(), {'business': row.drop('business_id').to_dict()}
        clock_start = datetime.now()
        self.db.json().set(*row_inp)
        return datetime.now() - clock_start
    
    def test_insert_100(self) -> float:
        clock_start = datetime.now()
        for _, row in self.file_data.tail(100).iterrows():
            self.db.json().set(f"business:{row.business_id}", Path.root_path(), {'business': row.drop('business_id').to_dict()})
        return datetime.now() - clock_start
    
    def test_delete_index(self, index: str) -> float:
        clock_start = datetime.now()
        self.db.delete(f'business:{index}')
        return datetime.now() - clock_start
    
    def test_delete_where(self, key: str, value:str) -> float:
        clock_start = datetime.now()
        for result in self.db.ft('business').search(Query(value).limit_fields(key)).docs:
            self.db.delete(result.id)
        return datetime.now() - clock_start
    
    def test_update_index(self, search_index: str, update_key: str, update_value:str) -> float:
        clock_start = datetime.now()
        val = self.db.json().get(f'business:{search_index}')
        val['business'][update_key] = update_value
        self.db.json().set(f"business:{search_index}", Path.root_path(), val)
        return datetime.now() - clock_start
    
    def test_update_where(self, search_key: str, search_value:str, update_key: str, update_value:str) -> float:
        clock_start = datetime.now()
        for search_index in self.db.ft('business').search(Query(search_value).limit_fields(search_key)).docs:
            val = self.db.json().get(f'business:{search_index}')
            val['business'][update_key] = update_value
            self.db.json().set(f"business:{search_index}", Path.root_path(), val)
        return datetime.now() - clock_start