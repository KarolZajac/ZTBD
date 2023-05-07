class TestDB():
    def __init__(self, db_size):
        self.db_size = db_size
        self.db = None
    
    def test_select_index(self, index: str) -> float:
        pass
    
    def test_select_where(self, key: str, value:str) -> float:
        pass
    
    def test_insert_1(self) -> float:
        pass
    
    def test_insert_100(self) -> float:
        pass
    
    def test_delete_index(self, index: str) -> float:
        pass
    
    def test_delete_where(self, key: str, value:str) -> float:
        pass
    
    def test_update_index(self, search_index: str, update_key: str, update_value:str) -> float:
        pass
    
    def test_update_where(self, search_key: str, search_value:str, update_key: str, update_value:str) -> float:
        pass