
from Helpers import *
from DbConnector import *

class DBMS():
    
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.dirty_tables = set()
    
    def create_table(self, name, cols, cache=None):
        # All tables dirty to begin with (i.e. lazy caching)
        self.dirty_tables.add(name)
        # ...
        pass
    
    def delete_table(self, name):
        # ...
        pass
    
    def query(self, query):
        
        # If a table is modified, it's considered "dirty" for future reads and re-caching may be needed
        if get_query_type(query) is not 'SELECT':
            self.dirty_tables.add( get_table_from_query(query) )
        
        # ...
        pass
    
    def get_cached(self, cache, row):
        
        # If dirty, recompute cached values
        if cache.table in self.dirty_tables:
            cache.recompute()
        
        return cache.get_row(row)
    