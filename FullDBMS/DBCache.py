
class DBCache():
    
    table: str
    cached: dict
    
    def __init__(self, table, evaluator, dependencies):
        self.table = table
        self.evaluator = evaluator
        self.dependencies = dependencies
        self.cached = {}
    
    def recompute(self):
        self.cached = {}
        # fetch values from SQL table and compute w/ evaluator (lambda) and dependencies (col. names)
    
    def get_row(self, row):
        return self.cached[row]
    