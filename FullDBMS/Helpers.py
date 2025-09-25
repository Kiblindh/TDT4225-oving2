
def get_query_type(query: str) -> str:
    return query.split(" ", maxsplit=1)[0]

def get_table_from_query(query: str) -> str:
    return query.split(" ", maxsplit=1)[1]
