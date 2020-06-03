class Request():
    def __init__(self, request_type, collection_name, key, value, query):
        self.request_type = request_type
        self.collection_name = collection_name
        self.key = key
        self.value = value
        self.query = query
