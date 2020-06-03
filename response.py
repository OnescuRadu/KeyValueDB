class Response():
    def __init__(self, success, message, collection_name, data):
        self.success = success
        self.message = message
        self.collection_name = collection_name
        self.data = data

    def __str__(self):
        return f"Response(success={self.success}, message={self.message}, collection_name={self.collection_name}, data={self.data})"

    def __repr__(self):
        return f"Response(success={self.success}, message={self.message}, collection_name={self.collection_name}, data={self.data})"
