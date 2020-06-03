import socket
import pickle
from request import Request
from response import Response


class Client:
    """
    This is a class for connecting to the server's database and to read, add, delete or query entries (key-value pairs).
    It establishes a TCP connection with the server.
    It communicates with the server using Request and Response objects sent and received via TCP Sockets.

    Note: In order for tests to work the server should be running on 127.0.0.1:65534
    Tests:
        >>> client = Client("127.0.0.1", 65535)
        Traceback (most recent call last):
        ...
        ConnectionRefusedError: Connection to the server was refused.
        >>> client = Client("127.0.0.1", 65534)
        >>> client.create_collection("firstName")
        Response(success=True, message=None, collection_name=firstName, data=None)
        >>> client.create_collection("lastName")
        Response(success=True, message=None, collection_name=lastName, data=None)
        >>> client.create_collection("age")
        Response(success=True, message=None, collection_name=age, data=None)
        >>> client.delete_collection("age")
        Response(success=True, message=None, collection_name=None, data=None)
        >>> client.add("firstName", 1, "Radu")
        Response(success=True, message=None, collection_name=firstName, data=[(1, 'Radu')])
        >>> client.add("lastName", 1, "Onescu")
        Response(success=True, message=None, collection_name=lastName, data=[(1, 'Onescu')])
        >>> client.add("firstName", 2, "John")
        Response(success=True, message=None, collection_name=firstName, data=[(2, 'John')])
        >>> client.read("firstName", 2)
        Response(success=True, message=None, collection_name=firstName, data=[(2, 'John')])
        >>> client.delete("firstName", 2)
        Response(success=True, message=None, collection_name=firstName, data=None)
        >>> client.delete("firstName", 2)
        Response(success=False, message=Entry could not be deleted., collection_name=None, data=None)
        >>> client.delete("age", 2)
        Response(success=False, message=Collection does not exist., collection_name=None, data=None)
        >>> client.read("age", 1)
        Response(success=False, message=Collection does not exist., collection_name=None, data=None)
        >>> client.query("join firstName with lastName")
        Response(success=True, message=None, collection_name=['firstName', 'lastName'], data={1: ['Radu', 'Onescu']})
        >>> client.query("join firstName with age")
        Response(success=False, message=age does not exist., collection_name=None, data=None)
        >>> client.create_collection("age")
        Response(success=True, message=None, collection_name=age, data=None)
        >>> client.add("age", 2, 25)
        Response(success=True, message=None, collection_name=age, data=[(2, 25)])
        >>> client.add("age", 1, 20)
        Response(success=True, message=None, collection_name=age, data=[(1, 20)])
        >>> client.query("read value >= int ( 20 ) from age")
        Response(success=True, message=None, collection_name=age, data=[(2, 25), (1, 20)])
    """

    def __init__(self, host, port):
        """
        The constructor for the database's client class.

        Parameters:
           host (String): The host of the server that is trying to establish a connection to.
           port (int): The port on which the server is bound.
        """
        self._connect_to_server(host, port)

    def read(self, collection_name, key):
        """
        The method reads an entry (Key-Value pair), from the given collection, based on the given key.
        It sends a request to the server and waits for the response.

        Parameters:
            key (Any hashable data type): The key of the entry.
            collection_name (String): The name of the collection.

        Returns:
            Response(success=True, message=None, collection_name=THE GIVEN COLLECTION NAME, data=[(...)]): If the read action was succesful. Note: The data list will contain the read entry.
            Response(success=False, message=Entry does not exist., collection_name=None, data=None): If the entry does not exist.
            Response(success=False, message=Collection does not exist., collection_name=None, data=None): If the collection does not exist.
        """
        request = Request(0, collection_name,  key, None, None)
        request = pickle.dumps(request)
        self.client_socket.send(request)
        return self._listen_for_response()

    def add(self, collection_name, key, value):
        """
        The method adds an entry (Key-Value pair) to the given collection.
        It sends a request to the server and waits for the response.

        Parameters:
            key (Any hashable data type): The key of the entry.
            value (Any data type): The value of the entry.
            collection_name (String): The name of the collection.

        Returns:
            Response(success=True, message=None, collection_name=THE GIVEN COLLECTION NAME, data=[(...)]): If the add action was succesful. Note: The data list will contain the added entry.
            Response(success=False, message=Entry could not be added., collection_name=None, data=None): If the add action was not succesful.
            Response(success=False, message=Collection does not exist., collection_name=None, data=None): If the collection does not exist.
        """
        request = Request(1, collection_name, key, value, None)
        request = pickle.dumps(request)
        self.client_socket.send(request)
        return self._listen_for_response()

    def delete(self, collection_name, key):
        """
        The method deletes an entry (Key-Value pair), from the given collection, based on the given key.
        It sends a request to the server and waits for the response.

        Parameters:
            key (Any hashable data type): The key of the entry that will be deleted.
            collection_name (String): The name of the collection.

        Returns:
            Response(success=True, message=None, collection_name=THE GIVEN COLLECTION NAME, data=[(...)]): If the delete action was succesful.
            Response(success=False, message=Entry could not be deleted., collection_name=None, data=None): If the delete action was not succesful.
            Response(success=False, message=Collection does not exist., collection_name=None, data=None): If the collection does not exist.
        """
        request = Request(2, collection_name, key, None, None)
        request = pickle.dumps(request)
        self.client_socket.send(request)
        return self._listen_for_response()

    def query(self, query):
        """
        The method queries the database based on the given query.
        It sends a request to the server and waits for the response.

        Parameters:
           query (String): The query string.
                            Accepted formats: 
                                - "[ACTION] [ELEMENT] [OPERATOR] [VALUE] from [COLLECTION]" or "[ACTION] [ELEMENT] [OPERATOR] [DATATYPE] ( [VALUE] ) from [COLLECTION]"
                                - "JOIN [COLLECTION] with [COLLECTION]"
                            [ACTION] can be "read" or "delete"
                            [ELEMENT] can be "value" or "key"
                            [OPERATOR] can be "<", ">", "=", "<=", ">=", "contains"
                            [DATATYPE] can be "int", "float", "complex", "str".
                            [VALUE] is the value on which the query will be done.
                            [COLLECTION] is the name of the collection on which the query will be done.
                            Examples: "read key > 1234 from cars"
                                      "read value < int ( 4 ) from computers"
                            Note: If no datatype is provided, the value will have the String data type by default.

        Returns:
            Response(success=True, message=None, data=[(...)]): If the query action was succesful. Note: The data list will contain the entries matching the query.
            Response(success=False, message=Invalid query syntax., data=None): If the query action was not succesful.
        """
        request = Request(3, None, None, None, query)
        request = pickle.dumps(request)
        self.client_socket.send(request)
        return self._listen_for_response()

    def create_collection(self, collection_name):
        """
        The method creates a collection with the given name.
        It sends a request to the server and waits for the response.

        Parameters:
            collection_name (String): The name of the collection.

        Returns:
            Response(success=True, message=None, collection_name=THE GIVEN COLLECTION NAME, data=None): If the collection was succesfully created.
            Response(success=False, message=Collection already exists., collection_name=None, data=None): If the collection already exists.
            Response(success=False, message=Invalid collection name., collection_name=None, data=None): If the collection name is invalid.
        """
        request = Request(4, collection_name, None, None, None)
        request = pickle.dumps(request)
        self.client_socket.send(request)
        return self._listen_for_response()

    def delete_collection(self, collection_name):
        """
        The method deletes the collection with the given name.
        It sends a request to the server and waits for the response.

        Parameters:
            collection_name (String): The name of the collection.

        Returns:
            Response(success=True, message=None, collection_name=None, data=None): If the collection was succesfully deleted.
            Response(success=False, message=Collection does not exist, collection_name=None, data=None): If the collection does not exist.        
        """
        request = Request(5, collection_name, None, None, None)
        request = pickle.dumps(request)
        self.client_socket.send(request)
        return self._listen_for_response()

    def _listen_for_response(self):
        """
        The method waits and listens for a response from the server.

        Returns: 
            Response: When the response has been received from the server. 
        """
        while True:
            response = self.client_socket.recv(1024)
            if response:
                response = pickle.loads(response)
                return response

    def _connect_to_server(self, host, port):
        """
        The method connects to the server using the given host and port. 

        Parameters: 
           host (String): The host of the server that is trying to establish a connection to.
           port (int): The port on which the server is bound. 

        Raises:
            ConnectionRefusedError: Connection to the server was refused.
        """
        try:
            self.client_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((host, port))
        except:
            raise ConnectionRefusedError(
                "Connection to the server was refused.")


if __name__ == "__main__":
    import doctest
    doctest.testmod()
