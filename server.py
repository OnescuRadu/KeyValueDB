import socket
import pickle
import operator
import os
from Models.request import Request
from Models.response import Response
from apscheduler.schedulers.background import BackgroundScheduler
from configparser import ConfigParser
from pyparsing import Keyword, Word, Literal, printables, alphas
from itertools import chain
from collections import defaultdict


class Server():
    """
    This is a class for using and initializing a key-value pair database that can be accesed over the network.
    It communicates with the client using Request and Response objects sent and received via TCP Sockets.
    It supports only one connection at a time.

    Functionalities:
        - Initializes a key-value pair database based on the given collections' filenames in the config file.
        - Initializes a TCP Socket Server bound to the given host and port in the config file. (default hostname and port: 127.0.0.1:65535)
        - On initialization it reads the collection files if they exists.
        - Provides read, add, delete, query and join functionalities for the database.
        - Creates a snapshot of the collections (the key-value pair dictionaries) at a given time based on the interval value from the config file. (default: every 60 mins)

    Attributes:
        collections (dict): The dictionary object that contains all the collections.
        host (String): The host on which the server is bound.
        port (Int): The port on which the server is bound.
        collection_names (List): List containing all the collections' names.
        snapshot_interval (Int): The interval on which the snapshot is created.
        server_socket: The server's TCP Socket.

    """

    def __init__(self):
        """
        The constructor for the database's server class.
        """
        self._read_config()
        self._init_db()
        self._schedule_snapshot()
        self._start_server()
        self._listen()

    def _init_db(self):
        """
        The method tries to open the files with the given name and to add the containing data to their respective collection.
        If the file does not exist or is corrupted it will not initilize its specific collection.
        """
        self.collections = {}
        for collection_name in self.collection_names:
            try:
                with open(f"Data/{collection_name}.pickle", 'rb') as handle:
                    self.collections[collection_name] = pickle.loads(
                        handle.read())
            except IOError:
                self.collections[collection_name] = {}

    def _start_server(self):
        """
        The method creates a socket bounded to the given host and port and listens for an incoming client connection.

        Raises:
            ConnectionError: Server could not be started.
        """
        try:
            self.server_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(1)
            print(f"Started server on {self.host}:{self.port}")
        except:
            raise ConnectionError("Server could not be started.")

    def _listen(self):
        """
        The method accepts the incoming client connection and permanently listens for any incoming request from the client.
        When a request is received, it checks its type, calls the corresponding action and then sends back the response to the client.
        When the client disconnects, it waits for other incoming client connections.
        """
        while True:
            # Starting the connection
            client_socket, address = self.server_socket.accept()

            while True:
                request = client_socket.recv(1024)

                if request:
                    request = pickle.loads(request)

                    request_types = {
                        0: lambda: self._read(request.key, request.collection_name),
                        1: lambda: self._add(request.key, request.value, request.collection_name),
                        2: lambda: self._delete(request.key, request.collection_name),
                        3: lambda: self._query(request.query),
                        4: lambda: self._create_collection(request.collection_name),
                        5: lambda: self._delete_collection(request.collection_name),
                        6: lambda: self._send_error("Request type does not exist.")
                    }
                    response = request_types.get(request.request_type, 6)()
                    response = pickle.dumps(response)
                    client_socket.send(response)
                else:  # client disconnected
                    break

    def _create_collection(self, collection_name):
        """
        The method creates a collection with the given name.

        Parameters:
            collection_name (String): The name of the collection.

        Returns:
            Response(success=True, message=None, collection_name=THE GIVEN COLLECTION NAME, data=None): If the collection was succesfully created.
            Response(success=False, message=Collection already exists., collection_name=None, data=None): If the collection already exists.
            Response(success=False, message=Invalid collection name., collection_name=None, data=None): If the collection name is invalid.
        """
        if collection_name.isalpha():
            try:
                self.collections[collection_name]
            except:
                self.collections[collection_name] = {}
                return Response(True, None, collection_name, None)
            else:
                return self._send_error("Collection already exists.")
        else:
            return self._send_error("Invalid collection name.")

    def _delete_collection(self, collection_name):
        """
        The method deletes the collection with the given name.

        Parameters:
            collection_name (String): The name of the collection.

        Returns:
            Response(success=True, message=None, collection_name=None, data=None): If the collection was succesfully deleted.
            Response(success=False, message=Collection does not exist., collection_name=None, data=None): If the collection does not exist.        
        """
        try:
            del self.collections[collection_name]
            return Response(True, None, None, None)
        except KeyError:
            pass
            return self._send_error("Collection does not exist.")

    def _read(self, key, collection_name):
        """
        The method reads an entry (Key-Value pair), from the given collection, based on the given key.

        Parameters:
            key (Any hashable data type): The key of the entry.
            collection_name (String): The name of the collection.

        Returns:
            Response(success=True, message=None, collection_name=THE GIVEN COLLECTION NAME, data=[(...)]): If the read action was succesful. Note: The data list will contain the read entry.
            Response(success=False, message=Entry does not exist., collection_name=None, data=None): If the entry does not exist.
            Response(success=False, message=Collection does not exist., collection_name=None, data=None): If the collection does not exist.
        """
        if self._collection_exists(collection_name):
            try:
                return Response(True, None, collection_name, [(key, self.collections[collection_name][key])])
            except:
                return self._send_error("Entry does not exist.")
        else:
            return self._send_error("Collection does not exist.")

    def _add(self, key, value, collection_name):
        """
        The method adds an entry (Key-Value pair) to the given collection.

        Parameters:
            key (Any hashable data type): The key of the entry.
            value (Any data type): The value of the entry.
            collection_name (String): The name of the collection.

        Returns:
            Response(success=True, message=None, collection_name=THE GIVEN COLLECTION NAME, data=[(...)]): If the add action was succesful. Note: The data list will contain the added entry.
            Response(success=False, message=Entry could not be added., collection_name=None, data=None): If the add action was not succesful.
            Response(success=False, message=Collection does not exist., collection_name=None, data=None): If the collection does not exist.
        """
        if self._collection_exists(collection_name):
            try:
                self.collections[collection_name][key] = value
                return Response(True, None, collection_name, [(key, self.collections[collection_name][key])])
            except:
                return self._send_error("Entry could not be added.")
        else:
            return self._send_error("Collection does not exist.")

    def _delete(self, key, collection_name):
        """
        The method deletes an entry (Key-Value pair), from the given collection, based on the given key.

        Parameters:
            key (Any hashable data type): The key of the entry that will be deleted.
            collection_name (String): The name of the collection.

        Returns:
            Response(success=True, message=None, collection_name=THE GIVEN COLLECTION NAME, data=[(...)]): If the delete action was succesful.
            Response(success=False, message=Entry could not be deleted., collection_name=None, data=None): If the delete action was not succesful.
            Response(success=False, message=Collection does not exist., collection_name=None, data=None): If the collection does not exist.
        """
        if self._collection_exists(collection_name):
            try:
                del self.collections[collection_name][key]
                return Response(True, None, collection_name,  None)
            except:
                return self._send_error("Entry could not be deleted.")
        else:
            return self._send_error("Collection does not exist.")

    def _collection_exists(self, collection_name):
        """
        The method checks if the given collection exists or not.

        Parameters:
            collection_name (String): The name of the collection.

        Returns:
            True: If the collection exists.
            False: If the collection does not exist.
        """
        try:
            self.collections[collection_name]
        except:
            return False
        else:
            return True

    def _query(self, query):
        """
        The method queries the database based on the given query.

        Parameters:
            query (String): The query string.

        Returns:
            Response(success=True, message=None, collection_name=THE COLLECTION NAME, data=[(...)]): If the query action was succesful. Note: The data list will contain the entries matching the query.
            Response(success=False, message=Invalid query syntax., collection_name=None, data=None): If the query action was not succesful.
        """
        try:
            query = self._parse_query_string(query)

            if not self._collection_exists(query["collection1"]):
                return self._send_error(f"{query['collection1']} does not exist.")

            if query["action"] == "join":  # JOIN ACTION
                if not self._collection_exists(query["collection2"]):
                    return self._send_error(f"{query['collection2']} does not exist.")

                matches = defaultdict(list)

                for key, value in chain(self.collections[query["collection1"]].items(), self.collections[query["collection2"]].items()):
                    matches[key].append(value)

                return Response(True, None, [query["collection1"], query["collection2"]], dict(matches))

            else:  # QUERY ACTION (READ | DELETE)
                operators = {
                    ">": operator.gt,
                    "<": operator.lt,
                    "=": operator.eq,
                    "<=": operator.le,
                    ">=": operator.ge,
                    "contains": operator.contains,
                }

                query_elements = {
                    "key": self._execute_query_by_key,
                    "value": self._execute_query_by_value
                }

                query_actions = {
                    "read": None,
                    "delete": self._delete_from_query
                }

                query_action = query_actions[query["action"]]

                matches = query_elements[query["element"]](
                    query_action, query, operators, query["collection1"])

                return Response(True, None, query["collection1"], matches)

        except Exception as e:
            print(e)
            return self._send_error("Invalid query syntax.")

    def _parse_query_string(self, query):
        """
        The method checks the format and parses the given query.

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
           query (List): A list containing the parsed query string.
        """
        ACTION_QUERY = Keyword("read")("action") | Keyword("delete")("action")

        ELEMENT = Keyword("key")("element") | Keyword("value")("element")

        OPERATOR = Keyword("<=")("operator") | Keyword(">=")("operator") | Keyword("<")("operator") | Keyword(">")("operator") | Keyword(
            "=")("operator") | Keyword("contains")("operator")

        VALUE = (Word(printables)("value_type") +
                 "(" + Word(printables)("value") + ")") | Word(printables)("value")

        SYNTAX_QUERY = ACTION_QUERY + ELEMENT + OPERATOR + \
            VALUE + "from" + Word(alphas)("collection1")

        SYNTAX_JOIN = Keyword("join")("action") + Word(alphas)("collection1") + "with" + Word(
            alphas)("collection2")

        SYNTAX = SYNTAX_JOIN | SYNTAX_QUERY

        query = SYNTAX.parseString(query)

        if "value_type" in query:
            query["value"] = self._parse_value_to_type(
                query["value"], query["value_type"])

        return query

    def _parse_value_to_type(self, value, value_type):
        """
        The method casts the given value to the given type.

        Parameters:
            value (String): The value.
            value_type (String): The type to which the value will be casted to.

        Returns:
           The value casted to the given type.
        """
        types = {
            "int": int,
            "float": float,
            "complex": complex,
            "str": str
        }
        return types[value_type](value)

    def _execute_query_by_value(self, query_action, query, operators, collection_name):
        """
        The method queries the database by value using the given query.

        Parameters:
            query_action (Method): The query action that will be executed. Note: If the query action is "read" the parameter will be None, because the method reads the matching data in any case.
            query (List): A list containing the parsed query string.
            operators (Dict): A dictionary containing the operators that will be used.
            collection_name: The name of the collection on which the query will be executed.

        Returns:
           A list containing all the entries that match the query.
        """
        matches = []
        for key, value in self.collections[collection_name].copy().items():
            try:
                if operators[query["operator"]](value, query["value"]):
                    matches.append((key, value))
                    query_action(key, collection_name)
            except:
                pass
        return matches

    def _execute_query_by_key(self, query_action, query, operators, collection_name):
        """
        The method queries the database by key using the given query.
        Note: If the query operator is "=" the method will access the entry, using the key, right away.

        Parameters:
            query_action (Method): The query action that will be executed. Note: If the query action is "read" the parameter will be None, because the method reads the matching data in any case.
            query (List): A list containing the parsed query string.
            operators (Dict): A dictionary containing the operators that will be used.
            collection_name: The name of the collection on which the query will be executed.

        Returns:
           A list containing all the entries that match the query.
        """
        matches = []
        if query["operator"] == "=":
            try:
                matches.append((query["value"], self.data[query["value"]]))
                query_action(query["value"])
            except:
                pass
            return matches
        else:
            for key, value in self.collections[collection_name].copy().items():
                try:
                    if operators[query["operator"]](key, query["value"]):
                        matches.append((key, value))
                        query_action(key)
                except:
                    pass
        return matches

    def _delete_from_query(self, key, collection_name):
        """
        The method deletes an entry from the given collection using the given key.

        Parameters:
            key: The key of the database entry.
            collection_name: The name of the collection.
        """
        del self.collections[collection_name][key]

    def _send_error(self, description):
        """
        The method creates an error Response object based on the given description.

        Parameters:
            description (String): The error's description.

        Returns:
            Response(success=False, message=THE GIVEN DESCRIPTION., collection_name = None, data=None)
        """
        return Response(False, description, None, None)

    def _create_snapshot(self):
        """
        The method creates a snapshot (a backup) of all the collections.

        Raises:
            PermissionError: Permission denied to write to file.
        """
        os.makedirs("Data", exist_ok=True)
        for collection_name, collection_data in self.collections.copy().items():
            try:
                with open(f"Data/{collection_name}.pickle", 'wb') as handle:
                    pickle.dump(collection_data, handle)
            except Exception as e:
                print(e)
                raise PermissionError("Permission denied to write to file.")

    def _schedule_snapshot(self):
        """
        The method creates a background thread that will call the _create_snapshot() method at a given time interval.
        """
        scheduler = BackgroundScheduler()
        scheduler.add_job(self._create_snapshot, 'interval',
                          minutes=self.snapshot_interval)
        scheduler.start()

    def _read_config(self):
        """
        The method reads the data from the config file.
        If there is no 'config.ini' file or it is corrupted, it will assign the default values.
        """
        try:
            config = ConfigParser()
            config.read('config.ini')
            self.host = config.get("database", "host")
            self.port = int(config.get("database", "port"))
            self.collection_names = config.get(
                "database", "collections").split(",")
            self.snapshot_interval = int(config.get("snapshot", "interval"))
        except:
            self.host = "127.0.0.1"
            self.port = 65535
            self.collection_names = []
            self.snapshot_interval = 60


server = Server()
