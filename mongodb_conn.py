from urllib.parse import quote_plus
import certifi
from pymongo import MongoClient


class MongoDBConnection:
    """MongoDB Connection Class to connect to MongoDB"""


    def __init__(
        self,uri,database
    ):
        """Constructor for MongoDBConnection class

        :Parameters:
            - `host` (str): MongoDB host
            - `username` (str): MongoDB username
            - `password` (str): MongoDB password
            - `database` (str): MongoDB database
            - `srvServiceName` (str): MongoDB srvServiceName
            - `retryWrites` (str): MongoDB retryWrites
           - `w` (integer or string): If this is a replica set, write operations will block until they have been replicated to the specified number or tagged set of servers. w=<int> always includes the replica set primary (e.g. w=3 means write to the primary and wait until replicated to **two** secondaries). Passing w=0 **disables write acknowledgement** and all other write concern options.
        """
        # self.host = host
        # self.username = username
        # self.password = password
        # self.database = database
        # self.srvServiceName = srvServiceName
        # self.retryWrites = retryWrites
        # self.w = w
        self.uri = uri
        self.connection = None
        self.db = None
        self.database = database


    def __enter__(self):
        """
        Connect to MongoDB when entering the context manager


        .. warning:: This method will be called automatically when entering the context manager. Do not call it manually.


        :Returns:
            - `MongoDBConnection` (MongoDBConnection): MongoDBConnection object
        """
        # safe_url = "%s://%s:%s@%s/?retryWrites=%s&w=%s" % (
        #     self.srvServiceName,
        #     quote_plus(self.username),
        #     quote_plus(self.password),
        #     self.host,
        #     self.retryWrites,
        #     self.w,
        # )
        # print(safe_url)
        #self.connection = MongoClient(safe_url)
        self.connection = MongoClient(self.uri, tlsCAFile=certifi.where())
        print(self.connection.list_database_names())
        self.db = self.connection[self.database]
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close the connection to MongoDB when exiting the context manager


        .. warning:: This method will be called automatically when exiting the context manager. Do not call it manually.


        :Parameters:
            - `exc_type` (Exception): Exception type
            - `exc_val` (Exception): Exception value
            - `exc_tb` (Exception): Exception traceback
        """
        self.connection.close()


    def connect(self):
        """
        Connect to MongoDB manually without using the context manager


        .. warning:: This method needs to be called when not using the context manager. Do not call it when using the context manager.
        """
        # safe_url = "%s://%s:%s@%s/?retryWrites=%s&w=%s" % (
        #     self.srvServiceName,
        #     quote_plus(self.username),
        #     quote_plus(self.password),
        #     self.host,
        #     self.retryWrites,
        #     self.w,
        # )
        # print(safe_url)
        self.connection = MongoClient(self.uri, tlsCAFile=certifi.where())
        print(self.connection.list_database_names())
        self.db = self.connection[self.database]


    def get_db(self):
        """
        Get the database object


        :Returns:
            - `db` (MongoDB): MongoDB database object
        """
        return self.db
    def get_collection(self, collection_name):
        """
        Get the collection object


        :Parameters:
            - `collection_name` (str): MongoDB collection name


        :Returns:
            - `collection` (MongoDBCollection): MongoDB collection object
        """
        return self.db[collection_name]


    def close(self):
        """
        Close the connection to MongoDB manually without using the context manager


        .. warning:: This method needs to be called ALWAYS when not using the context manager and when your CRUD operations are finished. Do not call it when using the context manager.
        """
        self.connection.close()
