from pymongo import MongoClient
import certifi

# Author: Tan Zi Yang

CONNECTION_STRING = "PLACEHOLDER"

class PyMongoUtils:
    
    def __init__(self, uri=CONNECTION_STRING):
        self.uri = uri    

    def get_database(self, database_name):
        client = MongoClient(self.uri, tlsCAFile=certifi.where())
        return client[database_name]

