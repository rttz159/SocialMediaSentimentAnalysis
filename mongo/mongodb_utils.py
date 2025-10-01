from pymongo import MongoClient
import certifi

# Author: Tan Zi Yang

CONNECTION_STRING = "mongodb+srv://public:xX2ZTACexOWl6jEb@cluster.riznieq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster"

class PyMongoUtils:
    
    def __init__(self, uri=CONNECTION_STRING):
        self.uri = uri    

    def get_database(self, database_name):
        client = MongoClient(self.uri, tlsCAFile=certifi.where())
        return client[database_name]

