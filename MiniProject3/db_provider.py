from cassandra.cluster import Cluster
import hashlib
import json
class Server:
    def __init__(self):
        self.cluster = Cluster(port=7545)
        #self.cluster = Cluster(['127.0.0.1', 7545])
        self.session = self.cluster.connect()
        self.merkle_tree = None
        self.keyspace = "project3"  # keyspace(database) name for storing data
        self.table = "data" # table name for storing data

        # Create keyspace and corresponding table
        self.session.execute(
            "CREATE KEYSPACE IF NOT EXISTS " + self.keyspace + " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};")
        self.session.set_keyspace(self.keyspace)
        self.session.execute("CREATE TABLE IF NOT EXISTS " + self.table + " (key text PRIMARY KEY, value text);")


    # Use insert syntax to add new key value pair to the target table
    def add_data(self, key, value):
        self.session.execute("INSERT INTO " + self.table + "(" + key + ") VALUES (" + value + ");")


    # Retrieve value by key
    def get_data(self, key):
        self.session.execute("SELECT value FROM " + self.table + " WHERE key = " + key + ";")
