from cassandra.cluster import Cluster
import hashlib
import json
class Server:
    def __init__(self):
        self.cluster = Cluster()
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
        # b_val = value.encode('utf-8')
        # hash = hashlib.sha256()
        # hash.update(b_val)
        # query = "INSERT INTO " + self.table + "(key, value) VALUES ('" + key + "', '" + hash.hexdigest() + "');"
        query = "INSERT INTO " + self.table + "(key, value) VALUES ('" + key + "', '" + value + "');"
        self.session.execute(query)


    # Retrieve value by key
    def get_data(self, key):
        query = "SELECT value FROM " + self.table + " WHERE key = '" + key + "';"
        result = self.session.execute(query)
        row_list = []
        for row in result:
            row_list.append({"value": row.value})
        str_result = ','.join(str(dct) for dct in row_list)
        str_result = str_result.replace("'", "\"")
        res_json = json.loads(str_result)
        return res_json
