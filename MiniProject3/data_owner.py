from db_provider import Server
import merkletools
class DataOwner:
    #init data owner with the key value data
    #specify the server object
    def __init__(self,key_value_data,server,blockchain):
        self.data= key_value_data
        self.server= server
        self.merkle_tree = None
        self.blockchain = blockchain

    #insert data to self.server
    def insert_data_to_server(self):
        for k, v in self.data:
            self.server.add_data(k, v)

    # build merkle tree on data owner side to get the merkle root, key+value as values
    # You can use functions provided by merkletools
    def build_merkle_tree(self):
        mt = merkletools.MerkleTools()
        for k, v in self.data:
            mt.add_leaf(str(k) + ":" + str(v))

        mt.make_tree()
        self.merkle_tree = mt

    # upload self.merkle_tree to self.server
    def upload_merkle_tree_to_server(self):
        self.server.merkle_tree = self.merkle_tree


    # get merkle root from self.merkle_tree
    def get_merkle_root(self):
        return self.merkle_tree.get_merkle_root()

    #upload merkle root to self.blockchains
    def upload_merkle_root_to_blockchain(self):
        root = self.get_merkle_root()
        self.blockchain.set_merkle_root(root)





