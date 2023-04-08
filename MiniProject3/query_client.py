import hashlib
from blockchain import  Blockchain
from db_provider import Server
from data_owner import DataOwner
import merkletools
from adv_client import  MaliciousClient
#This class serves as a query client and also perform verification
class QueryClient:

    def __init__(self, server,blockchain):
        self.server = server
        self.blockchain = blockchain

    #perform query to server
    def query_by_key(self,key):
        return self.server.get_data(key)

    # get proof from server's merkle tree
    def retrieve_verification_path_by_tree(self, key_index):
        pass

    # get index from server's merkle tree
    def retrieve_key_index_in_tree(self,key):
        pass

    # get merkle root from blockchain
    def retrieve_root_from_blockchain(self):
        pass

    #Query clients issue query verifications
    def query_verification(self, retrieved_value, proofs, root_from_contract):
        pass













