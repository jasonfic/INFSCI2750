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
        result = self.server.get_data(key)
        for row in result:
            val = row.value
            print(val)
            return val

    # get proof from server's merkle tree
    def retrieve_verification_path_by_tree(self, key_index):
        mt = self.server.merkle_tree
        return mt.get_proof(key_index)

    # get index from server's merkle tree
    def retrieve_key_index_in_tree(self,key):
        mt = self.server.merkle_tree
        return mt.get_leaf(key)

    # get merkle root from blockchain
    def retrieve_root_from_blockchain(self):
        return self.blockchain.get_merkle_root()

    #Query clients issue query verifications
    def query_verification(self, retrieved_value, proofs, root_from_contract):
        mt = self.server.merkle_tree
        target_hash = self.retrieve_key_index_in_tree(retrieved_value)
        return mt.validate_proof(proofs, target_hash, root_from_contract)













