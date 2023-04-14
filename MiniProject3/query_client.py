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
        print("Query By Key Result: " + str(result['value']))
        return result['value']

    # get proof from server's merkle tree
    def retrieve_verification_path_by_tree(self, key_index):
        mt = self.server.merkle_tree
        return mt.get_proof(key_index)

    # get index from server's merkle tree
    def retrieve_key_index_in_tree(self,key):
        idx = 0
        mt = self.server.merkle_tree
        for i in range(0, mt.get_leaf_count()):
            print("Tree index " + str(i) + ": " + str(mt.get_leaf(i)))
            if key == mt.get_leaf(i):
                idx = i
        return idx

    # get merkle root from blockchain
    def retrieve_root_from_blockchain(self):
        return self.blockchain.get_merkle_root()

    #Query clients issue query verifications
    def query_verification(self, retrieved_value, proofs, root_from_contract):
        mt = self.server.merkle_tree
        index = self.retrieve_key_index_in_tree(retrieved_value)
        mt_leaf = mt.get_leaf(index)
        b_val = memoryview(retrieved_value.encode('utf-8')).tobytes()
        target_hash = hashlib.sha256()
        target_hash.update(b_val)
        print("Blockchain roots match?: " + str(self.retrieve_root_from_blockchain() == root_from_contract))
        print("Retrieved value, SHA-256 encoded:" + str(target_hash.hexdigest()))
        print("Merkle leaf at index " + str(index) + ": " + str(mt_leaf))
        print("Query and Merkle Tree values match?: " + str(target_hash.hexdigest() == mt_leaf[0]))
        return mt.validate_proof(proofs, target_hash.hexdigest(), root_from_contract)












