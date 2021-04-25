from Benchmark import testBuildCollection
from MerkleTree import MerkleTree
import hashlib

if __name__ == '__main__':
    MT = MerkleTree(hashlib.sha256)
    testBuildCollection(MT)
