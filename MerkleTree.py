from CompareCollection import CompareCollection
import hashlib
import merkletools


class MerkleTreeNotReadyException(ValueError):
    pass


class MerkleTreeWrapper(CompareCollection):
    """Easy implementation of Merkle Tree"""

    def __init__(self, hash_type="SHA256"):
        self.mt = merkletools.MerkleTools(hash_type=hash_type)

    def build(self, data):
        self.data = data
        for leaf in data:
            self.mt.add_leaf(str(leaf), True)
        self.mt.make_tree()

    def compare(self, collectionB):
        if not self.mt.is_ready or not collectionB.mt.is_ready:
            raise MerkleTreeNotReadyException

        return self.check(collectionB, 0, 0)

    def check(self, collectionB, item_index, level_index):
        if item_index >= len(self.mt.levels[level_index]) or \
                self.mt.levels[level_index][item_index] == collectionB.mt.levels[level_index][item_index]:
            return []
        else:
            if level_index == len(self.mt.levels) - 1:
                return [{'id': str(collectionB.data[item_index][0]),
                         'value': str(collectionB.data[item_index][1])}]
            else:
                return self.check(collectionB, item_index * 2, level_index + 1) + \
                       self.check(collectionB, item_index * 2 + 1, level_index + 1)
