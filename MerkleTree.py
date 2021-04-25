from CompareCollection import CompareCollection


class MerkleTreeNode:
    def __init__(self, value, hashValue):
        self.left = None
        self.right = None
        self.value = value
        self.hashValue = hashValue


class MerkleTree(CompareCollection):
    """Easy implementation of Merkle Tree"""
    def __init__(self, hashFunc):
        self.hash = hashFunc
        self.nodes = []
        self.root = ''

    def build(self, data):
        for i in data:
            self.nodes.append(MerkleTreeNode(i, self.hash(i.encode('utf-8')).hexdigest()))

        while len(self.nodes) != 1:
            temp = []
            for i in range(0, len(self.nodes), 2):
                node1 = self.nodes[i]
                if i + 1 < len(self.nodes):
                    node2 = self.nodes[i + 1]
                else:
                    temp.append(self.nodes[i])
                    break

                concatenatedHash = node1.hashValue + node2.hashValue
                parent = MerkleTreeNode(concatenatedHash, self.hash(concatenatedHash.encode('utf-8')).hexdigest())
                parent.left = node1
                parent.right = node2
                temp.append(parent)
            self.nodes = temp

        self.root = self.nodes[0]

    def compare(self, collectionA, collectionB):
        pass
