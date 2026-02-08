import hashlib
import bisect
import struct

class ConsistentHash:
    def __init__(self, nodes=None, replicas=100):
        """
        initializes the consistent hash ring.
        :param nodes: Initial list of physical nodes (strings).
        :param replicas: Number of virtual nodes (vnodes) per physical node.
        """
        self.replicas = replicas
        self.ring = dict()  # Maps hash -> physical_node
        self.sorted_keys = []  # Sorted list of hash values for binary search

        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key):
        """
        Returns a 32-bit integer hash for a given key.
        Using MD5 as it is deterministic and distributes well for this use case.
        """
        # Encode key to bytes, hash with MD5, digest to bytes
        m = hashlib.md5(key.encode('utf-8'))
        # Take the first 4 bytes and convert to a standard integer
        return struct.unpack('>I', m.digest()[:4])[0]

    def add_node(self, node):
        """Adds a physical node (and its vnodes) to the ring."""
        for i in range(self.replicas):
            # Create a unique key for the virtual node, e.g., "NodeA:0", "NodeA:1"
            vnode_key = f"{node}:{i}"
            h = self._hash(vnode_key)
            self.ring[h] = node
            self.sorted_keys.append(h)
        
        self.sorted_keys.sort()

    def remove_node(self, node):
        """Removes a physical node (and its vnodes) from the ring."""
        for i in range(self.replicas):
            vnode_key = f"{node}:{i}"
            h = self._hash(vnode_key)
            del self.ring[h]
            self.sorted_keys.remove(h)

    def get_node(self, key):
        """
        Given a key, returns the physical node responsible for it.
        Uses Binary Search.
        """
        if not self.ring:
            return None

        h = self._hash(key)
        
        # Binary search to find the first hash on the ring >= key's hash
        idx = bisect.bisect_left(self.sorted_keys, h)

        # Wrap-around: if idx is at the end, the key belongs to the first node
        if idx == len(self.sorted_keys):
            idx = 0

        return self.ring[self.sorted_keys[idx]]

# --- SIMULATION DRIVER ---

def run_simulation():
    import uuid
    import statistics

    print("--- Starting Consistent Hashing Simulation ---")

    # 1. Initial State: 10 physical nodes
    # Using 100 vnodes per physical node is a common industry standard (e.g., Cassandra)
    nodes = [f"Node_{i}" for i in range(10)]
    ch = ConsistentHash(nodes=nodes, replicas=100)
    
    # 2. Distribute Keys: 10,000 unique keys
    num_keys = 10000
    keys = [str(uuid.uuid4()) for _ in range(num_keys)]
    
    # Initial mapping: key -> node
    initial_mapping = {}
    node_load = {n: 0 for n in nodes}

    for k in keys:
        node = ch.get_node(k)
        initial_mapping[k] = node
        node_load[node] += 1

    print(f"\n[Initial Distribution] Keys mapped to {len(nodes)} nodes.")
    print(f"Load Stats: Mean={statistics.mean(node_load.values())}, "
          f"StDev={statistics.stdev(node_load.values()):.2f}")
    
    # 3. Fail Nodes: Remove 2 nodes (20% failure)
    nodes_to_remove = ["Node_8", "Node_9"]
    print(f"\n[Failure Event] Removing nodes: {nodes_to_remove}")
    for n in nodes_to_remove:
        ch.remove_node(n)

    # 4. Remap and Calculate Stats
    moved_keys = 0
    new_node_load = {n: 0 for n in nodes if n not in nodes_to_remove}

    for k in keys:
        new_node = ch.get_node(k)
        if new_node != initial_mapping[k]:
            moved_keys += 1
        new_node_load[new_node] += 1

    percent_moved = (moved_keys / num_keys) * 100
    print(f"\n[Remapping Stats] Total keys: {num_keys}")
    print(f"Keys moved: {moved_keys} ({percent_moved:.2f}%)")
    
    # Validation: In ideal consistent hashing, if we lose 20% of nodes, 
    # we expect roughly 20% of keys to move.
    print(f"Ideal theoretical movement: ~20.00%")

    print("\n[New Distribution Stats]")
    print(f"Min keys per node: {min(new_node_load.values())}")
    print(f"Max keys per node: {max(new_node_load.values())}")
    print(f"Standard Deviation: {statistics.stdev(new_node_load.values()):.2f}")

if __name__ == "__main__":
    run_simulation()
