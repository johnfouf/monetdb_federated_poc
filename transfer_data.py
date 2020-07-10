import merge_broadcast

def merge(global_node, local_nodes, localtable, globaltable):
    merge_broadcast.merge(global_node[0], local_nodes, localtable, globaltable)


def broadcast(global_node, local_nodes, globalresulttable):
    merge_broadcast.broadcast(global_node[1], local_nodes, globalresulttable)
    
def transfer(node1,  localtable, node2):
    merge_broadcast.transferdirect(node1, localtable, node2)
