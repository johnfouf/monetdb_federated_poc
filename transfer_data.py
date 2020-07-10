import merge_broadcast
import asyncio

async def merge(global_node, local_nodes, localtable, globaltable):
    await asyncio.sleep(0)
    merge_broadcast.merge(global_node[2], local_nodes, localtable, globaltable)


async def broadcast(global_node, local_nodes, globalresulttable):
    await asyncio.sleep(0)
    merge_broadcast.broadcast(global_node[1], local_nodes, globalresulttable)
    
async def transfer(node1,  localtable, node2):
    await asyncio.sleep(0)
    merge_broadcast.transferdirect(node1, localtable, node2)