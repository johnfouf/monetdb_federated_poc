import merge_broadcast
import asyncio

async def merge(global_node, local_nodes, localtable, globaltable, localschema):
    await asyncio.sleep(0)
    merge_broadcast.merge(global_node[2], local_nodes, localtable, globaltable, localschema)


async def broadcast(global_node, local_nodes, globalresulttable, globalschema):
    await asyncio.sleep(0)
    merge_broadcast.broadcast(global_node[1], local_nodes, globalresulttable, globalschema)
    
async def transfer(node1,  localtable, node2, transferschema):
    await asyncio.sleep(0)
    merge_broadcast.transferdirect(node1, localtable, node2)