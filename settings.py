import mapi_async
from pymonetdb import mapi
import asyncio


#### connect to servers each server has a synchronous and an async connection due to limited concurrency of monetdb on create {remote} tables
async def initialize(args):
    global_node = []
    local_nodes = []
    server = mapi_async.Connection()
    await server.connect(hostname="127.0.0.1", port=50000, username="monetdb",
                 password="monetdb", database=args[1], language="sql")
    
    global_node.append(server)
    global_node.append(args[1])
    
    server = mapi.Connection()
    server.connect(hostname="127.0.0.1", port=50000, username="monetdb",
                 password="monetdb", database=args[1], language="sql")
    
    global_node.append(server)
     
    for db in args[2:]:
        server = mapi_async.Connection()
        await server.connect(hostname="127.0.0.1", port=50000, username="monetdb",
                 password="monetdb", database=db, language="sql")
        server2 = mapi.Connection()
        server2.connect(hostname="127.0.0.1", port=50000, username="monetdb",
                 password="monetdb", database=db, language="sql")
        local_nodes.append((server, db, server2))
    return global_node, local_nodes
    
async def disconnect(global_node, local_nodes):
    await  global_node[0].disconnect()
    global_node[2].disconnect()
    for local in local_nodes:
        await local[0].disconnect()
        local[2].disconnect()
    global_node = []
    local_nodes = []
    



