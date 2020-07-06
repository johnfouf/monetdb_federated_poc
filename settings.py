import pymonetdb
from pymonetdb import mapi


global_node = []
local_nodes = []

def initialize(args):
    server = mapi.Connection()
    server.connect(hostname="127.0.0.1", port=50000, username="monetdb",
                 password="monetdb", database=args[1], language="sql")
    
    global_node.append(server)
    global_node.append(args[1])
    global_node.append(pymonetdb.connect(username="monetdb", password="monetdb",port=50000,hostname="127.0.0.1", database=args[1]))

    for db in args[2:]:
        server = mapi.Connection()
        server.connect(hostname="127.0.0.1", port=50000, username="monetdb",
                 password="monetdb", database=db, language="sql")
        local_nodes.append((server, db))
    
    



