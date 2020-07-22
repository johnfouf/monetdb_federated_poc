import mapi_async
from pymonetdb import mapi
import asyncio
from urllib.parse import urlparse


#### connect to servers each server has a synchronous and an async connection due to limited concurrency of monetdb on create {remote} tables
async def initialize(args):
    db_objects = {}
    db_objects['local'] = []
    db_objects['global'] = {}

    glob = urlparse(args[1])
    server = mapi_async.Connection()
    await server.connect(hostname=glob.hostname, port=glob.port, username="monetdb",
                 password="monetdb", database=glob.path[1:], language="sql")
    
    db_objects['global']['async_con'] = server   ## global asynchronous connection object - this is used to execute commands on the remote database
    db_objects['global']['dbname'] = args[1]  ## global database name - required by remote tables to connect to the remote database
    
    server = mapi.Connection()
    server.connect(hostname=glob.hostname, port=glob.port, username="monetdb",
                 password="monetdb", database=glob.path[1:], language="sql")
    
    db_objects['global']['con'] = server ## global blocking connection objects - required at this time because monetdb does not support create commands concurrently
     
    for db in args[2:]:
        loc = urlparse(db)
        server = mapi_async.Connection()
        await server.connect(hostname=loc.hostname, port=loc.port, username="monetdb",
                 password="monetdb", database=loc.path[1:], language="sql")
        server2 = mapi.Connection()
        server2.connect(hostname=loc.hostname, port=loc.port, username="monetdb",
                 password="monetdb", database=loc.path[1:], language="sql")
        
        local_node = {}
        local_node['async_con'] = server
        local_node['dbname'] = db
        local_node['con'] = server2
        db_objects['local'].append(local_node)  # for each local node an asynchronous connection object, the database name, and a blocking connection object
    return db_objects
  
    
    
async def disconnect(db_objects):
    await  db_objects['global']['async_con'].disconnect()
    db_objects['global']['con'].disconnect()
    for local in db_objects['local']:
        await local['async_con'].disconnect()
        local['con'].disconnect()
    db_objects = {}
    



