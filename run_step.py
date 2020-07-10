from threading import Thread
import algorithms
import pymonetdb
import json
import asyncio
import parse_mapi_result

 

@asyncio.coroutine
async def local_run_inparallel(local,query):
    #await asyncio.sleep(0)
    await local.cmd(query)
    
    

async def createlocalviews(local_nodes, viewlocaltable, params):
      params = json.loads(params)
      await asyncio.sleep(0)
      for i,local in enumerate(local_nodes):
           local[2].cmd("sCREATE VIEW %s AS select * from data;" %viewlocaltable)



@asyncio.coroutine
async def run_local_init(local_nodes,localtable, algorithm, viewlocaltable):
      for i,local in enumerate(local_nodes):
           local[2].cmd("screate table %s (c1 INT);" %(localtable+"_"+str(i),))
           
      await asyncio.gather(*[local_run_inparallel(local[0],"s"+algorithms.count_local_init(localtable+"_"+str(i),viewlocaltable)) for i,local in enumerate(local_nodes)] )
      


@asyncio.coroutine
async def run_local(local_nodes,localtable, algorithm, viewlocaltable):
       for i,local in enumerate(local_nodes):
           local[2].cmd("screate table %s (c1 INT);" %(localtable+"_"+str(i),))
       await asyncio.gather(*[local_run_inparallel(local[0],"s"+algorithms.count_local(localtable+"_"+str(i),viewlocaltable)) for i,local in enumerate(local_nodes)] )

      
              
              
async def run_local_iter(local_nodes,localtable,globalresulttable, algorithm, viewlocaltable):
      for i,local in enumerate(local_nodes):
           local[2].cmd("screate table %s (c1 INT);" %(localtable+"_"+str(i),))
      await asyncio.gather(*[local_run_inparallel(local[0],"s"+algorithms.count_local_iter(localtable+"_"+str(i),globalresulttable)) for i,local in enumerate(local_nodes)] )


      
async def run_global_final(global_node, globaltable, algorithm):
      #con = pymonetdb.connect(username="monetdb", password="monetdb",port=50000,hostname="127.0.0.1", database=global_node[1])
      #cur = con.cursor()
      #cur.execute(algorithms.count_global(globaltable))
      #result = cur.fetchall()
      #cur.close()
      #con.close()
      #return result
      result = await global_node[0].cmd("s"+algorithms.count_global(globaltable))
      return parse_mapi_result.parse(result)
      
async def run_global_iter(global_node, local_nodes, globaltable, localtable, globalresulttable, algorithm, viewlocaltable):
      global_node[2].cmd("sdrop table if exists %s;" %globalresulttable)
      global_node[2].cmd("screate table %s (c1 INT);" %(globalresulttable,))
      await global_node[0].cmd("s"+algorithms.count_global_iter(globaltable, globalresulttable))

      
      #print(global_node[0].cmd("sselect * from %s;" %globalresulttable))
      await partialclean_up(global_node, local_nodes, globaltable, localtable, viewlocaltable)

async def partialclean_up(global_node, local_nodes, globaltable, localtable, viewlocaltable):
      await asyncio.sleep(0)
      global_node[2].cmd("sdrop table if exists %s;" %globaltable)
      for i,local in enumerate(local_nodes):
          local[2].cmd("sdrop table if exists "+localtable+"_"+str(i)+";")
          global_node[2].cmd("sdrop table if exists "+localtable+"_"+str(i)+";")

    
async def clean_up(global_node, local_nodes, globaltable, localtable, viewlocaltable, globalrestable):
      await asyncio.sleep(0)
      global_node[2].cmd("sdrop table if exists %s;" %globaltable)
      global_node[2].cmd("sdrop table if exists %s;" %globalrestable)
      for i,local in enumerate(local_nodes):
          local[2].cmd("sdrop view if exists "+viewlocaltable+";")
          local[2].cmd("sdrop table if exists "+globalrestable+";")
          local[2].cmd("sdrop table if exists "+localtable+"_"+str(i)+";")
          global_node[2].cmd("sdrop table if exists "+localtable+"_"+str(i)+";")
