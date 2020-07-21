from threading import Thread
import algorithms
import importlib
import pymonetdb
import json
import asyncio
import parse_mapi_result

def getpackage(algorithm):
    mpackage = "algorithms"
    importlib.import_module(mpackage)
    algo = importlib.import_module("."+algorithm,mpackage)
    return algo 

@asyncio.coroutine
async def local_run_inparallel(local,query):
    await local.cmd(query)
    
async def createlocalviews(local_nodes, viewlocaltable, params):
      params = json.loads(params)
      ####### do it  in parallel, check if dataset and params exists #########
      for i,local in enumerate(local_nodes):
           result = parse_mapi_result.parse(await local[0].cmd(local[0].bind("sselect id from tables where tables.system = false and tables.name = %s;",(params['table'],))));
           if result == []:
               raise Exception('Dataset does not exist in all local nodes')
      #############################################################
      
           for attribute in params['attributes']:
               attr = parse_mapi_result.parse(await local[0].cmd(local[0].bind("sselect name from columns where table_id = '"+str(result[0][0])+"' and name = %s;",(attribute,))));
               if attr == []:
                   raise Exception('Attribute '+attribute+' does not exist in all local nodes')
           for attribute in params['filters']:
               attr = parse_mapi_result.parse(await local[0].cmd(local[0].bind("sselect name from columns where table_id = '"+str(result[0][0])+"' and name = %s;",(attribute[0],))));
               if attr == []:
                   raise Exception('Attribute '+attribute[0]+' does not exist in all local nodes')
      #############################################################
      
      filterpart = " "
      vals = []
      for i,filt in enumerate(params["filters"]):
          if filt[1] not in [">","<","<>",">=","<=","="]:
              raise Exception('Operator '+filt[1]+' not valid')
          filterpart += filt[0] + filt[1] + "%s"
          vals.append(filt[2]) 
          if i < len(params["filters"])-1:
              filterpart += ' and '
      for i,local in enumerate(local_nodes):
           local[2].cmd(local[0].bind("sCREATE VIEW "+viewlocaltable+" AS select "+','.join(params['attributes'])+" from "+params['table']+" where"+ filterpart +";", vals))


async def run_local_init(local_nodes,localtable, algorithm, viewlocaltable, localschema):
      for i,local in enumerate(local_nodes):
           local[2].cmd("screate table %s (%s);" %(localtable+"_"+str(i),localschema))
      await asyncio.gather(*[local_run_inparallel(local[0],"s"+getpackage(algorithm)._local_init(localtable+"_"+str(i),viewlocaltable)) for i,local in enumerate(local_nodes)] )
      
async def run_local(local_nodes,localtable, algorithm, viewlocaltable, localschema):
       for i,local in enumerate(local_nodes):
           local[2].cmd("screate table %s (%s);" %(localtable+"_"+str(i),localschema))
       await asyncio.gather(*[local_run_inparallel(local[0],"s"+getpackage(algorithm)._local(localtable+"_"+str(i),viewlocaltable)) for i,local in enumerate(local_nodes)] )

async def run_local_iter(local_nodes,localtable,globalresulttable, algorithm, viewlocaltable, localschema):
      for i,local in enumerate(local_nodes):
           local[2].cmd("screate table %s (%s);" %(localtable+"_"+str(i),localschema))
      await asyncio.gather(*[local_run_inparallel(local[0],"s"+getpackage(algorithm)._local_iter(localtable+"_"+str(i),globalresulttable)) for i,local in enumerate(local_nodes)] )
      
async def run_global_final(global_node, globaltable, algorithm):
      result = await global_node[0].cmd("s"+getpackage(algorithm)._global(globaltable))
      return parse_mapi_result.parse(result)
      
async def run_global_iter(global_node, local_nodes, globaltable, localtable, globalresulttable, algorithm, viewlocaltable, globalschema):
      global_node[2].cmd("sdrop table if exists %s;" %globalresulttable)
      global_node[2].cmd("screate table %s (%s);" %(globalresulttable,globalschema))
      await global_node[0].cmd("s"+getpackage(algorithm)._global_iter(globaltable, globalresulttable))
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
