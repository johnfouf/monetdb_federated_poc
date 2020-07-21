import datetime
import random
import run_step
import transfer_data
import json
import importlib


def get_package(algorithm):
    mpackage = "algorithms"
    importlib.import_module(mpackage)
    algo = importlib.import_module("."+algorithm,mpackage)
    return algo 

def get_uniquetablename():
      return 'user{0}'.format(datetime.datetime.now().microsecond + (random.randrange(1, 100+1) * 100000))

######### simple local global algorithm params:
# algorithm: the algorithm name provided by the user 
# global_node, local_nodes: the connection objects and the database names 
# localtable: the name of the result table in localnodes
# globaltable: the name of the table in global node which contains the merge of all the local result tables
# viewlocaltable: the view which contains the data that will be processed
# localschema: the schema of the result table in localnodes                  

async def run_simple(algorithm, global_node, local_nodes, localtable, globaltable, viewlocaltable, localschema):
    await run_step.run_local(local_nodes,localtable, algorithm, viewlocaltable, localschema)
    await transfer_data.merge(global_node, local_nodes, localtable, globaltable, localschema)
    return await run_step.run_global_final(global_node, globaltable, algorithm)
      

######### iterative algorithm params:
# algorithm: the algorithm name provided by the user 
# global_node, local_nodes: the connection objects and the database names 
# localtable: the name of the result table in localnodes
# globaltable: the name of the table in global node which contains the merge of all the local result tables
# viewlocaltable: the view which contains the data that will be processed
# globalresulttable: the name of the result table in globalnode, not used here since simple local global algorithms just return their global result without storing it.
# localschema: the schema of the result table in localnodes 
# globalschema: the schema of the result table in global node
          
async def run_iterative(algorithm, global_node, local_nodes, localtable, globaltable, globalresulttable, viewlocaltable, localschema, globalschema):
    await run_step.run_local_init(local_nodes,localtable, algorithm, viewlocaltable, localschema)
    for i in range(20):
        await transfer_data.merge(global_node, local_nodes, localtable, globaltable, localschema)
        await run_step.run_global_iter(global_node, local_nodes, globaltable, localtable, globalresulttable, algorithm, viewlocaltable, globalschema)
        await transfer_data.broadcast(global_node, local_nodes, globalresulttable, globalschema)
        await run_step.run_local_iter(local_nodes,localtable, globalresulttable, algorithm, viewlocaltable, localschema)
    await transfer_data.merge(global_node, local_nodes, localtable, globaltable, localschema)
    return await run_step.run_global_final(global_node, globaltable, algorithm)


#### run function:
# get the algorithm name and accesses the corresponding python module
# parses and processes the params which contain: the table name, the attributes and the filters
# gets also global_node, local_nodes: the connection objects to all the nodes of the federation
# it decides the type of the algorithm using schema.json file, calls the appropriate execution function
# cleans up the servers and returns the results

     
async def run(algorithm, params, global_node, local_nodes):
      #### create unique table names
      table_id = get_uniquetablename()
      localtable = "local"+table_id
      globaltable = "global"+table_id
      viewlocaltable = 'localview'+table_id
      globalresulttable = "globalres"+table_id
      result = []
      
      ### get the corresponding algorithm python module using algorithm name
      module = get_package(algorithm)
      # create database views on local databases - each view processes the filters and the selected attributes on the requested table
      # the algorithm won't run directly on the local dataset but on the view
      await run_step.createlocalviews(local_nodes, viewlocaltable,params)
      
      ##### schema.json contains info about each algorithm: the name, the type (simple, iterative etc.) and the intermediate result schema
      with open('schema.json') as json_file:
          data = json.load(json_file)
      
      ##### according to the input algorithm param and the json algorithm properties select to run the algorithm
      for c,algo in enumerate([ data['algorithms'][i]['name'] for i,j in enumerate(data['algorithms'])]):
          if algorithm == algo:
            try:
                 if data['algorithms'][c]['type'] == 'simple':
                     result  = await run_simple(module, global_node, local_nodes, localtable, globaltable, viewlocaltable, data['algorithms'][c]['local_schema'])
                 elif  data['algorithms'][c]['type'] == 'multiple':
                     result =  await run_iterative(module,global_node, local_nodes, localtable, globaltable, globalresulttable, viewlocaltable, data['algorithms'][c]['local_schema'], data['algorithms'][c]['global_schema'])
            except:
                 await run_step.clean_up(global_node,local_nodes, globaltable,localtable, viewlocaltable, globalresulttable)
                 raise
      ### clean up tables that are created during the execution
      await run_step.clean_up(global_node,local_nodes, globaltable,localtable, viewlocaltable, globalresulttable)
      return result