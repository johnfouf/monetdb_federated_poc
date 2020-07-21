import datetime
import random
import run_step
import transfer_data
import json

def get_uniquetablename():
      return 'user{0}'.format(datetime.datetime.now().microsecond + (random.randrange(1, 100+1) * 100000))

async def run_simple(algorithm, params, global_node, local_nodes, localtable, globaltable, viewlocaltable, globalresulttable, localschema):
      try:
          await run_step.run_local(local_nodes,localtable, algorithm, viewlocaltable, localschema)
          await transfer_data.merge(global_node, local_nodes, localtable, globaltable, localschema)
          result = await run_step.run_global_final(global_node, globaltable, algorithm)
      except:
          await run_step.clean_up(global_node,local_nodes, globaltable,localtable,viewlocaltable, globalresulttable )
          raise
      await run_step.clean_up(global_node,local_nodes, globaltable,localtable,viewlocaltable, globalresulttable)
      return result
      
      
async def run_iterative(algorithm, global_node, local_nodes, localtable, globaltable, globalresulttable, viewlocaltable, localschema, globalschema):
      j = 0
      try:
          await run_step.run_local_init(local_nodes,localtable, algorithm, viewlocaltable, localschema)
          j+=1
          for i in range(20):
              await transfer_data.merge(global_node, local_nodes, localtable, globaltable, localschema)

              await run_step.run_global_iter(global_node, local_nodes, globaltable, localtable, globalresulttable, algorithm, viewlocaltable, globalschema)
              j+=1

              await transfer_data.broadcast(global_node, local_nodes, globalresulttable, globalschema)

              await run_step.run_local_iter(local_nodes,localtable, globalresulttable, algorithm, viewlocaltable, localschema)
              j+=1

          await transfer_data.merge(global_node, local_nodes, localtable, globaltable, localschema)
          result = await run_step.run_global_final(global_node, globaltable, algorithm)
          j+=1
          print(j)
      except:
          await  run_step.clean_up(global_node,local_nodes, globaltable,localtable, viewlocaltable,globalresulttable )
          raise
      
      
      await run_step.clean_up(global_node,local_nodes, globaltable,localtable, viewlocaltable, globalresulttable)
      
      return result


async def run(algorithm, params, global_node, local_nodes):
      
      table_id = get_uniquetablename()
      localtable = "local"+table_id
      globaltable = "global"+table_id
      viewlocaltable = 'localview'+table_id
      globalresulttable = "globalres"+table_id
      ## create viewlocaltable with params
      await run_step.createlocalviews(local_nodes, viewlocaltable,params)
      ### check algorithm category
      ### get schema of intermediate tables
      

      with open('schema.json') as json_file:
          data = json.load(json_file)
      
      for c,algo in enumerate([ data['algorithms'][i]['name'] for i,j in enumerate(data['algorithms'])]):
          if algorithm == algo:
              if data['algorithms'][c]['type'] == 'simple':
                  result  = await run_simple(algorithm, params, global_node, local_nodes, localtable, globaltable, viewlocaltable, globalresulttable, data['algorithms'][c]['local_schema'])
              elif  data['algorithms'][c]['type'] == 'multiple':
                  result =  await run_iterative(algorithm,global_node, local_nodes, localtable, globaltable, globalresulttable, viewlocaltable, data['algorithms'][c]['local_schema'], data['algorithms'][c]['global_schema'])
      return result
      
      
