import datetime
import random
import run_step
import transfer_data

def get_uniquetablename():
      return 'user{0}'.format(datetime.datetime.now().microsecond + (random.randrange(1, 100+1) * 100000))

def run_simple(algorithm,  global_node, local_nodes, localtable, globaltable, viewlocaltable):
      try:
          run_step.run_local(local_nodes,localtable, algorithm, viewlocaltable)
          transfer_data.merge(global_node, local_nodes, localtable, globaltable)
          result = run_step.run_global_final(global_node, globaltable, algorithm)
      except:
          run_step.clean_up(global_node,local_nodes, globaltable,localtable)
          raise
      run_step.clean_up(global_node,local_nodes, globaltable,localtable)
      return result
      
def run_iterative(algorithm, global_node, local_nodes, localtable, globaltable, globalresulttable, viewlocaltable):
      j = 0
      try:
          run_step.run_local_init(local_nodes,localtable, algorithm, viewlocaltable)
          j+=1
          for i in range(20):
              transfer_data.merge(global_node, local_nodes, localtable, globaltable)

              run_step.run_global_iter(global_node, local_nodes, globaltable, localtable, globalresulttable, algorithm, viewlocaltable)
              j+=1

              transfer_data.broadcast(global_node, local_nodes, globalresulttable)

              run_step.run_local_iter(local_nodes,localtable, globalresulttable, algorithm, viewlocaltable)
              j+=1

          transfer_data.merge(global_node, local_nodes, localtable, globaltable)
          result = run_step.run_global_final(global_node, globaltable, algorithm)
          j+=1
          print(j)
      except:
          run_step.clean_up(global_node,local_nodes, globaltable,localtable, viewlocaltable,globalresulttable )
          raise
      
      
      run_step.clean_up(global_node,local_nodes, globaltable,localtable, viewlocaltable, globalresulttable)
      
      return result


def run(algorithm, params, global_node, local_nodes):
      
      table_id = get_uniquetablename()
      localtable = "local"+table_id
      globaltable = "global"+table_id
      viewlocaltable = 'localview'+table_id
      globalresulttable = "globalres"+table_id
      ## create viewlocaltable with params
      run_step.createlocalviews(local_nodes, viewlocaltable,params)
      ### check algorithm category

      #return run_simple(algorithm, params, global_node, local_nodes, localtable, globaltable, viewlocaltable)
      return run_iterative(algorithm,global_node, local_nodes, localtable, globaltable, globalresulttable, viewlocaltable)
      
      
