from threading import Thread
import algorithms
import pymonetdb
import json


def local_run_inparallel(local,query):
    local.cmd(query)


def createlocalviews(local_nodes, viewlocaltable, params):
      threads = []
      params = json.loads(params)
      ### todo check tables and attributes to avoid sql injection, run filters
      for i,local in enumerate(local_nodes):
          t = Thread(target = local_run_inparallel, args = (local[0],"sCREATE VIEW %s AS select * from data;" %viewlocaltable))
          t.start()
          threads.append(t)
      
      for t in threads:
          t.join()


def run_local_init(local_nodes,localtable, algorithm, viewlocaltable):
      threads = []
      
      for i,local in enumerate(local_nodes):
          t = Thread(target = local_run_inparallel, args = (local[0],"s"+algorithms.count_local_init(localtable+"_"+str(i),viewlocaltable)))
          t.start()
          threads.append(t)
      
      for t in threads:
          t.join()
          

def run_local(local_nodes,localtable, algorithm, viewlocaltable):
      threads = []
      
      for i,local in enumerate(local_nodes):
          t = Thread(target = local_run_inparallel, args = (local[0],"s"+algorithms.count_local(localtable+"_"+str(i),viewlocaltable)))
          t.start()
          threads.append(t)
      
      for t in threads:
          t.join()
          
          
def run_local_iter(local_nodes,localtable,globalresulttable, algorithm, viewlocaltable):
      threads = []
      
      for i,local in enumerate(local_nodes):
          t = Thread(target = local_run_inparallel, args = (local[0],"s"+algorithms.count_local_iter(localtable+"_"+str(i),globalresulttable)))
          t.start()
          threads.append(t)
      
      for t in threads:
          t.join()
     

      
def run_global_final(global_node, globaltable, algorithm):
      con = pymonetdb.connect(username="monetdb", password="monetdb",port=50000,hostname="127.0.0.1", database=global_node[1])
      cur = con.cursor()
      cur.execute(algorithms.count_global(globaltable))
      result = cur.fetchall()
      cur.close()
      con.close()
      return result
      #return global_node[0].cmd("s"+algorithms.count_global(globaltable))
      
def run_global_iter(global_node, local_nodes, globaltable, localtable, globalresulttable, algorithm, viewlocaltable):
      global_node[0].cmd("sdrop table if exists %s;" %globalresulttable)
      global_node[0].cmd("s"+algorithms.count_global_iter(globaltable, globalresulttable))
      print(global_node[0].cmd("sselect * from %s;" %globalresulttable))
      partialclean_up(global_node, local_nodes, globaltable, localtable, viewlocaltable)

def partialclean_up(global_node, local_nodes, globaltable, localtable, viewlocaltable):
    global_node[0].cmd("sdrop table if exists %s;" %globaltable)
    for i,local in enumerate(local_nodes):
          local[0].cmd("sdrop table if exists "+localtable+"_"+str(i)+";")
          global_node[0].cmd("sdrop table if exists "+localtable+"_"+str(i)+";")

    
def clean_up(global_node, local_nodes, globaltable, localtable, viewlocaltable, globalrestable):
      global_node[0].cmd("sdrop table if exists %s;" %globaltable)
      global_node[0].cmd("sdrop table if exists %s;" %globalrestable)
      for i,local in enumerate(local_nodes):
          local[0].cmd("sdrop view if exists "+viewlocaltable+";")
          local[0].cmd("sdrop table if exists "+globalrestable+";")
          local[0].cmd("sdrop table if exists "+localtable+"_"+str(i)+";")
          global_node[0].cmd("sdrop table if exists "+localtable+"_"+str(i)+";")
