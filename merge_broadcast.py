def merge(global_node, local_nodes, localtable, globaltable, localschema):
    global_node.cmd("sDROP TABLE IF EXISTS %s;" %globaltable);
    global_node.cmd("sCREATE MERGE TABLE %s (%s);" %(globaltable,localschema));
    for i,local_node in enumerate(local_nodes):
        global_node.cmd("sDROP TABLE IF EXISTS %s_%s;" %(localtable, i))
        global_node.cmd("sCREATE REMOTE TABLE %s_%s (%s) on 'mapi:monetdb://127.0.0.1:50000/%s'; " %(localtable, i, localschema,local_node[1]))
        global_node.cmd("sALTER TABLE %s ADD TABLE %s_%s;" %(globaltable,localtable,i));  
        
def broadcast(global_node, local_nodes, globalresulttable, globalschema):
    for i,local_node in enumerate(local_nodes):
        local_node[2].cmd("sDROP TABLE IF EXISTS %s;" %globalresulttable)
        local_node[2].cmd("sCREATE REMOTE TABLE %s (%s) on 'mapi:monetdb://127.0.0.1:50000/%s';" %(globalresulttable, globalschema, global_node))   
        
def transferdirect(node1, localtable, node2, transferschema):
    node2[2].cmd("sDROP TABLE IF EXISTS %s;" %localtable)
    node2[2].cmd("sCREATE REMOTE TABLE %s (%s) on 'mapi:monetdb://127.0.0.1:50000/%s';" %(localtable, transferschema,node1[1]))
        
def transferviaglobal(node1, globalnode, node2, localtable):
    #same as above but not direct between the 2 local nodes
    pass