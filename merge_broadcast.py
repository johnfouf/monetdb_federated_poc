def merge(global_node, local_nodes, localtable, globaltable):
    global_node.cmd("sDROP TABLE IF EXISTS %s;" %globaltable);
    global_node.cmd("sCREATE MERGE TABLE %s (c1 int);" %globaltable);
    for i,local_node in enumerate(local_nodes):
        global_node.cmd("sDROP TABLE IF EXISTS %s_%s;" %(localtable, i))
        global_node.cmd("sCREATE REMOTE TABLE %s_%s (c1 int) on 'mapi:monetdb://127.0.0.1:50000/%s'; " %(localtable, i, local_node[1]))
        global_node.cmd("sALTER TABLE %s ADD TABLE %s_%s;" %(globaltable,localtable,i));
        
        
        
def broadcast(global_node, local_nodes, globalresulttable):
    for i,local_node in enumerate(local_nodes):
        local_node[2].cmd("sDROP TABLE IF EXISTS %s;" %globalresulttable)
        local_node[2].cmd("sCREATE REMOTE TABLE %s (c1 int) on 'mapi:monetdb://127.0.0.1:50000/%s';" %(globalresulttable, global_node))

        
        
def transferdirect(node1, localtable, node2):
    node2[2].cmd("sDROP TABLE IF EXISTS %s;" %localtable)
    node2[2].cmd("sCREATE REMOTE TABLE %s (c1 int) on 'mapi:monetdb://127.0.0.1:50000/%s';" %(localtable, node1[1]))
        
 
        
def transferviaglobal(node1, globalnode, node2, localtable):
    #same as above but not direct between the 2 local nodes
    pass