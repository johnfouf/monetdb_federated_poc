def _local_init(viewlocaltable):
    return "select numpy_count(c1) as c1 from %s;" %viewlocaltable
    
def _global_iter(globaltable):
    return "select numpy_sum(c1) as c1 from %s;" %globaltable
    
def _local_iter(globalresulttable):
    return "select numpy_sum(c1) as c1 from %s;" %(globalresulttable)
    
def _global(globaltable):
    return "select numpy_sum(c1) as c1 from %s;" %globaltable
