def _local_init(localtable, viewlocaltable):
    return "insert into %s select numpy_count(c1) as c1 from %s;" %(localtable,viewlocaltable)
    
def _global_iter(globaltable, globalresulttable):
    return "insert into %s select numpy_sum(c1) as c1 from %s;" %(globalresulttable, globaltable)
    
def _local_iter(localtable, globalresulttable):
    return "insert into %s select numpy_sum(c1) as c1 from %s;" %(localtable, globalresulttable)
    
def _global(globaltable):
    return "select numpy_sum(c1) as c1 from %s;" %globaltable
