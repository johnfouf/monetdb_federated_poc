def count_local(localtable, viewlocaltable):
    return "insert into %s select numpy_count(c1) as c1 from %s;" %(localtable,viewlocaltable)
    
def count_global(globaltable):
    return "select numpy_sum(c1) as c1 from %s;" %globaltable
    
def count_local_init(localtable, viewlocaltable):
    return "insert into %s select numpy_count(c1) as c1 from %s;" %(localtable,viewlocaltable)
    
def count_global_iter(globaltable, globalresulttable):
    return "insert into %s select numpy_sum(c1) as c1 from %s;" %(globalresulttable, globaltable)
    
def count_local_iter(localtable, globalresulttable):
    return "insert into %s select numpy_sum(c1) as c1 from %s;" %(localtable, globalresulttable)
    
def count_global_final(globaltable):
    return "select numpy_sum(c1) as c1 from %s;" %globaltable