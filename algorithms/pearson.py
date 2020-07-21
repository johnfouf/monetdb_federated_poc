def _local(localtable, viewlocaltable):
    return "insert into %s select * from pearson_local((select * from %s));" %(localtable,viewlocaltable)
    
def _global(globaltable):
    return "select * from pearson_global((select * from %s));" %globaltable
