def _local(viewlocaltable):
    return "select * from pearson_local((select * from %s));" %viewlocaltable
    
def _global(globaltable):
    return "select * from pearson_global((select * from %s));" %globaltable
