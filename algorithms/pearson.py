def _local(viewlocaltable, parameters, attr):
    return "select * from pearson_local((select * from %s));" %viewlocaltable
    
def _global(globaltable, parameters, attr):
    return "select * from pearson_global((select * from %s));" %globaltable
