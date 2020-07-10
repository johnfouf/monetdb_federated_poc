CREATE or replace AGGREGATE numpy_sum(val INTEGER) 
RETURNS INTEGER 
LANGUAGE PYTHON {
    return numpy.sum(val)
};


CREATE or replace AGGREGATE numpy_count(val INTEGER) 
RETURNS INTEGER 
LANGUAGE PYTHON {
    return val.size
};


select * from python_aggregate(c2) from data2 group by c1;


CREATE AGGREGATE python_aggregate(val INTEGER) 
RETURNS INTEGER 
LANGUAGE PYTHON {
    try:
        unique = numpy.unique(aggr_group)
        x = numpy.zeros(shape=(unique.size))
        for i in range(0, unique.size):
            x[i] = numpy.sum(val[aggr_group==unique[i]])
    except NameError:
        # aggr_group doesn't exist. no groups, aggregate on all data
        x = numpy.sum(val)
    return(x)
};




CREATE or replace AGGREGATE python_aggregate2(val INTEGER) 
RETURNS INTEGER 
LANGUAGE PYTHON {
    from cffi import FFI
    ffi = FFI()
    arr_vals = ffi.from_buffer('int[]', val)
    arr_groups = ffi.from_buffer('int[]', aggr_group)
    
    res = [0]*numpy.unique(aggr_group).size
    j = 0
    for v in arr_vals:
        res[arr_groups[j]] +=  v
        j+=1
    
    return numpy.array(res) 
};