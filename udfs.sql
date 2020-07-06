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