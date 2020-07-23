Execution: `python3 mfederate.py http://host:port`
host:port is where mserver is running.
<br>example query:<br>
`select pearson(c1,c2) from data where c1>1 and c3>4;`
<br>

Table `data` and attributes `c1,c2,c3` should exist in all the local nodes
