# monetdb_federated_poc

<b>Installation</b>
1) Install monetdb from source (https://www.monetdb.org/Developers/SourceCompile) to all the nodes of the federation
2) Create databases in each node. The tables that will take place in the federation should have the same schema in all local nodes
3) Python libraries for algorithms are in `algorithms` folder. Set this to path and update udfs.sql file that appends the path hard-coded.
4) Run udfs.sql file in mclient in all the monetdb databases.



<b>Usage:</b> 
Run server: <br>

`python3 mserver.py monetdb://127.0.0.1:50000/voc monetdb://127.0.0.1:50000/voc2 monetdb://127.0.0.1:50000/voc3`

<b>URL Request Post:</b> <br>
Two fields: <br>
<br> 1) `algorithm` (e.g., "pearson")
<br> 2) `params`: valid json including table name, attributes and filters. e.g. 
`{"table":"data", "attributes":["c1","c2"],"filters":[["c1",">","0"],["c1","<","10000"]]}`
