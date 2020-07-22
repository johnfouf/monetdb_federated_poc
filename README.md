# monetdb_federated_poc

<b>Installation</b>
1) Install monetdb from source (https://www.monetdb.org/Developers/SourceCompile) to all the nodes of the federation
2) Create databases in each node. The tables that will take place in the federation should have the same schema in all local nodes
3) Python libraries for algorithms are in `algorithms` folder. Set this to path and update udfs.sql file that appends the path hard-coded.
4) Run udfs.sql file in mclient in all the monetdb databases.
5) Install dependencies: `pip3 install tornado`, `pip3 install pymonetdb`, `pip3 install numpy`



<b>Usage:</b> 
Run server (the first argument is the global node): <br>

`python3 mserver.py monetdb://hostname:port/dbname monetdb://hostname:port/dbname monetdb://hostname:port/dbname`


<b>URL Request Post:</b> <br>
Two fields: <br>
<br> 1) `algorithm` (e.g., "pearson")
<br> 2) `params`: valid json including table name, attributes and filters. e.g. 
`{"table":"data", "attributes":["c1","c2"],"filters":[["c1",">","0"],["c1","<","10000"]]}`
