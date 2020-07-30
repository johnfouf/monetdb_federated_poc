# monetdb_federated_poc

<b>Installation</b>
1) Install monetdb from source (https://www.monetdb.org/Developers/SourceCompile) to all the nodes of the federation
2) Create databases in each node. The tables that will take place in the federation should have the same schema in all local nodes
3) Create the databases in the tmpfs of your VMs, since remote tables at the time are created on disk, it makes a big difference in execution times.
4) Python libraries for algorithms are in `algorithms` folder. Set this to path and update udfs.sql file that appends the path hard-coded.
5) Run udfs.sql file in mclient in all the monetdb databases.
6) Install dependencies: `pip3 install tornado`, `pip3 install numpy`
7) Include in servers.py file all the global/local nodes. The first node is the global.


<b>Usage:</b> 
Run server: <br>

`python3 mserver.py`


<b>URL Request Post:</b> <br>
Two fields: <br>
<br> 1) `algorithm` (e.g., "pearson")
<br> 2) `params`: valid json including table name, attributes and filters. e.g. Filters follow the DNF (disjunctive normal form:
The innermost tuples each describe a single column predicate. The list of inner predicates is interpreted as a conjunction (AND), forming a more selective and multiple column predicate. Finally, the most outer list combines these filters as a disjunction (OR).
`{"table":"data", "attributes":["c1","c2"],"parameters":[0.7,4],"filters":[[["c1",">","2"],["c1","<","10000"]],[["c1",">","0"]]]}`

<br>
<b>Implement a new algorithm:</b> <br>

1) Add its UDFs to udf.sql file
2) Add its lib to algorithms folder (optional)
3) Add an [algorithm name].py file to algorithms folder which returns the sql query for each step of the algorithm
4) Update schema.json file accordingly

<br>
<b>Other features:</b> <br>
1) Updating servers.py file the module is auto reloaded online and does not require restarting
2) A simple fault tolerance has been added for local nodes. Currently, when a local node fails once does not take part in the federation any more.

<br>
<br>
<b>Todo:</b> <br>

1) Evaluate fault tolerance and make it more robust
3) Global node failure -> assign another global
