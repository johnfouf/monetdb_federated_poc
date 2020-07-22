# monetdb_federated_poc

Usage: 
Run server: <br>

`python3 mserver.py monetdb://127.0.0.1:50000/voc monetdb://127.0.0.1:50000/voc2 monetdb://127.0.0.1:50000/voc3`

URL Request Post:
Two fields: <br>
<br> 1) `algorithm` (e.g., "pearson")
<br> 2) `params`: valid json including table name, attributes and filters. e.g. 
`{"table":"data", "attributes":["c1","c2"],"filters":[["c1",">","0"],["c1","<","10000"]]}`
