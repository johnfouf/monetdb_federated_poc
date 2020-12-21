import algorithms
import json
import asyncio
import time

current_time = lambda: int(round(time.time() * 1000))

class Task:
    def __init__(self, db_objects, table_id, params, transfer_runner, algorithm_name):
        self.localtable = "local" + table_id
        self.globaltable = "global" + table_id
        self.viewlocaltable = "localview" + table_id
        self.globalresulttable = "globalres" + table_id
        self.params = params
        self.attributes = params['attributes']
        self.parameters = params['parameters']
        self.db_objects = db_objects
        self.transfer_runner = transfer_runner
        self.local_schema = None
        self.global_schema = None
        self.iternum = 0
        self.table_id = table_id
        self.algorithm_name = algorithm_name




    async def _initialize_local(self, step):
        _local_execute_calls = [
            local["async_con"].submit(self.algorithm_name, step, self.table_id, self.params, 0, id)
            for id, local in enumerate(self.db_objects["local"])
        ]
        await asyncio.gather(*_local_execute_calls)

    async def _initialize_global_schema(self):
        query = "drop table if exists %s; create table if not exists %s (%s);" % (
            self.globalresulttable,
            self.globalresulttable,
            self.global_schema,
        )
        await self.db_objects["global"]["async_con"].cursor().execute(query)


    def create_task_generator(self, algorithm):
        return algorithm(
            self.viewlocaltable,
            self.globaltable,
            self.parameters,
            self.attributes,
            self.globalresulttable
        )

    async def init_tables(self, step, local_schema, global_schema):
        self.global_schema = global_schema
        self.local_schema = local_schema
        await self._initialize_local(step)
        await self._initialize_global_schema()
        await self.transfer_runner.initialize_global(local_schema)

    # parameters binding is an important processing step on the parameters that will be concatenated in an SQL query
    # to avoid SQL injection vulnerabilities. This step is not implemented for postgres yet but only for monetdb
    # so algorithms that contain parameters (other than attribute names) will raise an exception if running with postgres
    def bindparameters(self, parameters):
        boundparam = []
        for i in parameters:
            if isinstance(i, (int, float, complex)):
                boundparam.append(i)
            else:
                boudparam.append(self.db_objects["global"]["async_con"].bind_str(i))
        return boundparam

    async def createlocalviews(self, step):
        t1 = current_time()
        _create_view_calls = [
            local["async_con"].submit(self.algorithm_name, step, self.table_id, self.params, 0, id )
            for id,local in enumerate(self.db_objects["local"])
        ]
        await asyncio.gather(*_create_view_calls)
        print("time " + str(current_time() - t1))

    #### run a task on all local nodes and sets up the transfer of the results to global node

    async def task_local(self, schema, step):
        t1 = current_time()
        static_schema = 1
        if self.local_schema == None or self.local_schema != schema:
            self.local_schema = schema
            static_schema = 0
            await self.transfer_runner.initialize_global(self.local_schema)

        _local_execute_calls = [
            local['async_con'].submit(self.algorithm_name, step, self.table_id, self.params, static_schema, id)
            for id, local in enumerate(self.db_objects["local"])
        ]
        await asyncio.gather(*_local_execute_calls)

        print("time " + str(current_time() - t1))

    ### runs a task on global node using data received by the local nodes
    async def task_global(self, step, schema, sqlscript):

        t1 = current_time()
        if self.global_schema == None or self.global_schema != schema:
            self.global_schema = schema
            await self._initialize_global_schema()
            _local_execute_calls = [
                local["async_con"].submit(self.algorithm_name, step, self.table_id, self.params, 0, id)
                for id, local in enumerate(self.db_objects["local"])
            ]
            await asyncio.gather(*_local_execute_calls)
        if 'iternum' not in schema and 'history' not in schema:
            query = (
                "delete from "
                + self.globalresulttable
                + "; insert into "
                + self.globalresulttable
                + " "
                + sqlscript
                + "; "
                + f" select * from {self.globalresulttable};"
            )
        elif 'iternum' in schema:
            query = (
                    "insert into "
                    + self.globalresulttable
                    + " "
                    + sqlscript
                    + "; "
                    +"delete from "
                    + self.globalresulttable
                    + " where iternum <= "+ str(self.iternum-1)
                    + ";"
                    + f" select * from {self.globalresulttable};"
            )
        else:
            query = (
                    "insert into "
                    + self.globalresulttable
                    + " "
                    + sqlscript
                    + "; "
                    + f"select * from {self.globalresulttable} where iternum = {self.iternum};"
            )

        cur = self.db_objects["global"]["async_con"].cursor()
        result = await cur.execute(query)
        print("iternum: "+str(self.iternum))
        print("time " + str(current_time() - t1))
        self.iternum += 1

        return await cur.fetchall()


    async def clean_up(self):
        await self.db_objects["global"]["async_con"].clean_tables(
            self.db_objects, self.globaltable, self.localtable, self.viewlocaltable, self.globalresulttable
        )
        _local_execute_calls = [
            local["async_con"].submit(self.algorithm_name, -1, self.table_id, self.params, 0, id)
            for id, local in enumerate(self.db_objects["local"])
        ]
        await asyncio.gather(*_local_execute_calls)
