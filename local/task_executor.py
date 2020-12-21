import algorithms
import json
import asyncio
import time

current_time = lambda: int(round(time.time() * 1000))

class Task:
    def __init__(self, db_objects, table_id, params, node_id, transfer_runner):
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
        self.node_id = node_id
        self.iternum = 0

    async def _local_execute(self, local, sqlscript, insert):
        if not insert:
            query = (
                "delete from "
                + self.localtable
                + "_"
                + str(self.node_id)
                + "; insert into "
                + self.localtable
                + "_"
                + str(self.node_id)
                + " select "+ str(self.node_id) +" as node_id, * from ("
                + sqlscript
                + ") inputquery;"
            )
        else:
            query = (
                    "insert into "
                    + self.localtable
                    + "_"
                    + str(self.node_id)
                    + " select "+str(self.node_id)+" as node_id, * from ("
                    + sqlscript
                    + ") inputquery;"
            )
        await local.cursor().execute(query)

    async def _create_view(self, local, attributes, table, filterpart, vals):
        if filterpart == " ":
            query = (
                "CREATE VIEW "
                + self.viewlocaltable
                + " AS select "
                + ",".join(attributes)
                + " from "
                + table
                + ";"
            )
            await local.cursor().execute(query)
        else:
            query = (
                "CREATE VIEW "
                + self.viewlocaltable
                + " AS select "
                + ",".join(attributes)
                + " from "
                + table
                + " where "
                + filterpart
                + ";"
            )
            await local.cursor().execute(query, vals)


    async def _initialize_local_schema(self):

        query = "drop table if exists %s; create table %s (node_id INT, %s);" % (

            self.localtable + "_" + str(self.node_id),
            self.localtable + "_" + str(self.node_id),
            self.local_schema,
        )
        await self.db_objects["local"]["async_con"].cursor().execute(query)


    def create_task_generator(self, algorithm):
        return algorithm(
            self.viewlocaltable,
            self.globaltable,
            self.parameters,
            self.attributes,
            self.globalresulttable
        )

    async def init_global_remote_table(self, global_schema):
        await self.transfer_runner.initialize_local(global_schema)


    async def init_tables(self, local_schema, global_schema):
        self.local_schema = local_schema
        await self._initialize_local_schema()
        await self.transfer_runner.initialize_local(global_schema)

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

    async def createlocalviews(self):
        t1 = current_time()
        table = self.params["table"]
        attributes = []
        for attribute in self.params["attributes"]:
            attributes.append(attribute)
        for formula in self.params["filters"]:
            for attribute in formula:
                if attribute[0] not in attributes:
                    attributes.append(attribute[0])

        await self.db_objects["local"]["async_con"].check_for_params(table, attributes)


        filterpart = " "
        vals = []
        for j, formula in enumerate(self.params["filters"]):
            andpart = " "
            for i, filt in enumerate(formula):
                if filt[1] not in [">", "<", "<>", ">=", "<=", "="]:
                    raise Exception("Operator " + filt[1] + " not valid")
                andpart += filt[0] + filt[1] + "%s"
                vals.append(filt[2])
                if i < len(formula) - 1:
                    andpart += " and "
            if andpart != " ":
                filterpart += "(" + andpart + ")"
            if j < len(self.params["filters"]) - 1:
                filterpart += " or "


        await self._create_view(
                self.db_objects["local"]["async_con"],
                self.params["attributes"],
                self.params["table"],
                filterpart,
                vals,
            )

        print("time " + str(current_time() - t1))

    #### run a task on all local nodes and sets up the transfer of the results to global node

    async def task_local(self, schema, static_schema, sqlscript):
        t1 = current_time()
       # insert = False
       # if 'iternum'  in  schema:
       #     insert = True
       # if self.local_schema == None or self.local_schema != schema:
       #     self.local_schema = schema
       #     await self._initialize_local_schema()
       #     await self.transfer_runner.initialize_local(self.local_schema)
        if int(static_schema):
            await self._local_execute(self.db_objects["local"]["async_con"], sqlscript, 0)
        else:
            self.local_schema = schema
            await self._initialize_local_schema()
            await self._local_execute(self.db_objects["local"]["async_con"], sqlscript, 0)



        ## for debug, print local contents
        #for id, local in enumerate(self.db_objects["local"]):
        #    result = await local["async_con"].cursor().execute("select * from %s_%s;" % (self.localtable, str(id)))
        print("time " + str(current_time() - t1))


    async def get_global_result(self):
        cur = self.db_objects["local"]["async_con"].cursor()
        query = f'select * from {self.globalresulttable};'
        await cur.execute(query)
        return await cur.fetchall()


    async def register_udf(self, udf):
        # this is not implemented
        await self.db_objects["local"]["async_con"].create_function(udf)



    async def clean_up(self):
        await self.db_objects["local"]["async_con"].clean_tables(self.db_objects, self.globaltable, self.localtable+"_"+str(self.node_id), self.viewlocaltable+"_"+str(self.node_id), self.globalresulttable)