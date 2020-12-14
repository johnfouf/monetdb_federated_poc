import asyncio
from urllib.parse import urlparse
import servers
import importlib
import pkgutil
import algorithms
import settings
DEBUG = settings.DEBUG


def get_udfs(reload = False):
    modules = []
    for importer, modname, ispkg in pkgutil.iter_modules(algorithms.__path__):
        modules.append(modname)
## get all statically defined udfs from algorithms package
    mpackage = "algorithms"


    importlib.import_module(mpackage)
    all_udfs = []

    for algorithm in modules:
        try:
            algo = importlib.import_module("." + algorithm, mpackage)
            if reload:
                importlib.reload(algo)
            for udf in algo.udf_list:
                all_udfs.append(udf)
        except:
            pass
    return all_udfs


class Connections:
    def __init__(self):
        self.db_objects = {}
        self.db_objects["local"] = {}
        self.db_objects["global"] = []
        self.mservers = []
        self.udfs_list = []
        self.glob = urlparse(servers.servers[0])
        if self.glob.scheme == "monetdb":
            from aiopymonetdb import pool
            self.user = "monetdb"
            self.password = "monetdb"
        if self.glob.scheme == "postgres":
            from aiopg import pool

            self.user = "postgres"
            self.password = "mypassword"
        self.pool = pool

    async def initialize(self):  ### create connection pools
        if self.db_objects["local"] == {}:
            self.mservers = servers.servers
            glob = self.glob
            self.db_objects["local"]["pool"] = await self.pool.create_pool(
                host=glob.hostname,
                port=glob.port,
                user=self.user,
                password=self.password,
                database=glob.path[1:],
            )
            self.db_objects["local"]["dbname"] = servers.servers[0]
            ## local database name - required by remote tables to connect to the remote database

            for i, db in enumerate(servers.servers[1:]):
                global_node = {}
                global_node["dbname"] = db
                self.db_objects["global"].append(
                    global_node
                )  # for each local node the database name for use with remote tables


            con = await self.acquire()
            await con["local"]["async_con"].init_remote_connections(con)
            self.udfs_list = get_udfs()
            for udf in self.udfs_list:
                    try:
                        await con["local"]["async_con"].cursor().execute(udf)
                    except:
                        raise
                    # at this time due to minimal error handling and due to testing there may be tables in the DB which
                    #  are not dropped and are dependent on some UDFs, so their recreation may fail
                    # (You cannot replace a UDF which is in use)

            await self.release(con)

    async def acquire(self):  #### get connection objects
        db_conn = {}
        db_conn["local"] = {}
        db_conn["global"] = []
        await self._reload()
        conn = await self.db_objects["local"]["pool"].acquire()
        db_conn["local"][
            "async_con"
        ] = conn  ## local asynchronous connection object - this is used to execute commands on the remote database
        db_conn["local"]["dbname"] = self.db_objects["local"][
            "dbname"
        ]  ## local database name - required by remote tables to connect to the remote database
        for db_object in self.db_objects["global"]:
            global_node = {}
            global_node["dbname"] = db_object["dbname"]
            db_conn["global"].append(
                global_node
            )
        if DEBUG:
            await self._reload_udfs(db_conn)
        return db_conn

    ### TODO asyncio locks are needed because there may be a reload
    async def release(self, db_conn):  ### release connection objects back to pool
        if db_conn["local"]["dbname"] == self.db_objects["local"]["dbname"]:
            await self.db_objects["local"]["pool"].release(
                db_conn["local"]["async_con"]
            )

    async def _reload_udfs(self, con):
        udfs_list = get_udfs(True)

        if udfs_list != self.udfs_list:
            for udf in list(set(udfs_list) - set(self.udfs_list)):
                await con["local"]["async_con"].cursor().execute(udf)

        self.udfs_list = udfs_list

    ###### reload federation nodes
    async def _reload(self):
        importlib.reload(servers)
        if self.mservers != servers.servers:
            await self.clearall()  #### re-init all the connections
            self.__init__()
            await self.initialize()
        return 0


    async def clearall(self):
        await self.db_objects["local"]["pool"].clear()
