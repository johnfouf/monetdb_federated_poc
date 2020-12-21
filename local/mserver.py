import logging
import sys
import connections
import run_algorithm
import tornado.web
from tornado.log import enable_pretty_logging
from tornado.options import define, options
import json
import importlib
import settings

DEBUG = settings.DEBUG

WEB_SERVER_PORT = 7676
define("port", default=WEB_SERVER_PORT, help="run on the given port", type=int)

class AlgorithmException(Exception):
    def __init__(self, message):
        super(AlgorithmException, self).__init__(message)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [(r"/", MainHandler)]
        tornado.web.Application.__init__(self, handlers)

class BaseHandler(tornado.web.RequestHandler):
    def __init__(self, *args):
        tornado.web.RequestHandler.__init__(self, *args)

class MainHandler(BaseHandler):
    # logging stuff..
    enable_pretty_logging()
    logger = logging.getLogger("MainHandler")
    hdlr = logging.FileHandler("./mserver.log", "w+")
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)
    access_log = logging.getLogger("tornado.access")
    app_log = logging.getLogger("tornado.application")
    gen_log = logging.getLogger("tornado.general")
    access_log.addHandler(hdlr)
    app_log.addHandler(hdlr)
    gen_log.addHandler(hdlr)
    dbs = connections.Connections()
    states = {}

    def get_package(self,algorithm):
        try:
            mpackage = "algorithms"
            importlib.import_module(mpackage)
            algo = importlib.import_module("." + algorithm, mpackage)
            if DEBUG:
                importlib.reload(algo)
        except ModuleNotFoundError:
            raise Exception(f"`{algorithm}` does not exist in the algorithms library")
        return algo

    async def post(self):
        ## get params, algorithm contains the name of the algorithm, params is a valid json file
        print(self.get_argument("params"))
        parameters = json.loads(self.get_argument("params"))
        algorithm = parameters["algorithm"]
        hash_value = parameters["hash"]
        step = parameters["step"]
        params = parameters["params"]
        static_schema = parameters["schema"]
        node_id = parameters["node_id"]

        if hash_value in self.states:
            db_objects = self.states[hash_value]['db_objects']
            algorithm_instance =  self.states[hash_value]['algorithm']
        #### new connection per request - required since connection objects are not thread safe at the time
        else:
            await self.dbs.initialize()
            db_objects = await self.dbs.acquire()
            algorithm_instance = self.get_package(algorithm).Algorithm()
            self.states[hash_value] = {}
            self.states[hash_value]['db_objects'] = db_objects
            self.states[hash_value]['algorithm'] = algorithm_instance


        try:
            result = await run_algorithm.run(algorithm_instance, params, hash_value, step, static_schema, node_id, db_objects, self.states[hash_value])
            self.write("{}".format(result))
        except Exception as e:
            # raise tornado.web.HTTPError(status_code=500,log_message="...the log message??")
            self.logger.debug(
                "(MadisServer::post) QueryExecutionException: {}".format(str(e))
            )
            # print "QueryExecutionException ->{}".format(str(e))
            if step == -1:
                self.states.pop(hash_value)
                await self.dbs.release(db_objects)
            self.write("Error: " + str(e))
            self.finish()
            raise

        self.logger.debug("(MadisServer::post) str_result-> {}".format(result))
        # self.write("{}".format(result))
        self.finish()
        if step == -1:
            self.states.pop(hash_value)
            await self.dbs.release(db_objects)

def main(args):
    sockets = tornado.netutil.bind_sockets(options.port)
    server = tornado.httpserver.HTTPServer(Application())
    server.add_sockets(sockets)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main(sys.argv)
