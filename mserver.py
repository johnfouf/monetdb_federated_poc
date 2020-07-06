import tornado.web
from threading import Thread
from tornado import gen 
from tornado.log import enable_pretty_logging
from tornado.options import define, options
import logging
import pymonetdb
import sys
import settings
import json
import asyncio
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
import run_algorithm
MAX_WORKERS = 4
    
PROCESSES_PER_CPU = 2
WEB_SERVER_PORT=9999

class AlgorithmException(Exception):
    def __init__(self,message):
      super(AlgorithmException,self).__init__(message)

define("port", default=WEB_SERVER_PORT, help="run on the given port", type=int)


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", MainHandler)
        ]
        tornado.web.Application.__init__(self, handlers)

class BaseHandler(tornado.web.RequestHandler):
    def __init__(self, *args):
        tornado.web.RequestHandler.__init__(self, *args)
        
import time  
async def mysleep():
      return await asyncio.sleep(5)

class MainHandler(BaseHandler):
  #logging stuff..
  enable_pretty_logging()
  logger = logging.getLogger('MainHandler')
  hdlr = logging.FileHandler('/var/log/MadisServer.log','w+')
  formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
  hdlr.setFormatter(formatter)
  logger.addHandler(hdlr)
  logger.setLevel(logging.DEBUG)

  access_log = logging.getLogger("tornado.access")
  app_log = logging.getLogger("tornado.application")
  gen_log = logging.getLogger("tornado.general")
  access_log.addHandler(hdlr)
  app_log.addHandler(hdlr)
  gen_log.addHandler(hdlr)
  
  executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
  

      
  
  async def post(self):
    algorithm = self.get_argument("algorithm")
    params = self.get_argument("params")

    try:
      #result = await mysleep()
      #result = "lala"
      result = await run_algorithm.run(algorithm,params, settings.global_node, settings.local_nodes)
    except AlgorithmException as e:
      #raise tornado.web.HTTPError(status_code=500,log_message="...the log message??")
      self.logger.debug("(MadisServer::post) QueryExecutionException: {}".format(str(e)))
      #print "QueryExecutionException ->{}".format(str(e))
      self.set_status(500)
      self.write(str(e))
      self.finish()
      return
    
    self.logger.debug("(MadisServer::post) str_result-> {}".format(result))
    self.write("{}".format(result))
    
    self.finish()

def main(args):
    settings.initialize(args)
    sockets = tornado.netutil.bind_sockets(options.port)
    #tornado.process.fork_processes(tornado.process.cpu_count() * PROCESSES_PER_CPU)
    server = tornado.httpserver.HTTPServer(Application())
    server.add_sockets(sockets)
    tornado.ioloop.IOLoop.instance().start()
    

if __name__ == "__main__":
    main(sys.argv)

