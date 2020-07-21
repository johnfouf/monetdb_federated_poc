
import logging
import pymonetdb
import sys
import settings
import json
import asyncio
import run_algorithm
import tornado.web
from threading import Thread
from tornado import gen 
from tornado.log import enable_pretty_logging
from tornado.options import define, options


MAX_WORKERS = 4
    
PROCESSES_PER_CPU = 2
WEB_SERVER_PORT=7779
define("port", default=WEB_SERVER_PORT, help="run on the given port", type=int)


class AlgorithmException(Exception):
    def __init__(self,message):
      super(AlgorithmException,self).__init__(message)




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
      await asyncio.sleep(5)
      await asyncio.sleep(5)


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
  

  async def post(self):
    algorithm = self.get_argument("algorithm")
    params = self.get_argument("params")
    global_node, local_nodes = await settings.initialize(sys.argv)
    try:
      result = await run_algorithm.run(algorithm,params, global_node, local_nodes)
      self.write("{}".format(result))
    except AlgorithmException as e:
      #raise tornado.web.HTTPError(status_code=500,log_message="...the log message??")
      self.logger.debug("(MadisServer::post) QueryExecutionException: {}".format(str(e)))
      #print "QueryExecutionException ->{}".format(str(e))
      await settings.disconnect(global_node, local_nodes)
      
      self.set_status(500)
      self.write(str(e))
      self.finish()
      return
    
    await settings.disconnect(global_node, local_nodes)
    self.logger.debug("(MadisServer::post) str_result-> {}".format(result))
    #self.write("{}".format(result))
    
    self.finish()




def main(args):   
    sockets = tornado.netutil.bind_sockets(options.port)
    #tornado.process.fork_processes(tornado.process.cpu_count() * PROCESSES_PER_CPU)
    server = tornado.httpserver.HTTPServer(Application())
    server.add_sockets(sockets)
    tornado.ioloop.IOLoop.instance().start()
    

if __name__ == "__main__":
    main(sys.argv)

