import cherrypy
import config
import db.cluster as cluster
import db.base as base
import os
import route

if __name__ == "__main__":
  #base.init_databases(config.db_config)
  cherrypy.config.update({
    'environment': 'production',
    'log.error_file': 'site.log',
    'log.screen': True
  })
  cherrypy.server.socket_port = config.server_config["server_port"]
  cherrypy.server.socket_host = "0.0.0.0"
  base.initialize(config.db_config["postgresql"])
  cluster.cluster_engine.run()
  route.mount("/")
