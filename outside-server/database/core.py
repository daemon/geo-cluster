from cassandra.cluster import Cluster
from rediscluster import StrictRedisCluster

class Table:
  def __init__(self, name):
    self.name = name

  def insert(self, column_names, ops={}):
    columns = ",".join(column_names)
    value_holders = []
    for name in column_names:
      placeholder = "%({})s".format(name)
      if name in ops:
        value_holders.append(ops[name].format(placeholder))
      else:
        value_holders.append(placeholder)
    value_holders = ",".join(value_holders)
    stmt = "INSERT INTO {} ({}) VALUES ({})".format(self.name, columns, value_holders)

class AlreadyConnectedError(Exception):
  pass

class Config:
  def __init__(self, **options_kwargs):
    self.options_kwargs = options_kwargs
    if "redis_hosts" not in options_kwargs:
      options_kwargs["redis_hosts"] = ["127.0.0.1"]
    if "redis_ports" not in options_kwargs:
      options_kwargs["redis_ports"] = [6379]
    if "default_keyspace" not in options_kwargs:
      options_kwargs["default_keyspace"] = "fatauth"
    if "lb_class" not in options_kwargs:
      options_kwargs["lb_class"] = "SimpleStrategy"
    if "replication_factor" not in options_kwargs:
      options_kwargs["replication_factor"] = 1

class Connector:
  def __init__(self, cfg=Config()):
    self.cfg = cfg
    self.cluster = None
    self.session = None

  def connect(self):
    if self.cluster:
      raise AlreadyConnectedError
    self.cluster = Cluster(**self.cfg.options_kwargs)
    if "keyspace" in self.cfg.options_kwargs:
      self.session = self.cluster.connect(self.cfg.options_kwargs["keyspace"])
    else:
      self.session = self.cluster.connect()
    hosts = self.cfg.options_kwargs["redis_hosts"]
    ports = self.cfg.options_kwargs["redis_ports"]
    self.redis = StrictRedisCluster(startup_nodes=[dict(host=host, port=port) for host, port in zip(hosts, ports)],
      decode_responses=True)
    self.initialize_database()

  def initialize_database(self):
    self.get_session().execute('''
      CREATE KEYSPACE %s WITH replication={'class': %s, 'replication_factor': %s}
      ''', (self.cfg.options_kwargs["default_keyspace"], self.cfg.options_kwargs["lb_class"], 
        self.cfg.options_kwargs["replication_factor"]))

  def get_redis(self):
    return self.redis

  def get_cluster(self):
    return self.cluster

  def get_session(self):
    return self.session
