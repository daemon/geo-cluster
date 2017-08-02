from collections import deque
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import *
import db.base as base
import math
import operator
import os
import threading
import time

cluster = Table("clusters", base.Base.metadata,
  Column("id", Integer, primary_key=True),
  Column("title", String(64), nullable=False, server_default=text("''")))

class TabularizedNormal:
  def __init__(self, mean=0, sd=1, granularity=0.1, limit=4):
    self.mean = mean
    self.sd = sd
    self.granularity = granularity
    self.limit = limit
    self.table = {}
    pi = math.pi
    y = self.mean
    for x in range(int(limit / granularity)):
      self.table[-int(y / granularity)] = self.table[int(y / granularity)] = (1 / (math.sqrt(2 * pi) * sd)) * math.exp(-(y - mean) * (y - mean) / (2 * sd * sd))
      y = mean + x * granularity

  def at(self, value):
    try:
      return self.table[int(value / self.granularity)]
    except:
      return 0

class Cluster:
  @base.access_point()
  def create(id, title="", connection=base.INIT_CONNECTION):
    c = connection.cursor()
    c.execute("INSERT INTO clusters (id, title) VALUES (%s, %s)", (id, title))

class config:
  planet_radius = 6371000
  cluster_radius = 15
  cluster_deg = (cluster_radius / planet_radius) * 180 / math.pi
  no_cluster_id = 0

  def m_to_deg(m):
    return (m / config.planet_radius) * 180 / math.pi

class cluster_engine:
  lock = threading.Lock()

  @base.access_point()
  def timeout_old_locations(connection=base.INIT_CONNECTION):
    with cluster_engine.lock:
      c = connection.cursor()
      c.execute("DELETE FROM user_locations WHERE ts < now() - interval '30 minutes'")
      c.execute("DELETE clusters FROM clusters LEFT JOIN user_locations ON cluster_id=clusters.id WHERE user_locations.id IS NULL")

  def start_engine():
    counter = 0
    while True:
      time.sleep(60)
      counter += 1
      self.run_cluster()
      if counter == 30:
        counter = 0
        self.timeout_old_locations()

  def run():
    threading.Thread(target=cluster_engine.start_engine).start()

  @base.access_point()
  def run_cluster(connection=base.INIT_CONNECTION):
    sd = config.m_to_deg(config.cluster_radius)
    tn = TabularizedNormal(sd=sd, granularity=sd / 100, limit=sd * 4)
    with cluster_engine.lock:
      c = connection.cursor()
      stmt = "SELECT user_id, cluster_id, longitude, latitude FROM user_locations".format(config.cluster_deg)
      c.execute(stmt)
      total_set = {int(row[0]): row[1:] for row in c.fetchall()}
      reset_users = []
      cluster_assignments = {}
      for user_id in total_set:
        open_set = deque()
        closed_set = set()
        cluster_nodes = []
        open_set.append((user_id,) + total_set[user_id])
        tree_root = user_id
        is_homogeneous = True
        target_cluster_id = total_set[user_id][0]
        while open_set:
          current_row = open_set.pop()
          (user_id, cluster_id, longitude, latitude) = current_row
          cluster_nodes.append(current_row)
          closed_set.add(user_id)
          if target_cluster_id != cluster_id:
            is_homogeneous = False
          stmt = '''SELECT user_id, cluster_id, longitude, latitude FROM user_locations WHERE longitude > {longitude} - {radius} AND
            latitude > {latitude} - {radius} AND longitude < {longitude} + {radius} AND latitude < {latitude} + {radius} AND user_id != {user_id}'''.format(
              longitude=longitude, latitude=latitude, user_id=user_id)
          c.execute(stmt)
          for row in c:
            row = (int(row[0]),) + row[1:]
            if row[0] in closed_set:
              continue
            open_set.appendleft(row)
        if len(cluster_nodes) == 1 and target_cluster_id != config.no_cluster_id:
          reset_users.append(cluster_nodes[0][0])
        elif len(cluster_nodes) > 1 and is_homogeneous and target_cluster_id == config.no_cluster_id:
          cluster_assignments[os.urandom()] = [c[0] for c in cluster_nodes]
        elif not is_homogeneous:
          cluster_tmp = {}
          for node1 in cluster_nodes:
            cluster_weights = {}
            (uid, cid1, long1, lat1) = node1
            for node2 in cluster_nodes:
              if node1 is node2:
                continue
              (_, cid2, long2, lat2) = node2
              if cid2 == config.no_cluster_id:
                continue
              x = lat2 - lat1
              y = long2 - long1
              val = tn.at(math.sqrt(x * x + y * y))
              try:
                cluster_weights[cid2] += val
              except:
                cluster_weights[cid2] = val
            cid = max(cluster_weights.items(), key=operator.itemgetter(1))[0]
            try:
              cluster_tmp[cid].append(uid)
            except:
              cluster_tmp[cid] = [uid]
          for (cid, users) in cluster_tmp.items():
            if cid in cluster_assignments:
              if len(users) > len(cluster_assignments[cid]):
                cluster_assignments[os.urandom()] = cluster_assignments[cid]
                cluster_assignments[cid] = users
              else:
                cluster_assignments[os.urandom()] = users
            else:
              cluster_assignments[cid] = users
        for uid in closed_set:
          del total_set[uid]
      for assignment in cluster_assignments:
        stmt = "UPDATE user_locations SET cluster_id={} WHERE user_id IN ({})".format(assignment, ",".join(cluster_assignments[assignment]))
        c.execute(stmt)      
      stmt = "UPDATE user_locations SET cluster_id={} WHERE user_id IN ({})".format(config.no_cluster_id, ",".join(reset_users))
      c.execute(stmt)
