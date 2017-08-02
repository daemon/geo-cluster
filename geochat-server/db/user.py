import base64
import datetime
import hashlib
import json
import random
import redis
import db.base as base
from sqlalchemy import *
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION

user = Table("users", base.Base.metadata,
  Column("id", Integer, primary_key=True),
  Column("email", String(32), nullable=False, unique=True, index=True),
  Column("username", String(32), nullable=False, unique=True, index=True),
  Column("password", String(44), nullable=False),
  Column("salt", String(16), nullable=False))

user_location = Table("user_locations", base.Base.metadata,
  Column("user_id", Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False, unique=True),
  Column("latitude", DOUBLE_PRECISION, index=True, nullable=False),
  Column("longitude", DOUBLE_PRECISION, index=True, nullable=False),
  Column("ts", TIMESTAMP, index=True, server_default=text("now()")),
  Column("cluster_id", Integer, ForeignKey("clusters.id", ondelete="cascade"), nullable=False, server_default=text("0")))

class session_store:
  store = redis.StrictRedis(host="127.0.0.1", port=6379, db=0)
  @staticmethod
  def create_token(user):
    token = sec_random_gen(24)
    session_store.store.set(token, user.id, ex=3600 * 24 * 30)
    return token

  @staticmethod
  def get_user(token):
    user_id = session_store.store.get(token)
    if not user_id:
      return None
    return User.find(id=int(user_id))

  @staticmethod
  def delete_token(token):
    session_store.store.delete(token)

class User:
  def __init__(self, *args):
    base.init_from_row(self, base.column_names(user), args)

  def login(self, password):
    if self.password.encode() == sha256x2(password, self.salt):
      return session_store.create_token(self)
    else:
      return None

  def logout(self, token):
    session_store.delete_token(token)

  @staticmethod
  @base.access_point()
  def find(**kwargs):
    connection = kwargs["connection"]
    c = connection.cursor()
    (conditions, params) = base.join_conditions(kwargs, "AND", ["id", "email", "username"])
    stmt = "SELECT * FROM users WHERE " + conditions
    c.execute(stmt, params)
    for row in c.fetchall():
      return User(*row)
    return None

  @staticmethod
  @base.access_point()
  def create(email, username, password, connection=base.INIT_CONNECTION):
    salt = sec_random_gen(16)
    stmt = "INSERT INTO users (email, username, password, salt) VALUES (%s, %s, %s, %s)"
    c = connection.cursor()
    c.execute(stmt, (email, username, sha256x2(password, salt).decode("utf-8"), salt))

def sec_random_gen(length, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789$!@#$%^&*()"):
  return ''.join(random.SystemRandom().choice(alphabet) for _ in range(length))

def sha256x2(password, salt):
  image1 = ''.join([hashlib.sha256(password.encode()).hexdigest(), salt])
  image2 = base64.b64encode(hashlib.sha256(image1.encode()).digest())
  return image2
