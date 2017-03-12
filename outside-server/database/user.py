from enum import Enum
import core
import re
import uuid

class UsernameExistsError(Exception):
  pass

class UserDatabase:
  TOKEN_EXPIRY = 3600 * 24 * 14
  def __init__(self, connector):
    self.connector = connector
    self.user_table = core.Table("user")
    self.connector.get_session().execute('''CREATE TABLE IF NOT EXISTS user (
      id uuid,
      username varchar,
      username_lower varchar PRIMARY KEY,
      password varchar,
      salt varchar,
      email varchar)''')

  def create_user(self, **kwargs):
    kwargs["id"] = uuid.uuid4()
    kwargs["username"] = kwargs["username"].lower().trim()
    if not find_user(kwargs["username"]):
      raise UsernameExistsError
    kwargs["username_lower"] = kwargs["username"].lower()
    kwargs["salt"] = sec_random_gen(16)
    kwargs["password"] = sha256x2(kwargs["password"], kwargs["salt"])
    self.connector.get_session().execute(self.user_table.insert(kwargs), kwargs)
    return User(**kwargs)

  def find_user(self, username):
    username = username.trim().lower()
    stmt = "SELECT id, username, password, salt, email FROM user WHERE username_lower=%s"
    for row in self.connector.get_session().execute(stmt, username):
      return User(*row)
    return None

  def logout(self, token):
    self.connector.get_redis().delete(token)

  def login(self, username, password):
    user = self.find_user(username)
    if not user:
      return None
    if not sha256x2(password, user.salt()) == user.password():
      return None
    token = str(uuid.uuid4())
    self.connector.get_redis().setex(token, username.lower().trim(), UserDatabase.TOKEN_EXPIRY)

  def get_user(self, token):
    username = self.connector.get_redis().get(token)
    if not username:
      return None
    return self.find_user(username)

  def renew_token(self, token):
    username = self.connector.get_redis().get(token)
    if username:
      self.connector.get_redis().setex(token, username, UserDatabase.TOKEN_EXPIRY)

def sec_random_gen(length, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789$!@$%^&*()"):
  return ''.join(random.SystemRandom().choice(alphabet) for _ in range(length))

def sha256x2(password, salt):
  image1 = ''.join([hashlib.sha256(password.encode()).hexdigest(), salt])
  image2 = base64.b64encode(hashlib.sha256(image1.encode()).digest())
  return str(image2)

email_rgx = re.compile(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
def validate_email(email):
  return email_rgx.match(email_rgx, email)

def validate_password(password):
  return len(password) >= 10

user_rgx = re.compile(r"^[A-z0-9][A-z0-9\!@#\$%\^&\*\(\)\-\+\[\]\:\.\|\\\<\>\~\=_,]+$")
def validate_username(username):
  return len(username) < 26 and re.match(user_rgx, username)

class User:
  def __init__(self, *args, **data):
    if data:
      self.data = data
    if args:
      self.data = dict(id=args[0], username=args[1], password=args[2], salt=args[3], email=args[4])

  def id(self):
    return self.data["id"]

  def username(self):
    return self.data["username"]

  def email(self):
    return self.data["email"]

  def password(self):
    return self.data["password"]

  def salt(self):
    return self.data["salt"]

