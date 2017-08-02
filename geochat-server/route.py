from mako.template import Template
from mako.lookup import TemplateLookup
import cherrypy
import db.user
import datetime
import json
import mako
import os
import requests

class config:
  root_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "."))

def json_in(f):
  def merge_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z
  def wrapper(*args, **kwargs):
    cl = cherrypy.request.headers["Content-Length"]
    data = json.loads(cherrypy.request.body.read(int(cl)).decode("utf-8"))
    kwargs = merge_dicts(kwargs, data)
    return f(*args, **kwargs)
  return wrapper

class UserEndpoint:
  exposed = True
  @cherrypy.tools.json_out()
  def GET(self, **kwargs):
    try:
      email = kwargs["email"]
      password = kwargs["password"]
    except:
      cherrypy.response.status = 400
      return
    user = db.user.User.find(email=email)
    if not user:
      cherrypy.response.status = 404
      return
    token = user.login(password)
    if not token:
      cherrypy.response.status = 404
      return
    cherrypy.response.cookie["auth_token"] = token
    cherrypy.response.cookie["auth_token"]["max-age"] = 3600 * 24 * 16
    return dict(success=True)

  @cherrypy.tools.json_out()
  @json_in
  def POST(self, **kwargs):
    CREATE_SUCCESS = 0
    USERNAME_TAKEN = 1
    PASSWORD_INSECURE = 2
    EMAIL_TAKEN = 4
    try:
      email = kwargs["email"]
      password = kwargs["password"]
      username = kwargs["username"]
    except:
      cherrypy.response.status = 400
      return
    code = CREATE_SUCCESS
    if len(password) < 8:
      code = PASSWORD_INSECURE
    if db.user.User.find(email=email):
      code = code | EMAIL_TAKEN
    if db.user.User.find(username=username):
      code = code | USERNAME_TAKEN
    if code != CREATE_SUCCESS:
      return dict(field=code)
    try:
      db.user.User.create(email, username, password)
      cherrypy.response.status = 200
      return dict(field=code)
    except:
      cherrypy.response.status = 403

def mount(path):
  template_lookup = TemplateLookup(directories=[os.path.join(config.root_dir, "templates")], input_encoding="utf-8",
    output_encoding="utf-8", encoding_errors="replace")
  cherrypy_conf = {
    "/assets": {
      "tools.staticdir.on": True,
      "tools.staticdir.dir": os.path.join(config.root_dir, "assets")
    }
  }
  rest_conf = {"/": {"request.dispatch": cherrypy.dispatch.MethodDispatcher()}}
  cherrypy.tree.mount(UserEndpoint(), "/user/", rest_conf)
  cherrypy.engine.start()
  cherrypy.engine.block()
