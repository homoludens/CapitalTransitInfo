import requests

from pprint import pprint
import json

r = requests.get('http://api.wmata.com/Bus.svc/json/JRoutes?api_key=skg97gkn9ght66s3aq87absf')
r.raise_for_status()

routes = json.loads(r.content)

print json.dumps(routes, sort_keys=True, indent=2)
