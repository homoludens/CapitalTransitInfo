from flaskext.script import Manager
from pymongo import Connection

from app import app
from wmata import WMATA

manager = Manager(app)

@manager.command
def load_wmata():
    app.config.from_envvar('CTI_SETTINGS')
    connection = Connection()
    db = connection[app.config['DB_NAME']]
    wmata = WMATA(app.config['WMATA_KEY'], db)
    wmata.fetchStops()
    print "Loaded stops"
    wmata.fetchStations()
    print "Loaded stations"

if __name__ == "__main__":
    manager.run()
