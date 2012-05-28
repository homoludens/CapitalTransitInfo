from flaskext.script import Manager
from pymongo import Connection

from app import app
from wmata import WMATA
from nextbus import NextBus
from connexionz import Connexionz

manager = Manager(app)

@manager.command
def load_wmata():
    app.config.from_envvar('CTI_SETTINGS')
    connection = Connection()
    db = connection[app.config['DB_NAME']]
    wmata = WMATA(app.config['WMATA_KEY'], db)
    wmata.fetchStops()
    print "Loaded WMATA stops"
    wmata.fetchStations()
    print "Loaded WMATA stations"

@manager.command
def load_circulator():
    app.config.from_envvar('CTI_SETTINGS')
    connection = Connection()
    db = connection[app.config['DB_NAME']]
    circulator = NextBus("dc-circulator", db, 'nextbus')
    circulator.loadAgencyData()
    print "Loaded Circulator stops and routes"
    circulator.fixLongStopNames()
    print "Fixed long stop names"

@manager.command
def load_art():
    app.config.from_envvar('CTI_SETTINGS')
    connection = Connection()
    db = connection[app.config['DB_NAME']]
    art = Connexionz("http://realtime.commuterpage.com", "ART", db, 'art')
    art.fetchStops()
    print "Loaded ART stops"
    art.fetchRoutes()
    print "Loaded ART routes"

if __name__ == "__main__":
    manager.run()
