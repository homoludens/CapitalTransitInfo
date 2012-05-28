from flask import Flask, render_template, request, redirect, url_for, jsonify
from pymongo import Connection

from wmata import WMATA
from nextbus import NextBus
from connexionz import Connexionz

app = Flask(__name__)
app.config.from_envvar('CTI_SETTINGS')
connection = Connection()
db = connection[app.config['DB_NAME']]

wmata = WMATA(app.config['WMATA_KEY'], db)
circulator = NextBus("dc-circulator", db, 'nextbus')
art = Connexionz("http://realtime.commuterpage.com", "ART", db, 'art')

@app.route("/")
def index():
    wmataRailIncidentCount = wmata.getRailIncidentCount()
    wmataELESIncidentCount = wmata.getELESIncidentCount()
    return render_template("index.html", wmataRailIncidentCount=wmataRailIncidentCount,
                           wmataELESIncidentCount=wmataELESIncidentCount)

@app.route("/about")
def about():
    return render_template("about.html")

@app.route("/wmata/bus")
def wmata_bus():
    return render_template("wmata/bus.html")

@app.route("/wmata/bus/routes")
def wmata_bus_routes():
    pass

@app.route("/wmata/bus/stops")
def wmata_stop_lookup():
    stopid = request.args['stopid']
    return redirect(url_for('wmata_stop', stopID=stopid), code=303)

@app.route("/wmata/bus/stop/<stopID>")
def wmata_stop(stopID):
    stop = wmata.getStop(stopID)
    predictions = wmata.getBusPredictions(stopID)
    return render_template("wmata/stop.html", stop=stop, predictions=predictions)

@app.route("/wmata/bus/stops/geo")
def wmata_stops_geo():
    if 'latitude' in request.args and 'longitude' in request.args:
        latitude = float(request.args['latitude'])
        longitude = float(request.args['longitude'])

        stops = wmata.getStopsNear(longitude, latitude)
        return render_template("wmata/stops.html", stops=stops)    
    else:
        return render_template("geo.html", destination=url_for('wmata_stops_geo'))

@app.route("/wmata/stations")
def wmata_stations():
    stations = wmata.getStations()
    return render_template("wmata/stations.html", stations=stations)

@app.route("/wmata/stations/geo")
def wmata_stations_geo():
    if 'latitude' in request.args and 'longitude' in request.args:
        latitude = float(request.args['latitude'])
        longitude = float(request.args['longitude'])

        stations = wmata.getStationsNear(longitude, latitude)
        return render_template("wmata/stations.html", stations=stations)    
    else:
        return render_template("geo.html", destination=url_for('wmata_stations_geo'))

@app.route("/wmata/station/<path:rtuCodes>")
def wmata_station(rtuCodes):
    rtuCodes = rtuCodes.split('/')
    predictions = wmata.getRailPredictions(rtuCodes)
    predictionGroups = sorted(predictions.keys(), key=lambda x: getTrack(x[0], x[1]))
    return render_template("wmata/station.html", predictions=predictions, predictionGroups=predictionGroups,
                           getTrack=getTrack, getTitle=getTitle)

def getTrack(rtu, track):
    return rtu[0] + str(track)

def getTitle(track):
    return {'A1': 'To Glenmont via Downtown',
            'A2': 'To Shady Grove',
            'B1': 'To Glenmont',
            'B2': 'To Shady Grove via Downtown',
            'C1': 'To Maryland via Downtown',
            'C2': 'To Virginia',
            'D1': 'To Maryland',
            'D2': 'To Virginia via Downtown',
            'E1': 'To Greenbelt',
            'E2': 'To Maryland and Virginia via Downtown',
            'F1': 'To Greenbelt via Downtown',
            'F2': 'To Maryland and Virginia',
            'G1': 'To Largo',
            'G2': 'To Virginia via Downtown',
            'J1': 'To Maryland via Downtown',
            'J2': 'To Franconia-Springfield',
            'K1': 'To Maryland via Downtown',
            'K2': 'To Vienna',
            }[track]

@app.route("/wmata/incidents/rail")
def wmata_rail_incidents():
    incidents = wmata.getRailIncidents()
    return render_template("wmata/incidents.html", incidents=incidents)

@app.route("/wmata/incidents/eles")
def wmata_eles_incidents():
    incidentStations = wmata.getELESIncidentStations()
    return render_template("wmata/eles/stations.html", incidentStations=incidentStations,
                           incidentCountByStation=wmata.getELESIncidentCountByStation)

@app.route("/wmata/incidents/eles/<path:rtuCodes>")
def wmata_eles_incidents_station(rtuCodes):
    rtuCodes = rtuCodes.split('/')
    incidentsAtStation = wmata.getELESIncidentsByStation(rtuCodes)
    return render_template("wmata/eles/incidents.html", incidents=incidentsAtStation)

@app.route("/wmata/incidents/eles/json")
def wmata_eles_incidents_json():
    stationList = []
    
    incidentStations = wmata.getELESIncidentStations()

    for station in incidentStations:
        stationList.append((station['name'], wmata.getELESIncidentCountByStation(station['rtus'])))

    stationList.sort(key=lambda x: x[1], reverse=True)

    failCount = len(stationList)
    failRate = "%.2f%%" % (100 * (failCount / 86.0))

    return jsonify(stationList=stationList, failRate=failRate, failCount=failCount)

@app.route("/circulator/routes")
def circulator_routes():
    routes = circulator.getRoutes()
    return render_template("circulator/routes.html", routes=routes)

@app.route("/circulator/route/<routeTag>")
def circulator_route(routeTag):
    route = circulator.getRoute(routeTag)
    return render_template("circulator/route.html", route=route)

@app.route("/circulator/route/<routeTag>/<directionTag>/stops")
def circulator_stops(routeTag, directionTag):
    route = circulator.getRoute(routeTag)
    direction = route['directions'][directionTag]
    stopTags = direction['stops']
    stops = [circulator.getStopByTag(stopTag) for stopTag in stopTags]
    return render_template("circulator/stops.html", route=route, stops=stops)

@app.route("/circulator/stops/geo")
def circulator_stops_geo():
    if 'latitude' in request.args and 'longitude' in request.args:
        latitude = float(request.args['latitude'])
        longitude = float(request.args['longitude'])

        stops = circulator.getStopsNear(longitude, latitude)
        return render_template("circulator/stops.html", stops=stops)    
    else:
        return render_template("geo.html", destination=url_for('circulator_stops_geo'))
    
@app.route("/circulator/stop/<int:stopID>")
def circulator_stop_id(stopID):
    stop = circulator.getStopByID(stopID)
    predictions = circulator.getPredictionsByID(stopID)
    return render_template("circulator/stop.html", stop=stop, predictions=predictions)

@app.route("/circulator/stop/<stopTag>/<routeTag>")
def circulator_stop_tag(stopTag, routeTag):
    stop = circulator.getStopByTag(stopTag)
    predictions = circulator.getPredictionsByTag(stopTag, routeTag)
    return render_template("circulator/stop.html", stop=stop, predictions=predictions)

@app.route("/art/")
def art_index():
    return render_template("art/index.html")

@app.route("/art/routes/")
def art_routes():
    routes = art.getRoutes()
    return render_template("art/routes.html", routes=routes)

@app.route("/art/route/<routeNumber>")
def art_route(routeNumber):
    route = art.getRoute(routeNumber)
    patterns = route['patterns'].values()
    return render_template("art/route.html", routeNumber=routeNumber, patterns=patterns)

@app.route("/art/route/<routeNumber>/<routeTag>")
def art_stops(routeNumber, routeTag):
    route = art.getRoute(routeNumber)
    pattern = route['patterns'][routeTag]
    stops = [art.getStop(stopTag) for stopTag in pattern['platforms']]
    return render_template("art/stops.html", stops=stops)

@app.route("/art/stops/geo")
def art_stops_geo():
    if 'latitude' in request.args and 'longitude' in request.args:
        latitude = float(request.args['latitude'])
        longitude = float(request.args['longitude'])

        stops = art.getStopsNear(longitude, latitude)
        return render_template("art/stops.html", stops=stops)    
    else:
        return render_template("geo.html", destination=url_for('art_stops_geo'))

@app.route("/art/stop")
def art_stop_lookup():
    stopid = request.args['stopid']
    return redirect(url_for('art_stop', stopTag=art.getStopTag(stopid)), code=303)

@app.route("/art/stop/<stopTag>")
def art_stop(stopTag):
    stop = art.getStop(stopTag)
    predictions = art.getPredictions(stopTag)
    return render_template("art/stop.html", stop=stop,
                           predictions=predictions['predictions'])
    

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
