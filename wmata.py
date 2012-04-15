from datetime import datetime, timedelta
import json
from collections import defaultdict

import requests
#from lxml import etree

from pprint import pprint

from pymongo import Connection, GEO2D, ASCENDING

NSMAP = {'w':"http://www.wmata.com"}

class WMATA(object):
    def __init__(self, apiKey, db):
        self.rs = requests.session()
        self.apiKey = apiKey
        self.stopsCollection = db['wmataStops']
        self.stopsCollection.create_index([("location", GEO2D)])
        self.routesCollection = db['wmataRoutes']
        self.stationsCollection = db['wmataStations']
        self.stationsCollection.create_index([("location", GEO2D)])
        self.busPredictionsCollection = db['wmataBusPredictions']
        self.railPredictionsCollection = db['wmataRailPredictions']
        self.railIncidentsCollection = db['wmataRailIncidents']
        self.elesIncidentsCollection = db['wmataELESIncidents']

    def fetchStops(self):
        r = self.rs.get("http://api.wmata.com/Bus.svc/json/JStops?api_key=" + self.apiKey)
        r.raise_for_status()

        stops = json.loads(r.text)

        for stop in stops['Stops']:
            try:
                stopData = {'name': stop['Name'] or 'Stop ' + stop['stopID'],
                            'id': stop['StopID'],
                            'location': [float(stop['Lon']),
                                         float(stop['Lat'])]}
                self.stopsCollection.update({'id': stopData['id']},
                                            stopData,
                                            upsert=True)
            except Exception:
                print stop

    def fetchStations(self):
        r = self.rs.get("http://api.wmata.com/Rail.svc/json/JStations?api_key=" + self.apiKey)
        r.raise_for_status()

        stations = json.loads(r.text)

        stationsOut = {}

        for station in stations['Stations']:
            name = station['Name']
            rtu = set([station['Code']])
            lines = set([station[key] for key in ['LineCode1', 'LineCode2',
                                                  'LineCode3', 'LineCode4'] if key in station and station[key] is not None])
            
            if name not in stationsOut:
                stationData = {'name': name,
                               'lines': lines,
                               'rtus': rtu,
                               'location': [float(station['Lon']),
                                            float(station['Lat'])]}
                stationsOut[stationData['name']] = stationData
            else:
                stationData = stationsOut[name]
                stationData['rtus'] |= rtu
                stationData['lines'] |= lines

        pprint(stationsOut)

        for station in stationsOut.values():
            station['rtus'] = list(station['rtus'])
            station['lines'] = sorted(list(station['lines']))
            self.stationsCollection.update({'name': station['name']},
                                           station,
                                           upsert=True)

    @staticmethod
    def minutesForSort(minutes):
        if minutes == 'BRD':
            return -1
        elif minutes == 'ARR':
            return 0
        else:
            return int(minutes)

    def fetchRailPredictions(self):
        r = self.rs.get("http://api.wmata.com/StationPrediction.svc/json/GetPrediction/All?api_key=" + \
                         self.apiKey)
        r.raise_for_status()

        predictions = json.loads(r.text)
        expirationTime = datetime.utcnow() + timedelta(seconds=20)
        predictionsOut = []
        
        for train in predictions['Trains']:
            if (train['Car'] is None) or (train['Min'] is None) or \
               (len(train['Car']) == 0) or (len(train['Min']) == 0):
                continue
            
            predictionData = {'line': train['Line'],
                              'car': train['Car'],
                              'dest': train['DestinationName'],
                              'min': train['Min'],
                              'minSort': self.minutesForSort(train['Min']),
                              'track': train['Group'],
                              'locationCode': train['LocationCode'],
                              'expiration': expirationTime}
            predictionsOut.append(predictionData)
            
        self.railPredictionsCollection.remove()
        if len(predictionsOut) > 0:
            self.railPredictionsCollection.insert(predictionsOut)

    def fetchRailIncidents(self):
        r = self.rs.get("http://api.wmata.com/Incidents.svc/json/Incidents?api_key=" + self.apiKey)
        r.raise_for_status()

        incidents = json.loads(r.text)
        
        expirationTime = datetime.utcnow() + timedelta(minutes=2)
        incidentsOut = []
        for incident in incidents['Incidents']:
            incidentData = {'lines': incident['LinesAffected'].rstrip(';').split(';'),
                            'description': incident['Description'],
                            'expiration': expirationTime
                            }
            incidentsOut.append(incidentData)

        self.railIncidentsCollection.remove()
        if len(incidentsOut) > 0:
            self.railIncidentsCollection.insert(incidentsOut)
        

    def fetchELESIncidents(self):
        r = self.rs.get("http://api.wmata.com/Incidents.svc/json/ElevatorIncidents?api_key=" + self.apiKey)
        r.raise_for_status()

        incidents = json.loads(r.text)
        
        expirationTime = datetime.utcnow() + timedelta(minutes=10)
        incidentsOut = []
        for incident in incidents['ElevatorIncidents']:
            incidentData = {'displayOrder': int(incident['DisplayOrder']),
                            'locationDescription': incident['LocationDescription'],
                            'stationCode': incident['StationCode'],
                            'symptomDescription': incident['SymptomDescription'],
                            'unitName': incident['UnitName'],
                            'unitStatus': incident['UnitStatus'],
                            'unitType': incident['UnitType'],
                            'expiration': expirationTime
                            }
            incidentsOut.append(incidentData)

        self.elesIncidentsCollection.remove()
        if len(incidentsOut) > 0:
            self.elesIncidentsCollection.insert(incidentsOut)
    
    def fetchBusPredictions(self, stopID):
        r = requests.get("http://api.wmata.com/NextBusService.svc/json/JPredictions?StopID=" + \
                         stopID + "&api_key=" + self.apiKey)
        r.raise_for_status()

        predictions = json.loads(r.text)
        expirationTime = datetime.utcnow() + timedelta(minutes=1)
        predictionsOut = []
        
        for prediction in predictions['Predictions']:
            predictionData = {'stopID': stopID,
                              'route': prediction['RouteID'],
                              'direction': prediction['DirectionText'],
                              'minutes': int(prediction['Minutes']),
                              'expiration': expirationTime}
            predictionsOut.append(predictionData)
            
        self.busPredictionsCollection.remove({'stopID': stopID})
        if len(predictionsOut) > 0:
            self.busPredictionsCollection.insert(predictionsOut)
    
    def getStations(self):
        return self.stationsCollection.find(sort=[('name', ASCENDING)])

    def getStationsNear(self, longitude, latitude):
        return self.stationsCollection.find({'location': {'$nearSphere': [longitude, latitude],
                                                          '$maxDistance': 0.5/3959}})
    
    def getStopsNear(self, longitude, latitude):
        return self.stopsCollection.find({'location': {'$nearSphere': [longitude, latitude],
                                                       '$maxDistance': 0.25/3959}})

    def getStop(self, stopID):
        return self.stopsCollection.find_one({'id': stopID})

    def getRailPredictions(self, rtus):
        expiration = self.railPredictionsCollection.find_one(fields={'expiration': '1'})
        if expiration is None or expiration['expiration'] < datetime.utcnow():
            self.fetchRailPredictions()

        predictions = self.railPredictionsCollection.find({'locationCode': {'$in': rtus}})

        predictionsOut = defaultdict(list)

        for prediction in predictions:
            predictionsOut[(prediction['locationCode'], prediction['track'])].append(prediction)

        for predictionsList in predictionsOut.values():
            predictionsList.sort(key=lambda x: x['minSort'])

        return predictionsOut

    def _conditionalUpdateRailIncidents(self):
        expiration = self.railIncidentsCollection.find_one(fields={'expiration': '1'})
        if expiration is None or expiration['expiration'] < datetime.utcnow():
            self.fetchRailIncidents()
            
    def getRailIncidents(self):
        self._conditionalUpdateRailIncidents()

        incidents = self.railIncidentsCollection.find({})
        return incidents

    def getRailIncidentCount(self):
        self._conditionalUpdateRailIncidents()

        return self.railIncidentsCollection.count()

    def _conditionalUpdateELESIncidents(self):
        expiration = self.elesIncidentsCollection.find_one(fields={'expiration': '1'})
        if expiration is None or expiration['expiration'] < datetime.utcnow():
            self.fetchELESIncidents()
            
    def getELESIncidentsByStation(self, rtus):
        self._conditionalUpdateELESIncidents()

        incidents = self.elesIncidentsCollection.find({'unitStatus': 'O', 'stationCode': {'$in': rtus}},
                                                      sort=[('displayOrder', ASCENDING)])
        return incidents

    def getELESIncidentStations(self):
        self._conditionalUpdateELESIncidents()

        openIncidents = self.elesIncidentsCollection.find({'unitStatus': 'O'})
        incidentRTUs = openIncidents.distinct('stationCode')

        stations = self.stationsCollection.find({'rtus': {'$in': incidentRTUs}}, sort=[('name', ASCENDING)])
        return stations

    def getELESIncidentCount(self):
        self._conditionalUpdateELESIncidents()

        return self.elesIncidentsCollection.find({'unitStatus': 'O'}).count()

    def getELESIncidentCountByStation(self, rtus):
        self._conditionalUpdateELESIncidents()

        return self.elesIncidentsCollection.find({'unitStatus': 'O', 'stationCode': {'$in': rtus}}).count()

    def getBusPredictions(self, stopID):
        expiration = self.busPredictionsCollection.find_one({'stopID': stopID},
                                                           fields={'expiration': '1'})
        if expiration is None or expiration['expiration'] < datetime.utcnow():
            self.fetchBusPredictions(stopID)

        return self.busPredictionsCollection.find({'stopID': stopID})
    
