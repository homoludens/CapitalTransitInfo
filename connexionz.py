from datetime import datetime

import requests
from lxml import etree

from pymongo import Connection, GEO2D
from pyrfc3339 import parse

from pprint import pprint

NSMAP = {'c': 'urn:connexionz-co-nz'}

class Connexionz(object):
    def __init__(self, baseURL, agency, db, collectionPrefix):
        self.baseURL = baseURL
        self.agency = agency
        self.stopsCollection = db[collectionPrefix + "Stops"]
        self.routesCollection = db[collectionPrefix + "Routes"]
        self.predictionsCollection = db[collectionPrefix + "Predictions"]
        self.stopsCollection.create_index([("location", GEO2D)])


    def fetchStops(self):
        r = requests.get(self.baseURL + "/rtt/public/utility/file.aspx?contenttype=SQLXML&Name=Platform.xml")
        r.raise_for_status()
        stopTree = etree.fromstring(r.content)

        for stop in stopTree.xpath('/c:Platforms/c:Platform', namespaces=NSMAP):
            stopData = {'agency': self.agency,
                        'name': stop.attrib['Name'],
                        'tag': stop.attrib['PlatformTag'],
                        'location': [float(position.attrib['Long']), float(position.attrib['Lat'])]}
            if 'PlatformNo' in stop.attrib:
                stopData['number'] = stop.attrib['PlatformNo']
            position = stop.xpath('c:Position', namespaces=NSMAP)[0]
            self.stopsCollection.update({'agency': self.agency,
                                         'tag': stop.attrib['PlatformTag']},
                                        stopData,
                                        upsert=True)
        

    def fetchRoutes(self):
        r = requests.get(self.baseURL + "/rtt/public/utility/file.aspx?contenttype=SQLXML&Name=RoutePattern.rxml")
        r.raise_for_status()
        routeTree = etree.fromstring(r.content)
        for route in routeTree.xpath('/RoutePattern/Project/Route'):
            routeData = {'agency': self.agency,
                         'number': route.attrib['RouteNo'],
                         'name': route.attrib['Name'],
                         'patterns': {}}
            for pattern in route.xpath('Destination/Pattern'):
                if pattern.attrib['Schedule'] != 'Active':
                    continue
                patternData = {'destinationName': pattern.xpath('../@Name')[0],
                               'direction': pattern.attrib['Direction'],
                               'routeTag': pattern.attrib['RouteTag'],
                               'name': pattern.attrib['Name'],
                               'platforms': []}

                for platform in pattern.xpath('Platform'):
                    tag = platform.attrib['PlatformTag']
                    patternData['platforms'].append(tag)
                routeData['patterns'][patternData['routeTag']] = patternData
            self.routesCollection.update({'agency': self.agency,
                                          'number': routeData['number']},
                                         routeData,
                                         upsert=True)

    def fetchPredictions(self, stopTag):
        r = requests.get(self.baseURL + "/rtt/public/utility/file.aspx?contenttype=SQLXML&Name=RoutePositionET.xml&PlatformTag=" + stopTag)
        r.raise_for_status()

        predictionsTree = etree.fromstring(r.content)

        
        predictionData = {'agency': self.agency,
                          'tag': stopTag,
                          'expires': parse(predictionsTree.xpath('/c:RoutePositionET/c:Content/@Expires',
                                                                 namespaces=NSMAP)[0], utc=True),
                          'predictions': []}        
        
        for trip in predictionsTree.xpath('/c:RoutePositionET/c:Platform/c:Route/c:Destination/c:Trip',
                                          namespaces=NSMAP):
            predictionData['predictions'].append({'minutes': int(trip.attrib['ETA']),
                                                  'destination': trip.xpath('../@Name')[0],
                                                  'route': trip.xpath('../../@RouteNo')[0]})

        predictionData['predictions'].sort(key=lambda x: x['minutes'])
        

        return predictionData
        

    def getRoutes(self):
        routes = self.routesCollection.find({"agency": self.agency})
        return routes
    
    def getRoute(self, routeNumber):
        route = self.routesCollection.find_one({"agency": self.agency,
                                                "number": routeNumber})
        return route

    def getStopsNear(self, longitude, latitude):
        return self.stopsCollection.find({'location': {'$nearSphere': [longitude, latitude],
                                                       '$maxDistance': 0.25/3959}})

    def getStop(self, stopTag):
        stop = self.stopsCollection.find_one({"agency": self.agency,
                                              "tag": stopTag})
        return stop

    def getStopTag(self, stopID):
        stop = self.stopsCollection.find_one({"agency": self.agency,
                                              "number": stopID})
        return stop['tag']

    def getPredictions(self, stopTag):
        predictions = self.predictionsCollection.find_one({"agency": self.agency,
                                                           "tag": stopTag,
                                                           "expires": {"$gte": datetime.utcnow()}})
        if predictions is None:
            predictions = self.fetchPredictions(stopTag)
            self.predictionsCollection.update({"agency": self.agency,
                                               "tag": stopTag},
                                              predictions,
                                              upsert=True)

        return predictions
                                                    

def main():
    connection = Connection()
    db = connection['dctm']
    collectionPrefix = "art"

    cnx = Connexionz("http://realtime.commuterpage.com", "ART", db, collectionPrefix)

    cnx.loadStops()
    cnx.loadRoutes()

if __name__ == '__main__':
    main()

            
