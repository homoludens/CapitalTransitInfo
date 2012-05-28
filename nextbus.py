import requests
from lxml import etree
from pymongo import Connection, GEO2D

from datetime import datetime, timedelta

from pprint import pprint

class NextBus(object):

    def __init__(self, agencyID, db, collectionPrefix):
        self.rs = requests.session()
        self.agencyID = agencyID
        self.stopsCollection = db[collectionPrefix + "Stops"]
        self.routesCollection = db[collectionPrefix + "Routes"]
        self.predictionsCollection = db[collectionPrefix + "Predictions"]
        self.stopsCollection.create_index([("location", GEO2D)])

        
    def fetchRoutesForAgency(self):
        r = self.rs.get('http://webservices.nextbus.com/service/publicXMLFeed?command=routeList&a=' + \
                         self.agencyID)
        r.raise_for_status()
        routesTree = etree.fromstring(r.content)
        routeTags = routesTree.xpath("/body/route/@tag")
        return routeTags

    def fetchPredictionsByID(self, stopID):
        r = self.rs.get('http://webservices.nextbus.com/service/publicXMLFeed?command=predictions&a=' + \
                        self.agencyID + '&stopId=' + str(stopID) + '&useShortTitles=true')
        r.raise_for_status()
        stopTree = etree.fromstring(r.content)

        expirationTime = datetime.utcnow() + timedelta(minutes=1)
        predictionsOut = []

        for prediction in stopTree.xpath("/body/predictions/direction/prediction"):
            predictionData = {'agency': self.agencyID,
                              'stopID': stopID,
                              'minutes': int(prediction.attrib['minutes']),
                              'direction': prediction.getparent().attrib['title'],
                              'route': prediction.getparent().getparent().attrib['routeTitle'],
                              'expiration': expirationTime}
            predictionsOut.append(predictionData)

        self.predictionsCollection.remove({'agency': self.agencyID,
                                           'stopID': stopID})
        if len(predictionsOut) > 0:
            self.predictionsCollection.insert(predictionsOut)

    def fetchPredictionsByTag(self, stopTag, routeTag):
        r = self.rs.get('http://webservices.nextbus.com/service/publicXMLFeed?command=predictions&a=' + \
                         self.agencyID + '&r=' + routeTag + '&s=' + stopTag + '&useShortTitles=true')
        r.raise_for_status()
        stopTree = etree.fromstring(r.content)

        expirationTime = datetime.utcnow() + timedelta(minutes=1)
        predictionsOut = []

        for prediction in stopTree.xpath("/body/predictions/direction/prediction"):
            predictionData = {'agency': self.agencyID,
                              'stopTag': stopTag,
                              'routeTag': routeTag,
                              'minutes': int(prediction.attrib['minutes']),
                              'direction': prediction.getparent().attrib['title'],
                              'route': prediction.getparent().getparent().attrib['routeTitle'],
                              'expiration': expirationTime}
            predictionsOut.append(predictionData)

        self.predictionsCollection.remove({'agency': self.agencyID,
                                           'stopTag': stopTag,
                                           'routeTag': routeTag})
        if len(predictionsOut) > 0:
            self.predictionsCollection.insert(predictionsOut)
    
    def fetchStopsForRoute(self, routeTag):
        r = self.rs.get('http://webservices.nextbus.com/service/publicXMLFeed?command=routeConfig&a=' + \
                         self.agencyID + '&r=' + routeTag + '&terse')
        r.raise_for_status()
        routeTree = etree.fromstring(r.content)
        
        routes = routeTree.xpath("/body/route[@tag = '" + routeTag + "']")
        assert len(routes) == 1
        route = routes[0]

        routeData = {
            'agency': self.agencyID,
            'tag': route.attrib['tag'],
            'directions': {}
            }

        if 'shortTitle' in route.attrib:
            routeData['title'] = route.attrib['shortTitle']
        else:
            routeData['title'] = route.attrib['title']

        for direction in route.xpath("./direction[@useForUI='true']"):
            directionTag = direction.attrib['tag']
            directionTitle = direction.attrib['title']
            stopTags = [stop.attrib['tag'] for stop in direction.xpath("./stop")]
            routeData['directions'][directionTag] = {'tag': directionTag,
                                                     'title': directionTitle,
                                                     'stops': stopTags}

        stops = []

        for stop in route.xpath("./stop"):
            stopData = {
                'agency': self.agencyID,
                'tag': stop.attrib['tag'],
                'location': [float(stop.attrib['lon']), float(stop.attrib['lat'])]
                }
            if 'shortTitle' in stop.attrib:
                stopData['title'] = stop.attrib['shortTitle']
            else:
                stopData['title'] = stop.attrib['title']
            
            if 'stopId' in stop.attrib:
                stopData['stopID'] = int(stop.attrib['stopId'])
            stops.append(stopData)

        return (routeData, stops)
            
    def loadAgencyData(self):
        routes = self.fetchRoutesForAgency()
        for routeTag in routes:
            (routeData, stops) = self.fetchStopsForRoute(routeTag)
            self.routesCollection.update({'agency': routeData['agency'],
                                          'tag': routeData['tag']},
                                         routeData,
                                         upsert=True)
            for stop in stops:
                self.stopsCollection.update({'agency': stop['agency'],
                                             'tag': stop['tag']},
                                            stop,
                                            upsert=True)
                self.stopsCollection.update({'agency': stop['agency'],
                                             'tag': stop['tag']},
                                            {"$addToSet": {'routes': routeTag}})

    def getShortTitle(self, routeTag, stopTag):
        r = self.rs.get('http://webservices.nextbus.com/service/publicXMLFeed?command=predictions&a=' + \
                         self.agencyID + '&r=' + routeTag + '&s=' + stopTag + '&useShortTitles=true')
        r.raise_for_status()
        stopTree = etree.fromstring(r.content)
        return stopTree.xpath('/body/predictions/@stopTitle')[0]

    def fixLongStopNames(self):
        stops = self.stopsCollection.find()
        for stop in stops:
            stop['title'] = self.getShortTitle(stop['routes'][0], stop['tag'])
            self.stopsCollection.save(stop)

    def getRoutes(self):
        routes = self.routesCollection.find({"agency": self.agencyID})
        return routes

    def getRoute(self, routeTag):
        route = self.routesCollection.find_one({"agency": self.agencyID,
                                                "tag": routeTag})
        return route

    def getStopsNear(self, longitude, latitude):
        return self.stopsCollection.find({'location': {'$nearSphere': [longitude, latitude],
                                                       '$maxDistance': 0.25/3959}})
    
    def getStopByTag(self, stopTag):
        stop = self.stopsCollection.find_one({"agency": self.agencyID,
                                              "tag": stopTag})
        return stop

    def getStopByID(self, stopID):
        stop = self.stopsCollection.find_one({"agency": self.agencyID,
                                              "stopID": stopID})
        return stop

    def getPredictionsByID(self, stopID):
        expiration = self.predictionsCollection.find_one({"agency": self.agencyID,
                                                          'stopID': stopID},
                                                         fields={'expiration': '1'})
        
        if expiration is None or expiration['expiration'] < datetime.utcnow():
            self.fetchPredictionsByID(stopID)

        return self.predictionsCollection.find({"agency": self.agencyID,
                                                'stopID': stopID})

    def getPredictionsByTag(self, stopTag, routeTag):
        expiration = self.predictionsCollection.find_one({"agency": self.agencyID,
                                                          'stopTag': stopTag,
                                                          'routeTag': routeTag},
                                                         fields={'expiration': '1'})
        
        if expiration is None or expiration['expiration'] < datetime.utcnow():
            self.fetchPredictionsByTag(stopTag, routeTag)

        return self.predictionsCollection.find({"agency": self.agencyID,
                                                'stopTag': stopTag,
                                                'routeTag': routeTag})
