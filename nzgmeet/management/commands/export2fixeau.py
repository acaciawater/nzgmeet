# -*- coding: utf-8 -*-
import json
import logging
import os, pytz, datetime
import string

from django.conf import settings
from django.core.management.base import BaseCommand
import requests
from requests.exceptions import HTTPError

from acacia.data.models import Project
from iom.models import Waarnemer, Meetpunt, Waarneming

logger = logging.getLogger(__name__)

def genstring(charset, length):
    ''' generate a string of length characters selected randomly from charset '''
    import random
    return ''.join([random.choice(charset) for _ in range(length)])

def genpasswd(length=8):
    ''' generate a password of length characters '''
    charset = string.ascii_letters + string.digits + '!@#$%&*+=-?.:'
    return genstring(charset,length)
    
class Api:
    ''' Interface to api with JWT authorization '''

    def __init__(self, url):
        self.url = url
        self.headers = {}
        
    def post(self, path, data):
        url = self.url + path
        return requests.post(url, json=data, headers=self.headers)

    def put(self, path, id, data):
        url = self.url + path + str(id) +'/'
        return requests.put(url, json=data, headers=self.headers)

    def patch(self, path, id, data):
        url = self.url + path + str(id) +'/'
        return requests.patch(url, json=data, headers=self.headers)

    def get(self, path, params=None):
        # prepend self.url to path if required
        url = path if path.startswith('http') else self.url + path
        return requests.get(url, params=params, headers=self.headers)

    def login(self, username, password):
        response = requests.post(self.url+'/token/',{
            'username': username,
            'password': password
        })
        response.raise_for_status()
        json = response.json()
        self.token = json.get('token')
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization':'JWT '+self.token
        }
        return self.token

class Command(BaseCommand):
    args = ''
    help = 'Exporteer data naar fixeau.com '
    
    def add_arguments(self, parser):
        
        parser.add_argument('-u','--url',
                action='store',
                dest = 'url',
                default = 'https://test.fixeau.com/api/v1',
                help = 'API url')

        parser.add_argument('-f','--folder',
                action='store',
                dest = 'folder',
                default = 6,
                help = 'Folder id for data sources and time series')

    def findObjects(self, path, query):
        ''' returns json iterator of all objects that satisfies query '''
        response = self.api.get(path,query)
        next = True
        while next:
            response.raise_for_status()
            json = response.json()
            results = json.get('results')
            if not results:
                break
            for result in results:
                yield result
            next = json.get('next')
            if next:
                response = self.api.get(next)

    def findFirstObject(self, path, query):
        ''' returns json of first object that satisfies query '''
        results = self.findObjects(path, query)
        return next(results,None)

    def getObject(self, path, pk):
        ''' get object by primary key. Path must end with a slash '''
        response = self.api.get(path+pk)
        if response.ok:
            return response.json()
                
        if response.status_code != 404:
            # raise exception (not for 'not found' errors)
            response.raise_for_status()
            
        return None 

    def findGroup(self, name):
        ''' finds a group by name. returns json of group or None when not found '''
        return self.findFirstObject('/group/', {'name': name})
    
    def createGroup(self, name):
        ''' Create a group with name. Returns json of created group '''
        response = self.api.post('/group/', {'name': name})
        response.raise_for_status()
        return response.json()
    
    def findUser(self, waarnemer):
        ''' find a user corresponding to a waarnemer or None when not found '''
        username = str(waarnemer).lower().replace(' ', '')
        return self.findFirstObject('/user/', {'username': username})

    def createUser(self, waarnemer, group):
        ''' create user for waarnemer, add index number (max 10) if user already exists. Returns json of created user '''
        basename = str(waarnemer).lower().replace(' ', '')
        username = basename
        password = genpasswd(8)
        if waarnemer.tussenvoegsel:
            last_name = waarnemer.tussenvoegsel + ' ' + waarnemer.achternaam
        else:
            last_name = waarnemer.achternaam
        for index in range(1,10):
            response = self.api.post('/user/', {
                'username': username, 
                'password': password,
                'first_name': waarnemer.voornaam or waarnemer.initialen,
                'last_name': last_name,
                'email': waarnemer.email,
                'groups': [group],
                'is_active': False,
                'details': {
                    'phone_number': waarnemer.telefoon
                }
            })
            if response.ok:
                break
            
            # find out what the problem is..
            reason = response.json()
            if 'username' in reason:
                problems = reason['username']
                if 'A user with that username already exists.' in problems:
                    # try again with new username
                    username = '{}{}'.format(basename,index)
                    continue
                    
        response.raise_for_status()
        return response.json()

    def getSource(self, sourceId):
        ''' return datasource object with sourceid '''
        return self.getObject('/source/', sourceId)
        
    def createSource(self, device, users, group, folder=None):
        ''' create a datasource for a device. First add source_type AkvoMobile to database
        device: akvo phone device identifier
        users: list of user names that use the device
        group: group id for device
        '''
        response = self.api.post('/source/', {
            'id': device,
            'name': device,
            'description': 'Akvo phone '+device,
            'source_type': 'AkvoMobile',
            'folder': folder,
            'group': group,
            'users': users 
        })
        response.raise_for_status()
        return response.json()
    
    def findSeries(self, meetpunt, category):
        ''' find EC time series for a meetpunt and category combination '''
        # need to make series name unique, filter on category does not work
        name = '{} ({})'.format(meetpunt.name, category) if category else meetpunt.name
        return self.findFirstObject('/series/', {
            'name': name,
            'source': meetpunt.device,
            'parameter': 'EC',
            'category': category
            })

    def createSeries(self, meetpunt, category, folder = None):
        ''' create timeseries for a meetpunt, category combination '''
        location = meetpunt.latlng()
        # need to make series name unique, filter on category does not work
        name = '{} ({})'.format(meetpunt.name, category) if category else meetpunt.name 
        response = self.api.post('/series/', {
            'name': name,
            'description': meetpunt.displayname,
            'location': {
                'coordinates': [
                    location[1],
                    location[0]
                ],
                'type': 'Point'
            },
            'meta': {'identifier': meetpunt.identifier},
            'folder': folder,
            'source': meetpunt.device,
            'parameter': 'EC',
            'category': category,
            'unit': 'mS/cm'      
        })
        response.raise_for_status()
        return response.json()
    
    def addMeasurements(self, meetpunt, source, target):
        ''' add all measurements for meetpunt from source time series and set series id to target '''
        location = meetpunt.latlng()
        device = meetpunt.device
        measurements = [{
            "time": p.date.isoformat(),
            "value": p.value/1000.0,
            'location': {
                'coordinates': [
                    location[1],
                    location[0]
                ],
                'type': 'Point'
            },
#             "meta": null,
            "source": device,
            "parameter": 'EC',
            "unit": 'mS/cm',
            "series": target} for p in source.datapoints.order_by('date')]
        response = self.api.post('/measurement/',measurements)
        response.raise_for_status()
        return response.json()
    
    def addMeasurements1(self, meetpunt):
        ''' add all measurements for meetpunt to time series '''
        location = meetpunt.latlng()
        waarnemingen = meetpunt.waarneming_set.all()
        
        for category in ('Deep', 'Shallow'):
            cat = 'Diep' if category == 'Deep' else 'Ondiep'
            sources = meetpunt.series_set.filter(name__icontains=cat)
            if not sources:
                logger.warning('Time series EC_{} for meetpunt {} does not exist.'.format(cat,meetpunt.name))
                continue
            source = next(sources)
            target = self.findSeries(meetpunt, category)
            if not target:
                logger.error('Time series {} for EC ({}) does not exist.'.format(meetpunt.name,category))
                continue

            # bulk create measurements
            measurements = [{
                "time": p.datum,
                "value": p.waarde/1000.0,
                'location': {
                    'coordinates': [
                        location[1],
                        location[0]
                    ],
                    'type': 'Point'
                },
                "meta": null,
                "source": meetpunt.device,
                "parameter": 'EC',
                "unit": 'mS/cm',
                "series": target['id']} for p in source.datapoints.order_by('date')]
            
    def handle(self, *args, **options):

        url = options.get('url')
        folder = options.get('folder')        
        project = Project.objects.first()

        self.api = Api(url)
        logger.info('Logging in, url={}'.format(url))
        self.api.login(settings.FIXEAU_USERNAME,settings.FIXEAU_PASSWORD)
        
        # get or create project group
        groupName = project.name
        group = self.findGroup(groupName)
        if not group:
            logger.info('Creating group {}'.format(groupName))
            group = self.createGroup(groupName)
        groupId = group['id']
            
        # get or create users
        logger.info('Creating users')
        users = {}
        for w in Waarnemer.objects.all():
            try:
                user = self.findUser(w)
                if user:
                    password = ''
                    logger.debug('Found user {} with username {} for {}'.format(user['id'], user['username'], w))
                else:
                    user = self.createUser(w,groupId)
                    logger.info('Created user {} with username {} and password {} for {}'.format(user['id'], user['username'], password, w))
                users[w] = user
                        
            except HTTPError as error:
                response = error.response
                print('ERROR creating user {}: {}'.format(w,response.json()))
                
        # build dictionary of devices with set of waarnemers that have used the device
        logger.info('Querying unique devices in '+project.name)
        devices = {}    
        for w in Waarneming.objects.all():
            if w.device in devices:
                devices[w.device].add(w.waarnemer)
            else:
                devices[w.device] = set([w.waarnemer])
        logger.debug('{} devices found'.format(len(devices)))
    
        # add devices (create data sources)
        logger.info('Creating data sources')
        for device, waarnemers in devices.items():
            usernames = [users[w]['username'] for w in waarnemers if w in users] 
            try:
                source = self.getSource(device)
                if source:
                    logger.debug('Found existing data source {}'.format(device))
                else:
                    source = self.createSource(device, usernames, groupId, folder=folder)
                    logger.debug('Created data source {}'.format(device))
            except HTTPError as error:
                response = error.response
                print('ERROR creating data source {}: {}'.format(device,response.json()))
                break
   
        logger.info('Creating time series')
        for m in Meetpunt.objects.all():
            try:
                for source in m.series_set.filter(name__icontains='EC'):
                    name = source.name.lower()
                    if 'ondiep' in name:
                        category = 'Shallow'
                    elif 'diep' in name:
                        category = 'Deep'
                    else:
                        category = None
                    target = self.findSeries(m, category)
                    if target:
                        msg = 'Found existing time series {} for {}'.format(target['id'], m)
                    else:
                        target = self.createSeries(m, category, folder=folder)
                        msg = 'Created time series {} for {}'.format(target['id'], m)
                    if category:
                        msg += ' ({})'.format(category)
                    logger.debug(msg)
                    response = self.addMeasurements(m, source, target['id'])
                    if response:
                        # response is unicode, not dict??
                        resp = json.loads(response)
                        logger.debug('Added {} measurements'.format(resp.get('count')))
                        
            except HTTPError as error:
                response = error.response
                print('ERROR creating time series {} ({}): {}'.format(m,category,response.json()))
                break # abort
        
#         # update data sources  (set folder id = 6)
#         logger.info('Updating data sources')
#         sources = self.findObjects('/source/', {'source_type':'AkvoMobile'})
#         for s in sources:
#             response = self.api.patch('/source/', s['id'], {"folder":6})
#             response.raise_for_status()

#         # update series (set folder id = 6)
#         logger.info('Updating series')
#         for m in Meetpunt.objects.all():
#             ser = self.findSeries(m)
#             if ser:
#                 logger.debug('Update series {}'.format(ser.name))
#                 ser['folder'] = 6
#                 response = self.api.patch('/series/', ser['id'], {"folder":6})
#                 response.raise_for_status()
