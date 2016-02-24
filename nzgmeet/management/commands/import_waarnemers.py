'''
Created on Aug 6, 2015

@author: theo
'''
from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from optparse import make_option
from iom.models import Waarnemer, Meetpunt, CartoDb
from django.contrib.gis.geos import Point
from acacia.data.models import ProjectLocatie
import csv, logging, uuid
from dateutil import parser
from iom.util import updateSeries, updateCartodb 

logger = logging.getLogger('akvo')

class Command(BaseCommand):
    args = ''
    help = 'Importeer csv file met waarnemenrs'
    option_list = BaseCommand.option_list + (
            make_option('--file',
                action='store',
                dest = 'file',
                default = '/media/sf_F_DRIVE/projdirs/NZG/waarnemers.csv',
                help = 'naam van csv bestand'),
        )
    
    def handle(self, *args, **options):
        fname = options.get('file', None)
        if not fname:
            print 'filenaam ontbreekt'
            return
        project = ProjectLocatie.objects.get(name='NZG') # NZG
        user = User.objects.get(username='theo') # admin

        with open(fname) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                aname = row['Achternaam']
                vname = row['Voornaam']
                init = row['Initialen']
                tus = row['Tussen']
                email = row['Mail']
                tel = row['Nummer']
                waarnemer,created = Waarnemer.objects.get_or_create(achternaam=aname,voornaam=vname,initialen=init,tussenvoegsel=tus,defaults={'telefoon': tel,'email': email})
