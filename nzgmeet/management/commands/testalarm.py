'''
Created on May 18, 2014

@author: theo
'''
from django.core.management.base import BaseCommand
from django.core.mail import send_mail

class Command(BaseCommand):
    args = ''
    help = 'Check email functionality'

    def handle(self, *args, **options):
        subject = '[NZGMeet] Email test'
        message = 'Hallo,\nDeze mail komt van de nzgmeet.nl server en is bedoeld om de email te testen.\nGroeten, Theo'
        fromaddr = 'NZGMeet Alarm <alarm@nzgmeet.nl>'
        recipients = ['theo.kleinendorst@acaciawater.com',]
        send_mail(subject, message, fromaddr, recipients)
    