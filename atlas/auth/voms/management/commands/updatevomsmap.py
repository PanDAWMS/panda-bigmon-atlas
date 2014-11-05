__author__ = 'sbel'


from django.core.management.base import BaseCommand, CommandError
from atlas.auth.voms import collector


class Command(BaseCommand):

    def handle(self, *args, **options):
        # TODO: add verbose and quiet mode
        info = collector.run()
        # TODO: use logging here
        print "Added records: %s" % (info['added'])
        print "Removed records: %s" % (info['removed'])

