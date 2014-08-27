__author__ = 'sbel'


from django.core.management.base import BaseCommand, CommandError
from atlas.auth.voms import collector


class Command(BaseCommand):

    def handle(self, *args, **options):
        collector.run()



