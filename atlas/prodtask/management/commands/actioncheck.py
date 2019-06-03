from django.core.management.base import BaseCommand, CommandError
import time

from atlas.prestage.views import find_action_to_execute


class Command(BaseCommand):
    args = '<request_id, request_id>'
    help = 'Check action steps '

    def handle(self, *args, **options):
        self.stdout.write('Start checking action steps %s'%time.ctime())
        if not args:
            try:
                find_action_to_execute()
            except Exception,e:
                raise CommandError('Some problem during waiting step approval: %s'%e)
        self.stdout.write('Successfully finished waiting step check: %s'%time.ctime())