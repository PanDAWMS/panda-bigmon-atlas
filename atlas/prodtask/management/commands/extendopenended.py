from django.core.management.base import BaseCommand, CommandError
import time
from atlas.prodtask.open_ended import check_open_ended


class Command(BaseCommand):
    args = '<request_id, request_id>'
    help = 'Extend open ended requests'

    def handle(self, *args, **options):
        self.stdout.write('Start open ended at %s'%time.ctime())
        if not args:
            try:
                check_open_ended()
            except Exception as e:
                raise CommandError('Some problem during request extension: %s'%e)
        self.stdout.write('Successfully finished request extension: %s'%time.ctime())