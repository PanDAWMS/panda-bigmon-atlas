from django.core.management.base import BaseCommand, CommandError
import time

from atlas.prestage.views import submit_all_tapes_processed


class Command(BaseCommand):
    args = 'None'
    help = 'Submit request for all tapes '

    def handle(self, *args, **options):
        self.stdout.write('Start submit tape rules %s'%time.ctime())
        if not args:
            try:
                submit_all_tapes_processed()
            except Exception as e:
                raise CommandError('Some problem during tape rules check: %s'%e)
        self.stdout.write('Successfully finished tape rules  check: %s'%time.ctime())