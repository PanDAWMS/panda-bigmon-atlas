from django.core.management.base import BaseCommand, CommandError
import time
from atlas.prodtask.step_manage_views import check_waiting_steps
from atlas.prodtask.waiting_actions import find_waiting


class Command(BaseCommand):
    args = '<request_id, request_id>'
    help = 'Check waiting steps and approve or reject them'

    def handle(self, *args, **options):
        self.stdout.write('Start checking waiting steps %s'%time.ctime())
        if not args:
            try:
                find_waiting()
            except Exception as e:
                raise CommandError('Some problem during waiting step approval: %s'%e)
        self.stdout.write('Successfully finished waiting step check: %s'%time.ctime())