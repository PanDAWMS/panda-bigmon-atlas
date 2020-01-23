from django.core.management.base import BaseCommand, CommandError
import time

from atlas.prodtask.task_views import sync_old_tasks


class Command(BaseCommand):
    args = '<task_id>'
    help = 'Sync tasks < task_id'

    def add_arguments(self, parser):
        parser.add_argument('start_id', nargs=1, type=int)

    def handle(self, *args, **options):
        self.stdout.write('Start sync from request to tasks at %s'%time.ctime())
        if options:
            try:
                task_id = int(options['start_id'][0])
                sync_old_tasks(task_id)
                pass
            except Exception as e:
                raise CommandError('Some problem during syncing: %s'%str(e))
        self.stdout.write('Successfully finished tasks sync: %s'%time.ctime())