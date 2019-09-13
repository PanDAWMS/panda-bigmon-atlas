from django.core.management.base import BaseCommand, CommandError
import time

from atlas.prodtask.mcevgen import sync_cvmfs_db


class Command(BaseCommand):
    args = ''
    help = 'Sync cvmfs JOs'

    def handle(self, *args, **options):
        self.stdout.write('Start sync cvmfs for JOs at %s'%time.ctime())
        try:
            sync_cvmfs_db()
        except Exception,e:
            raise CommandError('Some problem during syncing: %s'%str(e))
        self.stdout.write('Successfully finished cvmfs sync: %s'%time.ctime())