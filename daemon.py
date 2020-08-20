import argparse
import logging
import logging.handlers
import os
import time
from daemonize import Daemonize


pid = '/var/log/prodtasklog/prodtask-messaging.pid'



def main():

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'atlas.settings')
    import django
    django.setup()
    from atlas.messaging.manager import start_processing, start_bunch
    from atlas.settings.messaging import TEST_CONFIG, IDDS_PRODUCTION_CONFIG
    if args.is_test:
        config = TEST_CONFIG['connection']
        logger.info('Starting messaging waiting for test')
        start_processing(TEST_CONFIG['queue'], 'atlas.special_workflows.views.idds_recive_message', config)
        while True:
            time.sleep(10)
    else:
        config = IDDS_PRODUCTION_CONFIG['connection']
        logger.info('Starting messaging waiting for idds')
        start_bunch(IDDS_PRODUCTION_CONFIG['queue'], 'atlas.special_workflows.views.idds_recive_message', config)



if __name__ == "__main__":

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'atlas.settings')
    os.environ.setdefault('LD_LIBRARY_PATH', '/usr/local/lib')
    import django

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-t',
        '--test',
        action='store_true',
        dest='is_test',
        default=False,
        help='run on test queue'
    )

    args = parser.parse_args()


    django.setup()
    logger = logging.getLogger('prodtask_messaging')


    logger.info('Starting the daemon for messaging')

    daemon = Daemonize(
        app='prodtask messaging daemon',
        pid=pid,
        action=main,
        verbose=True,
        logger=logger,

    )

    daemon.start()

    logger.info('The daemon ended gracefully')