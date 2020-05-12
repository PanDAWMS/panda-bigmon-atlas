import logging
import uuid
from copy import deepcopy
from time import sleep
from typing import Optional
import ssl
from django.conf import settings
from atlas.messaging.consumer import Listener
from atlas.messaging.consumer import Payload, build_listener
from django.utils.module_loading import import_string
from stomp import connect
import socket
import threading
logger = logging.getLogger("prodtask_messaging")

wait_to_connect = int(getattr(settings, "STOMP_WAIT_TO_CONNECT", 10))



def start_bunch( destination_name: str,
    callback_str: str,
    connection_settings: dict,
    is_testing=False,
    testing_disconnect=True,
    param_to_callback=None,
    return_listener=False,):
    i = 0
    for info in socket.getaddrinfo(connection_settings['host'], connection_settings['port'], 0, 0, socket.IPPROTO_TCP):
        if info[0] == socket.AF_INET6:
            continue
        hostname = info[4][0]
        new_connection = deepcopy(connection_settings)
        new_connection['host'] = hostname
        new_connection['prefix_name'] = socket.getfqdn(hostname).split('.')[0] + '-'
        i+=1
        x = threading.Thread(target=start_processing, args=(destination_name,callback_str,new_connection,is_testing,testing_disconnect,param_to_callback,return_listener,), daemon=True)
        x.start()
    while True:
        sleep(10)


def start_processing(
    destination_name: str,
    callback_str: str,
    connection_settings: dict,
    is_testing=False,
    testing_disconnect=True,
    param_to_callback=None,
    return_listener=False,
):
    callback_function = import_string(callback_str)


    listener = build_listener(connection_settings, callback_function,  destination_name)

    def main_logic():
        try:
            logger.info("Starting listener...")

            def _callback(payload: Payload) -> None:
                try:
                    if param_to_callback:
                        callback_function(payload, param_to_callback)
                    else:
                        callback_function(payload)
                except BaseException as e:
                    logger.exception(f"A exception of type {type(e)} was captured during callback logic")
                    logger.warning("Trying to do NACK explicitly sending the message to DLQ...")
                    if listener.is_open():
                        payload.nack()
                        logger.warning("Done!")
                    raise e


            listener.start(_callback, wait_forever=is_testing is False)

            if is_testing is True:
                return listener
        except BaseException as e:
            logger.exception(f"A exception of type {type(e)} was captured during listener logic")
        finally:
            if is_testing is False:
                logger.info(f"Trying to close listener...")
                if listener.is_open():
                    listener.close()
                logger.info(f"Waiting {wait_to_connect} seconds before trying to connect again...")
                sleep(wait_to_connect)

    if is_testing is False:
        while True:
            main_logic()
    else:
        max_tries = 3
        tries = 0
        testing_listener = None
        while True:
            if tries == 0:
                testing_listener = main_logic()
                if return_listener:
                    return testing_listener
                tries += 1
            elif tries >= max_tries:
                if testing_disconnect is True:
                    testing_listener.close()
                break
            else:
                sleep(0.2)
                tries += 1

