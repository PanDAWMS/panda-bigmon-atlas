import json
import logging
import ssl
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Callable
from typing import Dict

import stomp

logger = logging.getLogger("prodtask_messaging")


class Acknowledgements(Enum):
    """
    See more details at:
        - https://pubsub.github.io/pubsub-specification-1.2.html#SUBSCRIBE_ack_Header
        - https://jasonrbriggs.github.io/pubsub.py/api.html#acks-and-nacks
    """

    CLIENT = "client"
    CLIENT_INDIVIDUAL = "client-individual"
    AUTO = "auto"


@dataclass(frozen=True)
class Payload:
    ack: Callable
    nack: Callable
    headers: Dict
    body: Dict


class Listener(stomp.ConnectionListener):
    def __init__(
        self,
        connection: stomp.connect.StompConnection11,
        callback: Callable,
        subscription_configuration: Dict,
        connection_configuration: Dict,
        is_testing: bool = False,
        subscription_id=None,
        prefix_name=''
    ) -> None:
        self._subscription_configuration = subscription_configuration
        self._connection_configuration = connection_configuration
        self._connection = connection
        self._callback = callback
        self._subscription_id = f"{subscription_id if subscription_id else str(uuid.uuid4())}-listener"
        self._listener_id = prefix_name+str(uuid.uuid4())
        self._is_testing = is_testing

        if self._is_testing:
            from stomp.listener import TestListener

            self._test_listener = TestListener()
        else:
            self._test_listener = None

    def on_message(self, headers, message):
        message_id = headers["message-id"]
        logger.info(f"Message ID: {message_id}")
        logger.debug("Listener %s Received headers: %s"%(self._listener_id, headers))
        logger.debug("Listener %s Received message: %s"%(self._listener_id, message))

        # https://jasonrbriggs.github.io/stomp.py/api.html#acks-and-nacks
        def ack_logic():
            self._connection.ack(message_id, self._subscription_id)

        def nack_logic():
            # Requeue is used because of RabbitMQ: https://www.rabbitmq.com/stomp.html#ack-nack
            self._connection.nack(message_id, self._subscription_id, requeue=False)

        self._callback(Payload(ack_logic, nack_logic, headers, json.loads(message)))

    def is_open(self):
        return self._connection.is_connected()

    def start(self, callback: Callable = None, wait_forever=True):
        logger.info(f"Starting listener with name: {self._listener_id}")
        logger.info(f"Subscribe/Listener auto-generated ID: {self._subscription_id}")

        if self._is_testing:
            self._connection.set_listener("TESTING", self._test_listener)
        else:
            self._connection.set_listener(self._listener_id, self)

        self._callback = callback if callback else self._callback
        self._connection.connect(**self._connection_configuration)
        self._connection.subscribe(
            id=self._subscription_id,
            headers=self._connection_configuration["headers"],
            **self._subscription_configuration,
        )
        logger.info(f"{self._listener_id} Connected")
        if wait_forever:
            while True:
                if not self.is_open():
                    logger.info(f"{self._listener_id} is not open. Starting...")
                    self.start(self._callback, wait_forever=False)
                time.sleep(1)

    def close(self):
        disconnect_receipt = str(uuid.uuid4())
        self._connection.disconnect(receipt=disconnect_receipt)
        logger.info(f"{self._listener_id} Disconnected")


def build_listener(connection_settings, callback, destination_name ,ack_type=Acknowledgements.CLIENT,
                    durable_topic_subscription=False,
                    is_testing=False,):

    logger.info("Building listener...")
    hosts, vhost = [(connection_settings.get("host"), connection_settings.get("port"))], connection_settings.get("vhost")
    prefix_name = connection_settings.get("prefix_name",'')
    if connection_settings.get("hostStandby") and connection_settings.get("portStandby"):
        hosts.append((connection_settings.get("hostStandby"), connection_settings.get("portStandby")))
    use_ssl = connection_settings.get("use_ssl", False)
    ssl_version = connection_settings.get("ssl_version", ssl.PROTOCOL_TLS)
    logger.info(f"Use SSL? {use_ssl}. Version: {ssl_version}")
    outgoing_heartbeat = int(connection_settings.get("outgoingHeartbeat", 0))
    incoming_heartbeat = int(connection_settings.get("incomingHeartbeat", 0))
    # http://stomp.github.io/stomp-specification-1.2.html#Heart-beating
    # http://jasonrbriggs.github.io/stomp.py/api.html
    conn = stomp.connect.StompConnection11(
        hosts,
        ssl_version=ssl_version,
        use_ssl=use_ssl,
        heartbeats=(outgoing_heartbeat, incoming_heartbeat),
        vhost=vhost,
    )
    client_id = connection_settings.get("client_id", uuid.uuid4())
    routing_key = destination_name
    subscription_configuration = {
        "destination": routing_key,
        "ack": ack_type.value,
        # RabbitMQ
        "x-queue-name": only_destination_name(destination_name),
        "auto-delete": "false",
    }
    header_setup = {
        # ActiveMQ
        "client-id": f"{client_id}-listener",
        "activemq.prefetchSize": "1",

    }

    if connection_settings.get("durable_topic_subscription") is True:
        durable_subs_header = {
            # ActiveMQ
            "activemq.subscriptionName": header_setup["client-id"],
            # RabbitMQ
            "durable": "true",
        }
        header_setup.update(durable_subs_header)
    connection_configuration = {
        "username": connection_settings.get("username"),
        "passcode": connection_settings.get("password"),
        "wait": True,
        "headers": header_setup,
    }
    listener = Listener(
        conn,
        callback,
        subscription_configuration,
        connection_configuration,
        is_testing=is_testing,
        subscription_id=connection_settings.get("subscriptionId"),
        prefix_name=prefix_name,
    )
    return listener

def only_destination_name(destination: str) -> str:
    position = destination.rfind("/")
    if position > 0:
        return destination[position + 1 :]
    return destination