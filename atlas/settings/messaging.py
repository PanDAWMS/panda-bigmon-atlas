from .runtime import (IDDS_MESSAGING_PASSWORD, IDDS_MESSAGING_HOST,
                      IDDS_MESSAGING_PORT, IDDS_MESSAGING_USERNAME, IDDS_MESSAGING_QUEUE)

TEST_CONFIG = {"connection": {'host': '0.0.0.0', 'port': 61613}, "queue": "A.B.C.D"}
IDDS_PRODUCTION_CONFIG = {
    "queue": IDDS_MESSAGING_QUEUE,
    "connection": {'host': IDDS_MESSAGING_HOST, 'port': IDDS_MESSAGING_PORT,
                   'username': IDDS_MESSAGING_USERNAME, 'password': IDDS_MESSAGING_PASSWORD},
}
