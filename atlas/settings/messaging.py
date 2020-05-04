from .local import IDDS_MESSAGING_PASSWORD
TEST_CONFIG = {"connection":{'host':'0.0.0.0','port':61613},"queue":"A.B.C.D"}

IDDS_PRODUCTION_CONFIG = {"queue":'/queue/Consumer.prodsys.atlas.idds',
                          "connection":{'host':'atlas-mb.cern.ch','port':61013 ,'username':'atlasidds','password':IDDS_MESSAGING_PASSWORD}}