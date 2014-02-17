from django.conf import settings
from django.conf.urls.static import static

from core.common.urls import *

import atlas.settings

urlpatterns = patterns('',)
urlpatterns += common_patterns
#urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

