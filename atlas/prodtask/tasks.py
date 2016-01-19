from __future__ import absolute_import

from celery import shared_task

from atlas.prodtask.models import TRequest
from atlas.prodtask.views import set_request_status


@shared_task
def approve_request(request_id):
    # logger = approve_request.get_logger()
    # logger.info('Approve_request', request_id)
    req = TRequest.objects.get(reqid=request_id)
    req.cstatus = 'approved'
    set_request_status('cron',request_id,'approved','Automatic  approve', 'Request was approved')
    return request_id
