from __future__ import absolute_import
from time import sleep
from celery import shared_task, current_task

from atlas.backend.celery import app
from atlas.prodtask.models import TRequest


#
# @app.task(bind=True)
# def find_input_datasets_task(self,slices,reqid):
#     slice_dataset_dict = {}
#     for i,slice_number in enumerate(slices):
#         try:
#             slice_dataset_dict.update(form_skipped_slice(slice_number,reqid))
#             if not self.request.called_directly:
#                 self.update_state(state='PROGRESS',
#                     meta={'current': i, 'total': len(slices)})
#         except Exception,e:
#             pass
#     return slice_dataset_dict


@shared_task
def test_percentage():
    for i in range(100):
        sleep(0.1)
        current_task.update_state(state='PROGRESS',
            meta={'current': i, 'total': 100})



@shared_task
def approve_request(request_id):
    # logger = approve_request.get_logger()
    # logger.info('Approve_request', request_id)
    req = TRequest.objects.get(reqid=request_id)
    req.cstatus = 'approved'
    #set_request_status('cron',request_id,'approved','Automatic  approve', 'Request was approved')
    return request_id
