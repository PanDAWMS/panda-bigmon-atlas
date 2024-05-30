import json
from copy import deepcopy
from dataclasses import dataclass, field, asdict
from datetime import timedelta
from enum import Enum, auto
from pprint import pprint
from typing import Dict, List, Literal, Any
from uuid import uuid1

from django.core.cache import cache
from django.db.models.signals import post_save
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.db import connection
from django.db import connections
from django.db.models import CASCADE
from django.utils import timezone
from jinja2.nativetypes import NativeEnvironment
from rest_framework import serializers

from ..prodtask.helper import Singleton
import logging
from django.dispatch import receiver
_logger = logging.getLogger('prodtaskwebui')


def days_ago(days: int): return timezone.now() - timedelta(days=days)

MC_STEPS = ['Evgen',
            'Evgen Merge',
             'Simul',
             'Merge',
             'Digi',
             'Reco',
             'Rec Merge',
             'Atlfast',
             'Atlf Merge',
             'TAG',
             'Deriv',
             'Deriv Merge']

class sqliteID(Singleton):
    def get_id(self,cursor,id_field_name,table_name):
        if (id_field_name+table_name) in list(self.__id_dict.keys()):
            self.__id_dict[id_field_name+table_name] = self.__id_dict[id_field_name+table_name] + 1
        else:
            self.__id_dict[id_field_name+table_name] = self.__get_first_id(cursor,id_field_name,table_name)
        return self.__id_dict[id_field_name+table_name]

    def __get_first_id(self, cursor, id_field_name,table_name):
        new_id = None
        try:
            query = "SELECT MAX(%s) AS max_id FROM %s"%(id_field_name,table_name)
            cursor.execute(query)
            rows = cursor.fetchall()
            if not(rows[0][0]):
                new_id = 1
            else:
                new_id = rows[0][0] + 1
        finally:
            if cursor:
                cursor.close()
        return new_id

    def __init__(self):
        self.__id_dict = {}

def prefetch_id(db, seq_name, table_name=None, id_field_name=None):
    """ Fetch the next value in a django id oracle sequence """
    cursor = connections[db].cursor()
    new_id = None
    if cursor.db.client.executable_name != 'sqlite3':

        try:
            query = "SELECT %s.nextval FROM dual" % seq_name
            cursor.execute(query)
            rows = cursor.fetchall()
            new_id = rows[0][0]
        finally:
            if cursor:
                cursor.close()
    else:
        #only for tests
        sqlite_id = sqliteID.getInstance()
        new_id = sqlite_id.get_id(cursor, id_field_name, table_name)
    return new_id

class TProject(models.Model):
    project = models.CharField(max_length=60, db_column='PROJECT', primary_key=True)
    begin_time = models.DecimalField(decimal_places=0, max_digits=10, db_column='BEGIN_TIME')
    end_time = models.DecimalField(decimal_places=0, max_digits=10, db_column='END_TIME')
    status = models.CharField(max_length=8, db_column='STATUS')
    description = models.CharField(max_length=500, db_column='DESCRIPTION')
    time_stamp = models.DecimalField(decimal_places=0, max_digits=10, db_column='TIMESTAMP')

    def save(self, *args, **kwargs):
        raise NotImplementedError

    def __str__(self):
        return "%s" % self.project

    class Meta:

        db_table = 'T_PROJECTS'



class TRequest(models.Model):
    # PHYS_GROUPS=[(x,x) for x in ['physics','BPhysics','Btagging','DPC','Detector','EGamma','Exotics','HI','Higgs',
    #                              'InDet','JetMet','LAr','MuDet','Muon','SM','Susy','Tau','Top','Trigger','TrackingPerf',
    #                              'reprocessing','trig-hlt','Validation']]
    class STATUS():
        REGISTERED = 'registered'
        HOLD = 'hold'
        WAITING = 'waiting'
        WORKING = 'working'
        APPROVED = 'approved'
        FINISHED = 'finished'
        MONITORING = 'monitoring'
        REWORKING = 'reworking'
        REMONITORING = 'remonitoring'
        CANCELLED = 'cancelled'
        TEST = 'test'


    PHYS_GROUPS=[(x,x) for x in ['BPHY',
                                 'COSM',
                                 'DAPR',
                                 'EGAM',
                                 'EXOT',
                                 'FTAG',
                                 'HDBS',
                                 'HIGG',
                                 'HION',
                                 'IDET',
                                 'IDTR',
                                 'JETM',
                                 'LARG',
                                 'MCGN',
                                 'MDET',
                                 'MUON',
                                 'PHYS',
                                 'REPR',
                                 'SIMU',
                                 'SOFT',
                                 'STDM',
                                 'SUSY',
                                 'TAUP',
                                 'TCAL',
                                 'TDAQ',
                                 'TOPQ',
                                 'THLT',
                                 'TRIG',
                                 'VALI',
                                 'UPGR']]

    REQUEST_TYPE = [(x,x) for x in ['MC','GROUP','REPROCESSING','ANALYSIS','HLT','TIER0','EVENTINDEX']]
    PROVENANCE_TYPE = [(x,x) for x in ['AP','GP','XP']]
    TERMINATE_STATE = ['test','cancelled']
    DEFAULT_ASYNC_ACTION_TIMEOUT = 3600 * 4

    reqid = models.DecimalField(decimal_places=0, max_digits=12, db_column='PR_ID', primary_key=True)
    manager = models.CharField(max_length=32, db_column='MANAGER', null=False, blank=True)
    description = models.CharField(max_length=256, db_column='DESCRIPTION', null=True, blank=True)
    ref_link = models.CharField(max_length=256, db_column='REFERENCE_LINK', null=True, blank=True)
    cstatus = models.CharField(max_length=32, db_column='STATUS', null=False, blank=True)
    provenance = models.CharField(max_length=32, db_column='PROVENANCE', null=False, blank=True,choices=PROVENANCE_TYPE)
    request_type = models.CharField(max_length=32, db_column='REQUEST_TYPE',choices=REQUEST_TYPE, null=False, blank=True)
    campaign = models.CharField(max_length=32, db_column='CAMPAIGN', null=False, blank=True)
    subcampaign = models.CharField(max_length=32, db_column='SUB_CAMPAIGN', null=False, blank=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=False, choices=PHYS_GROUPS, blank=True)
    energy_gev = models.DecimalField(decimal_places=0, max_digits=8, db_column='ENERGY_GEV', null=False, blank=True)
    project = models.ForeignKey(TProject,db_column='PROJECT', on_delete=CASCADE, null=True, blank=False)
    is_error = models.BooleanField(db_column='EXCEPTION', null=True, blank=False)
    jira_reference = models.CharField(max_length=50, db_column='REFERENCE', null=True, blank=True)
    info_fields = models.TextField(db_column='INFO_FIELDS', null=True, blank=True)
    is_fast = models.BooleanField(db_column='IS_FAST', null=True, blank=False)
    #locked = models.DecimalField(decimal_places=0, max_digits=1, db_column='LOCKED', null=True)

    def get_next_slice(self):
        if InputRequestList.objects.filter(request=self).count() == 0:
            new_slice_number = 0
        else:
            new_slice_number = (InputRequestList.objects.filter(request=self).order_by('-slice')[0]).slice + 1
        return new_slice_number

    @property
    def request_created(self):
        try:
            date = RequestStatus.objects.filter(request=self.reqid,status='waiting').values('timestamp')
            if len(date)==0:
                return ""
        except:
            return ""
        return date[0].get('timestamp').strftime('%Y-%m-%d')

    @property
    def request_approved(self):
        try:
            date = RequestStatus.objects.filter(request=self.reqid,status='registered').values('timestamp')
            if len(date)==0:
                return ""
        except:
            return ""
        return date[0].get('timestamp').strftime('%Y-%m-%d')

    @property
    def priority(self):
        try:
            priority = self.info_field('priority')
            if priority:
                return priority.replace('-2','0+')
            else:
                return ''
        except:
            return ""


    @property
    def request_events(self):
        try:
            request_events = self.info_field('request_events')
            if request_events:
                return request_events
            else:
                return ''
        except:
            return ""

    @property
    def long_description(self):
        if self.info_field('long_description'):
            return self.info_field('long_description')
        else:
            return ''


    def update_priority(self, new_priorities):
        info_field_dict = {}
        if self.info_fields:
            info_field_dict = json.loads(self.info_fields)
        priorities_set = set()
        old_priorities = [int(x) for x in info_field_dict.get('priority','').split(',') if x]
        priorities_set.update(old_priorities)
        priorities_set.update(new_priorities)
        priorities = list(priorities_set)
        priorities.sort()
        info_field_dict['priority'] = ','.join([str(x) for x in priorities])
        self.info_fields = json.dumps(info_field_dict)
        return info_field_dict['priority']

    def set_info_field(self,field,value):
        if self.info_fields:
            info_field_dict = json.loads(self.info_fields)

        else:
            info_field_dict={}
        info_field_dict.update({field:value})
        self.info_fields = json.dumps(info_field_dict)

    def info_field(self,field):
        if self.info_fields:
            info_field_dict = json.loads(self.info_fields)
            return info_field_dict.get(field,None)
        else:
            return None

    def save(self, *args, **kwargs):
        if not self.reqid:
            self.reqid = prefetch_id('deft','ATLAS_DEFT.T_PRODMANAGER_REQUEST_ID_SEQ','T_PRODMANAGER_REQUEST','PR_ID')

        super(TRequest, self).save(*args, **kwargs)

    class Meta:
        db_table = 'T_PRODMANAGER_REQUEST'


class ProductionRequestSerializer(serializers.ModelSerializer):
    class Meta:
        model = TRequest
        fields = '__all__'

class RequestStatus(models.Model):
    STATUS_TYPES = (
                    ('Created', 'Created'),
                    ('Pending', 'Pending'),
                    ('Unknown', 'Unknown'),
                    ('Approved', 'Approved'),
                    )
    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='REQ_S_ID', primary_key=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=CASCADE)
    comment = models.CharField(max_length=256, db_column='COMMENT', null=True)
    owner = models.CharField(max_length=32, db_column='OWNER', null=False)
    status = models.CharField(max_length=32, db_column='STATUS', choices=STATUS_TYPES, null=False)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=False)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','ATLAS_DEFT.T_PRODMANAGER_REQ_STAT_ID_SEQ','T_PRODMANAGER_REQUEST_STATUS','REQ_S_ID')
        super(RequestStatus, self).save(*args, **kwargs)

    def save_with_current_time(self, *args, **kwargs):
        if not self.timestamp:
            self.timestamp = timezone.now()
        self.save(*args, **kwargs)

    class Meta:
        db_table = 'T_PRODMANAGER_REQUEST_STATUS'

class StepTemplate(models.Model):
    id =  models.DecimalField(decimal_places=0, max_digits=12,  db_column='STEP_T_ID', primary_key=True)
    step = models.CharField(max_length=12, db_column='STEP_NAME', null=False)
    def_time = models.DateTimeField(db_column='DEF_TIME', null=False)
    status = models.CharField(max_length=12, db_column='STATUS', null=False)
    ctag = models.CharField(max_length=12, db_column='CTAG', null=False)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=False)
    cpu_per_event = models.DecimalField(decimal_places=0, max_digits=7, db_column='CPU_PER_EVENT', null=True)
    output_formats = models.CharField(max_length=2000, db_column='OUTPUT_FORMATS', null=True)
    memory = models.DecimalField(decimal_places=0, max_digits=5, db_column='MEMORY', null=True)
    trf_name = models.CharField(max_length=128, db_column='TRF_NAME', null=True)
    lparams = models.CharField(max_length=2000, db_column='LPARAMS', null=True)
    vparams = models.CharField(max_length=4000, db_column='VPARAMS', null=True)
    swrelease = models.CharField(max_length=80, db_column='SWRELEASE', null=True)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','ATLAS_DEFT.T_STEP_TEMPLATE_ID_SEQ','T_STEP_TEMPLATE','STEP_T_ID')
        super(StepTemplate, self).save(*args, **kwargs)

    class Meta:
        #db_table = u'T_STEP_TEMPLATE'
        db_table = 'T_STEP_TEMPLATE'
#
# class Ttrfconfig(models.Model):
#     tag = models.CharField(max_length=1, db_column='TAG', default='-')
#     cid = models.DecimalField(decimal_places=0, max_digits=5, db_column='CID', primary_key=True, default=0)
#     trf = models.CharField(max_length=80, db_column='TRF', null=True, default='transformation')
#     lparams = models.CharField(max_length=1024, db_column='LPARAMS', null=True, default='parameter list')
#     vparams = models.CharField(max_length=4000, db_column='VPARAMS', null=True, default='')
#     trfv = models.CharField(max_length=40, db_column='TRFV', null=True)
#     status = models.CharField(max_length=12, db_column='STATUS', null=True)
#     ami_flag = models.DecimalField(decimal_places=0, max_digits=10, db_column='AMI_FLAG', null=True)
#     createdby = models.CharField(max_length=60, db_column='CREATEDBY', null=True)
#     input = models.CharField(max_length=20, db_column='INPUT', null=True)
#     step = models.CharField(max_length=12, db_column='STEP', null=True)
#     formats = models.CharField(max_length=256, db_column='FORMATS', null=True)
#     cache = models.CharField(max_length=32, db_column='CACHE', null=True)
#     cpu_per_event = models.DecimalField(decimal_places=0, max_digits=5, db_column='CPU_PER_EVENT', null=True, default=1)
#     memory = models.DecimalField(decimal_places=0, max_digits=5, db_column='MEMORY', default=1000)
#     priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', default=100)
#     events_per_job = models.DecimalField(decimal_places=0, max_digits=10, db_column='EVENTS_PER_JOB', default=1000)
#
#
#     class Meta:
#         app_label = 'grisli'
#         db_table = 'T_TRF_CONFIG'

# class TDataFormatAmi(models.Model):
#     format = models.CharField(max_length=32, db_column='FORMAT', primary_key=True)
#     description = models.CharField(max_length=256, db_column='DESCRIPTION')
#     status = models.CharField(max_length=8, db_column='STATUS')
#     last_modified = models.DateTimeField(db_column='LASTMODIFIED')
#
#     class Meta:
#         app_label = 'grisli'
#         db_table = 'T_DATA_FORMAT_AMI'

class ProductionDataset(models.Model):
    name = models.CharField(max_length=160, db_column='NAME', primary_key=True)
    #task = models.ForeignKey(ProducitonTask,db_column='TASK_ID')
    task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASKID', null=True)
    #parent_task = models.ForeignKey(ProducitonTask,db_column='TASK_ID')
    parent_task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID', null=True)
    rid = models.DecimalField(decimal_places=0, max_digits=12, db_column='PR_ID', null=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)
    events = models.DecimalField(decimal_places=0, max_digits=7, db_column='EVENTS', null=True)
    files = models.DecimalField(decimal_places=0, max_digits=7, db_column='FILES', null=False)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=False)
    campaign = models.CharField(max_length=32, db_column='campaign', null=False, blank=True)
    ddm_timestamp = models.DateTimeField(db_column='ddm_timestamp')
    ddm_status = models.CharField(max_length=32, db_column='DDM_STATUS', null=True)

    class Meta:
        #db_table = u'T_PRODUCTION_DATASET'
        db_table = 'T_PRODUCTION_DATASET'

class ProductionContainer(models.Model):
    name = models.CharField(max_length=150, db_column='NAME', primary_key=True)
    parent_task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID', null=True)
    rid = models.DecimalField(decimal_places=0, max_digits=12, db_column='PR_ID', null=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)

    class Meta:
        #db_table = u'T_PRODUCTION_DATASET'
        db_table = 'T_PRODUCTION_CONTAINER'



class InputRequestList(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='IND_ID', primary_key=True)
    #dataset = models.ForeignKey(ProductionDataset, db_column='INPUTDATASET',null=True)
    dataset = models.CharField(max_length=160, db_column='INPUTDATASET', null=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=CASCADE)
    slice = models.DecimalField(decimal_places=0, max_digits=12, db_column='SLICE', null=False)
    brief = models.CharField(max_length=150, db_column='BRIEF')
    phys_comment = models.CharField(max_length=256, db_column='PHYSCOMMENT')
    comment = models.CharField(max_length=512, db_column='SLICECOMMENT')
    input_data = models.CharField(max_length=150, db_column='INPUTDATA')
    project_mode = models.CharField(max_length=256, db_column='PROJECT_MODE')
    priority = models.DecimalField(decimal_places=0, max_digits=12, db_column='PRIORITY')
    input_events = models.DecimalField(decimal_places=0, max_digits=12, db_column='INPUT_EVENTS')
    is_hide = models.BooleanField(db_column='HIDED', null=True, blank=False)
    cloned_from = models.ForeignKey('self',db_column='CLONED_FROM', null=True, on_delete=CASCADE)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','ATLAS_DEFT.T_INPUT_DATASET_ID_SEQ','T_INPUT_DATASET','IND_ID')
        super(InputRequestList, self).save(*args, **kwargs)

    def tasks_in_slice(self):
        return ProductionTask.objects.filter(request=self.request, step__in=StepExecution.objects.filter(slice=self, request=self.request))

    class Meta:
        #db_table = u'T_INPUT_DATASET'
        db_table = 'T_INPUT_DATASET'

class SliceSerializer(serializers.ModelSerializer):
    class Meta:
        model = InputRequestList
        fields = '__all__'
class RetryAction(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=10, db_column='RETRYACTION_ID', primary_key=True)
    action_name = models.CharField(max_length=50, db_column='RETRY_ACTION')
    description = models.CharField(max_length=250, db_column='RETRY_DESCRIPTION')
    active = models.CharField(max_length=1, db_column='ACTIVE')

    @property
    def is_active(self):
        return self.active == 'Y'

    def save(self, *args, **kwargs):
        raise NotImplementedError('Only manual creation')

    def __str__(self):
        return "%i - %s" % (int(self.id), self.action_name)
    class Meta:
        app_label = 'panda'
        #db_table = u'T_INPUT_DATASET'
        db_table = '"ATLAS_PANDA"."RETRYACTIONS"'


class JediWorkQueue(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=10, db_column='QUEUE_ID', primary_key=True)
    queue_name = models.CharField(max_length=50, db_column='QUEUE_NAME')


    def save(self, *args, **kwargs):
        raise NotImplementedError('Only manual creation')

    def __str__(self):
        return "%i - %s" % (int(self.id), self.queue_name)

    class Meta:
        app_label = 'panda'
        #db_table = u'T_INPUT_DATASET'
        db_table = '"ATLAS_PANDA"."JEDI_WORK_QUEUE"'


#   ID NUMBER(10, 0) NOT NULL
# , ERRORSOURCE VARCHAR2(256 BYTE) NOT NULL
# , ERRORCODE NUMBER(10, 0) NOT NULL
# , RETRYACTION_FK NUMBER(10, 0) NOT NULL
# , PARAMETERS VARCHAR2(256 BYTE)
# , ARCHITECTURE VARCHAR2(256 BYTE)
# , RELEASE VARCHAR2(64 BYTE)
# , WORKQUEUE_ID NUMBER(5, 0)
# , DESCRIPTION VARCHAR2(250 BYTE)
# , EXPIRATION_DATE TIMESTAMP(6)
# , ACTIVE CHAR(1 BYTE) DEFAULT 'Y' NOT NULL


class RetryErrors(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=10, db_column='Retryerror_id', primary_key=True)
    error_source = models.CharField(max_length=256, db_column='ErrorSource',null=False)
    error_code = models.DecimalField(decimal_places=0, max_digits=10, db_column='ErrorCode')
    active = models.CharField(max_length=1, db_column='ACTIVE', null=True)
    retry_action = models.ForeignKey(RetryAction,db_column='RetryAction', on_delete=CASCADE)
    error_diag =  models.CharField(max_length=256, db_column='ErrorDiag')
    parameters = models.CharField(max_length=256, db_column='PARAMETERS')
    architecture = models.CharField(max_length=256, db_column='Architecture')
    release = models.CharField(max_length=64, db_column='RELEASE')
    work_queue = models.DecimalField(decimal_places=0, max_digits=5, db_column='WORKQUEUE_ID')
    description = models.CharField(max_length=250, db_column='DESCRIPTION')
    expiration_date = models.DateTimeField(db_column='EXPIRATION_DATE', null=True)




    @property
    def is_active(self):
        return self.active == 'Y'

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('panda','ATLAS_PANDA.RETRYERRORS_ID_SEQ','RETRYACTION','ID')

        super(RetryErrors, self).save(*args, **kwargs)

    class Meta:
        app_label = 'panda'
        db_table = '"ATLAS_PANDA"."RETRYERRORS"'


class TrainProduction(models.Model):

    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='GPT_ID', primary_key=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    departure_time =  models.DateTimeField(db_column='DEPARTURE_TIME')
    approval_time = models.DateTimeField(db_column='APPROVAL_TIME', null=True)
    timestamp = models.DateTimeField(db_column='TIMESTAMP')
    manager = models.CharField(max_length=32, db_column='OWNER', null=False, blank=True)
    description = models.CharField(max_length=256, db_column='DESCRIPTION')
    request  = models.DecimalField(decimal_places=0, max_digits=12, db_column='PR_ID', null=True)
    #ptag = models.CharField(max_length=5, db_column='PTAG', null=False)
    #release = models.CharField(max_length=32, db_column='RELEASE', null=False, blank=True)
    outputs = models.TextField( db_column='OUTPUTS_PATTERN', null=True)
    pattern_request = models.ForeignKey(TRequest, db_column='PATTERN_REQUEST', on_delete=CASCADE)

    def save(self, *args, **kwargs):
        self.timestamp = timezone.now()
        if not self.id:
            self.id = prefetch_id('deft','T_GROUP_TRAIN_ID_SEQ',"T_GROUP_TRAIN",'GPT_ID')
        super(TrainProduction, self).save(*args, **kwargs)


    @property
    def output_by_slice(self):
        if self.outputs:
            return json.loads(self.outputs)
        return []

    def __str__(self):
        return "%i - %s"%(self.pattern_request.reqid,self.pattern_request.description)

    class Meta:
        db_table = '"T_GROUP_TRAIN"'



class MCJobOptions(models.Model):

    dsid =  models.DecimalField(decimal_places=0, max_digits=12, db_column='DSID', primary_key=True)
    physic_short = models.CharField(max_length=200, db_column='PHYSIC_SHORT',null=False)
    timestamp = models.DateTimeField(db_column='UPDATE_TIME')
    events_per_job = models.DecimalField(decimal_places=0, max_digits=10, db_column='EVENTS_PER_JOB')
    files_per_job = models.DecimalField(decimal_places=0, max_digits=10, db_column='FILES_PER_JOB')
    content = models.CharField(max_length=2000, db_column='CONTENT')

    def save(self, *args, **kwargs):
        self.timestamp = timezone.now()
        super(MCJobOptions, self).save(*args, **kwargs)

    class Meta:
        #app_label = 'deft'
        db_table = "T_MC_JO_PHYS"

class TDataFormat(models.Model):
    name = models.CharField(max_length=64, db_column='NAME', primary_key=True)
    description = models.CharField(max_length=256, db_column='DESCRIPTION', null=True)

    class Meta:
        db_table = "T_DATA_FORMAT"

# class MCPileupOverlayGroupDescription(models.Model):
#
#     id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='POG_GROUP_ID', primary_key=True)
#     description = models.CharField(max_length=255, db_column='DESCRIPTION')
#
#     def save(self, *args, **kwargs):
#         #self.timestamp = timezone.now()
#         if not self.id:
#             self.id = prefetch_id('dev_db',u'T_MC_PO_PHYS_GROUP_SEQ',"T_MC_PO_PHYS_GROUP",'POG_GROUP_ID')
#         super(MCPileupOverlayGroupDescription, self).save(*args, **kwargs)
#
#     class Meta:
#         app_label = 'dev'
#         db_table = u'"ATLAS_DEFT"."T_MC_PO_PHYS_GROUP"'
#
# class MCPileupOverlayGroups(models.Model):
#
#     id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='POG_ID', primary_key=True)
#     campaign = models.CharField(max_length=50, db_column='CAMPAIGN')
#     dsid  = models.DecimalField(decimal_places=0, max_digits=12, db_column='DSID')
#     group = models.ForeignKey(MCPileupOverlayGroupDescription, db_column='POG_GROUP_ID')
#
#     def save(self, *args, **kwargs):
#         #self.timestamp = timezone.now()
#         if not self.id:
#             self.id = prefetch_id('dev_db',u'T_PILEUP_OVERLAY_GROUPS_SEQ',"T_PILEUP_OVERLAY_GROUPS",'POG_ID')
#         super(MCPileupOverlayGroups, self).save(*args, **kwargs)
#
#     class Meta:
#         app_label = 'dev'
#         db_table = u'"ATLAS_DEFT"."T_PILEUP_OVERLAY_GROUPS"'



class ParentToChildRequest(models.Model):
    RELATION_TYPE = (
                    ('BC', 'By creation'),
                    ('MA', 'Manually'),
                    ('SP', 'Evgen Split'),
                    ('CL', 'Cloned'),
                    ('MR', 'Merged'),
                    ('DP', 'Derivation'),
                    )

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PTC_ID', primary_key=True)
    parent_request = models.ForeignKey(TRequest, db_column='PARENT_PR_ID', on_delete=CASCADE, related_name='+')
    child_request = models.ForeignKey(TRequest, db_column='CHILD_PR_ID', null=True, on_delete=CASCADE, related_name='+')
    relation_type = models.CharField(max_length=2, db_column='RELATION_TYPE', choices=RELATION_TYPE, null=False)
    train = models.ForeignKey(TrainProduction, db_column='TRAIN_ID', null=True, on_delete=CASCADE)
    status = models.CharField(max_length=12, db_column='STATUS', null=False)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','PARENT_CHILD_REQUEST_ID_SEQ','T_PARENT_CHILD_REQUEST','PTC_ID')
        super(ParentToChildRequest, self).save(*args, **kwargs)


    class Meta:
        db_table = "T_PARENT_CHILD_REQUEST"








#
# class HashTagToHashTag(models.Model):
#
#     id = models.DecimalField(decimal_places=0, max_digits=12, db_column='HTTT_ID', primary_key=True)
#     hashtag_parent = models.ForeignKey(TRequest,  db_column='HT_ID_PARENT')
#     hashtag_child = models.ForeignKey(HashTag, db_column='HT_ID_CHILD')
#
#     def save(self, *args, **kwargs):
#         if not self.id:
#             self.id = prefetch_id('dev_db',u'T_HT_TO_TASK',"T_HT_TO_TASK",'HTTT_ID')
#         super(HashTagToHashTag, self).save(*args, **kwargs)
#
#     def save_last_update(self, *args, **kwargs):
#         self.last_update = timezone.now()
#         super(HashTagToHashTag, self).save(*args, **kwargs)
#
#     class Meta:
#         app_label = 'dev'
#         db_table = u'"T_HT_TO_HT"'


class StepExecution(models.Model):
    STEPS = MC_STEPS
    STEPS_STATUS = ['NotChecked','NotCheckedSkipped','Skipped','Approved']
    STEPS_APPROVED_STATUS = ['Skipped','Approved']
    INT_TASK_CONFIG_PARAMS = ['nEventsPerJob','nEventsPerMergeJob','nFilesPerMergeJob','nGBPerMergeJob','nMaxFilesPerMergeJob',
                              'nFilesPerJob','nGBPerJob','maxAttempt','nEventsPerInputFile','maxFailure','split_slice']
    TASK_CONFIG_PARAMS = INT_TASK_CONFIG_PARAMS + ['input_format','token','merging_tag','project_mode','evntFilterEff',
                                                   'PDA', 'PDAParams', 'container_name', 'onlyTagsForFC']

    class STATUS():
        NOT_CHECKED = 'NotChecked'
        APPROVED = 'Approved'
        SKIPPED = 'Skipped'
        NOT_CHECKED_SKIPPED = 'NotCheckedSkipped'

    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='STEP_ID', primary_key=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=CASCADE)
    step_template = models.ForeignKey(StepTemplate, db_column='STEP_T_ID', on_delete=CASCADE)
    status = models.CharField(max_length=12, db_column='STATUS', null=False)
    slice = models.ForeignKey(InputRequestList, db_column='IND_ID', null=False, on_delete=CASCADE)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=False)
    step_def_time = models.DateTimeField(db_column='STEP_DEF_TIME', null=False)
    step_appr_time = models.DateTimeField(db_column='STEP_APPR_TIME', null=True)
    step_exe_time = models.DateTimeField(db_column='STEP_EXE_TIME', null=True)
    step_done_time = models.DateTimeField(db_column='STEP_DONE_TIME', null=True)
    input_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='INPUT_EVENTS', null=True)
    task_config = models.CharField(max_length=2000, db_column='TASK_CONFIG')
    step_parent = models.ForeignKey('self', db_column='STEP_PARENT_ID', on_delete=CASCADE)

    def set_task_config(self, update_dict):
        if not self.task_config:
            self.task_config = ''
            currrent_dict = {}
        else:
            currrent_dict = json.loads(self.task_config)
        currrent_dict.update(update_dict)
        self.task_config = json.dumps(currrent_dict)

    def remove_task_config(self, key):
        if self.task_config:
            currrent_dict = json.loads(self.task_config)
            if key in currrent_dict:
                currrent_dict.pop(key)
                self.task_config = json.dumps(currrent_dict)

    def get_task_config(self, field = None):
        return_dict = {}
        try:
            return_dict = json.loads(self.task_config)
        except:
            pass
        if field:
            return return_dict.get(field,None)
        else:
            return return_dict

    def save_with_current_time(self, *args, **kwargs):
        if not self.step_def_time:
            self.step_def_time = timezone.now()
        if self.status == 'Approved':
            if not self.step_appr_time:
                self.step_appr_time = timezone.now()
            self.post_approve_action()
        self.save(*args, **kwargs)

    def update_project_mode(self,token,value=None):
        current_project_mode = ''
        if self.get_task_config('project_mode'):
            current_project_mode = self.get_task_config('project_mode')
        tokens = []
        for current_token in current_project_mode.split(';'):
            if not current_token.startswith(token):
                tokens.append(current_token)
        if value:
            tokens.append(token+'='+str(value))
        else:
            tokens.append(token)
        new_project_mode = ';'.join(tokens)
        self.set_task_config({'project_mode':new_project_mode})

    def remove_project_mode(self,token):
        return_value = None
        if self.get_task_config('project_mode'):
            current_project_mode = self.get_task_config('project_mode')
            tokens = []
            for current_token in current_project_mode.split(';'):
                if not current_token.strip().startswith(token):
                    tokens.append(current_token)
                else:
                    return_value = current_token
            new_project_mode = ';'.join(tokens)
            self.set_task_config({'project_mode': new_project_mode})
        return return_value

    def get_project_mode(self,token):
        if self.get_task_config('project_mode'):
            current_project_mode = self.get_task_config('project_mode')
            for current_token in current_project_mode.split(';'):
                if current_token.strip().startswith(token):
                    return current_token[current_token.find('=')+1:].strip()
        return None

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','ATLAS_DEFT.T_PRODUCTION_STEP_ID_SEQ','T_PRODUCTION_STEP','STEP_ID')
        if not self.step_parent_id:
            self.step_parent_id = self.id
        super(StepExecution, self).save(*args, **kwargs)

    def post_approve_action(self):
        STARTING_REQUEST_ID = 5816
        if (self.request_id > STARTING_REQUEST_ID) and (((self.request_id % 10) == 2) or ((self.request_id % 10) == 8)):
            if 'cloud' not in self.get_task_config('project_mode'):
                self.update_project_mode('cloud','WORLD')

    @property
    def analysis_step_id(self):
        return self.get_task_config('analysis_step_id')

    @analysis_step_id.setter
    def analysis_step_id(self, value):
        self.set_task_config({'analysis_step_id':value})

    @property
    def analysis_step(self):
        if self.analysis_step_id:
            return AnalysisStepTemplate.objects.get(id=self.analysis_step_id)
        return None

    @property
    def broken_step(self):
        if self.slice.is_hide:
            return True
        if ProductionTask.objects.filter(step=self).exists():
            total_tasks = ProductionTask.objects.filter(step=self).count()
            broken_tasks = ProductionTask.objects.filter(step=self,
                                                         status__in=ProductionTask.RED_STATUS+[ProductionTask.STATUS.OBSOLETE]).count()
            if broken_tasks == total_tasks:
                return True
        return False

    class Meta:
        #db_table = u'T_PRODUCTION_STEP'
        db_table = 'T_PRODUCTION_STEP'



class TaskTemplate(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASK_TEMPLATE_ID', primary_key=True)
    step = models.ForeignKey(StepExecution, db_column='STEP_ID', on_delete=CASCADE)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=CASCADE)
    parent_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID')
    name = models.CharField(max_length=130, db_column='TASK_NAME')
    timestamp = models.DateTimeField(db_column='TIMESTAMP')
    template_type = models.CharField(max_length=128, db_column='TEMPLATE_TYPE', null=True)
    task_template = models.JSONField(db_column='TEMPLATE')
    task_error = models.CharField(max_length=4000, db_column='TRASK_ERROR', null=True)
    build = models.CharField(max_length=200, db_column='TAG', null=True)


    def save(self, *args, **kwargs):
        self.timestamp = timezone.now()
        super(TaskTemplate, self).save(*args, **kwargs)

    class Meta:
        app_label = 'dev'
        db_table =  "T_TASK_TEMPLATE"


class TTask(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASKID', primary_key=True)
    parent_tid = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    total_done_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_DONE_JOBS', null=True)
    submit_time = models.DateTimeField(db_column='SUBMIT_TIME', null=False)
    start_time = models.DateTimeField(db_column='START_TIME', null=True)
    total_req_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_REQ_JOBS', null=True)
    total_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_EVENTS', null=True)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=True)
    current_priority =  models.DecimalField(decimal_places=0, max_digits=5, db_column='CURRENT_PRIORITY', null=True)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=True)
    vo = models.CharField(max_length=16, db_column='VO', null=True)
    prodSourceLabel = models.CharField(max_length=20, db_column='PRODSOURCELABEL', null=True)
    name = models.CharField(max_length=128, db_column='TASKNAME', null=True)
    username = models.CharField(max_length=128, db_column='USERNAME', null=True)
    chain_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='CHAIN_TID', null=True)
    _jedi_task_parameters = models.TextField(db_column='JEDI_TASK_PARAMETERS')
    __params = None

    @property
    def jedi_task_parameters(self):
        if not self.__params:
            try:
                self.__params = json.loads(self._jedi_task_parameters)
            except:
                return
        return  self.__params

    @property
    def input_dataset(self):
        return self._get_dataset('input') or ""

    @property
    def output_dataset(self):
        return self._get_dataset('output') or ""

    def _get_dataset(self, ds_type):
        if ds_type not in ['input', 'output']:
            return
        params = self.jedi_task_parameters
        job_params = params.get('jobParameters')
        if not job_params:
            return
        for param in job_params:
            param_type, dataset = [ param.get(x) for x in ('param_type', 'dataset') ]
            if (param_type == ds_type) and (dataset is not None):
                return dataset.rstrip('/')
        return None

#    def save(self, **kwargs):
#        """ Read-only access to the table """
#        raise NotImplementedError

    def get_id(self):
        return prefetch_id('deft', 'ATLAS_DEFT.PRODSYS2_TASK_ID_SEQ', 'T_TASK', 'TASKID')

    def delete(self, *args, **kwargs):
         return

    class Meta:
#        managed = False
#        db_table =  u'"ATLAS_DEFT"."T_TASK"'
        db_table =  "T_TASK"
     #   app_label = 'taskmon'


@dataclass
class TemplateVariable:
    class VariableType(str, Enum):
        STRING = 'str'
        INTEGER = 'int'
        FLOAT = 'float'
        BOOLEAN = 'bool'
        TEMPLATE = 'template'

    name: str
    value: str
    keys: [str] = field(default_factory=list)
    type: VariableType = VariableType.TEMPLATE


    class KEY_NAMES:
        INPUT_BASE = 'input_base'
        OUTPUT_BASE = 'output_base'
        TASK_NAME = 'task_name'
        USER_NAME = 'user_name'
        TASK_PRIORITY = 'taskPriority'
        PARENT_ID = 'parent_tid'
        SOURCE_PREPARED = 'source_prepared'
        NO_EMAIL = 'noEmail'
        OUTPUT_SCOPE = 'output_scope'
        INPUT_DS = 'dsForIN'
        nEVENTS = 'nEvents'
        WORKING_GROUP = 'workingGroup'

    KEYS_SEPARATOR = ','

    @staticmethod
    def get_key_template(key: str) -> str:
        return '{{ ' + key + ' }}'

    @property
    def primary_key(self) -> str:
        return self.keys[0] if self.keys else None




class AnalysisTaskTemplate(models.Model):

    class STATUS:
        ACTIVE = 'ACTIVE'
        PREPARATION = 'PREPARATION'
        OBSOLETE = 'OBSOLETE'
        ERROR = 'ERROR'

    class SOURCE_ACTION:
        EVENTLOOP = 'EL'

    id = models.AutoField(db_column='AT_ID', primary_key=True)
    tag = models.CharField(max_length=50, db_column='TAG', null=True)
    task_parameters = models.JSONField(db_column='TASK_PARAMETERS')
    variables = models.JSONField(db_column='VARIABLES')
    build_task = models.DecimalField(decimal_places=0, max_digits=12, db_column='BUILD_TASKID', null=True)
    source_tar = models.CharField(max_length=300, db_column='SOURCE_TAR', null=True)
    source_action = models.CharField(max_length=50, db_column='SOURCE_ACTION', null=True)
    description = models.CharField(max_length=4000, db_column='DESCRIPTION', null=True)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=True)
    username = models.CharField(max_length=128, db_column='USERNAME', null=True)
    physics_group = models.CharField(max_length=128, db_column='PHYSICS_GROUP', null=True)
    software_release = models.CharField(max_length=128, db_column='SOFTWARE_RELEASE', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)

    def save(self, *args, **kwargs):
        if not self.status:
            self.status = self.STATUS.ACTIVE
        self.timestamp = timezone.now()
        if not self.tag:
            self.tag = "gtaNew"
        super(AnalysisTaskTemplate, self).save(*args, **kwargs)
        if self.tag == "gtaNew":
            self.tag = "gta" + str(self.id)
            super(AnalysisTaskTemplate, self).save(*args, **kwargs)


    @property
    def variables_data(self):
        result = []
        for x in self.variables:
            x['type'] = TemplateVariable.VariableType(x['type'])
            result.append(TemplateVariable(**x))
        return result

    @variables_data.setter
    def variables_data(self, value):
        self.variables = [asdict(x) for x in value]

    def get_variable(self, variable_name: str):
        for x in self.variables_data:
            if x.name == variable_name:
                return x.value
        return None

    def change_variable(self, variable_name: str, input_data: str):
        current_data = self.variables_data
        return_value = None
        for x in current_data:
            if x.name == variable_name:
                x.value = input_data
                return_value = x.value
                break
        self.variables_data = current_data
        return return_value

    def print(self):
        print(f'Tag: {self.id}')
        pprint(self.task_parameters)

    class Meta:
        app_label = 'dev'
        db_table = '"T_AT_TEMPLATE"'



class AnalysisStepTemplate(models.Model):


    class STATUS:
        NOT_CHECKED = 'NotChecked'
        APPROVED = 'Approved'

    class ANALYSIS_STEP_NAME:
        ANALYSIS = 'Analysis'
        MERGING = 'Merging'
        GROUP_ANALYSIS = 'GroupAnaly'


    id =  models.AutoField(db_column='AS_TEMPLATE_ID', primary_key=True)
    name = models.CharField(max_length=128, db_column='NAME', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    step_parameters = models.JSONField(db_column='STEP_PARAMETERS')
    variables = models.JSONField(db_column='VARIABLES')
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=True)
    task_template = models.ForeignKey(AnalysisTaskTemplate, on_delete=models.CASCADE, db_column='AT_ID', null=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=CASCADE)
    slice = models.ForeignKey(InputRequestList, db_column='IND_ID', null=False, on_delete=CASCADE)
    step_production_parent = models.ForeignKey(StepExecution, db_column='STEP_PARENT_ID', on_delete=CASCADE, null=True)
    step_analysis_parent = models.ForeignKey('self', db_column='STEP_AT_PARENT_ID', on_delete=CASCADE)

    def save(self, *args, **kwargs):
        self.timestamp = timezone.now()
        super(AnalysisStepTemplate, self).save(*args, **kwargs)

    @property
    def variables_data(self):
        result = []
        for x in self.variables:
            x['type'] = TemplateVariable.VariableType(x['type'])
            result.append(TemplateVariable(**x))
        return result

    @variables_data.setter
    def variables_data(self, value):
        self.variables = [asdict(x) for x in value]

    @property
    def project(self):
        return 'user'

    @property
    def priority(self):
        return self.get_variable(TemplateVariable.KEY_NAMES.TASK_PRIORITY)

    @property
    def input_dataset(self):
        return self.step_parameters['dsForIN']

    @property
    def vo(self):
        return self.step_parameters['vo']

    @property
    def prodSourceLabel(self):
        return self.step_parameters['prodSourceLabel']

    def change_variable(self, variable_name: str, input_data: str):
        current_data = self.variables_data
        return_value = None
        for x in current_data:
            if x.name == variable_name:
                x.value = input_data
                return_value = x.value
                break
        self.variables_data = current_data
        return return_value

    def get_variable(self, variable_name: str):
        for x in self.variables_data:
            if x.name == variable_name:
                return x.value
        return None

    def get_variable_key(self, variable_name: str):
        for x in self.variables_data:
            if x.name == variable_name:
                return x.primary_key
        return None

    def render_task_template(self) -> dict:
        render_template = deepcopy(self.step_parameters)
        key_values = {}
        for variable in self.variables_data:
            for key_chain in variable.keys:
                if key_chain not in key_values:
                    key_values[key_chain] = {}
                key_values[key_chain].update({variable.name: variable.value})
        rendered_keys = []
        for variable in self.variables_data:
            if variable.type == TemplateVariable.VariableType.TEMPLATE:
                for key_chain in variable.keys:
                    if key_chain not in rendered_keys:
                        current_node = render_template
                        current_key = ''
                        leaf_parent = None
                        for key in key_chain.split(TemplateVariable.KEYS_SEPARATOR):
                            leaf_parent = current_node
                            if key.isdigit():
                                current_key = int(key)
                            else:
                                current_key = key
                            current_node = current_node[current_key]
                        jinja_env = NativeEnvironment()
                        current_template = jinja_env.from_string(current_node)
                        rendered_template = current_template.render(key_values[key_chain])
                        leaf_parent[current_key] = rendered_template
                        rendered_keys.append(key_chain)

        return render_template

    def change_step_input(self, new_input: str) -> str:
        self.change_variable(TemplateVariable.KEY_NAMES.INPUT_BASE, new_input)
        new_name = self.get_new_name(new_input.rpartition(':')[2], self.get_variable(TemplateVariable.KEY_NAMES.TASK_NAME),
                                self.task_template.tag)
        self.change_variable(TemplateVariable.KEY_NAMES.OUTPUT_BASE, new_name)
        self.change_variable(TemplateVariable.KEY_NAMES.TASK_NAME, f'{new_name}/')
        return new_name
    @staticmethod
    def get_new_name(dataset: str, old_name: str, tag: str):
        if '_tid' in dataset:
            dataset = dataset.split('_tid')[0]
        prefix = '.'.join(old_name.split('.')[:2])
        return prefix + '.' + dataset + '_' + tag + '.v00'

    class Meta:
        app_label = 'dev'
        db_table = '"T_AT_STEP_TEMPLATE"'


from pydantic import BaseModel, Field, ConfigDict


class MCWorkflowChanges(BaseModel):
    class ChangeType(str, Enum):
        SUBCAMPAIGN = 'subcampaign'
        PROJECT_BASE = 'project_base'
        DESCRIPTION = 'description'
        CAMPAIGN = 'campaign'

    value: str
    type: ChangeType
class MCWorkflowTransition(BaseModel):
    class TransitionType(str, Enum):
        VERTICAL = 'vertical'
        HORIZONTAL = 'horizontal'

    class SimulationType(str, Enum):
        FULLSIM = 'fullsim'
        FASTSIM = 'fastsim'
        FULLSIM_BYRELEASE = 'fullsim_byrelease'
        FASTSIM_BYRELEASE = 'fastsim_byrelease'


    new_request: str
    parent_step: str
    transition_type: TransitionType
    event_ratio: float = 0.0
    pattern: Dict[str, Any] = field(default_factory=dict)
    changes: List[MCWorkflowChanges] = field(default_factory=list)

class MCWorkflowSubCampaign(BaseModel):
    BASE_REQUEST :  Literal['evgen'] = 'evgen'

    campaign: str
    subcampaign: str
    project_base: str
    transitions: List[MCWorkflowTransition] = field(default_factory=list)
class MCWorkflowRequest(BaseModel):
    workflows: Dict[str, MCWorkflowSubCampaign] = field(default_factory=dict)

class SystemParametersHandler:
    class PARAMETERS_NAMES:
        DAOD_PHYS_Production = 'DAOD_PHYS_Production'
        MC_CAMPAGINS = 'MC_Campaigns'
        EXCLUDED_STAGING_SITES = 'ExcludedStagingSites'
        GRL_DEFAULT_FILE = 'GRLDefaultFile'
        MCSubCampaignStats = 'MCSubCampaignStats'
        MCWorkflowRequest = 'MCWorkflowRequest'
        ANALYSIS_REQUEST_EMAIL = 'AnalysisRequestEmail'



    @dataclass
    class DAOD_PHYS_Production:
        campaign: str
        subcampaign: str
        outputs: List[str]
        train_id: int
        status: str
        fullSimOnly: bool = False

        ALL_SUBCAMPAIGNS = 'all'

        class STATUS:
            ACTIVE = 'Active'
            DISABLED = 'Disabled'

    @dataclass
    class MC_Campaign:
        campaign: str
        subcampaigns: List[str]

    @dataclass
    class MCSubCampaignStats:
        campaign: str
        pile_suffix: str

    @dataclass
    class GRL_DEFAULT_FILE:
        file_by_project: Dict[str, str]

    @dataclass
    class ExcludedStagingSites:
        sites: List[str]

    @dataclass
    class AnalysisRequestEmail:
        emails: List[str]

    def __init__(self, name):
        self.name = name

    # Get and set methods for GRL_DEFAULT_FILE

    @staticmethod
    def get_mc_workflow_request() -> MCWorkflowRequest:
        return MCWorkflowRequest.model_validate(SystemParameters.get_parameter(SystemParametersHandler.PARAMETERS_NAMES.MCWorkflowRequest))

    @staticmethod
    def set_mc_workflow_request(values: MCWorkflowRequest):
        SystemParameters.set_parameter(SystemParametersHandler.PARAMETERS_NAMES.MCWorkflowRequest,
                                       values.model_dump())
    @staticmethod
    def get_grl_default_file() -> GRL_DEFAULT_FILE:
        values = SystemParameters.get_parameter(SystemParametersHandler.PARAMETERS_NAMES.GRL_DEFAULT_FILE)
        return SystemParametersHandler.GRL_DEFAULT_FILE(**values)

    @staticmethod
    def set_grl_default_file(values: GRL_DEFAULT_FILE):
        SystemParameters.set_parameter(SystemParametersHandler.PARAMETERS_NAMES.GRL_DEFAULT_FILE,
                                       asdict(values))

    # Get and set methods for DAOD_PHYS_Production
    @staticmethod
    def get_daod_phys_production() -> List[DAOD_PHYS_Production]:
        values = SystemParameters.get_parameter(SystemParametersHandler.PARAMETERS_NAMES.DAOD_PHYS_Production)
        return [SystemParametersHandler.DAOD_PHYS_Production(**x) for x in values]

    @staticmethod
    def set_daod_phys_production(values: [DAOD_PHYS_Production]):
        SystemParameters.set_parameter(SystemParametersHandler.PARAMETERS_NAMES.DAOD_PHYS_Production,
                                       [asdict(x) for x in values])


    @staticmethod
    def get_mc_sub_campaigns_stats() -> List[MCSubCampaignStats]:
        values = SystemParameters.get_parameter(SystemParametersHandler.PARAMETERS_NAMES.MCSubCampaignStats)
        return [SystemParametersHandler.MCSubCampaignStats(**x) for x in values]

    @staticmethod
    def set_mc_sub_campaigns_stats(values: [MCSubCampaignStats]):
        SystemParameters.set_parameter(SystemParametersHandler.PARAMETERS_NAMES.MCSubCampaignStats,
                                       [asdict(x) for x in values])
    @staticmethod
    def get_mc_campaigns() -> List[MC_Campaign]:
        values = SystemParameters.get_parameter(SystemParametersHandler.PARAMETERS_NAMES.MC_CAMPAGINS)
        return [SystemParametersHandler.MC_Campaign(**x) for x in values]

    @staticmethod
    def set_mc_campaigns(values: [MC_Campaign]):
        SystemParameters.set_parameter(SystemParametersHandler.PARAMETERS_NAMES.MC_CAMPAGINS,
                                       [asdict(x) for x in values])
    @staticmethod
    def get_excluded_staging_sites() -> ExcludedStagingSites:
        values = SystemParameters.get_parameter(SystemParametersHandler.PARAMETERS_NAMES.EXCLUDED_STAGING_SITES)
        return SystemParametersHandler.ExcludedStagingSites(**values)

    @staticmethod
    def set_excluded_staging_sites(values: ExcludedStagingSites):
        SystemParameters.set_parameter(SystemParametersHandler.PARAMETERS_NAMES.EXCLUDED_STAGING_SITES,
                                       asdict(values))

    @staticmethod
    def get_analysis_request_email() -> AnalysisRequestEmail:
        values = SystemParameters.get_parameter(SystemParametersHandler.PARAMETERS_NAMES.ANALYSIS_REQUEST_EMAIL)
        return SystemParametersHandler.AnalysisRequestEmail(**values)

    @staticmethod
    def set_analysis_request_email(values: AnalysisRequestEmail):
        SystemParameters.set_parameter(SystemParametersHandler.PARAMETERS_NAMES.ANALYSIS_REQUEST_EMAIL,
                                       asdict(values))

class SystemParameters(models.Model):



    name = models.CharField(max_length=128, db_column='NAME', primary_key=True)
    value = models.JSONField(db_column='value')
    typeName = models.CharField(max_length=128, db_column='TYPENAME')
    timestamp = models.DateTimeField(db_column='TIMESTAMP')
    cacheable = models.BooleanField(db_column='CACHEABLE')

    def save(self, *args, **kwargs):
        self.timestamp = timezone.now()
        super(SystemParameters, self).save(*args, **kwargs)

    def create_system_parameter(self, name, typeName, cacheable=True):
        self.name = name
        self.typeName = typeName
        self.cacheable = cacheable
        self.save()



    @staticmethod
    def get_parameter(name):
        if cache.get(name) is None:
            if SystemParameters.objects.filter(name=name).exists():
                cache.set(name, SystemParameters.objects.get(name=name).value)
                return cache.get(name)
            else:
                raise Exception("Parameter with name %s does not exist" % name)
        else:
            return cache.get(name)

    @staticmethod
    def set_parameter(name, value):
        if SystemParameters.objects.filter(name=name).exists():
            param = SystemParameters.objects.get(name=name)
            param.value = value
            param.save()
            if param.cacheable:
                cache.set(name, value, None)
        else:
            raise Exception("Parameter with name %s does not exist" % name)


    def __str__(self):
        return self.name


    class Meta:
        app_label = 'dev'
        db_table = '"T_DEFT_PARAMETERS"'

class ProductionTask(models.Model):


    class STATUS:
        WAITING = 'waiting'
        STAGING = 'staging'
        REGISTERED = 'registered'
        ASSIGNING = 'assigning'
        SUBMITTING = 'submitting'
        READY = 'ready'
        RUNNING = 'running'
        PAUSED = 'paused'
        EXHAUSTED = 'exhausted'
        DONE = 'done'
        FINISHED = 'finished'
        TORETRY = 'toretry'
        TOABORT = 'toabort'
        FAILED = 'failed'
        BROKEN = 'broken'
        ABORTED = 'aborted'
        OBSOLETE = 'obsolete'
        ABORTING = 'aborting'
        FINISHING = 'finishing'
        PENDING = 'pending'
        SCOUTING = 'scouting'


    ALL_STATUS = [STATUS.WAITING, STATUS.STAGING, STATUS.REGISTERED, STATUS.ASSIGNING, STATUS.SUBMITTING,
                    STATUS.READY, STATUS.RUNNING, STATUS.PAUSED, STATUS.EXHAUSTED, STATUS.DONE, STATUS.FINISHED,
                    STATUS.TORETRY, STATUS.TOABORT, STATUS.FAILED, STATUS.BROKEN, STATUS.ABORTED, STATUS.OBSOLETE]
    ALL_JEDI_STATUS = ALL_STATUS + [STATUS.FINISHING, STATUS.ABORTING, STATUS.PENDING, STATUS.SCOUTING]
    STATUS_ORDER = ['total'] + ALL_STATUS
    SYNC_STATUS = [STATUS.RUNNING, STATUS.REGISTERED, STATUS.PAUSED, STATUS.ASSIGNING, STATUS.TOABORT, STATUS.TORETRY,
                   STATUS.SUBMITTING, STATUS.READY, STATUS.EXHAUSTED, STATUS.WAITING, STATUS.STAGING]
    RED_STATUS = [STATUS.FAILED, STATUS.ABORTED, STATUS.BROKEN]
    NOT_RUNNING = RED_STATUS + [STATUS.FINISHED, STATUS.DONE, STATUS.OBSOLETE]
    OBSOLETE_READY_STATUS = [STATUS.FINISHED, STATUS.DONE, STATUS.OBSOLETE]
    BAD_STATUS = RED_STATUS + [STATUS.OBSOLETE, STATUS.ABORTING, STATUS.TOABORT]




    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASKID', primary_key=True)
    step = models.ForeignKey(StepExecution, db_column='STEP_ID', on_delete=CASCADE)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=CASCADE)
    parent_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID', null=False)
    chain_tid = models.DecimalField(decimal_places=0, max_digits=12, db_column='CHAIN_TID', null=False)
    name = models.CharField(max_length=130, db_column='TASKNAME', null=True)
    project = models.CharField(max_length=60, db_column='PROJECT', null=True)
    username = models.CharField(max_length=128, db_column='USERNAME', null=True)
    dsn = models.CharField(max_length=12, db_column='DSN', null=True)
    phys_short = models.CharField(max_length=80, db_column='PHYS_SHORT', null=True)
    simulation_type = models.CharField(max_length=20, db_column='SIMULATION_TYPE', null=True)
    vo = models.CharField(max_length=16, db_column='VO', null=True)
    prodSourceLabel = models.CharField(max_length=20, db_column='PRODSOURCELABEL', null=True)
    dynamic_jobdef = models.DecimalField(decimal_places=0, max_digits=1, db_column='DYNAMIC_JOB_DEFINITION', null=True)
    bug_report = models.DecimalField(decimal_places=0, max_digits=12, db_column='BUG_REPORT', null=False)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)
    provenance = models.CharField(max_length=12, db_column='PROVENANCE', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    total_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_EVENTS', null=True)
    total_req_events =  models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_REQ_EVENTS', null=True)
    total_req_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_REQ_JOBS', null=True)
    total_done_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_DONE_JOBS', null=True)
    submit_time = models.DateTimeField(db_column='SUBMIT_TIME', null=False)
    start_time = models.DateTimeField(db_column='START_TIME', null=True)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=True)
    pptimestamp = models.DateTimeField(db_column='PPTIMESTAMP', null=True)
    postproduction = models.CharField(max_length=128, db_column='POSTPRODUCTION', null=True)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=True)
    current_priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='CURRENT_PRIORITY', null=True)
    update_time = models.DateTimeField(db_column='UPDATE_TIME', null=True)
    update_owner = models.CharField(max_length=24, db_column='UPDATE_OWNER', null=True)
    comments = models.CharField(max_length=256, db_column='COMMENTS', null=True)
    inputdataset = models.CharField(max_length=150, db_column='INPUTDATASET', null=True)
    physics_tag = models.CharField(max_length=20, db_column='PHYSICS_TAG', null=True)
    reference = models.CharField(max_length=150, db_column='REFERENCE', null=False)
    campaign = models.CharField(max_length=32, db_column='CAMPAIGN', null=False, blank=True)
    jedi_info = models.CharField(max_length=256, db_column='JEDI_INFO', null=False, blank=True)
    total_files_failed = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILESFAILED', null=True)
    total_files_tobeused = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILESTOBEUSED', null=True)
    total_files_used = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILESUSED', null=True)
    total_files_onhold = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILESONHOLD', null=True)
    is_extension = models.BooleanField(db_column='IS_EXTENSION', null=True, blank=False)
    total_files_finished = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILESFINISHED', null=True)
    ttcr_timestamp = models.DateTimeField(db_column='TTCR_TIMESTAMP', null=True)
    ttcj_timestamp = models.DateTimeField(db_column='TTCJ_TIMESTAMP', null=True)
    ttcj_update_time = models.DateTimeField(db_column='TTCJ_UPDATE_TIME', null=True)
    primary_input = models.CharField(max_length=250, db_column='PRIMARY_INPUT', null=True)
    ami_tag = models.CharField(max_length=15, db_column='CTAG')
    output_formats = models.CharField(max_length=250, db_column='OUTPUT_FORMATS')
    pileup = models.DecimalField(decimal_places=0, max_digits=1, db_column='PILEUP', null=True)
    subcampaign = models.CharField(max_length=32, db_column='SUBCAMPAIGN', null=True)
    bunchspacing = models.CharField(max_length=32, db_column='BUNCHSPACING', null=True)

#    def save(self):
#         raise NotImplementedError

    @property
    def failure_rate(self):
        try:
            #rate = round(self.total_files_failed/self.total_files_tobeused*100,3);
            rate = self.total_files_failed/self.total_files_tobeused*100
            if rate == 0 or rate>=1:
                rate = int(rate)
            elif rate < .001:
                rate = .001
            elif rate < 1:
                rate = round(rate,3)
        except:
            return None
        return rate

    @property
    def input_dataset(self):
        try:
            dataset = TTask.objects.get(id=self.id).input_dataset
        except:
            return ""
        return dataset

    @property
    def output_dataset(self):
        try:
            dataset = TTask.objects.get(id=self.id).output_dataset
        except:
            return ""
        return dataset

    @property
    def hashtags(self):
        return get_hashtags_by_task(int(self.id))

    def hashtag_exists(self, hashtag):
        return task_hashtag_exists(int(self.id),hashtag)

    def set_hashtag(self, hashtag):
        set_hashtag(hashtag, [int(self.id)])

    def remove_hashtag(self, hashtag):
        remove_hashtag_from_task(int(self.id), hashtag)

    def output_non_log_datasets(self):
        for dataset in ProductionDataset.objects.filter(task_id=self.id):
            if '.log.' not in dataset.name.lower():
                yield dataset.name

    class Meta:
        #db_table = u'T_PRODUCTION_STEP'
        db_table = 'T_PRODUCTION_TASK'




class OpenEndedRequest(models.Model):

    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='OE_ID', primary_key=True)
    status = models.CharField(max_length=20, db_column='STATUS', null=True)
    request  = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=CASCADE)
    container = models.CharField(max_length=150, db_column='CONTAINER',null=False)
    last_update = models.DateTimeField(db_column='LAST_UPDATE')


    def save(self, *args, **kwargs):
        if not self.last_update:
            self.last_update = timezone.now()
        if not self.id:
            self.id = prefetch_id('deft','T_OPEN_ENDED_ID_SEQ',"T_OPEN_ENDED",'OE_ID')
        super(OpenEndedRequest, self).save(*args, **kwargs)

    def save_last_update(self, *args, **kwargs):
        self.last_update = timezone.now()
        super(OpenEndedRequest, self).save(*args, **kwargs)

    class Meta:
        db_table = '"T_OPEN_ENDED"'


class HashTag(models.Model):
    HASHTAG_TYPE = (
                ('UD', 'User defined'),
                ('KW', 'Key word'),
                )

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='HT_ID', primary_key=True)
    hashtag = models.CharField(max_length=80, db_column='HASHTAG')
    type = models.CharField(max_length=2, db_column='TYPE', choices=HASHTAG_TYPE)


    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','ATLAS_DEFT.T_HASHTAG_ID_SEQ',"T_HASHTAG",'HT_ID')
        super(HashTag, self).save(*args, **kwargs)

    @property
    def tasks_ids(self):
        return get_tasks_by_hashtag(self)

    @property
    def tasks(self):
        return ProductionTask.objects.filter(id__in=get_tasks_by_hashtag(self))

    @property
    def tasks_count(self):
        return count_tasks_by_hashtag(self)

    @property
    def last_task(self):
        return last_task_for_hashtag(self)

    def __str__(self):
        return self.hashtag

    class Meta:
        db_table = '"ATLAS_DEFT"."T_HASHTAG"'





class HashTagToTask(models.Model):
    task = models.ForeignKey(ProductionTask,  db_column='TASKID', on_delete=CASCADE)
    hashtag = models.ForeignKey(HashTag, db_column='HT_ID', on_delete=CASCADE)

    def save(self, *args, **kwargs):
        raise NotImplementedError('Only manual creation')

    def create_relation(self):
        print((self._meta.db_table))

    class Meta:
        db_table = '"ATLAS_DEFT"."T_HT_TO_TASK"'



def last_task_for_hashtag(hashtag):
    hashtag_id = HashTag.objects.get(hashtag=hashtag).id
    cursor = None
    last = None
    try:
        cursor = connections['deft'].cursor()
        cursor.execute(f"SELECT TASKID from {HashTagToTask._meta.db_table} WHERE HT_ID=%s AND  ROWNUM<=1 ORDER BY TASKID ASC",[hashtag_id])
        result = cursor.fetchall()
        last = result[0][0]
    finally:
        if cursor:
            cursor.close()
    return last

def set_hashtag(hashtag, tasks):
    """

    :param hashtag: Hashtag object or hashtag id
    :param task: Tasks ids
    :return:
    """
    if type(hashtag) == int:
        hashtag = HashTag.objects.get(id=hashtag)
    else:
        hashtag = HashTag.objects.get(hashtag=hashtag)
    hashtag_id = hashtag.id
    cursor = None
    try:
        cursor = connections['deft'].cursor()
        for task in tasks:
            if type(task) != int:
                raise ValueError('Wrong task type')
            cursor.execute(f"insert into {HashTagToTask._meta.db_table} (HT_ID,TASKID) values(%s, %s)",(hashtag_id,task))
    finally:
        if cursor:
            cursor.close()


def count_tasks_by_hashtag(hashtag):
    hashtag_id = HashTag.objects.get(hashtag=hashtag).id
    cursor = None
    total = 0
    try:
        cursor = connections['deft'].cursor()
        cursor.execute(f"SELECT COUNT(TASKID) from {HashTagToTask._meta.db_table} WHERE HT_ID=%s",[hashtag_id])
        result = cursor.fetchall()
        total = result[0][0]
    finally:
        if cursor:
            cursor.close()
    return total

def task_hashtag_exists(task_id,hashtag):
    hashtag_id = HashTag.objects.get(hashtag=hashtag).id
    exists = False
    cursor = None
    try:
        cursor = connections['deft'].cursor()
        cursor.execute(f"SELECT TASKID,HT_ID from {HashTagToTask._meta.db_table} WHERE HT_ID=%s AND TASKID=%s",(hashtag_id,task_id))
        result = cursor.fetchall()
        if result:
            exists = True
    finally:
        if cursor:
            cursor.close()
    return exists

def get_tasks_by_hashtag(hashtag):
    hashtag_id = HashTag.objects.get(hashtag=hashtag).id
    cursor = None
    try:
        cursor = connections['deft'].cursor()
        cursor.execute(f"SELECT TASKID from {HashTagToTask._meta.db_table} WHERE HT_ID=%s",[hashtag_id])
        tasks = cursor.fetchall()
    finally:
        if cursor:
            cursor.close()
    return [x[0] for x in tasks]

def get_hashtags_by_task(task_id):
    cursor = None
    try:
        cursor = connections['deft'].cursor()
        cursor.execute(f"SELECT HT_ID from {HashTagToTask._meta.db_table} WHERE TASKID=%s",[task_id])
        hashtags_id = cursor.fetchall()
    finally:
        if cursor:
            cursor.close()
    hashtags = [HashTag.objects.get(id=x[0]) for x in hashtags_id]
    return hashtags

def in_statement_list(input_list: List[Any]) -> str:
    return f"({','.join(['%s']*len(input_list))})"

def get_bulk_hashtags_by_task(task_ids: [int]) -> Dict[int, List[str]]:
     # split tasks by chunks of 1000
    tasks = [task_ids[i:i + 1000] for i in range(0, len(task_ids), 1000)]
    result = {}
    cursor = None
    ht_id_task_id = []
    try:
        cursor = connections['deft'].cursor()
        for task_chunk in tasks:
            cursor.execute(f"SELECT TASKID, HT_ID from {HashTagToTask._meta.db_table} WHERE TASKID IN {in_statement_list(task_chunk)}",
                           task_chunk)
            ht_id_task_id += cursor.fetchall()
    finally:
        if cursor:
            cursor.close()
    known_ht_ids = set([x[1] for x in ht_id_task_id])
    ht = {x.id:x.hashtag for x in HashTag.objects.filter(id__in=known_ht_ids)}
    for task_id, ht_id in ht_id_task_id:
        result[task_id] = result.get(task_id,[]) + [ht[ht_id]]
    return result

def remove_hashtag_from_task(task_id, hashtag):
    hashtag_id = HashTag.objects.get(hashtag=hashtag).id
    cursor = None
    deleted = False
    try:
        cursor = connections['deft'].cursor()
        cursor.execute(f"SELECT TASKID,HT_ID from {HashTagToTask._meta.db_table} WHERE HT_ID=%s AND TASKID=%s",(hashtag_id,task_id))
        result = cursor.fetchall()
        if result:
            cursor.execute(f"DELETE FROM  {HashTagToTask._meta.db_table} WHERE HT_ID=%s AND TASKID=%s",(hashtag_id,task_id))
            deleted = True
    finally:
        if cursor:
            cursor.close()
    return deleted


class StepAction(models.Model):

    class STATUS:
        ACTIVE = 'active'
        FAILED = 'failed'
        DONE = 'done'
        EXECUTING = 'executing'
        CANCELED = 'canceled'

    ACTIVE_STATUS = [STATUS.ACTIVE, STATUS.EXECUTING]

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='STEP_ACTION_ID', primary_key=True)
    request = models.ForeignKey(TRequest,  db_column='PR_ID', on_delete=CASCADE)
    step = models.DecimalField(decimal_places=0, max_digits=12, db_column='STEP_ID')
    action = models.DecimalField(decimal_places=0, max_digits=12, db_column='ACTION_TYPE')
    create_time = models.DateTimeField(db_column='SUBMIT_TIME')
    execution_time = models.DateTimeField(db_column='EXEC_TIME')
    done_time = models.DateTimeField(db_column='DONE_TIME')
    message = models.CharField(max_length=2000, db_column='MESSAGE')
    attempt = models.DecimalField(decimal_places=0, max_digits=12, db_column='ATTEMPT')
    status = models.CharField(max_length=20, db_column='STATUS', null=True)
    config = models.CharField(max_length=2000, db_column='CONFIG')

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','T_STEP_ACTION_SQ',"T_STEP_ACTION",'STEP_ACTION_ID')
        super(StepAction, self).save(*args, **kwargs)

    def set_config(self, update_dict):
        if not self.config:
            self.config = ''
            currrent_dict = {}
        else:
            currrent_dict = json.loads(self.config)
        currrent_dict.update(update_dict)
        self.config = json.dumps(currrent_dict)

    def remove_config(self, key):
        if self.config:
            currrent_dict = json.loads(self.config)
            if key in currrent_dict:
                currrent_dict.pop(key)
                self.config = json.dumps(currrent_dict)

    def get_config(self, field = None):
        return_dict = {}
        try:
            return_dict = json.loads(self.config)
        except:
            pass
        if field:
            return return_dict.get(field,None)
        else:
            return return_dict


    class Meta:
        db_table = '"T_STEP_ACTION"'


class DatasetStaging(models.Model):

    ACTIVE_STATUS = ['queued','staging']

    class STATUS:
        QUEUED = 'queued'
        STAGING = 'staging'
        CANCELED = 'canceled'
        DONE = 'done'

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='DATASET_STAGING_ID', primary_key=True)
    dataset = models.CharField(max_length=255, db_column='DATASET', null=True)
    start_time = models.DateTimeField(db_column='START_TIME')
    end_time = models.DateTimeField(db_column='END_TIME')
    rse = models.CharField(max_length=100, db_column='RSE')
    total_files = models.DecimalField(decimal_places=0, max_digits=12, db_column='TOTAL_FILES')
    staged_files = models.DecimalField(decimal_places=0, max_digits=12, db_column='STAGED_FILES')
    status = models.CharField(max_length=20, db_column='STATUS', null=True)
    source = models.CharField(max_length=200, db_column='SOURCE_RSE', null=True)
    update_time = models.DateTimeField(db_column='UPDATE_TIME')
    dataset_size = models.DecimalField(decimal_places=0, max_digits=20, db_column='DATASET_BYTES', null=True)
    staged_size = models.DecimalField(decimal_places=0, max_digits=20, db_column='STAGED_BYTES', null=True)
    source_expression = models.CharField(max_length=400, db_column='SOURCE_EXPRESSION', null=True)
    destination_rse = models.CharField(max_length=200, db_column='DESTINATION_RSE', null=True)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','T_DATASET_STAGING_SEQ',"T_DATASET_STAGING",'DATASET_STAGING_ID')
        super(DatasetStaging, self).save(*args, **kwargs)

    def active_tasks(self):
        for action in ActionStaging.objects.filter(dataset_stage=self):
            task = ProductionTask.objects.get(id=action.task)
            if task.status not in ProductionTask.NOT_RUNNING:
                yield task.id

    class Meta:
        db_table = '"T_DATASET_STAGING"'

class ActionStaging(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='ACT_ST_ID', primary_key=True)
    step_action = models.ForeignKey(StepAction, db_column='STEP_ACTION_ID' , on_delete=CASCADE)
    dataset_stage = models.ForeignKey(DatasetStaging, db_column='DATASET_STAGING_ID', on_delete=CASCADE)
    task = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASKID')
    share_name = models.CharField(max_length=100, db_column='SHARE_NAME')


    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','ACTION_STAGING_SEQ','T_ACTION_STAGING','ACT_ST_ID')
        super(ActionStaging, self).save(*args, **kwargs)



    class Meta:
        db_table = '"T_ACTION_STAGING"'

class ActionDefault(models.Model):

    ACTION_NAME_TYPE = {'postpone':1,'check2rep':2, 'checkEvgen':3, 'preStage':5, 'preStageWithTask':5,'activateStaging':6,
                        'followStaging':7,'preStageWithTaskArchive':8,
                        'followArchive': 9,'followRepeated':10,'empty':11, 'disableIDDS': 12}
    FILES_TO_RELEASE = 800

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='ACT_DEFAULT_ID', primary_key=True)
    name = models.CharField(max_length=30, db_column='NAME', null=True)
    type =  models.CharField(max_length=20, db_column='TYPE', null=True)
    config = models.CharField(max_length=2000, db_column='CONFIG')
    timestamp = models.DateTimeField(db_column='TIMESTAMP')


    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','ACTION_DEFAULT_CONFIG_SEQ','"ATLAS_DEFT"."T_ACTION_DEFAULT_CONFIG"','ACT_DEFAULT_ID')
        super(ActionDefault, self).save(*args, **kwargs)

    def set_config(self, update_dict):
        if not self.config:
            self.config = ''
            currrent_dict = {}
        else:
            currrent_dict = json.loads(self.config)
        currrent_dict.update(update_dict)
        self.config = json.dumps(currrent_dict)

    def remove_config(self, key):
        if self.config:
            currrent_dict = json.loads(self.config)
            if key in currrent_dict:
                currrent_dict.pop(key)
                self.config = json.dumps(currrent_dict)

    def get_config(self, field = None):
        return_dict = {}
        try:
            return_dict = json.loads(self.config)
        except:
            pass
        if field:
            return return_dict.get(field,None)
        else:
            return return_dict



    class Meta:
        db_table = '"ATLAS_DEFT"."T_ACTION_DEFAULT_CONFIG"'



class GroupProductionDeletionRequest(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='GPDR_ID', primary_key=True)
    username = models.CharField(max_length=30, db_column='USERNAME', null=True)
    timestamp = models.DateTimeField(db_column='LAST_UPDATE')
    deadline = models.DateTimeField(db_column='CUT_OFF')
    start_deletion = models.DateTimeField(db_column='DELETION_START')
    containers = models.DecimalField(decimal_places=0, max_digits=20, db_column='CONTAINERS')
    size = models.DecimalField(decimal_places=0, max_digits=20, db_column='BYTES')
    status = models.CharField(max_length=20, db_column='STATUS',null=False)


    def save(self, *args, **kwargs):
        if not self.timestamp:
            self.timestamp = timezone.now()
        if not self.id:
            self.id = prefetch_id('deft','T_GPDR_SEQ',"T_GP_DELETION_REQUEST",'GPDR_ID')
        super(GroupProductionDeletionRequest, self).save(*args, **kwargs)



    class Meta:
        db_table = '"T_GP_DELETION_REQUEST"'


class StorageResource(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='SRS_ID', primary_key=True)
    name = models.CharField(max_length=30, db_column='DATASET', null=True)
    timestamp = models.DateTimeField(db_column='LAST_UPDATE')
    end_time = models.DateTimeField(db_column='END_TIME')
    state = models.CharField(max_length=2000, db_column='STATE')


    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('dev_db','T_SRS_SEQ',"T_STORAGE_RESOURCE_STATE",'SRS_ID')
        super(StorageResource, self).save(*args, **kwargs)



    class Meta:
        app_label = 'dev'
        db_table = '"T_STORAGE_RESOURCE_STATE"'

class IAM_USER(models.Model):
    username = models.CharField(max_length=30, db_column='USERNAME', primary_key=True)
    userID = models.CharField(max_length=50, db_column='USERID', null=False)

    class Meta:
        app_label = 'dev'
        db_table = '"T_IAM_USERS"'

class MultiCampaignRequestBase(models.Model):

    class STATUS:
        NOT_DEFINED = 'not_defined'
        FILLED = 'filled'




    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='MCRB_ID', primary_key=True)
    value = models.JSONField(db_column='value')
    production_request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=CASCADE)
    timestamp = models.DateTimeField(db_column='LAST_UPDATE')
    username = models.CharField(max_length=30, db_column='USERNAME', primary_key=True)
    status = models.CharField(max_length=20, db_column='STATUS',null=False)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('dev_db','T_MCRB_SEQ',"T_MULTI_CAMPAIGN_REQ",'MCRB_ID')
        self.timestamp = timezone.now()
        super(MultiCampaignRequestBase, self).save(*args, **kwargs)

    @property
    def campaigns_ratio(self):
        return  self.value.get('campaigns_ratio',None)

    @campaigns_ratio.setter
    def campaigns_ratio(self, value: dict[str,float]):
        self.value = {'campaigns_ratio':value}

    class Meta:
        app_label = 'dev'
        db_table = '"T_MULTI_CAMPAIGN_REQ"'

class ETAGRelease(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TAG_RELASE_ID', primary_key=True)
    ami_tag = models.CharField(max_length=20, db_column='AMI_TAG')
    timestamp = models.DateTimeField(db_column='TIMESTAMP')
    sw_release = models.CharField(max_length=200, db_column='SOFTWARE_RELEASE')


    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','T_ETR_SEQ',"T_ETAG_RELEASE",'TAG_RELEASE_ID')
        super(ETAGRelease, self).save(*args, **kwargs)



    class Meta:
        db_table = '"T_ETAG_RELEASE"'


class GroupProductionAMITag(models.Model):

    ami_tag = models.CharField(max_length=10, db_column='AMI_TAG', primary_key=True)
    status = models.CharField(max_length=20, db_column='STATUS',null=False)
    skim = models.CharField(max_length=1, db_column='SKIM', null=False)
    real_data = models.BooleanField(db_column='IS_REAL_DATA', null=True, blank=False)
    cache = models.CharField(max_length=100, db_column='CACHE',null=True)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=False)
    comment = models.CharField(max_length=1000, db_column='TAG_COMMENT')

    def save(self, *args, **kwargs):
        if not self.timestamp:
            self.timestamp = timezone.now()
        super(GroupProductionAMITag, self).save(*args, **kwargs)


    class Meta:
        db_table = '"T_GP_AMI_TAG"'


class GroupProductionStats(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='GP_STATS_ID', primary_key=True)
    ami_tag = models.CharField(max_length=10, db_column='AMI_TAG')
    output_format = models.CharField(max_length=20, db_column='OUTPUT_FORMAT', null=False)
    real_data = models.BooleanField(db_column='IS_REAL_DATA', null=True, blank=False)
    size = models.DecimalField(decimal_places=0, max_digits=20, db_column='BYTES')
    containers = models.DecimalField(decimal_places=0, max_digits=20, db_column='CONTAINERS')
    to_delete_containers =  models.DecimalField(decimal_places=0, max_digits=20, db_column='TD_CONTAINERS')
    to_delete_size =  models.DecimalField(decimal_places=0, max_digits=20, db_column='TD_BYTES')
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=False)

    def save(self, *args, **kwargs):
        if not self.timestamp:
            self.timestamp = timezone.now()
        if not self.id:
            self.id = prefetch_id('deft','T_GP_STATS_SEQ',"T_GP_STATS",'GP_STATS_ID')
        super(GroupProductionStats, self).save(*args, **kwargs)


    class Meta:
        db_table = '"T_GP_STATS"'


class GroupProductionDeletion(models.Model):

    EXTENSIONS_DAYS = 60
    LIFE_TIME_DAYS = 60

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='GP_DELETION_ID', primary_key=True)
    container = models.CharField(max_length=255, db_column='CONTAINER', null=False)
    dsid = models.DecimalField(decimal_places=0, max_digits=12, db_column='DSID', null=False)
    output_format = models.CharField(max_length=20, db_column='OUTPUT_FORMAT', null=False)
    update_time = models.DateTimeField(db_column='UPDATE_TIMESTAMP', null=False)
    version = models.DecimalField(decimal_places=0, max_digits=12, db_column='VERSION', null=False)
    status = models.CharField(max_length=20, db_column='STATUS',null=False)
    size = models.DecimalField(decimal_places=0, max_digits=20, db_column='BYTES')
    skim = models.CharField(max_length=1, db_column='SKIM', null=False)
    input_key = models.CharField(max_length=200, db_column='INPUT_KEY', null=False)
    datasets_number = models.DecimalField(decimal_places=0, max_digits=12, db_column='DATASETS', null=False)
    events = models.DecimalField(decimal_places=0, max_digits=12, db_column='EVENTS', null=False)
    available_tags = models.CharField(max_length=200, db_column='AVALIABLE_TAGS', null=True)
    previous_container = models.ForeignKey('self', db_column='PREVIOUS_CONTAINER_ID', on_delete=CASCADE, null=True)
    extensions_number = models.DecimalField(decimal_places=0, max_digits=12, db_column='EXTNSIONS', null=True)
    last_extension_time = models.DateTimeField(db_column='EXTENSION_TIME', null=True)
    ami_tag = models.CharField(max_length=10, db_column='AMI_TAG')

    @property
    def days_to_delete(self):
        extensions_number = self.extensions_number
        if not extensions_number:
            extensions_number = 0
        if not self.last_extension_time:
            return self.LIFE_TIME_DAYS
        return (self.last_extension_time - timezone.now()).days + extensions_number * self.EXTENSIONS_DAYS + self.LIFE_TIME_DAYS

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','T_GP_DELETION_SEQ',"T_GP_DELETION",'GP_DELETION_ID')
        super(GroupProductionDeletion, self).save(*args, **kwargs)


    class Meta:
        db_table = '"T_GP_DELETION"'


class GroupProductionDeletionExtension(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='GP_DELETION_EXT_ID', primary_key=True)
    container = models.ForeignKey(GroupProductionDeletion, db_column='DELETION_CONTAINER_ID', on_delete=CASCADE)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=False)
    user = models.CharField(max_length=200, db_column='USER_NAME')
    message  = models.CharField(max_length=256, db_column='MESSAGE', null=True)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','T_GP_DELETION_EXT_SEQ',"T_GP_DELETION_EXT",'GP_DELETION_EXT_ID')
        super(GroupProductionDeletionExtension, self).save(*args, **kwargs)


    class Meta:
        db_table = '"T_GP_DELETION_EXT"'


class SliceError(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='SLICE_ERROR_ID', primary_key=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID', on_delete=CASCADE)
    slice = models.ForeignKey(InputRequestList, db_column='SLICE_ID', null=False, on_delete=CASCADE)
    exception_type = models.CharField(max_length=50, db_column='EXCEPTION')
    message  = models.CharField(max_length=2000, db_column='MESSAGE', null=True)
    timestamp = models.DateTimeField(db_column='UPDATE_TIME', null=False)
    exception_time = models.DateTimeField(db_column='EXCEPTION_TIME', null=False)
    is_active = models.BooleanField(db_column='IS_ACTIVE')


    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','T_SLICE_ERROR_SEQ',"T_SLICE_ERROR",'SLICE_ERROR_ID')
        self.timestamp = timezone.now()
        super(SliceError, self).save(*args, **kwargs)


    class Meta:
        db_table = '"T_SLICE_ERROR"'

class GroupProductionDeletionProcessing(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='GP_D_P_ID', primary_key=True)
    container = models.CharField(max_length=255, db_column='CONTAINER', null=False)
    timestamp = models.DateTimeField(db_column='UPDATE_TIMESTAMP', null=False)
    status = models.CharField(max_length=20, db_column='STATUS',null=False)
    deleted_datasets = models.DecimalField(decimal_places=0, max_digits=12, db_column='DELETED_DATASETS', null=True)
    command_timestamp = models.DateTimeField(db_column='COMMAND_TIMESTAMP',  null=True)

    def save(self, *args, **kwargs):
        self.timestamp = timezone.now()
        if not self.id:
            self.id = prefetch_id('deft','T_GP_DEL_P_SEQ',"T_GP_DELETION_PROC",'GP_D_P_ID')
        super(GroupProductionDeletionProcessing, self).save(*args, **kwargs)


    class Meta:
        db_table = '"T_GP_DELETION_PROC"'


class WaitingStep(models.Model):

    ACTIONS = {
        1 : {'name':'postpone', 'description': 'Postpone ', 'attempts': 3, 'delay':1},
        2 : {'name': 'check2rep', 'description': 'Check that 2 replicas are done ', 'attempts': 200, 'delay':1},
        3: {'name': 'checkEvgen', 'description': 'Check that evgen is > 50% done ', 'attempts': 90, 'delay':1},
        4: {'name': 'preStage', 'description': 'Check that dataset is pre-staged and do if not', 'attempts': 900, 'delay':1},
        5: {'name': 'preStageWithTask','description': 'Check that dataset is pre-staged and do if not', 'attempts': 900, 'delay':1},
        8: {'name': 'preStageWithTaskArchive', 'description': 'Check that archive exists and pre-staged it',
            'attempts': 900, 'delay': 1}
    }


    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='WSTEP_ID', primary_key=True)
    request = models.ForeignKey(TRequest,  db_column='PR_ID', on_delete=CASCADE)
    step = models.DecimalField(decimal_places=0, max_digits=12, db_column='STEP_ID')#models.ForeignKey(StepExecution, db_column='STEP_ID')
    action = models.DecimalField(decimal_places=0, max_digits=12, db_column='TYPE')
    create_time = models.DateTimeField(db_column='SUBMIT_TIME')
    execution_time = models.DateTimeField(db_column='EXEC_TIME')
    done_time = models.DateTimeField(db_column='DONE_TIME')
    message = models.CharField(max_length=2000, db_column='MESSAGE')
    attempt = models.DecimalField(decimal_places=0, max_digits=12, db_column='ATTEMPT')
    status = models.CharField(max_length=20, db_column='STATUS', null=True)
    config = models.CharField(max_length=2000, db_column='CONFIG')

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','T_WAITING_STEP_SEQ',"T_WAITING_STEP",'HTTR_ID')
        super(WaitingStep, self).save(*args, **kwargs)

    def set_config(self, update_dict):
        if not self.config:
            self.config = ''
            currrent_dict = {}
        else:
            currrent_dict = json.loads(self.config)
        currrent_dict.update(update_dict)
        self.config = json.dumps(currrent_dict)

    def remove_config(self, key):
        if self.config:
            currrent_dict = json.loads(self.config)
            if key in currrent_dict:
                currrent_dict.pop(key)
                self.config = json.dumps(currrent_dict)

    def get_config(self, field = None):
        return_dict = {}
        try:
            return_dict = json.loads(self.config)
        except:
            pass
        if field:
            return return_dict.get(field,None)
        else:
            return return_dict


    class Meta:
        app_label = 'dev'
        db_table = '"T_WAITING_STEP"'

class HashTagToRequest(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='HTTR_ID', primary_key=True)
    request = models.ForeignKey(TRequest,  db_column='PR_ID', on_delete=CASCADE)
    hashtag = models.ForeignKey(HashTag, db_column='HT_ID', on_delete=CASCADE)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','T_HT_TO_REQUEST_SEQ',"T_HT_TO_REQUEST",'HTTR_ID')
        super(HashTagToRequest, self).save(*args, **kwargs)

    def save_last_update(self, *args, **kwargs):
        self.last_update = timezone.now()
        super(HashTagToRequest, self).save(*args, **kwargs)


    class Meta:
        db_table = '"T_HT_TO_REQUEST"'



class TrainProductionLoad(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TC_ID', primary_key=True)
    train = models.ForeignKey(TrainProduction,db_column='TRAIN_NUMBER', null=False, on_delete=CASCADE)
    group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=False, choices=TRequest.PHYS_GROUPS)
    datasets = models.TextField( db_column='DATASETS')
    timestamp = models.DateTimeField(db_column='TIMESTAMP')
    #output_formats = models.CharField(max_length=250, db_column='OUTPUT_FORMATS', null=True)
    manager = models.CharField(max_length=32, db_column='MANAGER', null=False, blank=True)
    outputs = models.TextField( db_column='OUTPUTS')

    def save(self, *args, **kwargs):
        self.timestamp = timezone.now()
        try:
            input_datasets = self.datasets.replace('\n',',').replace('\r',',').replace('\t',',').replace(' ',',').split(',')
            cleared_datasets_set = set()
            if input_datasets == ['ongoing_transaction']:
                cleared_datasets_set.add('bad_value')
            for dataset in input_datasets:
                if dataset:
                    if ':' not in dataset:
                        if dataset.find('.')>-1:
                            cleared_datasets_set.add(dataset[:dataset.find('.')]+':'+dataset)

                    else:
                      cleared_datasets_set.add(dataset)
            cleared_datasets = list(cleared_datasets_set)
            self.datasets = '\n'.join([x for x in cleared_datasets if x])
        except Exception as e:
            self.datasets=''
            _logger.debug('Problem this loads datastes: %s',str(e))
        if not self.id:
            self.id = prefetch_id('deft','T_TRAIN_CARRIAGE_ID_SEQ',"T_TRAIN_CARRIAGE",'TC_ID')

        super(TrainProductionLoad, self).save(*args, **kwargs)

    class Meta:
        db_table = "T_TRAIN_CARRIAGE"

class MCPattern(models.Model):
    STEPS = MC_STEPS
    STATUS = [(x,x) for x in ['IN USE','Obsolete']]
    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='MCP_ID', primary_key=True)
    pattern_name =  models.CharField(max_length=150, db_column='PATTERN_NAME', unique=True)
    pattern_dict = models.CharField(max_length=2000, db_column='PATTERN_DICT')
    pattern_status = models.CharField(max_length=20, db_column='PATTERN_STATUS', choices=STATUS)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft','ATLAS_DEFT.T_PRODUCTION_MCP_ID_SEQ','T_PRODUCTION_MC_PATTERN','MCP_ID')
        super(MCPattern, self).save(*args, **kwargs)

    class Meta:
        db_table = 'T_PRODUCTION_MC_PATTERN'





class MCPriority(models.Model):
    STEPS = ['Evgen',
             'Evgen Merge',
             'Simul',
             'Simul(Fast)',
             'Merge',
             'Digi',
             'Reco',
             'Rec Merge',
             'Atlfast',
             'Atlf Merge',
             'TAG',
             'Deriv',
             'Deriv Merge']
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='MCPRIOR_ID', primary_key=True)
    priority_key = models.DecimalField(decimal_places=0, max_digits=12, db_column='PRIORITY_KEY', unique=True)
    priority_dict = models.CharField(max_length=2000, db_column='PRIORITY_DICT')

    def save(self, *args, **kwargs):
        if self.priority_key == -1:
            return
        if not self.id:
            self.id = prefetch_id('deft','ATLAS_DEFT.T_PRODUCTION_MCPRIOR_ID_SEQ','T_PRODUCTION_MC_PRIORITY','MCPRIOR_ID')
        super(MCPriority, self).save(*args, **kwargs)

    def priority(self, step, tag):
        priority_py_dict = json.loads(self.priority_dict)
        if step == 'Simul' and tag[0] == 'a':
            step = 'Simul(Fast)'
        if step in priority_py_dict:
            return priority_py_dict[step]
        else:
            raise LookupError('No step %s in priority dict' % step)


    class Meta:
        db_table = 'T_PRODUCTION_MC_PRIORITY'




def get_priority_object(priority_key):
    try:
        mcp = MCPriority.objects.get(priority_key=priority_key)
    except ObjectDoesNotExist:
        priority_py_dict = {}
        for step in MCPriority.STEPS:
            priority_py_dict.update({step:int(priority_key)})
        mcp=MCPriority.objects.create(priority_key=-1,priority_dict=json.dumps(priority_py_dict))
    except Exception as e:
        raise e
    return mcp


def get_default_nEventsPerJob_dict(version='2.0'):
    if version == '2.0':
        defult_dict = {
            'Evgen':5000,
            'Evgen Merge':10000,
            'Simul':100,
            'Merge':1000,
            'Digi':500,
            'Reco':500,
            'Rec Merge':5000,
            'Rec TAG':25000,
            'Atlfast':500,
            'Atlf Merge':5000,
            'TAG':25000,
            'Deriv':100000,
            'Deriv Merge':5000000
        }
        return defult_dict
    if version == '3.0':
        defult_dict = {
            'Evgen': 10000,
            'Evgen Merge': 10000,
            'Simul': 100,
            'Merge': 1000,
            'Digi': 500,
            'Reco': 500,
            'Rec Merge': 5000,
            'Rec TAG': 25000,
            'Atlfast': 500,
            'Atlf Merge': 5000,
            'TAG': 25000,
            'Deriv': 100000,
            'Deriv Merge': 5000000
        }
        return defult_dict


def get_default_project_mode_dict():
    default_dict = {
         'Evgen':'spacetoken=ATLASDATADISK',
         'Evgen Merge':'spacetoken=ATLASDATADISK',
         'Simul':'spacetoken=ATLASDATADISK',
         'Merge':'spacetoken=ATLASDATADISK',
         'Digi':'Npileup=5;spacetoken=ATLASDATADISK',
         'Reco':'Npileup=5;spacetoken=ATLASDATADISK',
         'Rec Merge':'spacetoken=ATLASDATADISK',
         'Atlfast':'Npileup=5;spacetoken=ATLASDATADISK',
         'Atlf Merge':'spacetoken=ATLASDATADISK',
         'TAG':'spacetoken=ATLASDATADISK',
         'Deriv':'spacetoken=ATLASDATADISK',
         'Deriv Merge':'spacetoken=ATLASDATADISK'
    }
    return default_dict

class Site(models.Model):
    site_name = models.CharField(max_length=52, db_column='SITE_NAME', primary_key=True)
    role = models.CharField(max_length=256, db_column='ROLE')

    def save(self, *args, **kwargs):
        raise NotImplementedError('Only manual creation')

    class Meta:
        app_label = 'panda'
        db_table = '"ATLAS_PANDA"."SITE"'

class GDPConfig(models.Model):
    app = models.CharField(max_length=64, db_column='APP', primary_key=True)
    component = models.CharField(max_length=64, db_column='COMPONENT')
    key = models.CharField(max_length=64, db_column='KEY')
    vo = models.CharField(max_length=16, db_column='VO')
    value = models.CharField(max_length=256, db_column='VALUE')
    type = models.CharField(max_length=64, db_column='TYPE')
    descr = models.CharField(max_length=256, db_column='DESCR')

    #def save(self, *args, **kwargs):
    #    raise NotImplementedError('Only manual creation')

    class Meta:
        unique_together = (('app', 'component' , 'key' , 'vo'),)
        app_label = 'panda'
        db_table = '"ATLAS_PANDA"."CONFIG"'


class GlobalShare(models.Model):
    name = models.CharField(max_length=32, db_column='NAME', primary_key=True)
    value = models.DecimalField(decimal_places=0, max_digits=3, db_column='VALUE')
    parent = models.CharField (max_length=32, db_column='PARENT')

    def save(self, *args, **kwargs):
        raise NotImplementedError('Only manual creation')

    class Meta:
        app_label = 'panda'
        db_table = '"ATLAS_PANDA"."GLOBAL_SHARES"'
#        db_table = u"GLOBAL_SHARES"



class Cloudconfig(models.Model):
    name = models.CharField(max_length=20, primary_key=True, db_column='NAME') # Field name made lowercase.
    description = models.CharField(max_length=50, db_column='DESCRIPTION') # Field name made lowercase.
    tier1 = models.CharField(max_length=20, db_column='TIER1') # Field name made lowercase.
    tier1se = models.CharField(max_length=400, db_column='TIER1SE') # Field name made lowercase.
    relocation = models.CharField(max_length=10, db_column='RELOCATION', blank=True) # Field name made lowercase.
    weight = models.IntegerField(db_column='WEIGHT') # Field name made lowercase.
    server = models.CharField(max_length=100, db_column='SERVER') # Field name made lowercase.
    status = models.CharField(max_length=20, db_column='STATUS') # Field name made lowercase.
    transtimelo = models.IntegerField(db_column='TRANSTIMELO') # Field name made lowercase.
    transtimehi = models.IntegerField(db_column='TRANSTIMEHI') # Field name made lowercase.
    waittime = models.IntegerField(db_column='WAITTIME') # Field name made lowercase.
#    comment_ = models.CharField(max_length=600, db_column='COMMENT_', blank=True) # Field name made lowercase.
    comment_ = models.CharField(max_length=200, db_column='COMMENT_', blank=True)  # Field name made lowercase.
    space = models.IntegerField(db_column='SPACE') # Field name made lowercase.
    moduser = models.CharField(max_length=30, db_column='MODUSER', blank=True) # Field name made lowercase.
    modtime = models.DateTimeField(db_column='MODTIME') # Field name made lowercase.
    validation = models.CharField(max_length=20, db_column='VALIDATION', blank=True) # Field name made lowercase.
    mcshare = models.IntegerField(db_column='MCSHARE') # Field name made lowercase.
    countries = models.CharField(max_length=80, db_column='COUNTRIES', blank=True) # Field name made lowercase.
    fasttrack = models.CharField(max_length=20, db_column='FASTTRACK', blank=True) # Field name made lowercase.
    nprestage = models.BigIntegerField(db_column='NPRESTAGE') # Field name made lowercase.
    pilotowners = models.CharField(max_length=300, db_column='PILOTOWNERS', blank=True) # Field name made lowercase.
    dn = models.CharField(max_length=100, db_column='DN', blank=True) # Field name made lowercase.
    email = models.CharField(max_length=60, db_column='EMAIL', blank=True) # Field name made lowercase.
    fairshare = models.CharField(max_length=256, db_column='FAIRSHARE', blank=True) # Field name made lowercase.


    class Meta:
        app_label = 'panda'
        db_table = '"ATLAS_PANDAMETA"."CLOUDCONFIG"'



class JediDatasets(models.Model):
    id = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    datasetid = models.BigIntegerField(db_column='DATASETID', primary_key=True)
    datasetname = models.CharField(max_length=765, db_column='DATASETNAME')
    type = models.CharField(max_length=60, db_column='TYPE')
    creationtime = models.DateTimeField(db_column='CREATIONTIME')
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    vo = models.CharField(max_length=48, db_column='VO', blank=True)
    cloud = models.CharField(max_length=30, db_column='CLOUD', blank=True)
    site = models.CharField(max_length=180, db_column='SITE', blank=True)
    masterid = models.BigIntegerField(null=True, db_column='MASTERID', blank=True)
    provenanceid = models.BigIntegerField(null=True, db_column='PROVENANCEID', blank=True)
    containername = models.CharField(max_length=396, db_column='CONTAINERNAME', blank=True)
    status = models.CharField(max_length=60, db_column='STATUS', blank=True)
    state = models.CharField(max_length=60, db_column='STATE', blank=True)
    statechecktime = models.DateTimeField(null=True, db_column='STATECHECKTIME', blank=True)
    statecheckexpiration = models.DateTimeField(null=True, db_column='STATECHECKEXPIRATION', blank=True)
    frozentime = models.DateTimeField(null=True, db_column='FROZENTIME', blank=True)
    nfiles = models.IntegerField(null=True, db_column='NFILES', blank=True)
    total_files_tobeused = models.IntegerField(null=True, db_column='NFILESTOBEUSED', blank=True)
    total_files_used = models.IntegerField(null=True, db_column='NFILESUSED', blank=True)
    nevents = models.BigIntegerField(null=True, db_column='NEVENTS', blank=True)
    neventstobeused = models.BigIntegerField(null=True, db_column='NEVENTSTOBEUSED', blank=True)
    neventsused = models.BigIntegerField(null=True, db_column='NEVENTSUSED', blank=True)
    lockedby = models.CharField(max_length=120, db_column='LOCKEDBY', blank=True)
    lockedtime = models.DateTimeField(null=True, db_column='LOCKEDTIME', blank=True)
    total_files_finished = models.IntegerField(null=True, db_column='NFILESFINISHED', blank=True)
    total_files_failed = models.IntegerField(null=True, db_column='NFILESFAILED', blank=True)
    attributes = models.CharField(max_length=300, db_column='ATTRIBUTES', blank=True)
    streamname = models.CharField(max_length=60, db_column='STREAMNAME', blank=True)
    storagetoken = models.CharField(max_length=180, db_column='STORAGETOKEN', blank=True)
    destination = models.CharField(max_length=180, db_column='DESTINATION', blank=True)
    total_files_onhold = models.IntegerField(null=True, db_column='NFILESONHOLD', blank=True)
    templateid = models.BigIntegerField(db_column='TEMPLATEID', blank=True)

    def save(self, *args, **kwargs):
        raise NotImplementedError('Read only')

    def delete(self, *args, **kwargs):
         return

    class Meta:
        app_label = 'panda'
        db_table = '"ATLAS_PANDA"."JEDI_DATASETS"'

class JediDatasetContents(models.Model):
    jeditaskid = models.BigIntegerField(db_column='JEDITASKID', primary_key=True)
    datasetid = models.BigIntegerField(db_column='DATASETID')
    fileid = models.BigIntegerField(db_column='FILEID')
    creationdate = models.DateTimeField(db_column='CREATIONDATE')
    lastattempttime = models.DateTimeField(null=True, db_column='LASTATTEMPTTIME', blank=True)
    lfn = models.CharField(max_length=768, db_column='LFN')
    guid = models.CharField(max_length=192, db_column='GUID', blank=True)
    type = models.CharField(max_length=60, db_column='TYPE')
    status = models.CharField(max_length=192, db_column='STATUS')
    fsize = models.BigIntegerField(null=True, db_column='FSIZE', blank=True)
    checksum = models.CharField(max_length=108, db_column='CHECKSUM', blank=True)
    scope = models.CharField(max_length=90, db_column='SCOPE', blank=True)
    attemptnr = models.IntegerField(null=True, db_column='ATTEMPTNR', blank=True)
    maxattempt = models.IntegerField(null=True, db_column='MAXATTEMPT', blank=True)
    nevents = models.IntegerField(null=True, db_column='NEVENTS', blank=True)
    keeptrack = models.IntegerField(null=True, db_column='KEEPTRACK', blank=True)
    startevent = models.IntegerField(null=True, db_column='STARTEVENT', blank=True)
    endevent = models.IntegerField(null=True, db_column='ENDEVENT', blank=True)
    firstevent = models.IntegerField(null=True, db_column='FIRSTEVENT', blank=True)
    boundaryid = models.BigIntegerField(null=True, db_column='BOUNDARYID', blank=True)
    pandaid = models.BigIntegerField(db_column='PANDAID', blank=True)
    jobsetid = models.BigIntegerField(db_column='JOBSETID', blank=True)
    maxfailure = models.IntegerField(null=True, db_column='MAXFAILURE', blank=True)
    failedattempt = models.IntegerField(null=True, db_column='FAILEDATTEMPT', blank=True)
    lumiblocknr = models.IntegerField(null=True, db_column='LUMIBLOCKNR', blank=True)
    procstatus = models.CharField(max_length=192, db_column='PROC_STATUS')

    def save(self, *args, **kwargs):
        raise NotImplementedError('Read only')

    def delete(self, *args, **kwargs):
        raise NotImplementedError('Read only')

    class Meta:
        app_label = 'panda'
        db_table = '"ATLAS_PANDA"."JEDI_DATASET_CONTENTS"'


class JediTasks(models.Model):
    id = models.BigIntegerField(primary_key=True, db_column='JEDITASKID')
    taskname = models.CharField(max_length=384, db_column='TASKNAME', blank=True)
    status = models.CharField(max_length=192, db_column='STATUS')
    username = models.CharField(max_length=384, db_column='USERNAME')
    creationdate = models.DateTimeField(db_column='CREATIONDATE')
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME')
    reqid = models.IntegerField(null=True, db_column='REQID', blank=True)
    oldstatus = models.CharField(max_length=192, db_column='OLDSTATUS', blank=True)
    cloud = models.CharField(max_length=30, db_column='CLOUD', blank=True)
    site = models.CharField(max_length=180, db_column='SITE', blank=True)
    start_time = models.DateTimeField(null=True, db_column='STARTTIME', blank=True)
    endtime = models.DateTimeField(null=True, db_column='ENDTIME', blank=True)
    frozentime = models.DateTimeField(null=True, db_column='FROZENTIME', blank=True)
    prodsourcelabel = models.CharField(max_length=60, db_column='PRODSOURCELABEL', blank=True)
    workinggroup = models.CharField(max_length=96, db_column='WORKINGGROUP', blank=True)
    vo = models.CharField(max_length=48, db_column='VO', blank=True)
    corecount = models.IntegerField(null=True, db_column='CORECOUNT', blank=True)
    tasktype = models.CharField(max_length=192, db_column='TASKTYPE', blank=True)
    processingtype = models.CharField(max_length=192, db_column='PROCESSINGTYPE', blank=True)
    taskpriority = models.IntegerField(null=True, db_column='TASKPRIORITY', blank=True)
    currentpriority = models.IntegerField(null=True, db_column='CURRENTPRIORITY', blank=True)
    architecture = models.CharField(max_length=768, db_column='ARCHITECTURE', blank=True)
    transuses = models.CharField(max_length=192, db_column='TRANSUSES', blank=True)
    transhome = models.CharField(max_length=384, db_column='TRANSHOME', blank=True)
    transpath = models.CharField(max_length=384, db_column='TRANSPATH', blank=True)
    lockedby = models.CharField(max_length=120, db_column='LOCKEDBY', blank=True)
    lockedtime = models.DateTimeField(null=True, db_column='LOCKEDTIME', blank=True)
    termcondition = models.CharField(max_length=300, db_column='TERMCONDITION', blank=True)
    splitrule = models.CharField(max_length=300, db_column='SPLITRULE', blank=True)
    walltime = models.IntegerField(null=True, db_column='WALLTIME', blank=True)
    walltimeunit = models.CharField(max_length=96, db_column='WALLTIMEUNIT', blank=True)
    outdiskcount = models.IntegerField(null=True, db_column='OUTDISKCOUNT', blank=True)
    outdiskunit = models.CharField(max_length=96, db_column='OUTDISKUNIT', blank=True)
    workdiskcount = models.IntegerField(null=True, db_column='WORKDISKCOUNT', blank=True)
    workdiskunit = models.CharField(max_length=96, db_column='WORKDISKUNIT', blank=True)
    ramcount = models.IntegerField(null=True, db_column='RAMCOUNT', blank=True)
    ramunit = models.CharField(max_length=96, db_column='RAMUNIT', blank=True)
    iointensity = models.IntegerField(null=True, db_column='IOINTENSITY', blank=True)
    iointensityunit = models.CharField(max_length=96, db_column='IOINTENSITYUNIT', blank=True)
    workqueue_id = models.IntegerField(null=True, db_column='WORKQUEUE_ID', blank=True)
    progress = models.IntegerField(null=True, db_column='PROGRESS', blank=True)
    failurerate = models.IntegerField(null=True, db_column='FAILURERATE', blank=True)
    errordialog = models.CharField(max_length=765, db_column='ERRORDIALOG', blank=True)
    countrygroup = models.CharField(max_length=20, db_column='COUNTRYGROUP', blank=True)
    parent_tid = models.BigIntegerField(db_column='PARENT_TID', blank=True)
    eventservice = models.IntegerField(null=True, db_column='EVENTSERVICE', blank=True)
    ticketid = models.CharField(max_length=50, db_column='TICKETID', blank=True)
    ticketsystemtype = models.CharField(max_length=16, db_column='TICKETSYSTEMTYPE', blank=True)
    statechangetime = models.DateTimeField(null=True, db_column='STATECHANGETIME', blank=True)
    superstatus = models.CharField(max_length=64, db_column='SUPERSTATUS', blank=True)
    campaign = models.CharField(max_length=72, db_column='CAMPAIGN', blank=True)
    cputime = models.IntegerField(null=True, db_column='cputime', blank=True)
    cputimeunit = models.CharField(max_length=72, db_column='cputimeunit', blank=True)
    basewalltime = models.IntegerField(null=True, db_column='basewalltime', blank=True)
    cpuefficiency = models.IntegerField(null=True, db_column='cpuefficiency', blank=True)
    nucleus = models.CharField(max_length=72, db_column='NUCLEUS', blank=True)
    ttcrequested = models.DateTimeField(null=True, db_column='TTCREQUESTED', blank=True)
    gshare = models.CharField(max_length=72, db_column='GSHARE', blank=True)
    diskio = models.IntegerField(null=True, db_column='diskio', blank=True)
    diskiounit = models.CharField(max_length=96, db_column='diskiounit', blank=True)

    def save(self, *args, **kwargs):
        raise NotImplementedError('Read only')

    def delete(self, *args, **kwargs):
         return

    class Meta:
        app_label = 'panda'
        db_table = '"ATLAS_PANDA"."JEDI_TASKS"'


