import json
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.db import connection
from django.db import connections
from django.utils import timezone
from ..prodtask.helper import Singleton
import logging

_logger = logging.getLogger('prodtaskwebui')

MC_STEPS = ['Evgen',
             'Simul',
             'Merge',
             'Digi',
             'Reco',
             'Rec Merge',
             'Rec TAG',
             'Atlfast',
             'Atlf Merge',
             'Atlf TAG',
             'Deriv']

class sqliteID(Singleton):
    def get_id(self,cursor,id_field_name,table_name):
        if (id_field_name+table_name) in self.__id_dict.keys():
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

        db_table = u'T_PROJECTS'

class TRequest(models.Model):
    # PHYS_GROUPS=[(x,x) for x in ['physics','BPhysics','Btagging','DPC','Detector','EGamma','Exotics','HI','Higgs',
    #                              'InDet','JetMet','LAr','MuDet','Muon','SM','Susy','Tau','Top','Trigger','TrackingPerf',
    #                              'reprocessing','trig-hlt','Validation']]
    PHYS_GROUPS=[(x,x) for x in ['BPHY',
                                 'COSM',
                                 'DAPR',
                                 'EGAM',
                                 'EXOT',
                                 'FTAG',
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
    project = models.ForeignKey(TProject,db_column='PROJECT', null=True, blank=False)
    is_error = models.NullBooleanField(db_column='EXCEPTION', null=True, blank=False)
    jira_reference = models.CharField(max_length=50, db_column='REFERENCE', null=True, blank=True)
    info_fields = models.TextField(db_column='INFO_FIELDS', null=True, blank=True)
    is_fast = models.NullBooleanField(db_column='IS_FAST', null=True, blank=False)


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

    def info_field(self,field):
        if self.info_fields:
            info_field_dict = json.loads(self.info_fields)
            return info_field_dict.get(field,None)
        else:
            return None

    def save(self, *args, **kwargs):
        if not self.reqid:
            self.reqid = prefetch_id('deft',u'ATLAS_DEFT.T_PRODMANAGER_REQUEST_ID_SEQ','T_PRODMANAGER_REQUEST','PR_ID')

        super(TRequest, self).save(*args, **kwargs)

    class Meta:
        db_table = u'T_PRODMANAGER_REQUEST'


class RequestStatus(models.Model):
    STATUS_TYPES = (
                    ('Created', 'Created'),
                    ('Pending', 'Pending'),
                    ('Unknown', 'Unknown'),
                    ('Approved', 'Approved'),
                    )
    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='REQ_S_ID', primary_key=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID')
    comment = models.CharField(max_length=256, db_column='COMMENT', null=True)
    owner = models.CharField(max_length=32, db_column='OWNER', null=False)
    status = models.CharField(max_length=32, db_column='STATUS', choices=STATUS_TYPES, null=False)
    timestamp = models.DateTimeField(db_column='TIMESTAMP', null=False)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_PRODMANAGER_REQ_STAT_ID_SEQ','T_PRODMANAGER_REQUEST_STATUS','REQ_S_ID')
        super(RequestStatus, self).save(*args, **kwargs)

    def save_with_current_time(self, *args, **kwargs):
        if not self.timestamp:
            self.timestamp = timezone.now()
        self.save(*args, **kwargs)

    class Meta:
        db_table = u'T_PRODMANAGER_REQUEST_STATUS'

class StepTemplate(models.Model):
    id =  models.DecimalField(decimal_places=0, max_digits=12,  db_column='STEP_T_ID', primary_key=True)
    step = models.CharField(max_length=12, db_column='STEP_NAME', null=False)
    def_time = models.DateTimeField(db_column='DEF_TIME', null=False)
    status = models.CharField(max_length=12, db_column='STATUS', null=False)
    ctag = models.CharField(max_length=12, db_column='CTAG', null=False)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=False)
    cpu_per_event = models.DecimalField(decimal_places=0, max_digits=7, db_column='CPU_PER_EVENT', null=True)
    output_formats = models.CharField(max_length=250, db_column='OUTPUT_FORMATS', null=True)
    memory = models.DecimalField(decimal_places=0, max_digits=5, db_column='MEMORY', null=True)
    trf_name = models.CharField(max_length=128, db_column='TRF_NAME', null=True)
    lparams = models.CharField(max_length=2000, db_column='LPARAMS', null=True)
    vparams = models.CharField(max_length=4000, db_column='VPARAMS', null=True)
    swrelease = models.CharField(max_length=80, db_column='SWRELEASE', null=True)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_STEP_TEMPLATE_ID_SEQ','T_STEP_TEMPLATE','STEP_T_ID')
        super(StepTemplate, self).save(*args, **kwargs)

    class Meta:
        #db_table = u'T_STEP_TEMPLATE'
        db_table = u'T_STEP_TEMPLATE'

class Ttrfconfig(models.Model):
    tag = models.CharField(max_length=1, db_column='TAG', default='-')
    cid = models.DecimalField(decimal_places=0, max_digits=5, db_column='CID', primary_key=True, default=0)
    trf = models.CharField(max_length=80, db_column='TRF', null=True, default='transformation')
    lparams = models.CharField(max_length=1024, db_column='LPARAMS', null=True, default='parameter list')
    vparams = models.CharField(max_length=4000, db_column='VPARAMS', null=True, default='')
    trfv = models.CharField(max_length=40, db_column='TRFV', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    ami_flag = models.DecimalField(decimal_places=0, max_digits=10, db_column='AMI_FLAG', null=True)
    createdby = models.CharField(max_length=60, db_column='CREATEDBY', null=True)
    input = models.CharField(max_length=20, db_column='INPUT', null=True)
    step = models.CharField(max_length=12, db_column='STEP', null=True)
    formats = models.CharField(max_length=256, db_column='FORMATS', null=True)
    cache = models.CharField(max_length=32, db_column='CACHE', null=True)
    cpu_per_event = models.DecimalField(decimal_places=0, max_digits=5, db_column='CPU_PER_EVENT', null=True, default=1)
    memory = models.DecimalField(decimal_places=0, max_digits=5, db_column='MEMORY', default=1000)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', default=100)
    events_per_job = models.DecimalField(decimal_places=0, max_digits=10, db_column='EVENTS_PER_JOB', default=1000)


    class Meta:
        app_label = 'grisli'
        db_table = u'T_TRF_CONFIG'

class TDataFormatAmi(models.Model):
    format = models.CharField(max_length=32, db_column='FORMAT', primary_key=True)
    description = models.CharField(max_length=256, db_column='DESCRIPTION')
    status = models.CharField(max_length=8, db_column='STATUS')
    last_modified = models.DateTimeField(db_column='LASTMODIFIED')

    class Meta:
        app_label = 'grisli'
        db_table = u'T_DATA_FORMAT_AMI'

class ProductionDataset(models.Model):
    name = models.CharField(max_length=150, db_column='NAME', primary_key=True)
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


    class Meta:
        #db_table = u'T_PRODUCTION_DATASET'
        db_table = u'T_PRODUCTION_DATASET'

class ProductionContainer(models.Model):
    name = models.CharField(max_length=150, db_column='NAME', primary_key=True)
    parent_task_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID', null=True)
    rid = models.DecimalField(decimal_places=0, max_digits=12, db_column='PR_ID', null=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)

    class Meta:
        #db_table = u'T_PRODUCTION_DATASET'
        db_table = u'T_PRODUCTION_CONTAINER'

class InputRequestList(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='IND_ID', primary_key=True)
    dataset = models.ForeignKey(ProductionDataset, db_column='INPUTDATASET',null=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID')
    slice = models.DecimalField(decimal_places=0, max_digits=12, db_column='SLICE', null=False)
    brief = models.CharField(max_length=150, db_column='BRIEF')
    phys_comment = models.CharField(max_length=256, db_column='PHYSCOMMENT')
    comment = models.CharField(max_length=512, db_column='SLICECOMMENT')
    input_data = models.CharField(max_length=150, db_column='INPUTDATA')
    project_mode = models.CharField(max_length=256, db_column='PROJECT_MODE')
    priority = models.DecimalField(decimal_places=0, max_digits=12, db_column='PRIORITY')
    input_events = models.DecimalField(decimal_places=0, max_digits=12, db_column='INPUT_EVENTS')
    is_hide = models.NullBooleanField(db_column='HIDED', null=True, blank=False)
    cloned_from = models.ForeignKey('self',db_column='CLONED_FROM', null=True)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_INPUT_DATASET_ID_SEQ','T_INPUT_DATASET','IND_ID')
        super(InputRequestList, self).save(*args, **kwargs)

    class Meta:
        #db_table = u'T_INPUT_DATASET'
        db_table = u'T_INPUT_DATASET'


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
        db_table = u'"ATLAS_PANDA"."RETRYACTIONS"'


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
        db_table = u'"ATLAS_PANDA"."JEDI_WORK_QUEUE"'


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
    retry_action = models.ForeignKey(RetryAction,db_column='RetryAction')
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
            self.id = prefetch_id('panda',u'ATLAS_PANDA.RETRYERRORS_ID_SEQ','RETRYACTION','ID')

        super(RetryErrors, self).save(*args, **kwargs)

    class Meta:
        app_label = 'panda'
        db_table = u'"ATLAS_PANDA"."RETRYERRORS"'


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
    pattern_request = models.ForeignKey(TRequest, db_column='PATTERN_REQUEST')

    def save(self, *args, **kwargs):
        self.timestamp = timezone.now()
        if not self.id:
            self.id = prefetch_id('dev_db',u'T_GROUP_TRAIN_ID_SEQ',"T_GROUP_TRAIN",'GPT_ID')
        super(TrainProduction, self).save(*args, **kwargs)

    def __str__(self):
        return "%i - %s"%(self.pattern_request.reqid,self.pattern_request.description)

    class Meta:
        app_label = 'dev'
        db_table = u'"T_GROUP_TRAIN"'

class ParentToChildRequest(models.Model):
    RELATION_TYPE = (
                    ('BC', 'By creation'),
                    ('MA', 'Manually'),
                    ('SP', 'Evgen Split')
                    )
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PTC_ID', primary_key=True)
    parent_request = models.ForeignKey(TRequest, db_column='PARENT_PR_ID')
    child_request = models.ForeignKey(TRequest, db_column='CHILD_PR_ID', null=True)
    relation_type = models.CharField(max_length=2, db_column='RELATION_TYPE', choices=RELATION_TYPE, null=False)
    train = models.ForeignKey(TrainProduction, db_column='TRAIN_ID')
    status = models.CharField(max_length=12, db_column='STATUS', null=False)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('dev_db',u'PARENT_CHILD_REQUEST_ID_SEQ','T_PARENT_CHILD_REQUEST','PTC_ID')
        super(ParentToChildRequest, self).save(*args, **kwargs)


    class Meta:
        app_label = 'dev'

        db_table = u"T_PARENT_CHILD_REQUEST"








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
    TASK_CONFIG_PARAMS = INT_TASK_CONFIG_PARAMS + ['input_format','token','merging_tag','project_mode']

    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='STEP_ID', primary_key=True)
    request = models.ForeignKey(TRequest, db_column='PR_ID')
    step_template = models.ForeignKey(StepTemplate, db_column='STEP_T_ID')
    status = models.CharField(max_length=12, db_column='STATUS', null=False)
    slice = models.ForeignKey(InputRequestList, db_column='IND_ID', null=False)
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=False)
    step_def_time = models.DateTimeField(db_column='STEP_DEF_TIME', null=False)
    step_appr_time = models.DateTimeField(db_column='STEP_APPR_TIME', null=True)
    step_exe_time = models.DateTimeField(db_column='STEP_EXE_TIME', null=True)
    step_done_time = models.DateTimeField(db_column='STEP_DONE_TIME', null=True)
    input_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='INPUT_EVENTS', null=True)
    task_config = models.CharField(max_length=2000, db_column='TASK_CONFIG')
    step_parent = models.ForeignKey('self', db_column='STEP_PARENT_ID')

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
        return_dict = json.loads(self.task_config)
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

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_PRODUCTION_STEP_ID_SEQ','T_PRODUCTION_STEP','STEP_ID')
        if not self.step_parent_id:
            self.step_parent_id = self.id
        super(StepExecution, self).save(*args, **kwargs)

    def post_approve_action(self):
        STARTING_REQUEST_ID = 5816
        if (self.request_id > STARTING_REQUEST_ID) and (((self.request_id % 10) == 2) or ((self.request_id % 10) == 8)):
            if 'cloud' not in self.get_task_config('project_mode'):
                self.update_project_mode('cloud','WORLD')

    class Meta:
        #db_table = u'T_PRODUCTION_STEP'
        db_table = u'T_PRODUCTION_STEP'



class TTask(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASKID', primary_key=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    total_done_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_DONE_JOBS', null=True)
    submit_time = models.DateTimeField(db_column='SUBMIT_TIME', null=False)
    start_time = models.DateTimeField(db_column='START_TIME', null=True)
    total_req_jobs = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_REQ_JOBS', null=True)
    total_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_EVENTS', null=True)

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

    def save(self, **kwargs):
        """ Read-only access to the table """
        raise NotImplementedError

    def delete(self, *args, **kwargs):
         return

    class Meta:
        managed = False
        db_table =  u'"ATLAS_DEFT"."T_TASK"'
        app_label = 'taskmon'



class ProductionTask(models.Model):

    RED_STATUS = ['failed','aborted','broken']

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TASKID', primary_key=True)
    step = models.ForeignKey(StepExecution, db_column='STEP_ID')
    request = models.ForeignKey(TRequest, db_column='PR_ID')
    parent_id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PARENT_TID', null=False)
    chain_tid = models.DecimalField(decimal_places=0, max_digits=12, db_column='CHAIN_TID', null=False)
    name = models.CharField(max_length=130, db_column='TASKNAME', null=True)
    project = models.CharField(max_length=60, db_column='PROJECT', null=True)
    username = models.CharField(max_length=128, db_column='USERNAME', null=True)
    dsn = models.CharField(max_length=12, db_column='DSN', null=True)
    phys_short = models.CharField(max_length=80, db_column='PHYS_SHORT', null=True)
    simulation_type = models.CharField(max_length=20, db_column='SIMULATION_TYPE', null=True)
    phys_group = models.CharField(max_length=20, db_column='PHYS_GROUP', null=True)
    provenance = models.CharField(max_length=12, db_column='PROVENANCE', null=True)
    status = models.CharField(max_length=12, db_column='STATUS', null=True)
    total_events = models.DecimalField(decimal_places=0, max_digits=10, db_column='TOTAL_EVENTS', null=True)
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
    is_extension = models.NullBooleanField(db_column='IS_EXTENSION', null=True, blank=False)
    total_files_finished = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILESFINISHED', null=True)
    ttcr_timestamp = models.DateTimeField(db_column='TTCR_TIMESTAMP', null=True)
    ttcj_timestamp = models.DateTimeField(db_column='TTCJ_TIMESTAMP', null=True)
    ttcj_update_time = models.DateTimeField(db_column='TTCJ_UPDATE_TIME', null=True)

    # def save(self):
    #     raise NotImplementedError

    @property
    def failure_rate(self):
        try:
            #rate = round(self.total_files_failed/self.total_files_tobeused*100,3);
            rate = self.total_files_failed/self.total_files_tobeused*100;
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

    class Meta:
        #db_table = u'T_PRODUCTION_STEP'
        db_table = u'T_PRODUCTION_TASK'




class OpenEndedRequest(models.Model):

    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='OE_ID', primary_key=True)
    status = models.CharField(max_length=20, db_column='STATUS', null=True)
    request  = models.ForeignKey(TRequest, db_column='PR_ID')
    container = models.CharField(max_length=150, db_column='CONTAINER',null=False)
    last_update = models.DateTimeField(db_column='LAST_UPDATE')


    def save(self, *args, **kwargs):
        if not self.last_update:
            self.last_update = timezone.now()
        if not self.id:
            self.id = prefetch_id('dev_db',u'T_OPEN_ENDED_ID_SEQ',"T_OPEN_ENDED",'OE_ID')
        super(OpenEndedRequest, self).save(*args, **kwargs)

    def save_last_update(self, *args, **kwargs):
        self.last_update = timezone.now()
        super(OpenEndedRequest, self).save(*args, **kwargs)

    class Meta:
        app_label = 'dev'
        db_table = u'"T_OPEN_ENDED"'


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
            self.id = prefetch_id('dev_db',u'T_HASHTAG_ID_SEQ',"T_HASHTAG",'HT_ID')
        super(HashTag, self).save(*args, **kwargs)



    def __str__(self):
        return self.hashtag

    class Meta:
        app_label = 'dev'
        db_table = u'"T_HASHTAG"'

class HashTagToRequest(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='HTTR_ID', primary_key=True)
    request = models.ForeignKey(TRequest,  db_column='PR_ID')
    hashtag = models.ForeignKey(HashTag, db_column='HT_ID')

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('dev_db',u'T_HT_TO_REQUEST_SEQ',"T_HT_TO_REQUEST",'HTTR_ID')
        super(HashTagToRequest, self).save(*args, **kwargs)

    def save_last_update(self, *args, **kwargs):
        self.last_update = timezone.now()
        super(HashTagToRequest, self).save(*args, **kwargs)


    class Meta:
        app_label = 'dev'
        db_table = u'"T_HT_TO_REQUEST"'

class HashTagToTask(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='HTTT_ID', primary_key=True)
    task = models.ForeignKey(ProductionTask,  db_column='TASKID')
    hashtag = models.ForeignKey(HashTag, db_column='HT_ID')

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('dev_db',u'T_HT_TO_TASK_SEQ',"T_HT_TO_TASK",'HTTT_ID')
        super(HashTagToTask, self).save(*args, **kwargs)

    def save_last_update(self, *args, **kwargs):
        self.last_update = timezone.now()
        super(HashTagToTask, self).save(*args, **kwargs)

    class Meta:
        app_label = 'dev'
        db_table = u'"T_HT_TO_TASK"'

class TrainProductionLoad(models.Model):

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='TC_ID', primary_key=True)
    train = models.ForeignKey(TrainProduction,db_column='TRAIN_NUMBER', null=False)
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
        except Exception,e:
            self.datasets=''
            _logger.debug('Problem this loads datastes: %s',str(e))
        if not self.id:
            self.id = prefetch_id('dev_db',u'T_TRAIN_CARRIAGE_ID_SEQ',"T_TRAIN_CARRIAGE",'TC_ID')

        super(TrainProductionLoad, self).save(*args, **kwargs)

    class Meta:
        app_label = 'dev'
        db_table = u"T_TRAIN_CARRIAGE"

class MCPattern(models.Model):
    STEPS = MC_STEPS
    STATUS = [(x,x) for x in ['IN USE','Obsolete']]
    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='MCP_ID', primary_key=True)
    pattern_name =  models.CharField(max_length=150, db_column='PATTERN_NAME', unique=True)
    pattern_dict = models.CharField(max_length=2000, db_column='PATTERN_DICT')
    pattern_status = models.CharField(max_length=20, db_column='PATTERN_STATUS', choices=STATUS)

    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_PRODUCTION_MCP_ID_SEQ','T_PRODUCTION_MC_PATTERN','MCP_ID')
        super(MCPattern, self).save(*args, **kwargs)

    class Meta:
        db_table = u'T_PRODUCTION_MC_PATTERN'





class MCPriority(models.Model):
    STEPS = ['Evgen',
             'Simul',
             'Simul(Fast)',
             'Merge',
             'Digi',
             'Reco',
             'Rec Merge',
             'Rec TAG',
             'Atlfast',
             'Atlf Merge',
             'Atlf TAG',
             'Deriv']
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='MCPRIOR_ID', primary_key=True)
    priority_key = models.DecimalField(decimal_places=0, max_digits=12, db_column='PRIORITY_KEY', unique=True)
    priority_dict = models.CharField(max_length=2000, db_column='PRIORITY_DICT')

    def save(self, *args, **kwargs):
        if self.priority_key == -1:
            return
        if not self.id:
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_PRODUCTION_MCPRIOR_ID_SEQ','T_PRODUCTION_MC_PRIORITY','MCPRIOR_ID')
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
        db_table = u'T_PRODUCTION_MC_PRIORITY'




def get_priority_object(priority_key):
    try:
        mcp = MCPriority.objects.get(priority_key=priority_key)
    except ObjectDoesNotExist:
        priority_py_dict = {}
        for step in MCPriority.STEPS:
            priority_py_dict.update({step:int(priority_key)})
        mcp=MCPriority.objects.create(priority_key=-1,priority_dict=json.dumps(priority_py_dict))
    except Exception,e:
        raise e
    return mcp


def get_default_nEventsPerJob_dict():
    defult_dict = {
        'Evgen':5000,
        'Simul':100,
        'Merge':1000,
        'Digi':500,
        'Reco':500,
        'Rec Merge':5000,
        'Rec TAG':25000,
        'Atlfast':500,
        'Atlf Merge':5000,
        'Atlf TAG':25000,
        'Deriv':100000
    }
    return defult_dict

def get_default_project_mode_dict():
    default_dict = {
         'Evgen':'spacetoken=ATLASDATADISK',
         'Simul':'spacetoken=ATLASDATADISK',
         'Merge':'spacetoken=ATLASDATADISK',
         'Digi':'Npileup=5;spacetoken=ATLASDATADISK',
         'Reco':'Npileup=5;spacetoken=ATLASDATADISK',
         'Rec Merge':'spacetoken=ATLASDATADISK',
         'Rec TAG':'spacetoken=ATLASDATADISK',
         'Atlfast':'Npileup=5;spacetoken=ATLASDATADISK',
         'Atlf Merge':'spacetoken=ATLASDATADISK',
         'Atlf TAG':'spacetoken=ATLASDATADISK',
         'Deriv':'spacetoken=ATLASDATADISK'
    }
    return default_dict

class Site(models.Model):
    site_name = models.CharField(max_length=52, db_column='SITE_NAME', primary_key=True)
    role = models.CharField(max_length=256, db_column='ROLE')

    def save(self, *args, **kwargs):
        raise NotImplementedError('Only manual creation')

    class Meta:
        app_label = 'panda'
        db_table = u'"ATLAS_PANDA"."SITE"'

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
        db_table = u'"ATLAS_PANDA"."CONFIG"'


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
        db_table = u'"ATLAS_PANDAMETA"."CLOUDCONFIG"'



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
        db_table = u'"ATLAS_PANDA"."JEDI_DATASETS"'

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

    def save(self, *args, **kwargs):
        raise NotImplementedError('Read only')

    def delete(self, *args, **kwargs):
         return

    class Meta:
        app_label = 'panda'
        db_table = u'"ATLAS_PANDA"."JEDI_TASKS"'


