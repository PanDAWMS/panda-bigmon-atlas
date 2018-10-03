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


    @property
    def output_by_slice(self):
        if self.outputs:
            return json.loads(self.outputs)
        return []

    def __str__(self):
        return "%i - %s"%(self.pattern_request.reqid,self.pattern_request.description)

    class Meta:
        app_label = 'dev'
        db_table = u'"T_GROUP_TRAIN"'



class MCPileupOverlayGroupDescription(models.Model):

    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='POG_GROUP_ID', primary_key=True)
    description = models.CharField(max_length=255, db_column='DESCRIPTION')

    def save(self, *args, **kwargs):
        #self.timestamp = timezone.now()
        if not self.id:
            self.id = prefetch_id('dev_db',u'T_MC_PO_PHYS_GROUP_SEQ',"T_MC_PO_PHYS_GROUP",'POG_GROUP_ID')
        super(MCPileupOverlayGroupDescription, self).save(*args, **kwargs)

    class Meta:
        app_label = 'dev'
        db_table = u'"ATLAS_DEFT"."T_MC_PO_PHYS_GROUP"'

class MCPileupOverlayGroups(models.Model):

    id =  models.DecimalField(decimal_places=0, max_digits=12, db_column='POG_ID', primary_key=True)
    campaign = models.CharField(max_length=50, db_column='CAMPAIGN')
    dsid  = models.DecimalField(decimal_places=0, max_digits=12, db_column='DSID')
    group = models.ForeignKey(MCPileupOverlayGroupDescription, db_column='POG_GROUP_ID')

    def save(self, *args, **kwargs):
        #self.timestamp = timezone.now()
        if not self.id:
            self.id = prefetch_id('dev_db',u'T_PILEUP_OVERLAY_GROUPS_SEQ',"T_PILEUP_OVERLAY_GROUPS",'POG_ID')
        super(MCPileupOverlayGroups, self).save(*args, **kwargs)

    class Meta:
        app_label = 'dev'
        db_table = u'"ATLAS_DEFT"."T_PILEUP_OVERLAY_GROUPS"'



class ParentToChildRequest(models.Model):
    RELATION_TYPE = (
                    ('BC', 'By creation'),
                    ('MA', 'Manually'),
                    ('SP', 'Evgen Split'),
                    ('CL', 'Cloned'),
                    ('MR', 'Merged')
                    )

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='PTC_ID', primary_key=True)
    parent_request = models.ForeignKey(TRequest, db_column='PARENT_PR_ID')
    child_request = models.ForeignKey(TRequest, db_column='CHILD_PR_ID', null=True)
    relation_type = models.CharField(max_length=2, db_column='RELATION_TYPE', choices=RELATION_TYPE, null=False)
    train = models.ForeignKey(TrainProduction, db_column='TRAIN_ID', null=True)
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
    TASK_CONFIG_PARAMS = INT_TASK_CONFIG_PARAMS + ['input_format','token','merging_tag','project_mode','evntFilterEff', 'PDA', 'PDAParams']

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
    priority = models.DecimalField(decimal_places=0, max_digits=5, db_column='PRIORITY', null=True)
    current_priority =  models.DecimalField(decimal_places=0, max_digits=5, db_column='CURRENT_PRIORITY', null=True)
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

    def delete(self, *args, **kwargs):
         return

    class Meta:
#        managed = False
#        db_table =  u'"ATLAS_DEFT"."T_TASK"'
        db_table =  u"T_TASK"
     #   app_label = 'taskmon'



class ProductionTask(models.Model):

    RED_STATUS = ['failed','aborted','broken']
    NOT_RUNNING = RED_STATUS + ['finished','done','obsolete']
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
    phys_short = models.CharField(max_length=80, db_column='PHYS_SHORT', null=True)
    simulation_type = models.CharField(max_length=20, db_column='SIMULATION_TYPE', null=True)
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
    is_extension = models.NullBooleanField(db_column='IS_EXTENSION', null=True, blank=False)
    total_files_finished = models.DecimalField(decimal_places=0, max_digits=10, db_column='NFILESFINISHED', null=True)
    ttcr_timestamp = models.DateTimeField(db_column='TTCR_TIMESTAMP', null=True)
    ttcj_timestamp = models.DateTimeField(db_column='TTCJ_TIMESTAMP', null=True)
    ttcj_update_time = models.DateTimeField(db_column='TTCJ_UPDATE_TIME', null=True)
    primary_input = models.CharField(max_length=250, db_column='PRIMARY_INPUT', null=True)
    ami_tag = models.CharField(max_length=15, db_column='CTAG')
    output_formats = models.CharField(max_length=250, db_column='OUTPUT_FORMATS')
#    def save(self):
#         raise NotImplementedError

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

    @property
    def hashtags(self):
        return get_hashtags_by_task(int(self.id))

    def hashtag_exists(self, hashtag):
        return task_hashtag_exists(int(self.id),hashtag)

    def set_hashtag(self, hashtag):
        set_hashtag(hashtag, [int(self.id)])

    def remove_hashtag(self, hashtag):
        remove_hashtag_from_task(int(self.id), hashtag)

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
            self.id = prefetch_id('deft',u'ATLAS_DEFT.T_HASHTAG_ID_SEQ',"T_HASHTAG",'HT_ID')
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
        db_table = u'"ATLAS_DEFT"."T_HASHTAG"'





class HashTagToTask(models.Model):
    task = models.ForeignKey(ProductionTask,  db_column='TASKID')
    hashtag = models.ForeignKey(HashTag, db_column='HT_ID')

    def save(self, *args, **kwargs):
        raise NotImplementedError('Only manual creation')

    def create_relation(self):
        print self._meta.db_table

    class Meta:
        db_table = u'"ATLAS_DEFT"."T_HT_TO_TASK"'



def last_task_for_hashtag(hashtag):
    hashtag_id = HashTag.objects.get(hashtag=hashtag).id
    cursor = None
    last = None
    try:
        cursor = connections['deft'].cursor()
        cursor.execute("SELECT TASKID from %s WHERE HT_ID=%s AND  ROWNUM<=1 ORDER BY TASKID ASC"%(HashTagToTask._meta.db_table,hashtag_id))
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
            cursor.execute("insert into %s (HT_ID,TASKID) values(%s, %s)"%(HashTagToTask._meta.db_table,hashtag_id,task))
    finally:
        if cursor:
            cursor.close()


def count_tasks_by_hashtag(hashtag):
    hashtag_id = HashTag.objects.get(hashtag=hashtag).id
    cursor = None
    total = 0
    try:
        cursor = connections['deft'].cursor()
        cursor.execute("SELECT COUNT(TASKID) from %s WHERE HT_ID=%s"%(HashTagToTask._meta.db_table,hashtag_id))
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
        cursor.execute("SELECT TASKID,HT_ID from %s WHERE HT_ID=%s AND TASKID=%s"%(HashTagToTask._meta.db_table,hashtag_id,task_id))
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
        cursor.execute("SELECT TASKID from %s WHERE HT_ID=%s"%(HashTagToTask._meta.db_table,hashtag_id))
        tasks = cursor.fetchall()
    finally:
        if cursor:
            cursor.close()
    return [x[0] for x in tasks]

def get_hashtags_by_task(task_id):
    cursor = None
    try:
        cursor = connections['deft'].cursor()
        cursor.execute("SELECT HT_ID from %s WHERE TASKID=%s"%(HashTagToTask._meta.db_table,task_id))
        hashtags_id = cursor.fetchall()
    finally:
        if cursor:
            cursor.close()
    hashtags = [HashTag.objects.get(id=x[0]) for x in hashtags_id]
    return hashtags


def remove_hashtag_from_task(task_id, hashtag):
    hashtag_id = HashTag.objects.get(hashtag=hashtag).id
    cursor = None
    deleted = False
    try:
        cursor = connections['deft'].cursor()
        cursor.execute("SELECT TASKID,HT_ID from %s WHERE HT_ID=%s AND TASKID=%s"%(HashTagToTask._meta.db_table,hashtag_id,task_id))
        result = cursor.fetchall()
        if result:
            cursor.execute("DELETE FROM %s WHERE HT_ID=%s AND TASKID=%s"%(HashTagToTask._meta.db_table,hashtag_id,task_id))
            deleted = True
    finally:
        if cursor:
            cursor.close()
    return deleted



class WaitingStep(models.Model):

    ACTIONS = {
        1 : {'name':'postpone', 'description': 'Postpone ', 'attempts': 3, 'delay':1},
        2 : {'name': 'check2rep', 'description': 'Check that 2 replicas are done ', 'attempts': 12, 'delay':8}
    }

    ACTION_NAME_TYPE = {'postpone':1,'check2rep':2}

    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='WSTEP_ID', primary_key=True)
    request = models.ForeignKey(TRequest,  db_column='PR_ID')
    step = models.ForeignKey(StepExecution, db_column='STEP_ID')
    action = models.DecimalField(decimal_places=0, max_digits=12, db_column='TYPE')
    create_time = models.DateTimeField(db_column='SUBMIT_TIME')
    execution_time = models.DateTimeField(db_column='EXEC_TIME')
    done_time = models.DateTimeField(db_column='DONE_TIME')
    message = models.CharField(max_length=2000, db_column='MESSAGE')
    attempt = models.DecimalField(decimal_places=0, max_digits=12, db_column='ATTEMPT')
    status = models.CharField(max_length=20, db_column='STATUS', null=True)


    def save(self, *args, **kwargs):
        if not self.id:
            self.id = prefetch_id('dev_db',u'T_WAITING_STEP_SEQ',"T_WAITING_STEP",'HTTR_ID')
        super(WaitingStep, self).save(*args, **kwargs)

    def save_last_update(self, *args, **kwargs):
        self.last_update = timezone.now()
        super(WaitingStep, self).save(*args, **kwargs)


    class Meta:
        app_label = 'dev'
        db_table = u'"T_WAITING_STEP"'

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


class GlobalShare(models.Model):
    name = models.CharField(max_length=32, db_column='NAME', primary_key=True)
    value = models.DecimalField(decimal_places=0, max_digits=3, db_column='VALUE')
    parent = models.CharField (max_length=32, db_column='PARENT')

    def save(self, *args, **kwargs):
        raise NotImplementedError('Only manual creation')

    class Meta:
        app_label = 'panda'
        db_table = u'"ATLAS_PANDA"."GLOBAL_SHARES"'
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


class Schedconfig(models.Model):
    name = models.CharField(max_length=180, db_column='NAME')  # Field name made lowercase.
    nickname = models.CharField(max_length=180, primary_key=True, db_column='NICKNAME')  # Field name made lowercase.
    queue = models.CharField(max_length=180, db_column='QUEUE', blank=True)  # Field name made lowercase.
    localqueue = models.CharField(max_length=60, db_column='LOCALQUEUE', blank=True)  # Field name made lowercase.
    system = models.CharField(max_length=180, db_column='SYSTEM')  # Field name made lowercase.
    sysconfig = models.CharField(max_length=60, db_column='SYSCONFIG', blank=True)  # Field name made lowercase.
    environ = models.CharField(max_length=750, db_column='ENVIRON', blank=True)  # Field name made lowercase.
    gatekeeper = models.CharField(max_length=120, db_column='GATEKEEPER', blank=True)  # Field name made lowercase.
    jobmanager = models.CharField(max_length=240, db_column='JOBMANAGER', blank=True)  # Field name made lowercase.
    se = models.CharField(max_length=1200, db_column='SE', blank=True)  # Field name made lowercase.
    ddm = models.CharField(max_length=360, db_column='DDM', blank=True)  # Field name made lowercase.
    jdladd = models.CharField(max_length=1500, db_column='JDLADD', blank=True)  # Field name made lowercase.
    globusadd = models.CharField(max_length=300, db_column='GLOBUSADD', blank=True)  # Field name made lowercase.
    jdl = models.CharField(max_length=180, db_column='JDL', blank=True)  # Field name made lowercase.
    jdltxt = models.CharField(max_length=1500, db_column='JDLTXT', blank=True)  # Field name made lowercase.
    version = models.CharField(max_length=180, db_column='VERSION', blank=True)  # Field name made lowercase.
    site = models.CharField(max_length=180, db_column='SITE')  # Field name made lowercase.
    region = models.CharField(max_length=180, db_column='REGION', blank=True)  # Field name made lowercase.
    gstat = models.CharField(max_length=180, db_column='GSTAT', blank=True)  # Field name made lowercase.
    tags = models.CharField(max_length=600, db_column='TAGS', blank=True)  # Field name made lowercase.
    cmd = models.CharField(max_length=600, db_column='CMD', blank=True)  # Field name made lowercase.
    lastmod = models.DateTimeField(db_column='LASTMOD')  # Field name made lowercase.
    errinfo = models.CharField(max_length=240, db_column='ERRINFO', blank=True)  # Field name made lowercase.
    nqueue = models.IntegerField(db_column='NQUEUE')  # Field name made lowercase.
    comment_field = models.CharField(max_length=1500, db_column='COMMENT_', blank=True)  # Field name made lowercase.
    appdir = models.CharField(max_length=1500, db_column='APPDIR', blank=True)  # Field name made lowercase.
    datadir = models.CharField(max_length=240, db_column='DATADIR', blank=True)  # Field name made lowercase.
    tmpdir = models.CharField(max_length=240, db_column='TMPDIR', blank=True)  # Field name made lowercase.
    wntmpdir = models.CharField(max_length=240, db_column='WNTMPDIR', blank=True)  # Field name made lowercase.
    dq2url = models.CharField(max_length=240, db_column='DQ2URL', blank=True)  # Field name made lowercase.
    special_par = models.CharField(max_length=240, db_column='SPECIAL_PAR', blank=True)  # Field name made lowercase.
    python_path = models.CharField(max_length=240, db_column='PYTHON_PATH', blank=True)  # Field name made lowercase.
    nodes = models.IntegerField(db_column='NODES')  # Field name made lowercase.
    status = models.CharField(max_length=30, db_column='STATUS', blank=True)  # Field name made lowercase.
    copytool = models.CharField(max_length=240, db_column='COPYTOOL', blank=True)  # Field name made lowercase.
    copysetup = models.CharField(max_length=600, db_column='COPYSETUP', blank=True)  # Field name made lowercase.
    releases = models.CharField(max_length=1500, db_column='RELEASES', blank=True)  # Field name made lowercase.
    sepath = models.CharField(max_length=1200, db_column='SEPATH', blank=True)  # Field name made lowercase.
    envsetup = models.CharField(max_length=600, db_column='ENVSETUP', blank=True)  # Field name made lowercase.
    copyprefix = models.CharField(max_length=480, db_column='COPYPREFIX', blank=True)  # Field name made lowercase.
    lfcpath = models.CharField(max_length=240, db_column='LFCPATH', blank=True)  # Field name made lowercase.
    seopt = models.CharField(max_length=1200, db_column='SEOPT', blank=True)  # Field name made lowercase.
    sein = models.CharField(max_length=1200, db_column='SEIN', blank=True)  # Field name made lowercase.
    seinopt = models.CharField(max_length=1200, db_column='SEINOPT', blank=True)  # Field name made lowercase.
    lfchost = models.CharField(max_length=240, db_column='LFCHOST', blank=True)  # Field name made lowercase.
    cloud = models.CharField(max_length=180, db_column='CLOUD', blank=True)  # Field name made lowercase.
    siteid = models.CharField(max_length=180, db_column='SITEID', blank=True)  # Field name made lowercase.
    proxy = models.CharField(max_length=240, db_column='PROXY', blank=True)  # Field name made lowercase.
    retry = models.CharField(max_length=30, db_column='RETRY', blank=True)  # Field name made lowercase.
    queuehours = models.IntegerField(db_column='QUEUEHOURS')  # Field name made lowercase.
    envsetupin = models.CharField(max_length=600, db_column='ENVSETUPIN', blank=True)  # Field name made lowercase.
    copytoolin = models.CharField(max_length=540, db_column='COPYTOOLIN', blank=True)  # Field name made lowercase.
    copysetupin = models.CharField(max_length=600, db_column='COPYSETUPIN', blank=True)  # Field name made lowercase.
    seprodpath = models.CharField(max_length=1200, db_column='SEPRODPATH', blank=True)  # Field name made lowercase.
    lfcprodpath = models.CharField(max_length=240, db_column='LFCPRODPATH', blank=True)  # Field name made lowercase.
    copyprefixin = models.CharField(max_length=1080, db_column='COPYPREFIXIN', blank=True)  # Field name made lowercase.
    recoverdir = models.CharField(max_length=240, db_column='RECOVERDIR', blank=True)  # Field name made lowercase.
    memory = models.IntegerField(db_column='MEMORY')  # Field name made lowercase.
    maxtime = models.IntegerField(db_column='MAXTIME')  # Field name made lowercase.
    space = models.IntegerField(db_column='SPACE')  # Field name made lowercase.
    tspace = models.DateTimeField(db_column='TSPACE')  # Field name made lowercase.
    cmtconfig = models.CharField(max_length=750, db_column='CMTCONFIG', blank=True)  # Field name made lowercase.
    setokens = models.CharField(max_length=240, db_column='SETOKENS', blank=True)  # Field name made lowercase.
    glexec = models.CharField(max_length=30, db_column='GLEXEC', blank=True)  # Field name made lowercase.
    priorityoffset = models.CharField(max_length=180, db_column='PRIORITYOFFSET', blank=True)  # Field name made lowercase.
    allowedgroups = models.CharField(max_length=300, db_column='ALLOWEDGROUPS', blank=True)  # Field name made lowercase.
    defaulttoken = models.CharField(max_length=300, db_column='DEFAULTTOKEN', blank=True)  # Field name made lowercase.
    pcache = models.CharField(max_length=300, db_column='PCACHE', blank=True)  # Field name made lowercase.
    validatedreleases = models.CharField(max_length=1500, db_column='VALIDATEDRELEASES', blank=True)  # Field name made lowercase.
    accesscontrol = models.CharField(max_length=60, db_column='ACCESSCONTROL', blank=True)  # Field name made lowercase.
    dn = models.CharField(max_length=300, db_column='DN', blank=True)  # Field name made lowercase.
    email = models.CharField(max_length=180, db_column='EMAIL', blank=True)  # Field name made lowercase.
    allowednode = models.CharField(max_length=240, db_column='ALLOWEDNODE', blank=True)  # Field name made lowercase.
    maxinputsize = models.IntegerField(null=True, db_column='MAXINPUTSIZE', blank=True)  # Field name made lowercase.
    timefloor = models.IntegerField(null=True, db_column='TIMEFLOOR', blank=True)  # Field name made lowercase.
    depthboost = models.IntegerField(null=True, db_column='DEPTHBOOST', blank=True)  # Field name made lowercase.
    idlepilotsupression = models.IntegerField(null=True, db_column='IDLEPILOTSUPRESSION', blank=True)  # Field name made lowercase.
    pilotlimit = models.IntegerField(null=True, db_column='PILOTLIMIT', blank=True)  # Field name made lowercase.
    transferringlimit = models.IntegerField(null=True, db_column='TRANSFERRINGLIMIT', blank=True)  # Field name made lowercase.
    cachedse = models.IntegerField(null=True, db_column='CACHEDSE', blank=True)  # Field name made lowercase.
    corecount = models.IntegerField(null=True, db_column='CORECOUNT', blank=True)  # Field name made lowercase.
    countrygroup = models.CharField(max_length=192, db_column='COUNTRYGROUP', blank=True)  # Field name made lowercase.
    availablecpu = models.CharField(max_length=192, db_column='AVAILABLECPU', blank=True)  # Field name made lowercase.
    availablestorage = models.CharField(max_length=192, db_column='AVAILABLESTORAGE', blank=True)  # Field name made lowercase.
    pledgedcpu = models.CharField(max_length=192, db_column='PLEDGEDCPU', blank=True)  # Field name made lowercase.
    pledgedstorage = models.CharField(max_length=192, db_column='PLEDGEDSTORAGE', blank=True)  # Field name made lowercase.
    statusoverride = models.CharField(max_length=768, db_column='STATUSOVERRIDE', blank=True)  # Field name made lowercase.
    allowdirectaccess = models.CharField(max_length=30, db_column='ALLOWDIRECTACCESS', blank=True)  # Field name made lowercase.
    gocname = models.CharField(max_length=192, db_column='GOCNAME', blank=True)  # Field name made lowercase.
    tier = models.CharField(max_length=45, db_column='TIER', blank=True)  # Field name made lowercase.
    multicloud = models.CharField(max_length=192, db_column='MULTICLOUD', blank=True)  # Field name made lowercase.
    lfcregister = models.CharField(max_length=30, db_column='LFCREGISTER', blank=True)  # Field name made lowercase.
    stageinretry = models.IntegerField(null=True, db_column='STAGEINRETRY', blank=True)  # Field name made lowercase.
    stageoutretry = models.IntegerField(null=True, db_column='STAGEOUTRETRY', blank=True)  # Field name made lowercase.
    fairsharepolicy = models.CharField(max_length=1536, db_column='FAIRSHAREPOLICY', blank=True)  # Field name made lowercase.
    allowfax = models.CharField(null=True, max_length=64, db_column='ALLOWFAX', blank=True)  # Field name made lowercase.
    faxredirector = models.CharField(null=True, max_length=256, db_column='FAXREDIRECTOR', blank=True)  # Field name made lowercase.
    maxwdir = models.IntegerField(null=True, db_column='MAXWDIR', blank=True)  # Field name made lowercase.
    celist = models.CharField(max_length=12000, db_column='CELIST', blank=True)  # Field name made lowercase.
    minmemory = models.IntegerField(null=True, db_column='MINMEMORY', blank=True)  # Field name made lowercase.
    maxmemory = models.IntegerField(null=True, db_column='MAXMEMORY', blank=True)  # Field name made lowercase.
    mintime = models.IntegerField(null=True, db_column='MINTIME', blank=True)  # Field name made lowercase.
    allowjem = models.CharField(null=True, max_length=64, db_column='ALLOWJEM', blank=True)  # Field name made lowercase.
    catchall = models.CharField(null=True, max_length=512, db_column='CATCHALL', blank=True)  # Field name made lowercase.
    faxdoor = models.CharField(null=True, max_length=128, db_column='FAXDOOR', blank=True)  # Field name made lowercase.
    wansourcelimit = models.IntegerField(null=True, db_column='WANSOURCELIMIT', blank=True)  # Field name made lowercase.
    wansinklimit = models.IntegerField(null=True, db_column='WANSINKLIMIT', blank=True)  # Field name made lowercase.

    def __str__(self):
        return 'Schedconfig:' + str(self.nickname)

    def getFields(self):
        return ["name", "nickname", "queue", "localqueue", "system", \
                "sysconfig", "environ", "gatekeeper", "jobmanager", "se", "ddm", \
                "jdladd", "globusadd", "jdl", "jdltxt", "version", "site", \
                "region", "gstat", "tags", "cmd", "lastmod", "errinfo", \
                "nqueue", "comment_", "appdir", "datadir", "tmpdir", "wntmpdir", \
                "dq2url", "special_par", "python_path", "nodes", "status", \
                "copytool", "copysetup", "releases", "sepath", "envsetup", \
                "copyprefix", "lfcpath", "seopt", "sein", "seinopt", "lfchost", \
                "cloud", "siteid", "proxy", "retry", "queuehours", "envsetupin", \
                "copytoolin", "copysetupin", "seprodpath", "lfcprodpath", \
                "copyprefixin", "recoverdir", "memory", "maxtime", "space", \
                "tspace", "cmtconfig", "setokens", "glexec", "priorityoffset", \
                "allowedgroups", "defaulttoken", "pcache", "validatedreleases", \
                "accesscontrol", "dn", "email", "allowednode", "maxinputsize", \
                 "timefloor", "depthboost", "idlepilotsupression", "pilotlimit", \
                 "transferringlimit", "cachedse", "corecount", "countrygroup", \
                 "availablecpu", "availablestorage", "pledgedcpu", \
                 "pledgedstorage", "statusoverride", "allowdirectaccess", \
                 "gocname", "tier", "multicloud", "lfcregister", "stageinretry", \
                 "stageoutretry", "fairsharepolicy", "allowfax", "faxredirector", \
                 "maxwdir", "celist", "minmemory", "maxmemory", "mintime"]

    def getValuesList(self):
        repre = []
        for field in self._meta.fields:
#            print field.name
#        for field in self.getFields():
#            repre.append((field, self.__dict__[field]))
            repre.append((field.name, field))
        return repre

    def get_all_fields(self):
        """Returns a list of all field names on the instance."""
        fields = []
        kys = {}
        for f in self._meta.fields:
            kys[f.name] = f
        kys1 = kys.keys()
        kys1.sort()
        for k in kys1:
            f = kys[k]
            fname = f.name
            # resolve picklists/choices, with get_xyz_display() function
            get_choice = 'get_'+fname+'_display'
            if hasattr( self, get_choice):
                value = getattr( self, get_choice)()
            else:
                try :
                    value = getattr(self, fname)
                except:
                    value = None

            # only display fields with values and skip some fields entirely
            if f.editable and value :

                fields.append(
                  {
                   'label':f.verbose_name,
                   'name':f.name,
                   'value':value,
                  }
                )
        return fields

    def save(self, *args, **kwargs):
        raise NotImplementedError('Read only')

    def delete(self, *args, **kwargs):
         return

    class Meta:
        app_label = 'panda'
        db_table = u'"ATLAS_PANDAMETA"."SCHEDCONFIG"'