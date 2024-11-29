
import ast
import logging
import re
import os
import sys
import time
import json
import math
import requests
from requests.exceptions import ConnectionError
from string import Template

from django.core.exceptions import ObjectDoesNotExist
from atlas.prodtask.models import StepExecution, StepTemplate, ProductionTag, PhysicsContainer, TDataFormat, TProject, \
    TTrfConfig, ProductionTask
from ..settings import amiclient as ami_settings

logger = logging.getLogger('prodtaskwebui')


class AMIException(Exception):
    def __init__(self, errors):
        self.errors = errors
        self.message = '\n'.join(errors)
        super(AMIException, self).__init__(self.message)

    def has_error(self, error):
        for e in self.errors or []:
            if str(error).lower() in e.lower():
                return True


# noinspection PyBroadException
class AMIClient(object):
    def __init__(self, cert=ami_settings.CERTIFICAT,
                 base_url=ami_settings.AMI_API_V2_BASE_URL,
                 base_url_replica=ami_settings.AMI_API_V2_BASE_URL_REPLICA):
        """Initializes new instance of AMIClient class

        :param cert: a tuple of certificate and private key file paths, ('/path/usercert.pem', '/path/userkey.pem')
        :param base_url: AMI REST API base url
        :param base_url_replica: AMI REST API base url (CERN replica)
        """

        try:
            self._default_base_url = base_url
            self._default_base_url_replica = base_url_replica
            self._cert = cert
            self._acquire_token()
        except Exception as ex:
            logger.exception('AMI initialization failed: {0}'.format(str(ex)))

    def _acquire_token(self, use_replica=False):
        self._verify_server_cert = ami_settings.CA_CERTIFICATES

        current_base_url = self._default_base_url
        response = None
        if not use_replica:
            try:
                response = requests.get('{0}token/certificate'.format(self._default_base_url), cert=self._cert,
                                        verify=self._verify_server_cert)
            except ConnectionError as ex:
                logger.exception('AMI authentication error: {0}'.format(str(ex)))
                use_replica = True
        if use_replica or (response is not None and response.status_code != requests.codes.ok):
            logger.warning('Access token acquisition error try to reconnect')
            self._verify_server_cert = ami_settings.CA_CERTIFICATES
            current_base_url = self._default_base_url_replica
            response = requests.get('{0}token/certificate'.format(current_base_url), cert=self._cert,
                                    verify=self._verify_server_cert)
            if response.status_code != requests.codes.ok:
                response.raise_for_status()
        self._headers = {'Content-Type': 'application/json', 'AMI-Token': response.text}
        self._base_url = current_base_url
        logger.info('AMIClient, currentUser={0}'.format(self.get_current_user()))

    def _get_url(self, command):
        return '{0}command/{1}/json'.format(self._base_url, command)

    @staticmethod
    def _get_rows(content, rowset_type=None):
        rows = list()
        for rowset in content['AMIMessage']['rowset']:
            if rowset_type is None or rowset.get('@type') == rowset_type:
                for row in rowset['row']:
                    row_dict = dict()
                    for field in row.get('field', []):
                        row_dict.update({field['@name']: field.get('$', 'NULL')})
                    rows.append(row_dict)
        return rows

    @staticmethod
    def raise_for_errors(content):
        errors = list()
        for error in [e.get('$') for e in content['AMIMessage'].get('error', [])]:
            if error is not None:
                errors.append(error)
        if len(errors) > 0:
            raise AMIException(errors)

    def _post_command(self, command, rowset_type=None, **kwargs):

        response = None
        try:
            url = self._get_url(command)
            response = requests.post(url, headers=self._headers, data=json.dumps(kwargs),
                                     verify=self._verify_server_cert)
        except Exception as ex:
            logger.exception("AMI failed: %s" % str(ex))
        if response is None or response.status_code == 403:
            logger.warning('Access error, try to re-connect')
            self._acquire_token(response is None)
            url = self._get_url(command)
            response = requests.post(url, headers=self._headers, data=json.dumps(kwargs),
                                     verify=self._verify_server_cert)
        if response.status_code != requests.codes.ok:
            response.raise_for_status()
        content = json.loads(response.content)
        self.raise_for_errors(content)
        return self._get_rows(content, rowset_type)

    def _get_command(self, command):
        url = self._get_url(command).replace('json', 'help/json')
        # url='https://ami.in2p3.fr/AMI/api/'
        response = requests.get(url, headers=self._headers, verify=ami_settings.CA_CERTIFICATES)
        if response.status_code != requests.codes.ok:
            response.raise_for_status()
        content = json.loads(response.content)
        return content

    def create_physics_container(self, super_tag: str, contained_datasets: [str], creation_comment: str):
        datasets = []
        for dataset in contained_datasets:
            if ':' not in dataset:
                datasets.append(dataset)
            else:
                datasets.append(dataset.split(':')[1])
        datasets_str = ','.join(datasets)
        return self._post_command('COMAPopulateSuperProductionDataset', None, superTag=super_tag,
                                  containedDatasets=datasets_str,
                                  separator=',', creationComment=creation_comment, selectionType='run_config',
                                  rucioRegistration='yes')

    def get_current_user(self):
        result = self._post_command('GetUserInfo')
        return str(result[0]['AMIUser'])

    def list_containers_for_hashtag(self, scope, name):
        containers = list()
        result = self._post_command('DatasetWBListDatasetsForHashtag', scope=scope, name=name)
        for row in result:
            containers.append(row['ldn'])
        return containers

    def add_hashtag_for_container(self, scope, name, dsn, comment=None, pattern='AMI_GLOBAL_SCOPE'):
        if comment is None:
            result = self._post_command('DatasetWBAddHashtag', pattern=pattern, scope=scope, name=name, ldn=dsn)
        else:
            result = self._post_command('DatasetWBAddHashtag', pattern=pattern, scope=scope, name=name, ldn=dsn,
                                        comment=comment)
        row_id = int(result[0]['id'])
        return row_id != 0

    def _ami_get_tag(self, tag_name):
        return self._post_command('AMIGetAMITagInfo', 'amiTagInfo', newStructure=True, amiTag=tag_name)

    def _ami_get_tag_old(self, tag_name):
        return self._post_command('AMIGetAMITagInfo', 'amiTagInfo', oldStructure=True, amiTag=tag_name)

    def _ami_get_tag_new(self, tag_name):
        return self._post_command('AMIGetAMITagInfo', 'amiTagInfo', hierarchicalView=True, amiTag=tag_name)

    def _ami_get_tag_flat(self, tag_name):
        result = self._post_command('AMIGetAMITagInfoNew', 'amiTagInfo', amiTag=tag_name)
        ami_tag = result[0]
        ami_tag['transformationName'] = ami_tag['transformName']
        return [ami_tag, ]

    def _ami_list_phys_containers(self, created_after=None):
        fields = [
            'logicalDatasetName',
            'created',
            'lastModified',
            'createdBy',
            'projectName',
            'dataType',
            'runNumber',
            'streamName',
            'prodStep'
        ]

        if created_after:
            conditions = \
                'WHERE (`ATLAS_AMI_DATASUPER_01`.`DATASET`.`AMISTATUS`=\'VALID\') ' + \
                'AND (`ATLAS_AMI_DATASUPER_01`.`DATASET`.`CREATED` >= TO_DATE(\'{0}\', \'YYYY-MM-DD\')) '.format(
                    created_after.strftime('%Y-%m-%d'))
        else:
            conditions = 'WHERE `ATLAS_AMI_DATASUPER_01`.`DATASET`.`AMISTATUS`=\'VALID\' '

        query = \
            'SELECT {0} FROM `ATLAS_AMI_DATASUPER_01`.`DATASET` '.format(','.join(
                ['`ATLAS_AMI_DATASUPER_01`.`DATASET`.`{0}`'.format(field.upper()) for field in fields])) + \
            conditions + \
            'ORDER BY `ATLAS_AMI_DATASUPER_01`.`DATASET`.`CREATED` ASC'

        return self._post_command('SearchQuery',
                                  catalog='dataSuper_001:real_data',
                                  entity='dataset',
                                  sql='{0}'.format(query))

    def _ami_list_projects(self, patterns):
        conditions = ' OR '.join(
            ['`ATLAS_AMI_PRODUCTION_01`.`PROJECTS`.`PROJECTTAG` like \'{0}\''.format(p) for p in patterns or []])

        if conditions:
            conditions = '({0}) AND '.format(conditions)

        query = \
            'SELECT `ATLAS_AMI_PRODUCTION_01`.`PROJECTS`.`PROJECTTAG` AS tag, ' + \
            '`ATLAS_AMI_PRODUCTION_01`.`PROJECTS`.`DESCRIPTION` AS description, ' + \
            '`ATLAS_AMI_PRODUCTION_01`.`PROJECTS`.`WRITESTATUS` AS write_status ' + \
            'FROM `ATLAS_AMI_PRODUCTION_01`.`PROJECTS` WHERE {0}'.format(conditions) + \
            '(`ATLAS_AMI_PRODUCTION_01`.`PROJECTS`.`READSTATUS`=\'valid\') ' + \
            'ORDER BY `ATLAS_AMI_PRODUCTION_01`.`PROJECTS`.`PROJECTTAG`, ' + \
            '`ATLAS_AMI_PRODUCTION_01`.`PROJECTS`.`DESCRIPTION`, ' + \
            '`ATLAS_AMI_PRODUCTION_01`.`PROJECTS`.`WRITESTATUS`'

        return self._post_command('SearchQuery',
                                  catalog='Atlas_Production:Atlas_Production',
                                  entity='projects',
                                  sql='{0}'.format(query))

    def _ami_list_types(self):
        query = \
            'SELECT `ATLAS_AMI_PRODUCTION_01`.`DATA_TYPE`.`DATATYPE` AS name, ' + \
            '`ATLAS_AMI_PRODUCTION_01`.`DATA_TYPE`.`DESCRIPTION` AS description, ' + \
            '`ATLAS_AMI_PRODUCTION_01`.`DATA_TYPE`.`WRITESTATUS` AS write_status ' + \
            'FROM `ATLAS_AMI_PRODUCTION_01`.`DATA_TYPE` ' + \
            'WHERE `ATLAS_AMI_PRODUCTION_01`.`DATA_TYPE`.`READSTATUS`=\'valid\' ' + \
            'ORDER BY `ATLAS_AMI_PRODUCTION_01`.`DATA_TYPE`.`DATATYPE`, ' + \
            '`ATLAS_AMI_PRODUCTION_01`.`DATA_TYPE`.`DESCRIPTION`, ' + \
            '`ATLAS_AMI_PRODUCTION_01`.`DATA_TYPE`.`WRITESTATUS`'

        return self._post_command('SearchQuery',
                                  catalog='Atlas_Production:Atlas_Production',
                                  entity='DATA_TYPE',
                                  sql='{0}'.format(query))

    def ami_list_tags(self, trf_name, trf_release):
        query = \
            "SELECT * WHERE (`transformationName` = '{0}') and (`cacheName` = '{1}')".format(trf_name, trf_release)

        return self._post_command('SearchQuery',
                                  catalog='AMITags:production',
                                  entity='V_AMITags',
                                  mql='{0}'.format(query))

    def get_nevents_per_file(self, dataset):
        dataset = dataset.split(':')[-1].strip('/')
        tid_pattern = r'(?P<tid>_tid\d+_\d{2})'
        if re.match(r'^.*{0}$'.format(tid_pattern), dataset):
            dataset = re.sub(tid_pattern, '', dataset)
        result = self._post_command('AMIGetDatasetInfo', logicalDatasetName=dataset)
        nfiles = float(result[0]['nFiles'])
        if nfiles == 0:
            return 0
        total_events = float(result[0]['totalEvents'])
        return math.ceil(total_events / nfiles)

    @staticmethod
    def get_types():
        return [e.name for e in TDataFormat.objects.all()]

    def ami_get_params(self, cache, release, trf_name):
        result = self._post_command('GetParamsForTransform',
                                    'params',
                                    releaseName='{0}_{1}'.format(cache, release),
                                    transformName=trf_name)
        trf_params = list()
        for param in result:
            name = param['paramName']
            if not name.startswith('--'):
                name = "--%s" % name
            trf_params.append(name)

        return trf_params

    @staticmethod
    def is_new_ami_tag(ami_tag):
        if 'notAKTR' in list(ami_tag.keys()) and ami_tag['notAKTR']:
            return True
        else:
            return False

    @staticmethod
    def apply_phconfig_ami_tag(ami_tag):
        if 'phconfig' in ami_tag:
            phconfig_dict = eval(ami_tag['phconfig'])
            for config_key in list(phconfig_dict.keys()):
                if isinstance(phconfig_dict[config_key], dict):
                    value_list = list()
                    for key in list(phconfig_dict[config_key].keys()):
                        if isinstance(phconfig_dict[config_key][key], list):
                            for value in ['{0}:{1}'.format(key, ss) for ss in phconfig_dict[config_key][key]]:
                                value_list.append(value)
                        else:
                            value = phconfig_dict[config_key][key]
                            value_list.append("%s:%s" % (key, value))
                    config_value = ' '.join([json.dumps(e) for e in value_list])
                elif isinstance(phconfig_dict[config_key], list):
                    config_value = ' '.join([json.dumps(e) for e in phconfig_dict[config_key]])
                else:
                    config_value = json.dumps(phconfig_dict[config_key])
                logger.debug("apply phconfig key=value: %s=%s" % (config_key, config_value))
                for key in list(ami_tag.keys()):
                    if key.lower() == config_key.lower():
                        ami_tag[key] = config_value
                ami_tag.update({config_key: config_value})
                if config_key.lower() == 'geometryversion':
                    ami_tag['Geometry'] = 'none'

    def get_ami_tag_owner(self, tag_name):
        result = self._ami_get_tag(tag_name)
        ami_tag = result[0]
        return [ami_tag['createdBy'], ami_tag['created']]

    def get_ami_tag_tzero(self, tag_name):
        result = self._ami_get_tag_new(tag_name)
        tzero_tag = result[0]['dict']
        return tzero_tag

    def get_ami_tag(self, tag_name):
        ami_tag = dict()

        try:
            if tag_name[0] in ['y']:
                result = self._ami_get_tag_new(tag_name)
            else:
                result = self._ami_get_tag_old(tag_name)
            ami_tag = result[0]
        except AMIException as ex:
            if ex.has_error('Invalid amiTag found'):
                try:
                    if tag_name.startswith('z500'):
                        result = self._ami_get_tag_flat(tag_name)
                    else:
                        result = self._ami_get_tag(tag_name)
                    ami_tag = result[0]
                    if str(ami_tag['transformationName']).endswith('.py'):
                        ami_tag['transformation'] = '{0}'.format(ami_tag['transformationName'])
                    else:
                        ami_tag['transformation'] = '{0}.py'.format(ami_tag['transformationName'])
                    ami_tag['SWReleaseCache'] = '{0}_{1}'.format(ami_tag['groupName'], ami_tag['cacheName'])
                except Exception as ex:
                    logger.exception('[1] Exception: {0}'.format(str(ex)))
            elif ex.has_error('[Errno 111] Connection refused'):
                raise
            else:
                logger.exception('AMIException: {0}'.format(ex.message))
        except Exception as ex:
            logger.exception('[2] Exception: {0}'.format(str(ex)))

        try:
            prodsys_tag = TTrfConfig.objects.get(tag=tag_name[0], cid=int(tag_name[1:]))

            if not ami_tag:
                ami_tag['transformation'] = prodsys_tag.trf
                ami_tag['SWReleaseCache'] = '{0}_{1}'.format(prodsys_tag.cache, prodsys_tag.trf_version)
                ami_tag.update(dict(list(zip(prodsys_tag.lparams.split(','), prodsys_tag.vparams.split(',')))))

            ami_tag['productionStep'] = prodsys_tag.prod_step
            ami_tag['notAKTR'] = False
        except ObjectDoesNotExist:
            logger.info('The tag {0} is not found in AKTR'.format(tag_name))
            if ami_tag:
                ami_tag['notAKTR'] = True
        except Exception as ex:
            logger.exception('Exception: {0}'.format(str(ex)))

        if not ami_tag:
            raise Exception('The configuration tag \"{0}\" is not registered'.format(tag_name))

        return ami_tag

    def get_ami_tag_prodsys(self, tag_name):
        return self._post_command('AMIGetAMITagInfo', 'amiTagInfo', amiTag=tag_name)[0]

    def set_ami_tag_invalid(self, tag_name):
        return self._post_command('SetAMITagStatus', None, amiTag=tag_name, status='invalid')

    def check_trf_params_in_ami_tag(self, tag_name, trf_params):
        ami_tag_params = []
        try:
            ami_tag_params = list(
                list(json.loads(self._ami_get_tag_new(tag_name)[0]['dict'])['transformation']['args'].keys()))
        except Exception as ex:
            logger.error('Error getting ami tags params: {0}'.format(str(ex)))
        cleaned_trf_params = [x.replace('--', '') for x in trf_params]
        for trf_param in ami_tag_params:
            if trf_param not in cleaned_trf_params:
                raise Exception(
                    'The parameter \"{0}\" is not found in the trf for the AMI tag \"{1}\"'.format(trf_param, tag_name))
        return True

    @staticmethod
    def _read_trf_params(fp):
        trf_params = list()
        for source_line in fp.read().splitlines():
            source_line = source_line.replace(' ', '')
            if 'ListOfDefaultPositionalKeys='.lower() in source_line.lower():
                trf_params.extend(ast.literal_eval(source_line.split('=')[-1]))
                break
        return trf_params

    @staticmethod
    def _trf_dump_args(list_known_path, trf_transform_path):
        list_known_python_path = list()
        for path in list_known_path:
            old_str_pattern = re.compile(re.escape('share/bin'), re.IGNORECASE)
            known_python_path = old_str_pattern.sub('python', os.path.dirname(path))
            if known_python_path and os.path.exists(known_python_path):
                list_known_python_path.append(known_python_path)
        for path in list_known_python_path:
            sys.path.append(path)

        trf_transform = os.path.basename(trf_transform_path)
        sys.path.append(os.path.dirname(trf_transform_path))
        trf_module = __import__(os.path.splitext(trf_transform)[0])
        if not hasattr(trf_module, 'getTransform'):
            raise Exception('The module {0} does not support for dumpArgs'.format(trf_transform))
        get_transform_method = getattr(trf_module, 'getTransform')
        trf = get_transform_method()
        list_key = ['--' + str(key) for key in trf.parser.allArgs if
                    key not in ('h', 'verbose', 'loglevel', 'dumpargs', 'argdict')]
        list_key.sort()
        return list_key

    @staticmethod
    def _trf_retrieve_sub_steps(list_known_path, trf_transform_path):
        list_known_python_path = list()
        for path in list_known_path:
            old_str_pattern = re.compile(re.escape('share/bin'), re.IGNORECASE)
            known_python_path = old_str_pattern.sub('python', os.path.dirname(path))
            if known_python_path and os.path.exists(known_python_path):
                list_known_python_path.append(known_python_path)
        for path in list_known_python_path:
            sys.path.append(path)

        trf_transform = os.path.basename(trf_transform_path)
        sys.path.append(os.path.dirname(trf_transform_path))
        trf_module = __import__(os.path.splitext(trf_transform)[0])
        if not hasattr(trf_module, 'getTransform'):
            raise Exception('The module {0} does not support for dumpArgs'.format(trf_transform))
        get_transform_method = getattr(trf_module, 'getTransform')
        trf = get_transform_method()
        if not hasattr(trf, 'executors'):
            raise Exception('The module {0} does not support for executors list'.format(trf_transform))
        executor_list = list()
        for executor in trf.executors:
            if executor.name:
                executor_list.append(executor.name)
            if executor.substep:
                executor_list.append(executor.substep)
        del sys.modules[os.path.splitext(trf_transform)[0]]
        return executor_list

    def get_trf_params(self, trf_cache, trf_release, trf_transform, sub_step_list=False, force_dump_args=False,
                       force_ami=False):
        root = '/afs/cern.ch/atlas/software/releases'
        trf_path_t = Template("$root/$base_rel/$cache/$rel/InstallArea/share/bin/$trf")
        trf_release_parts = trf_release.split('.')

        mapping = {'root': root, 'trf': trf_transform}
        list_known_path = list()

        list_known_path.append(trf_transform)

        mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                        'rel': trf_release,
                        'cache': trf_cache})
        list_known_path.append(trf_path_t.substitute(mapping))

        if len(trf_release.split('.')) == 5:
            mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                            'rel': '.'.join(trf_release_parts[:4]),
                            'cache': 'AtlasProduction'})
            list_known_path.append(trf_path_t.substitute(mapping))

        mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                        'rel': '.'.join(trf_release_parts[:3]),
                        'cache': 'AtlasOffline'})
        list_known_path.append(trf_path_t.substitute(mapping))

        mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                        'rel': '.'.join(trf_release_parts[:3]),
                        'cache': 'AtlasReconstruction'})
        list_known_path.append(trf_path_t.substitute(mapping))

        mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                        'rel': '.'.join(trf_release_parts[:3]),
                        'cache': 'AtlasCore'})
        list_known_path.append(trf_path_t.substitute(mapping))

        mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                        'rel': '.'.join(trf_release_parts[:3]),
                        'cache': 'AtlasTrigger'})
        list_known_path.append(trf_path_t.substitute(mapping))

        if len(trf_release.split('.')) == 5:
            mapping.update({'base_rel': '.'.join(trf_release_parts[:3]),
                            'rel': '.'.join(trf_release_parts[:4]),
                            'cache': 'AtlasP1HLT'})
            list_known_path.append(trf_path_t.substitute(mapping))

        trf_params = list()
        trf_transform_path = None

        for path in list_known_path:
            if not os.path.exists(path):
                continue
            with open(path, 'r') as fp:
                params = self._read_trf_params(fp)
                trf_transform_path = path
                if not params:
                    continue
                trf_params.extend(params)
                break

        if (not trf_params) or force_dump_args:
            try:
                trf_params = self._trf_dump_args(list_known_path, trf_transform_path)
            except Exception as ex:
                logger.debug("_trf_dump_args failed: %s" % str(ex))

        if ((not trf_params) or force_ami) and '_tf.' in trf_transform:
            try:
                trf_params = self.ami_get_params(trf_cache, trf_release, trf_transform)
            except Exception as ex:
                logger.exception("ami_get_params failed: %s" % str(ex))
        sub_steps = None
        if sub_step_list:
            # old way from PS1
            if ((trf_transform.lower() in [e.lower() for e in ['AtlasG4_tf.py', 'Sim_tf.py', 'StoppedParticleG4_tf.py',
                                                               'TrigFTKMergeReco_tf.py', 'Reco_tf.py',
                                                               'FullChain_tf.py', 'Trig_reco_tf.py',
                                                               'TrigMT_reco_tf.py',
                                                               'OverlayChain_tf.py', 'TrigFTKTM64SM1Un_tf.py',
                                                               'TrigFTKSMUn_Tower22_tf.py', 'Digi_tf.py',
                                                               'HITSMerge_tf.py',
                                                               'AODMerge_tf.py', 'EVNTMerge_tf.py' , 'HISTPostProcess_tf.py']]) or
                    trf_transform.lower().endswith('Merge_tf.py'.lower())):
                default_sub_steps = ['AODtoRED', 'FTKRecoRDOtoESD', 'all', 'n2n', 'AODtoHIST', 'DQHistogramMerge',
                                     'NTUPtoRED', 'SPSim', 'AODtoTAG', 'AtlasG4Tf', 'ESDtoAOD', 'e2d', 'e2a',
                                     'AODtoDPD', 'RAWtoALL',
                                     'sim', 'a2r', 'ESDtoDPD', 'r2e', 'a2d', 'HITtoRDO', 'RAWtoESD', 'default',
                                     'EVNTtoHITS', 'h2r', 'SPGenerator', 'first', 'BSRDOtoRAW', 'b2r', 'OverlayBS',
                                     'RDOFTKCreator', 'AODFTKCreator', 'RDOtoRDOTrigger', 'Overlay']
                sub_steps = default_sub_steps

        return trf_params, sub_steps

    def sync_ami_projects(self):
        try:
            ami_projects = self._ami_list_projects(['valid%', 'data%', 'mc%', 'user%'])
            project_names = [e.project for e in TProject.objects.all()]
            for ami_project in ami_projects:
                if ami_project['write_status'.upper()] != 'valid':
                    continue
                if not ami_project['tag'.upper()] in project_names:
                    description = None
                    if str(ami_project['description'.upper()]) != '@NULL':
                        description = str(ami_project['description'.upper()])
                    timestamp = int(time.time())
                    new_project = TProject(project=ami_project['tag'.upper()],
                                           status='active',
                                           description=description,
                                           timestamp=timestamp)
                    new_project.save()
                    logger.info(
                        'The project \"{0}\" is registered (timestamp = {1})'.format(ami_project['tag'.upper()],
                                                                                     timestamp))
        except Exception as ex:
            logger.exception('sync_ami_projects, exception occurred: {0}'.format(str(ex)))

    def sync_ami_types(self):
        try:
            ami_types = self._ami_list_types()
            format_names = [e.name for e in TDataFormat.objects.all()]
            for ami_type in ami_types:
                if ami_type['write_status'.upper()] != 'valid':
                    continue
                if not ami_type['name'.upper()] in format_names:
                    description = None
                    if str(ami_type['description'.upper()]) != '@NULL':
                        description = str(ami_type['description'.upper()])
                    new_format = TDataFormat(name=ami_type['name'.upper()],
                                             description=description)
                    new_format.save()
                    logger.info('The data format \"{0}\" is registered'.format(ami_type['name'.upper()]))
        except Exception as ex:
            logger.exception('sync_ami_types, exception occurred: {0}'.format(str(ex)))

    def sync_ami_phys_containers(self):
        try:
            last_created = None
            try:
                last_created = PhysicsContainer.objects.latest('created').created
            except ObjectDoesNotExist:
                pass
            new_datasets = self._ami_list_phys_containers(created_after=last_created)
            if new_datasets:
                for dataset in new_datasets:
                    if not PhysicsContainer.objects.filter(pk=dataset['logicalDatasetName'.upper()]).exists():
                        new_phys_cont = PhysicsContainer()
                        new_phys_cont.name = dataset['logicalDatasetName'.upper()]
                        new_phys_cont.created = dataset['created'.upper()]
                        new_phys_cont.last_modified = dataset['lastModified'.upper()]
                        new_phys_cont.username = dataset['createdBy'.upper()]
                        new_phys_cont.project = dataset['projectName'.upper()]
                        new_phys_cont.data_type = dataset['dataType'.upper()]
                        new_phys_cont.run_number = dataset['runNumber'.upper()]
                        new_phys_cont.stream_name = dataset['streamName'.upper()]
                        new_phys_cont.prod_step = dataset['prodStep'.upper()]
                        new_phys_cont.save()
                        logger.info(
                            'New physics container \"{0}\" is registered'.format(dataset['logicalDatasetName'.upper()]))
        except Exception as ex:
            logger.exception('sync_ami_phys_containers, exception occurred: {0}'.format(str(ex)))

    def sync_ami_tags(self):
        try:
            last_step_template_id = 0
            try:
                last_step_template_id = ProductionTag.objects.latest('step_template_id').step_template_id
            except ObjectDoesNotExist:
                pass
            result = StepTemplate.objects.filter(
                id__gt=last_step_template_id).order_by('id').values('id', 'ctag').distinct()
            for step_template in result:
                try:
                    step = StepExecution.objects.filter(step_template__id=step_template['id']).first()
                    if not step:
                        continue
                    task = ProductionTask.objects.filter(step=step).first()
                    if not task or task.id < 400000:
                        continue
                    tag_name = step_template['ctag']
                except ObjectDoesNotExist:
                    continue
                if not ProductionTag.objects.filter(pk=tag_name).exists():
                    new_tag = ProductionTag()
                    new_tag.name = tag_name
                    new_tag.task_id = task.id
                    new_tag.step_template_id = step_template['id']
                    try:
                        ami_tag = self.get_ami_tag(tag_name)
                        new_tag.username, new_tag.created = self.get_ami_tag_owner(tag_name)
                    except Exception:
                        continue
                    new_tag.trf_name = ami_tag['transformation']
                    new_tag.trf_cache = ami_tag['SWReleaseCache'].split('_')[0]
                    new_tag.trf_release = ami_tag['SWReleaseCache'].split('_')[1]
                    new_tag.tag_parameters = json.dumps(ami_tag)
                    new_tag.save()
                    logger.info('New tag \"{0}\" is registered'.format(tag_name))
        except Exception as ex:
            logger.exception('sync_ami_tags, exception occurred: {0}'.format(str(ex)))

    def ami_container_exists(self, image_name):
        query = \
            "SELECT COUNT(*) WHERE `IMAGENAME` = '{0}'".format(image_name)

        container_numbers = list(self._post_command('SearchQuery',
                                                    catalog='Container:production',
                                                    entity='IMAGE_VIEW',
                                                    mql='{0}'.format(query))[0].values())[0]
        return container_numbers != '0'

    def ami_cmtconfig_by_image_name(self, image_name):
        query = \
            "SELECT * WHERE `IMAGENAME` = '{0}'".format(image_name)

        container = self._post_command('SearchQuery',
                                       catalog='Container:production',
                                       entity='IMAGE_VIEW',
                                       mql='{0}'.format(query))[0]

        sw_tag = container['IMAGEREPOSITORYSWTAG']

        query = \
            "SELECT * WHERE `TAGNAME` = '{0}'".format(sw_tag)

        sw_tag_dict = self._post_command('SearchQuery',
                                         catalog='Container:production',
                                         entity='SWTAG_VIEW',
                                         mql='{0}'.format(query))[0]

        return sw_tag_dict['IMAGEARCH'] + '-' + sw_tag_dict['IMAGEPLATFORM'] + '-' + sw_tag_dict['IMAGECOMPILER']

    def ami_cmtconfig_by_image_manifest(self, image_name):
        query = \
            "SELECT * WHERE `IMAGENAME` = '{0}'".format(image_name)

        container = self._post_command('SearchQuery',
                                       catalog='Container:production',
                                       entity='IMAGE_VIEW',
                                       mql='{0}'.format(query))[0]

        manifest_base = ''
        if container.get("IMAGEARCH") and container.get("IMAGEARCH") == 'manifest':
            manifest_base = image_name
        sw_tag = container['IMAGEREPOSITORYSWTAG']

        query = \
            "SELECT * WHERE `TAGNAME` = '{0}'".format(sw_tag)

        sw_tag_dict = self._post_command('SearchQuery',
                                         catalog='Container:production',
                                         entity='SWTAG_VIEW',
                                         mql='{0}'.format(query))[0]
        if not manifest_base:
            return [sw_tag_dict['IMAGEARCH'] + '-' + sw_tag_dict['IMAGEPLATFORM'] + '-' + sw_tag_dict['IMAGECOMPILER']]
        ami_images = self.ami_image_by_sw(sw_tag)
        archs = []
        for image in ami_images:
            if image['IMAGENAME'].startswith(manifest_base) and image['IMAGEARCH'] != 'manifest':
                archs.append(image['IMAGEARCH'])
        return [x + '-' + sw_tag_dict['IMAGEPLATFORM'] + '-' + sw_tag_dict['IMAGECOMPILER'] for x in archs]

    def ami_sw_tag_by_cache(self, cache):
        query = \
            "SELECT * WHERE LOWER(`SWRELEASE`) = LOWER('{0}')".format(cache)

        return self._post_command('SearchQuery',
                                  catalog='Container:production',
                                  entity='SWTAG_VIEW',
                                  mql='{0}'.format(query))

    def ami_image_by_sw(self, swtag):
        query = \
            "SELECT * WHERE `IMAGEREPOSITORYSWTAG` = '{0}'".format(swtag)

        return self._post_command('SearchQuery',
                                  catalog='Container:production',
                                  entity='IMAGE_VIEW',
                                  mql='{0}'.format(query))

    def ami_image_by_name(self, image_name):
        query = \
            "SELECT * WHERE `IMAGENAME` = '{0}'".format(image_name)

        return self._post_command('SearchQuery',
                                  catalog='Container:production',
                                  entity='IMAGE_VIEW',
                                  mql='{0}'.format(query))

    def ami_cmtconfig_by_image_by_name(self, image_name):
        query = \
            "SELECT * WHERE `IMAGENAME` = '{0}'".format(image_name)

        container = self._post_command('SearchQuery',
                                       catalog='Container:production',
                                       entity='IMAGE_VIEW',
                                       mql='{0}'.format(query))[0]

        sw_tag = container['IMAGEREPOSITORYSWTAG']

        query = \
            "SELECT * WHERE `TAGNAME` = '{0}'".format(sw_tag)

        sw_tag_dict = self._post_command('SearchQuery',
                                         catalog='Container:production',
                                         entity='SWTAG_VIEW',
                                         mql='{0}'.format(query))[0]

        return sw_tag_dict['IMAGEARCH'] + '-' + sw_tag_dict['IMAGEPLATFORM'] + '-' + sw_tag_dict['IMAGECOMPILER']
