import logging
import re
from typing import Dict, List

from atlas.ami.client import AMIClient


import os
import json
from pydoc import locate
from atlas.cric.client import CRICClient
from .protocol import TaskDefConstants

logger = logging.getLogger('deftcore')
class UnknownProjectModeOption(Exception):
    def __init__(self, option_key):
        super(UnknownProjectModeOption, self).__init__('Invalid project_mode option: {0}'.format(option_key))


class InvalidProjectModeOptionValue(Exception):
    def __init__(self, key, value):
        super(InvalidProjectModeOptionValue, self).__init__(
            'Invalid project_mode option value: {0}=\"{1}\"'.format(key, value))


class ProjectMode(object):
    def __init__(self, step, cache=None, use_nightly_release=False):
        """
        :param step: object of StepExecution
        :param cache: string in format 'CacheName-CacheRelease', for example, 'AtlasProduction-19.2.1.2'
        :return: project_mode dict
        """
        self.project_mode_dict = dict()
        self.cache = cache
        self.use_nightly_release = use_nightly_release
        self.agis_client = CRICClient()

        project_mode = dict()
        self.task_config = self.get_task_config(step)
        if 'project_mode' in list(self.task_config.keys()):
            project_mode.update(self._parse_project_mode(self.task_config['project_mode']))

        project_mode_options = self.get_options()

        option_names = {key.lower(): key for key in list(project_mode_options.keys())}

        for key in list(project_mode.keys()):
            if key not in list(option_names.keys()):
                raise UnknownProjectModeOption(key)
            option_type = locate(project_mode_options[option_names[key]]['type'])
            option_value = project_mode[key]
            if option_type == bool:
                if option_value == 'yes':
                    option_value = True
                elif option_value == 'no':
                    option_value = False
                else:
                    raise InvalidProjectModeOptionValue(option_names[key], option_value)
            setattr(self, option_names[key], option_type(option_value))
            self.project_mode_dict.update({option_names[key]: option_type(option_value)})
        self._cmt_config_list = []
        self._multiple_cmtconfig = False
        if self.cmtconfig and (',' in self.cmtconfig or '|' in self.cmtconfig):
            self._multiple_cmtconfig = True
        self.set_cmtconfig()
        self.set_cmtconfig_options()
        self.is_json_form = False
        if self.jsonArch:
            self.get_json_cmtconfig()
        if self.nvidia:
            self.set_nvidia_options()
        self.project_mode_dict['cmtconfig'] = self.cmtconfig

    def __getattr__(self, item):
        return None

    @staticmethod
    def get_options():
        path = '{0}{1}projectmode.json'.format(os.path.dirname(__file__), os.path.sep)
        with open(path, 'r') as fp:
            return json.loads(fp.read())

    @staticmethod
    def get_task_config(step):
        task_config = dict()
        if step.task_config:
            content = json.loads(step.task_config)
            for key in list(content.keys()):
                if content[key] is None or content[key] == '':
                    continue
                task_config.update({key: content[key]})
        return task_config

    @staticmethod
    def set_task_config(step, task_config, keys_to_save=None):
        if keys_to_save is None:
            step.task_config = json.dumps(task_config)
            step.save(update_fields=['task_config'])
        else:
            if len(keys_to_save) > 0:
                config = {key: task_config[key] for key in keys_to_save}
                if config:
                    step_task_config = ProjectMode.get_task_config(step)
                    step_task_config.update(config)
                    step.task_config = json.dumps(step_task_config)
                    step.save(update_fields=['task_config'])

    @staticmethod
    def _parse_project_mode(project_mode_string):
        project_mode_dict = dict()
        for option in project_mode_string.replace(' ', '').split(';'):
            if not option:
                continue
            if '=' not in option:
                raise Exception('The project_mode option \"{0}\" has invalid format. '.format(option) +
                                'Expected format is \"optionName=optionValue\"')
            project_mode_dict.update({option.split('=')[0].lower(): option[option.find('=')+1:]})
        return project_mode_dict

    def _is_cmtconfig_exist(self, cache, cmtconfig):
        agis_cmtconfig_list = self.agis_client.get_cmtconfig(cache)
        if not agis_cmtconfig_list:
            try:
                agis_cmtconfig_list = self._get_cmtconfig_from_cvmfs(cache)
            except:
                agis_cmtconfig_list = []
        if '|' in cmtconfig or ',' in cmtconfig:
            if len(agis_cmtconfig_list) == 1:
                return False
        for agis_cmtconfig in agis_cmtconfig_list:
            if re.search(f'^{cmtconfig}$', agis_cmtconfig):
                return True
        return False

    def _get_cmtconfig_list(self, cache):
        agis_cmtconfig_list = self.agis_client.get_cmtconfig(cache)
        return list(agis_cmtconfig_list)

    def _get_cmtconfig_from_cvmfs(self, cache):
        release = cache.split('-')[-1]
        project = cache.split('-')[0]
        path = TaskDefConstants.DEAFULT_SW_RELEASE_PATH.format(release=release,project=project,release_base=".".join(release.split(".")[:2]))
        cmt_config_from_cvmfs = [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
        return cmt_config_from_cvmfs

    @staticmethod
    def _get_cmtconfig_for_container(cache: str, container_name: str):
        release = cache.split('-')[-1]
        project = cache.split('-')[0]
        cmt_config_from_cvmfs = []
        container_name_base = '/'.join(container_name.split('/')[:-1])
        container_postfix = container_name.split('/')[-1]
        path_base = TaskDefConstants.DEAFULT_CONTAINER_BASE_RELEASE_PATH_BASE
        container_base_path = TaskDefConstants.DEAFULT_CONTAINER_BASE_RELEASE_PATH.format(base=path_base, container_name_base=container_name_base)
        if not os.path.exists(container_base_path):
            path_base = TaskDefConstants.DEAFULT_OLD_CONTAINER_BASE_RELEASE_PATH_BASE
            container_base_path = TaskDefConstants.DEAFULT_CONTAINER_BASE_RELEASE_PATH.format(
                base=path_base, container_name_base=container_name_base)
        archs_cvmfs = [name for name in os.listdir(container_base_path) if os.path.isdir(os.path.join(container_base_path, name))]
        archs = [x.replace(f'{container_postfix}', '') for x in archs_cvmfs if x.startswith(container_postfix)]
        if not archs:
            path_base = TaskDefConstants.DEAFULT_OLD_CONTAINER_BASE_RELEASE_PATH_BASE
            container_base_path = TaskDefConstants.DEAFULT_CONTAINER_BASE_RELEASE_PATH.format(
                base=path_base, container_name_base=container_name_base)
            archs_cvmfs = [name for name in os.listdir(container_base_path) if
                           os.path.isdir(os.path.join(container_base_path, name))]
            archs = ['' for x in archs_cvmfs if x.startswith(container_postfix)]
        for arch in archs:
            path = TaskDefConstants.DEAFULT_CONTAINER_RELEASE_PATH.format(base=path_base, release=release,project=project,container_name=container_name,arch=arch)
            if not os.path.exists(path):
                raise Exception(f'Path {path} does not exist, cahce {cache} is missing in CVMFS')
            cmt_config_from_cvmfs += [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
        return cmt_config_from_cvmfs


    def _check_cmtconfig_for_container(self, cache: str, container_name: str, archs: [str], user_cmt_config: str):
        release = cache.split('-')[-1]
        project = cache.split('-')[0]
        cmt_config_from_cvmfs = []
        for arch in archs:
            for base_path in [TaskDefConstants.DEAFULT_CONTAINER_BASE_RELEASE_PATH_BASE, TaskDefConstants.DEAFULT_OLD_CONTAINER_BASE_RELEASE_PATH_BASE]:
                path = TaskDefConstants.DEAFULT_CONTAINER_RELEASE_PATH.format(base=base_path, release=release,project=project,container_name=container_name,arch='-'+arch)
                if os.path.exists(path):
                    break
            if not os.path.exists(path):
                raise Exception(f'Path {path} does not exist, cahce {cache} is missing in CVMFS')
            cmt_config_from_cvmfs += [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
        for cmt_config in cmt_config_from_cvmfs:
            if not re.search(f'^{user_cmt_config}$', cmt_config):
                raise Exception(f' {cmt_config} does not match {user_cmt_config}')



    def cmt_config_addon(self, cmt_config):
        addon = None
        if cmt_config.startswith('aarch64'):
            addon = 'aarch64'
        else:
            try:
                release = self.cache.split('-')[-1]
                version_parts = release.split('.')
                version = int(version_parts[0]) * 10000 + int(version_parts[1]) * 100 + int(version_parts[2])
                if version >= 230010:
                    addon = 'x86_64-*-v2'
            except:
                pass
        return addon

    def set_cmtconfig_options(self):
        if self.cache and  not self.skipCMTConfigCheck:
            if self.cmtconfig and '#' not in self.cmtconfig:
                if self._cmt_config_list:
                    addons = []
                    for cmt_config in self._cmt_config_list:
                        addon = self.cmt_config_addon(cmt_config)
                        if addon:
                            addons.append(addon)
                    if len(addons) > 0:
                        self.cmtconfig = f'{self.cmtconfig}#({"|".join(addons)})'
                elif not self._multiple_cmtconfig:
                    addon = self.cmt_config_addon(self.cmtconfig)
                    if addon:
                        self.cmtconfig = f'{self.cmtconfig}#{addon}'


    def set_multiple_cmtconfig(self):
        archs = []
        if ',' in self.cmtconfig:
            if '#' in self.cmtconfig:
                raise Exception('cmtconfig \"{0}\" specified by the user is not valid'.format(self.cmtconfig))
            self._cmt_config_list = self.cmtconfig.split(',')
            tokens = [token.split('-') for token in self._cmt_config_list]
            if len(list(set([len(token) for token in tokens]))) > 1:
                raise Exception('cmtconfig \"{0}\" specified by the user is not valid'.format(self.cmtconfig))
            new_cmtconfig_base_tokens = []
            archs = []
            for token_number in range(len(tokens[0])):
                if len(list(set([token[token_number] for token in tokens]))) > 1:
                    if token_number == 0:
                        archs = [token[token_number] for token in tokens]
                    new_cmtconfig_base_tokens.append(f"({'|'.join([token[token_number] for token in tokens])})")
                else:
                    new_cmtconfig_base_tokens.append(tokens[0][token_number])
            new_cmtconfig_base = '-'.join(new_cmtconfig_base_tokens)
            setattr(self, 'cmtconfig', new_cmtconfig_base)
        if self.cache and not self.skipCMTConfigCheck:
            architecture = self.cmtconfig.split('#')[0]
            if not archs:
                archs = architecture.split('-')[0].strip('()').split('|')
            if self.container_name:
                self._check_cmtconfig_for_container(self.cache, self.container_name, archs, architecture)
            else:
                if not self._is_cmtconfig_exist(self.cache, architecture):
                    available_cmtconfig_list = self._get_cmtconfig_list(self.cache)
                    raise Exception(
                        'cmtconfig \"{0}\" specified by user is not exist in cache \"{1}\" (available: \"{2}\")'.format(
                            self.cmtconfig, self.cache, str(', '.join(available_cmtconfig_list))))
    def set_nvidia_options(self):
        self.get_json_cmtconfig()
        if self.nvidia in ['any', "*"]:
            self.cmtconfig.update({'gpu_spec': {'vendor':'nvidia'}})
        else:
            self.cmtconfig.update({'gpu_spec': {'vendor': 'nvidia', 'version': f'>={self.nvidia}'}})

    def get_json_cmtconfig(self):
        #     split cmtconfig to tokens
        if not self.cmtconfig or self.is_json_form:
            return
        json_cmtconfig: Dict[str, str|List[str]] = {'sw_platform':self.cmtconfig.split('#')[0]}
        if '#' in self.cmtconfig:
            addons = self.cmtconfig.split('#')[1].strip('()').split('|')
            json_cmtconfig['cpu_specs'] = []
            for addon in addons:
                addon_dict = {}
                tokens = addon.split('-')
                addon_dict['arch'] = tokens[0]
                if len(tokens) > 1 and tokens[1] != '*':
                    addon_dict['vendor'] = tokens[1]
                if len(tokens) > 2 and tokens[2] != '*':
                    addon_dict['instr'] = tokens[2]
                json_cmtconfig['cpu_specs'].append(addon_dict)
        self.cmtconfig = json_cmtconfig
        self.is_json_form = True






    def set_cmtconfig(self):

        if self.use_nightly_release and (not self.cmtconfig or not self.skipCMTConfigCheck):
            raise Exception('cmtconfig parameter and skipCMTConfigCheck must be specified in project_mode when nightly release is used')

        if not self.container_name and 'container_name' in self.task_config:
            self.container_name = self.task_config['container_name']

        if self._multiple_cmtconfig:
            return self.set_multiple_cmtconfig()
        if self.cmtconfig and self.cache and not self.skipCMTConfigCheck:
            architecture = self.cmtconfig.split('#')[0]
            if self.container_name:
                ami_client = AMIClient()
                if ami_client.ami_container_exists(self.container_name):
                    try:
                        ami_cmtconfig = ami_client.ami_cmtconfig_by_image_name(self.container_name)
                    except Exception as e:
                        ami_cmtconfig = self._get_cmtconfig_for_container(self.cache, self.container_name)
                    if architecture not in ami_cmtconfig:
                        raise Exception(
                            'cmtconfig \"{0}\" specified by the user does not correspond one in the container \"{1}\" '.format(
                                self.cmtconfig, ami_cmtconfig))
            else:
                if not self._is_cmtconfig_exist(self.cache, architecture):
                    available_cmtconfig_list = self._get_cmtconfig_list(self.cache)
                    raise Exception(
                        'cmtconfig \"{0}\" specified by user is not exist in cache \"{1}\" (available: \"{2}\")'.format(
                            self.cmtconfig, self.cache, str(', '.join(available_cmtconfig_list))))
        if self.container_name and self.cache:
            cache_exists = False
            ami_client = AMIClient()
            sw_tags_per_cache = ami_client.ami_sw_tag_by_cache(self.cache.replace('-','_'))
            for sw_tag in sw_tags_per_cache:
                if sw_tag['TAGNAME'] in self.container_name:
                    cache_exists = True
                    break
            if not cache_exists and not self.skipCMTConfigCheck:
                self._check_cmtconfig_for_container(self.cache, self.container_name, [self.cmtconfig.split('-')[0]], self.cmtconfig)
                # raise Exception(
                #     'Cache \"{0}\" is not found in the container \"{1}\" '.format(
                #         self.cache, self.container_name))



        if not self.cmtconfig:
            setattr(self, 'cmtconfig', TaskDefConstants.DEFAULT_PROJECT_MODE['cmtconfig'])
            if self.cache:
                if self.container_name:
                        cmt_config_from_ami = False
                        try:
                            ami_client = AMIClient()
                            if ami_client.ami_container_exists(self.container_name):
                                setattr(self, 'cmtconfig', ami_client.ami_cmtconfig_by_image_name(self.container_name))
                                cmt_config_from_ami = True
                        except Exception as e:
                            logger.error(f'Error getting cmtconfig from AMI for container {self.container_name}: {e}')
                        if not cmt_config_from_ami:
                            cmtconfig_list = self._get_cmtconfig_for_container(self.cache, self.container_name)
                            if len(cmtconfig_list) == 1:
                                setattr(self, 'cmtconfig', cmtconfig_list[0])
                            if (len(cmtconfig_list) == 2 and len(
                                    set([cmtconfig.split('-')[0] for cmtconfig in cmtconfig_list])) == 2 and
                                    len(set([cmtconfig.split('-', 1)[1] for cmtconfig in cmtconfig_list])) == 1):
                                self._cmt_config_list = cmtconfig_list
                                joined_cmtconfig = f'({cmtconfig_list[0].split("-")[0]}|{cmtconfig_list[1].split("-")[0]})-{cmtconfig_list[0].split("-", 1)[1]}'
                                setattr(self, 'cmtconfig', joined_cmtconfig)
                                return
                            raise Exception(
                                'cmtconfig is required for containers which are not registered in AMI')
                else:
                    cmtconfig_list = self._get_cmtconfig_list(self.cache)
                    if len(cmtconfig_list) == 1:
                        setattr(self, 'cmtconfig', cmtconfig_list[0])
                    else:
                        if len(cmtconfig_list) > 1:
                            cmtconfig_list.sort()
                            if (len(cmtconfig_list) == 2 and 'AthGeneration' in self.cache) and\
                                     (cmtconfig_list[0].split('-')[-2:] == cmtconfig_list[1].split('-')[-2:]) and \
                                    ('centos7' in cmtconfig_list[0])  and ('slc6' in cmtconfig_list[1]):
                                setattr(self, 'cmtconfig', cmtconfig_list[1])
                                return
                            else:
                                if (len(cmtconfig_list) == 2 and len(
                                        set([cmtconfig.split('-')[0] for cmtconfig in cmtconfig_list])) == 2 and
                                        len(set([cmtconfig.split('-', 1)[1] for cmtconfig in cmtconfig_list])) == 1):
                                    self._cmt_config_list = cmtconfig_list
                                    joined_cmtconfig = f'({cmtconfig_list[0].split("-")[0]}|{cmtconfig_list[1].split("-")[0]})-{cmtconfig_list[0].split("-", 1)[1]}'
                                    setattr(self, 'cmtconfig', joined_cmtconfig)
                                    return
                                value = str(','.join(cmtconfig_list))
                                raise Exception(
                                    'cmtconfig is not specified but more than one cmtconfig is available ({0}).'.format(
                                        value) + ' The task is rejected')
                        # prodsys1
                        # ver_parts = step.step_template.swrelease.split('.')
                        release = self.cache.split('-')[-1]
                        ver_parts = release.split('.')
                        ver = int(ver_parts[0]) * 1000 + int(ver_parts[1]) * 100 + int(ver_parts[2])
                        if int(ver_parts[0]) <= 13:
                            setattr(self, 'cmtconfig', 'i686-slc3-gcc323-opt')
                        elif ver < 15603:
                            setattr(self, 'cmtconfig', 'i686-slc4-gcc34-opt')
                        elif ver < 19003:
                            setattr(self, 'cmtconfig', 'i686-slc5-gcc43-opt')
                        elif ver < 20100:
                            setattr(self, 'cmtconfig', 'x86_64-slc6-gcc47-opt')
                        else:
                            setattr(self, 'cmtconfig', 'x86_64-slc6-gcc48-opt')
                        if self.cmtconfig not in cmtconfig_list:
                            if len(cmtconfig_list) > 0:
                                setattr(self, 'cmtconfig', cmtconfig_list[0])
                            else:
                                logger.error(f'{self.cache} is not registered in CRIC')
                                cmtconfig_list = self._get_cmtconfig_from_cvmfs(self.cache)
                                if len(cmtconfig_list) != 1 :
                                    if (len(cmtconfig_list) == 2 and len(
                                            set([cmtconfig.split('-')[0] for cmtconfig in cmtconfig_list])) == 2 and
                                        len(set([cmtconfig.split('-',1)[1] for cmtconfig in cmtconfig_list])) == 1):
                                        self._cmt_config_list = cmtconfig_list
                                        joined_cmtconfig = f'({cmtconfig_list[0].split("-")[0]}|{cmtconfig_list[1].split("-")[0]})-{cmtconfig_list[0].split("-",1)[1]}'
                                        setattr(self, 'cmtconfig', joined_cmtconfig)
                                        return
                                    raise Exception(f'{self.cache} is not registered in CRIC and {cmtconfig_list} found in CVMFS')
                                else:
                                    setattr(self, 'cmtconfig', cmtconfig_list[0])
