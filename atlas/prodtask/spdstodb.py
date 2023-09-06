'''
Created on Nov 6, 2013

@author: mborodin
'''
import logging
from django.utils import timezone
import re

from atlas.prodtask.models import get_default_project_mode_dict, MCJobOptions, StepTemplate

import atlas.gspread as gspread
from datetime import datetime

from atlas.dkb.views import find_jo_by_dsid
from .models import StepExecution, InputRequestList, TRequest, RequestStatus, ETAGRelease
#from django.core.exceptions import ObjectDoesNotExist
import urllib.request, urllib.error, urllib.parse


from .xls_parser_new import XlrParser
#from prodtask.models import get_default_nEventsPerJob_dict
from .models import get_default_nEventsPerJob_dict

from django.conf import settings
import atlas.deftcore.api.client as deft
import ssl
import certifi
_deft_client = deft.Client(auth_user=settings.DEFT_AUTH_USER, auth_key=settings.DEFT_AUTH_KEY,base_url=settings.BASE_DEFT_API_URL)



_logger = logging.getLogger('prodtaskwebui')

TRANSLATE_EXCEL_LIST = { '1.0': ["brief", "ds", "format", "joboptions", "evfs", "eva2", "priority",
                         'Evgen',
                         'Simul',
                         'Merge',
                         'Digi',
                         'Reco',
                         'Rec Merge',
                         'Rec TAG',
                         'Atlfast',
                         'Atlf Merge',
                         'Atlf TAG',
                         "LO", "feff", "NLO", "gen", "ecm", "ef", "comment", "contact", "store"],

                        '2.0' : ["brief","joboptions","ecm","evevgen","evfs", "eva2","priority","format","LO",
                             "luminocity","feff","evgencpu","input_files","mctag","release","comment",'Evgen',
                             'Simul',
                             'Merge',
                             'Digi',
                             'Reco',
                             'Rec Merge',
                             'Atlfast',
                             'Atlf Merge'],
                         '3.0':["ds",'evgen_input',"ecm",'events','type','priority','format','evgen_release',
                                'comment','Evgen',
                                'Evgen Merge',
                             'Simul',
                             'Merge',
                             'Digi',
                             'Reco',
                             'Rec Merge',
                             'Deriv',
                             'Deriv Merge',
                                'rivet'
                                ]}

STRIPPED_FIELDS  = [ "format", "joboptions",'Evgen',
                    'Simul',
                    'Merge',
                    'Digi',
                    'Reco',
                    'Rec Merge',
                    'Atlfast',
                    'Atlf Merge',
                    'evgen_input',
                    'Deriv',
                    'Deriv Merge',
                    'type', 'evgen_release', 'rivet'
                    ]

NUMERIC_FIELDS = ["ecm","evevgen","evfs", "eva2","events","ds"]

def get_key_by_url(url):
        response = urllib.request.urlopen(url, context=ssl.create_default_context(cafile=certifi.where()))
        r = response.url
        format = ''
        if r.find('key')>0:
            format = 'xls'
            google_key = ''
            if r.find("key%3D") > 0:
                google_key = r[r.find("key%3D") + len("key%3D"):r.find('%26')]
            if not google_key:
                google_key = r[r.find("key=") + len("key="):r.find('#')]
            if not google_key:
                google_key = r[r.find("key=") + len("key="):r.find('&', r.find("key="))]
        else:
            format = 'xls'
            google_key = r[r.find("/d/") + len("/d/"):r.find('/edit', r.find("/d/"))]
        _logger.debug("Google key %s retrieved from %s"%(google_key,url))
        return (google_key, format)

STEP_FORMAT = { 'Evgen':'EVNT','Simul':'HITS','Merge':'HITS','Rec TAG':'TAG',
                'Atlf Merge':'AOD','Rec Merge':'AOD','Atlf TAG':'TAG', 'TAG':'TAG', 'Evgen Merge':'EVNT', 'Deriv Merge': 'NTUP_PILEUP',
                'Deriv' :  'NTUP_PILEUP'

}


def format_check(format):
    #TODO: implement regular expression for format
    if format.find(',')>-1:
        raise ValueError('Wrong format: %s'%format)


FORMAT_BY_STEP = {'Evgen':['TXT']}

def format_splitting(format_string, events_number):

    formats_percentage = format_string.split('.')
    result = []
    step_formats_percentage = []
    additional_formats_by_step = {}
    for format_percentage in formats_percentage:
        non_reco = False
        for step in FORMAT_BY_STEP:
            for format in FORMAT_BY_STEP[step]:
                if format in format_percentage:
                    additional_formats_by_step[step] = additional_formats_by_step.get(step,[]) + [format_percentage]
                    non_reco = True
        if not non_reco:
            step_formats_percentage += [format_percentage]
    if events_number==-1:
        return ([(-1,format_string,False)], additional_formats_by_step)
    percentages = set()
    formats_dict = []
    for format_percentage in step_formats_percentage:
        if '-' in format_percentage:
            if int(format_percentage.split('-')[1]) > 100:
                raise ValueError('Wrong format %s'%format_string)
            formats_dict.append((format_percentage.split('-')[0],int(format_percentage.split('-')[1]),True))
            percentages.add(int(format_percentage.split('-')[1]))
        else:
            formats_dict.append((format_percentage.split('-')[0],100,False))
            percentages.add(100)
    percentages_list = list(percentages)
    percentages_list.sort()
    processed_events = 0
    for percentage in percentages_list:
        section_events = (int(events_number) * percentage) / 100
        section_formats = []
        do_split_step = False
        for format in formats_dict:
            if format[1] >= percentage:
                section_formats.append(format[0])
            if format[2]:
                do_split_step = True
        result.append((section_events - processed_events,'.'.join(section_formats),do_split_step))
        processed_events = section_events
    if 100 not in percentages_list:
        result.append((int(events_number)-processed_events,'',True))
    return (result, additional_formats_by_step)

def format_from_jo(job_options):
    if ('/' in job_options) and re.match(r"(Ph)|(Powheg)|(aMcAtNlo)",job_options.split('/')[1]):
        return {'Evgen': ['TXT']}
    if ('.' in job_options) and re.match(r"(Ph)|(Powheg)|(aMcAtNlo)",job_options.split('.')[2]):
        return {'Evgen':['TXT']}
    return {}


def translate_excl_to_dict(excel_dict, version='2.0'):
        return_list = []
        index = 0
        checked_rows = []
        _logger.debug('Converting to input-step dict: %s' % excel_dict)

        translate_list = TRANSLATE_EXCEL_LIST[version]

        for row in excel_dict:
            translated_row = {}
            for key in excel_dict[row]:
                if key < len(translate_list):
                    if translate_list[key] in STRIPPED_FIELDS:
                        if excel_dict[row][key].strip():
                            translated_row[translate_list[key]] = excel_dict[row][key].strip()
                    elif translate_list[key] in NUMERIC_FIELDS:
                        if isinstance(excel_dict[row][key],float) or isinstance(excel_dict[row][key],int):
                           translated_row[translate_list[key]] = excel_dict[row][key]
                    else:
                        translated_row[translate_list[key]] = excel_dict[row][key]
            if ('joboptions' not in translated_row) and ('ds' in translated_row):
                translated_row['joboptions'] = str(int(translated_row['ds']))
                if translated_row['joboptions'].startswith('421') or int(translated_row['ds']) >= 500000:
                    try:
                        if MCJobOptions.objects.filter(dsid=int(translated_row['ds'])).exists():
                            translated_row['joboptions'] = str(int(translated_row['ds'])) + '/' + MCJobOptions.objects.get(dsid=int(translated_row['ds'])).physic_short
                    except:
                        pass
                else:
                    translated_row['joboptions'] =  find_jo_by_dsid(translated_row['joboptions'])
            if ('events' in translated_row):
                processing_type = translated_row.get('type','')
                if processing_type == 'AF2':
                    translated_row['eva2'] = translated_row['events']
                elif processing_type == 'Evgen':
                    translated_row['evevgen'] = translated_row['events']
                else:
                    translated_row['evfs'] = translated_row['events']
            if ('joboptions' in translated_row) and (('evfs' in translated_row) or ('eva2' in translated_row) or ('evevgen' in translated_row)):
                try:
                    total_input_events_evgen = int(translated_row.get('evevgen', 0))
                    total_input_events = int(translated_row.get('evfs', 0))
                    is_fullsym = True
                    total_input_events_fast = int(translated_row.get('eva2', 0))
                    if (total_input_events == 0) and (total_input_events_fast != 0):
                        total_input_events = total_input_events_fast
                        is_fullsym = False
                    if total_input_events == 0:
                        total_input_events = total_input_events_evgen
                        total_input_events_evgen = 0
                    filter_eff = translated_row.get('feff', 0)

                except:
                    continue
                if translated_row in checked_rows:
                    continue
                else:
                    input_events_format, additional_formats = format_splitting(translated_row.get('format', ''),total_input_events)
                    if (not additional_formats) and translated_row.get('joboptions', ''):
                        additional_formats = format_from_jo(translated_row.get('joboptions', ''))
                    for input_events, format, do_split in input_events_format:
                        irl = {}
                        st_sexec_list = []
                        sexec = {}
                        checked_rows.append(translated_row)

                        if translated_row.get('type', ''):
                            comment = '(%s)' % translated_row.get('type', '') + translated_row.get('comment', '')
                        else:
                            if is_fullsym:
                                comment = '(Fullsim)'+translated_row.get('comment', '')
                            else:
                                comment = '(Atlfast)'+translated_row.get('comment', '')


                        if translated_row.get('priority', 0) == '0+':
                            priority = -2
                        else:
                            priority = translated_row.get('priority', 0)
                        if len(translated_row.get('brief', ' '))>150:
                            raise RuntimeError("Brief description is too big, should be <150 characters")
                        if translated_row.get('evgen_input', ''):
                            irl = dict(slice=index, brief=translated_row.get('brief', ' '),
                                       comment=comment,
                                       input_data=translated_row.get('joboptions', ''),
                                       priority=int(priority),
                                       input_events=int(input_events),
                                       dataset=translated_row.get('evgen_input', ''))
                        else:
                            irl = dict(slice=index, brief=translated_row.get('brief', ' '),
                                       comment=comment,
                                       input_data=translated_row.get('joboptions', ''),
                                       priority=int(priority),
                                       input_events=int(input_events))

                        index += 1
                        reduce_input_format = None
                        step_index = 0
                        for currentstep in StepExecution.STEPS:
                            if ((total_input_events_evgen != 0) or additional_formats.get(currentstep,[]) or (filter_eff!=0) or translated_row.get('evgen_release','') )\
                                    and (currentstep == 'Evgen') and (not translated_row.get(currentstep,'').strip()) :
                                translated_row[currentstep]='e9999'
                            if format and (not [x for x in ['LHE','TXT','EVNT'] if x in format]) and (currentstep == 'Reco') and (not translated_row.get(currentstep,'').strip()) and (is_fullsym):
                                translated_row[currentstep]='r9999'
                            if format and (currentstep == 'Rec Merge') and reduce_input_format and (not translated_row.get(currentstep,'').strip()):
                                translated_row[currentstep]='p9999'
                            if format and (currentstep == 'Atlfast') and (not translated_row.get(currentstep,'').strip()) and (not is_fullsym):
                                translated_row[currentstep]='a9999'
                            if translated_row.get(currentstep):
                                st = currentstep
                                tag = translated_row[currentstep]
                                task_config = {}
                                project_mode_addition = []
                                # Store input events only for evgen
                                if StepExecution.STEPS.index(currentstep)==0:
                                    if tag=='e9999':
                                        sexec = dict(status='NotChecked', input_events=int(input_events))
                                        if translated_row.get('evgen_release','') :
                                            if  ETAGRelease.objects.filter(sw_release=translated_row['evgen_release']).exists():
                                                translated_row[currentstep] = ETAGRelease.objects.filter(sw_release=translated_row['evgen_release'])[0].ami_tag
                                                tag = translated_row[currentstep]
                                            else:
                                                try:
                                                    new_ami_tag = max([ x['AMITAG'] for x in _deft_client._get_tags('Gen_tf.py', translated_row.get('evgen_release',''))])
                                                    new_etag = ETAGRelease()
                                                    new_etag.ami_tag = new_ami_tag
                                                    new_etag.sw_release = translated_row['evgen_release']
                                                    new_etag.save()
                                                    translated_row[currentstep] = new_ami_tag
                                                    tag = translated_row[currentstep]
                                                except Exception as e:
                                                    _logger.error('Problem with a new ami tag: %s', str(e))

                                    else:
                                        sexec = dict(status='NotCheckedSkipped', input_events=int(input_events))
                                    if (total_input_events_evgen != 0):
                                        task_config.update({'split_events': total_input_events_evgen})
                                    if (filter_eff!=0):
                                        task_config.update({'evntFilterEff': filter_eff})
                                else:
                                    sexec = dict(status='NotChecked', input_events=-1)
                                if currentstep == 'Evgen':
                                    if translated_row.get('type','') == 'LHE':
                                        formats = 'TXT'
                                    else:
                                        formats = STEP_FORMAT[currentstep]
                                    if translated_row.get('rivet',''):
                                        project_mode_addition.append('rivet='+translated_row.get('rivet',''))

                                else:
                                    formats = None
                                if do_split:
                                    task_config.update({'split_slice':1,'spreadsheet_original':1,'maxAttempt':30,'maxFailure':3,'nEventsPerJob':get_default_nEventsPerJob_dict(version),
                                                                         'project_mode':';'.join([get_default_project_mode_dict().get(st,'')]+project_mode_addition)})
                                else:
                                    task_config.update({'maxAttempt':30,'spreadsheet_original':1,'maxFailure':3,'nEventsPerJob':get_default_nEventsPerJob_dict(version),
                                                                         'project_mode':';'.join([get_default_project_mode_dict().get(st,'')]+project_mode_addition)})

                                if reduce_input_format:
                                    task_config.update({'input_format':reduce_input_format})
                                    reduce_input_format = None
                                if (currentstep == 'Reco') or (currentstep == 'Atlfast'):
                                    if format:
                                        format_check(format)
                                        formats = 'AOD'+'.'+format
                                        reduce_input_format = 'AOD'
                                        if int(input_events) != -1:
                                            sexec.update({'input_events':int(input_events)})

                                    else:
                                        formats = 'AOD'
                                if currentstep in additional_formats:
                                    formats = '.'.join(additional_formats[currentstep]+[STEP_FORMAT[currentstep]])
                                    format = None

                                if step_index != 0:
                                    step_index_parent = step_index - 1
                                else:
                                    step_index_parent = 0
                                if re.match('\w(\d\d\d\d\d|\d\d\d\d|\d\d\d)$',tag):
                                    st_sexec_list.append({'step_name' :st, 'tag': tag, 'formats': formats, 'step_exec': sexec,
                                                          'task_config':task_config,'step_order':str(index)+'_'+str(step_index),
                                                          'step_parent':str(index)+'_'+str(step_index_parent)})

                                    step_index += 1
                        return_list.append({'input_dict':irl, 'step_exec_dict':st_sexec_list})
        return  return_list  


def fill_steptemplate_from_gsprd(gsprd_link, version='2.0'):
        """Parse google spreadsheet. 

        :param gsprd_link: A link to google sreapsheet.


        Returns a list of dict tuple [(InputRequestList,[(StepTemplate,StepExec)])]
        """

        try:
            excel_parser = XlrParser()
            url, xls_format = get_key_by_url(gsprd_link)
            excel_dict = excel_parser.open_by_key(url,xls_format)[0]
        except Exception as e:
            raise RuntimeError("Problem with link openning, \n %s" % e)
        try:
            result = translate_excl_to_dict(excel_dict, version)
            return result
        except Exception as e:
            raise RuntimeError("Problem with spreadsheet parsing, please check spreadsheet format. \n Error: %s"%str(e))


def fill_steptemplate_from_file(file_obj):
        try:
            excel_parser = XlrParser()
            excel_dict = excel_parser.open_by_open_file(file_obj)[0]
        except Exception as e:
            raise RuntimeError("Problem with file openning, \n %s" % e)
        return translate_excl_to_dict(excel_dict)  


 




class UrFromSpds:

    

        
              

    
    
    
    @staticmethod
    def fillAllFromSC2(rid, spreadSheetKey, guser, gpasswd):
            try:
                gc = gspread.login(guser, gpasswd)
                # If you want to be specific, use a key (which can be extracted from
                # the spreadsheet's url)
                sh = gc.open_by_key(spreadSheetKey)
            
                # Most common case: Sheet1
                worksheet = sh.sheet1
                
            
                # Get Dict of ALL cells in spreadsheet
                list_of_dict = worksheet.get_all_records(False)
                #print 'Start sync:', len(list_of_dict)
            except:
                return False
            row = list_of_dict[rid]
            if len(row['Approval']) < 2:
                appStat = 'Pending'
                apptime = None
            elif '/' in row['Approval']:
                appStat = 'Approved'
                apptime = datetime.strptime(row['Approval'], "%d/%m/%Y")
            elif 'yes'.upper() in  row['Approval'].upper():
                appStat = 'Approved'
                apptime = timezone.now()
            else:
                apptime = None
                appStat = row['Approval']
            # convert '' to 0 for N(full) and N(fast)
            managerRow = 'default'
            if(row['Manager']):
                managerRow = row['Manager']
        
            req = TRequest.objects.create(ref_link=row['Savannah'], provenance='ATLAS', request_type='MC',
                                          phys_group=row['Slice'], description=row['Description'], energy_gev=8000,
                                          campaign=row['Type'], manager=managerRow, status=appStat, subcampaign='')
            req.save()
        
            rs = RequestStatus.objects.create(request=req, comment='', owner=managerRow, timestamp=timezone.now(), status='Created')
            rs.save()
            statusList = [x[0] for x in RequestStatus.STATUS_TYPES]
            if not apptime:
                apptime = timezone.now()
         
            if appStat in statusList:
                rs = RequestStatus.objects.create(request=req, comment=row['Comment'], owner=managerRow, timestamp=apptime, status=appStat)
            else:
                rs = RequestStatus.objects.create(request=req, comment=row['Comment'], owner=managerRow, timestamp=timezone.now(), status='Unknown')
            rs.save()

            try:
                #===============================================================
                # worksheetsname = ['physics', 'Top', 'StandartModel', 'Exotrics', 'SUSY', 'Higgs', 'JetEtmiss', 'Tau', 'FlavourTag',
                #                'Egamma', 'BPhys', 'TrackingPerf', 'HeavyIons', 'Muon']
                #===============================================================
                #print row['Spreadsheet']
                spreadsheet_dict = fill_steptemplate_from_gsprd(row['Spreadsheet'])
                for current_slice in spreadsheet_dict:
                    input_data = current_slice["input_dict"]
                    input_data['request'] = req
                    irl = InputRequestList(**input_data)
                    irl.save()
                    for step in current_slice['step_exec_dict']:
                        st = fill_template(step['step_name'], step['tag'], step['step_exec']['priority'])
                        step['step_exec']['request'] = req
                        step['step_exec']['slice'] = irl
                        step['step_exec']['step_template'] = st
                        step['step_exec']['status'] = 'Done'
                        st_exec = StepExecution(**step['step_exec'])
                        st_exec.save_with_current_time()
            except Exception as e:
                #print  "Problem with uploading data from %s" % row['Spreadsheet']
                pass


 

    

        
    def __init__(self):
                pass


def fill_template(step_name, tag, priority, formats=None, ram=None):

        st = None
        try:
            if not step_name:
                if(not formats)and(not ram):
                    st = StepTemplate.objects.all().filter(ctag=tag)[0]
                if (not formats) and (ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, memory=ram)[0]
                if (formats) and (not ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, output_formats=formats)[0]
                if (formats) and (ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, output_formats=formats, memory=ram)[0]
            else:
                if(formats==None)and(not ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, step=step_name)[0]
                if (formats==None) and (ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, memory=ram, step=step_name)[0]
                if (formats!=None) and (not ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, output_formats=formats, step=step_name)[0]
                if (formats!=None) and (ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, output_formats=formats, memory=ram, step=step_name)[0]
        except:
            pass
        finally:
            if st:
                if (st.status == 'Approved') or (st.status == 'dummy'):
                    return st

            trtf = None
            if trtf:
                tr = trtf[0]
                if(formats):
                    output_formats = formats
                else:
                    output_formats = tr.formats
                if(ram):
                    memory = ram
                else:
                    memory = int(tr.memory)
                if not step_name:
                    step_name = tr.step
                if st:
                    st.status = 'Approved'
                    st.output_formats = output_formats
                    st.memory = memory
                    st.cpu_per_event = int(tr.cpu_per_event)
                else:
                    st = StepTemplate.objects.create(step=step_name, def_time=timezone.now(), status='Approved',
                                                   ctag=tag, priority=priority,
                                                   cpu_per_event=int(tr.cpu_per_event), memory=memory,
                                                   output_formats=output_formats, trf_name=tr.trf,
                                                   lparams='', vparams='', swrelease=tr.trfv)
                st.save()
                _logger.debug('Created step template: %i' % st.id)
                return st
            else:
                if not tag:
                    raise ValueError("Can't create an empty step")
                if not step_name:
                    step_name = 'Reco'
                if st:
                   return st
                output_formats = STEP_FORMAT.get(step_name,'')
                if formats:
                    output_formats = formats
                memory = 0
                if ram:
                    memory = ram
                st = StepTemplate.objects.create(step=step_name, def_time=timezone.now(), status='dummy',
                           ctag=tag, priority=0,
                           cpu_per_event=0, memory=memory,
                           output_formats=output_formats, trf_name='',
                           lparams='', vparams='', swrelease='')
                st.save()
                return st