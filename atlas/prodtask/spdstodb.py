'''
Created on Nov 6, 2013

@author: mborodin
'''
import logging
from django.utils import timezone
from atlas.prodtask.models import get_default_project_mode_dict

import core.gspread as gspread
from datetime import datetime
from .models import StepTemplate, StepExecution, InputRequestList, TRequest, Ttrfconfig, RequestStatus
#from django.core.exceptions import ObjectDoesNotExist
import urllib2


from .xls_parser_new import XlrParser, open_tempfile_from_url
#from prodtask.models import get_default_nEventsPerJob_dict
from .models import get_default_nEventsPerJob_dict

_logger = logging.getLogger('prodtaskwebui')

TRANSLATE_EXCEL_LIST = ["brief", "ds", "format", "joboptions", "evfs", "eva2", "priority",
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
                         "LO", "feff", "NLO", "gen", "ecm", "ef", "comment", "contact", "store"]

def get_key_by_url(url):
        response = urllib2.urlopen(url)
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
            format = 'xlsx'
            google_key = r[r.find("/d/") + len("/d/"):r.find('/edit', r.find("/d/"))]
        _logger.debug("Google key %s retrieved from %s"%(google_key,url))
        return (google_key, format)
    
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
                if(not formats)and(not ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, step=step_name)[0]
                if (not formats) and (ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, memory=ram, step=step_name)[0]
                if (formats) and (not ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, output_formats=formats, step=step_name)[0]
                if (formats) and (ram):
                    st = StepTemplate.objects.all().filter(ctag=tag, output_formats=formats, memory=ram, step=step_name)[0]
        except:
            pass
        finally:
            if st:
                if st.status == 'Approved':
                    return st

            trtf = Ttrfconfig.objects.all().filter(tag=tag.strip()[0], cid=int(tag.strip()[1:]))
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
                if (not step_name) or (not tag):
                    raise ValueError("Can't create an empty step")
                else:
                    if st:
                       return st
                    output_formats = ''
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

def translate_excl_to_dict(excel_dict):      
        return_list = []
        index = 0
        checked_rows = []
        _logger.debug('Converting to input-step dict: %s' % excel_dict)
        for row in excel_dict:
            irl = {}
            st_sexec_list = []
            translated_row = {}
            for key in excel_dict[row]:
                if key < len(TRANSLATE_EXCEL_LIST):
                    translated_row[TRANSLATE_EXCEL_LIST[key]] = excel_dict[row][key]
            st = ''
            sexec = {}
            if ('joboptions' in translated_row) and (('evfs' in translated_row) or ('eva2' in translated_row)) and ('ds' in translated_row):
                if translated_row in checked_rows:
                    continue
                else:
                    checked_rows.append(translated_row)
                    input_events = translated_row.get('evfs', 0)
                    is_fullsym = True
                    if input_events == 0:
                        input_events = translated_row.get('eva2', 0)
                        is_fullsym = False
                    if is_fullsym:
                        comment = '(Fullsim)'+translated_row.get('comment', '')
                    else:
                        comment = '(Atlfast)'+translated_row.get('comment', '')
                    if (translated_row.get('joboptions', '')) and (translated_row.get('ds', '')):
                        if str(int(translated_row['joboptions'].split('.')[1])) !=  str(int(translated_row['ds'])):
                            raise RuntimeError("DSID and joboption are different: %s - %s"%(translated_row['joboptions'],int(translated_row['ds'])))
                    irl = dict(slice=index, brief=translated_row.get('brief', ''),
                               comment=comment,
                               input_data=translated_row.get('joboptions', ''),
                               priority=int(translated_row.get('priority', 0)),
                               input_events=int(input_events))

                    index += 1
                    reduce_input_format = False
                    step_index = 0
                    for currentstep in StepExecution.STEPS:
                        if translated_row.get('format', '') and (currentstep == 'Reco') and (not translated_row.get(currentstep)) and (is_fullsym):
                            translated_row[currentstep]='r9999'
                        if translated_row.get('format', '') and reduce_input_format and (not translated_row.get(currentstep)):
                            translated_row[currentstep]='p9999'
                        if translated_row.get('format', '') and (currentstep == 'Atlfast') and (not translated_row.get(currentstep)) and (not is_fullsym):
                            translated_row[currentstep]='a9999'
                        if translated_row.get(currentstep):
                            st = currentstep
                            tag = translated_row[currentstep]


                            # Store input events only for evgen
                            if StepExecution.STEPS.index(currentstep)==0:
                                sexec = dict(status='NotChecked', input_events=int(input_events))
                            else:
                                sexec = dict(status='NotChecked', input_events=-1)
                            formats = None
                            task_config = {'maxAttempt':15,'nEventsPerJob':get_default_nEventsPerJob_dict(),
                                                                 'project_mode':get_default_project_mode_dict().get(st,'')}

                            if reduce_input_format:
                                task_config.update({'input_format':'AOD'})
                                reduce_input_format = False
                            if ((currentstep == 'Reco') and is_fullsym) or ((currentstep == 'Atlfast')and(not is_fullsym)):
                                if translated_row.get('format', ''):
                                    formats = 'AOD'+'.'+translated_row.get('format', '')
                                    reduce_input_format = True
                                else:
                                    formats = 'AOD'
                            if step_index != 0:
                                step_index_parent = step_index - 1
                            else:
                                step_index_parent = 0
                            st_sexec_list.append({'step_name' :st, 'tag': tag, 'formats': formats, 'step_exec': sexec,
                                                  'task_config':task_config,'step_order':str(index)+'_'+str(step_index),
                                                  'step_parent':str(index)+'_'+str(step_index_parent)})
                            step_index += 1
                    return_list.append({'input_dict':irl, 'step_exec_dict':st_sexec_list})
        return  return_list  

def fill_steptemplate_from_gsprd(gsprd_link):
        """Parse google spreadsheet. 

        :param gsprd_link: A link to google sreapsheet.


        Returns a list of dict tuple [(InputRequestList,[(StepTemplate,StepExec)])]
        """

        try:
            excel_parser = XlrParser()
            url, xls_format = get_key_by_url(gsprd_link)
            excel_dict = excel_parser.open_by_key(url,xls_format)[0]
        except Exception, e:
            raise RuntimeError("Problem with link openning, \n %s" % e)
        return translate_excl_to_dict(excel_dict) 

def fill_steptemplate_from_file(file_obj):
        try:
            excel_parser = XlrParser()
            excel_dict = excel_parser.open_by_open_file(file_obj)[0]
        except Exception, e:
            raise RuntimeError("Problem with file openning, \n %s" % e)
        return translate_excl_to_dict(excel_dict)  


 




class UrFromSpds:

    

        
              
   
    @staticmethod
    def create_template(stepname, tag, priority):
        st = {}
        try:
            st = StepTemplate.objects.all().filter(ctag=tag)[0]
        except:
            pass
        finally:
            if st:
                return st
            else:
                trtf = Ttrfconfig.objects.all().filter(tag=tag.strip()[0], cid=int(tag.strip()[1:]))
                tr = trtf[0]
                print tr.lparams
                st = dict(step=stepname, #def_time=timezone.now(),
                           status='Approved',
                                               ctag=tag, priority=priority,
                                               cpu_per_event=int(tr.cpu_per_event), memory=int(tr.memory),
                                               output_formats=tr.formats, trf_name=tr.trf,
                                               lparams=tr.lparams, vparams=tr.vparams, swrelease=tr.trfv)
                return st 

 

    
    
    
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
            except Exception, e:
                #print  "Problem with uploading data from %s" % row['Spreadsheet']
                pass


 

    

        
    def __init__(self):
                pass
            
        
