'''
Created on Nov 6, 2013

@author: mborodin
'''
from django.utils import timezone

import core.gspread as gspread
from datetime import datetime
from .models import StepTemplate, StepExecution, InputRequestList, TRequest, Ttrfconfig, RequestStatus
#from django.core.exceptions import ObjectDoesNotExist
import urllib2


from core.xls_parser import XlrParser

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
        #print response.url
        r = response.url
        google_key = r[r.find("key%3D") + len("key%3D"):r.find('%26')]
        if not google_key:
            google_key = r[r.find("key=") + len("key="):r.find('#')]
        return google_key 
    
def fill_template(stepname, tag, priority):
        st = None
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
  
                st = StepTemplate.objects.create(step=stepname, def_time=timezone.now(), status='Approved',
                                               ctag=tag, priority=priority,
                                               cpu_per_event=int(tr.cpu_per_event), memory=int(tr.memory),
                                               output_formats=tr.formats, trf_name=tr.trf,
                                               lparams=tr.lparams, vparams=tr.vparams, swrelease=tr.trfv)
                st.save()
                return st

def translate_excl_to_dict(excel_dict):      
        return_list = []
        index = 0
        for row in excel_dict:
            irl = {}
            st_sexec_list = []
            translated_row = {}
            for key in excel_dict[row]:
                if key < len(TRANSLATE_EXCEL_LIST):
                    translated_row[TRANSLATE_EXCEL_LIST[key]] = excel_dict[row][key]
            st = ''
            sexec = {}
            if translated_row.get('priority',None):
                irl = dict(slice=index, brief=translated_row.get('brief', ''), comment=translated_row.get('comment', ''),
                                                                     input_data=translated_row.get('joboptions', ''))
                index += 1
                for currentstep in StepExecution.STEPS:    
                    if translated_row.get(currentstep):            
                        st = currentstep
                        tag = translated_row[currentstep]
    
                        if StepExecution.STEPS.index(currentstep) < StepExecution.STEPS.index('Atlfast'):
                            input_events = translated_row.get('evfs', 0)
                        else:
                            input_events = translated_row.get('eva2', 0)          
                        sexec = dict(status='NotChecked', priority=int(translated_row.get('priority', 0)), input_events=int(input_events))
                        st_sexec_list.append({'step_name' :st, 'tag': tag, 'step_exec': sexec})
                return_list.append({'input_dict':irl, 'step_exec_dict':st_sexec_list})
        return  return_list  

def fill_steptemplate_from_gsprd(gsprd_link):
        """Parse google spreadsheet. 

        :param gsprd_link: A link to google sreapsheet.


        Returns a list of dict tuple [(InputRequestList,[(StepTemplate,StepExec)])]
        """

        try:
            excel_parser = XlrParser()
            excel_dict = excel_parser.open_by_key(get_key_by_url(gsprd_link))[0]
        except Exception, e:
            raise RuntimeError("Problem with link openning, \n %s" % e)
        return translate_excl_to_dict(excel_dict) 

def fill_steptemplate_from_file(file_obj):
        try:
            excel_parser = XlrParser()
            excel_dict = excel_parser.open_by_open_file(file_obj)[0]
            print excel_dict
        except Exception, e:
            raise RuntimeError("Problem with link openning, \n %s" % e)
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
            
        
