# A.Klimentov Mar 3, 2014
#
# DEFT tasks handling
# 
# Mar 15, 2014. Add json 
# Mar 19, 2014. Add JEDI Client.py
# Mar 20, 2014. Dataset tables synchronization
#               Ignore tasks and datasets with ID < 4 000 000
# Mar 24, 2014. Requests state update vs Task state
# Mar 31, 2014. Add taskinfo into task partition table
# Apr 03, 2014. t_production_task partitioned
# Apr 27, 2014. add SSO
#
# Last Edit : May 1, 2014 ak
#

import re
import sys
import os
import getopt
import commands
import datetime
import time
import cx_Oracle
import simplejson as json

# set path and import JEDI client
sys.path.append('/data/atlswing/site-packages')
import jedi.client

import deft_conf
import deft_pass
import deft_user_list

import DButils

#-
import base64
import cookielib
import requests
from random import choice
from pprint import pprint
import cernsso
#-

verbose = False

task_finish_states         = 'done,finish,failed,obsolete,aborted'
task_aborted_states        =  'aborted,failed,obsolete'
datasets_deletion_states   =  'toBeDeleted,Deleted'

JEDI_datasets_final_statesL         =  ['aborted','broken','done','failed','partial','ready']
DEFT_datasets_done_statesL          =  ['done']
DEFT_datasets_final_statesL         =  \
    ['aborted','broken','done','failed','deleted,toBeDeleted,toBeErased,waitErased,toBeCleaned,waitCleaned']
DEFT_datasets_postproduction_states =  'deleted,toBeErased,waitErased,toBeCleaned,waitCleaned'
DEFT_tasks_abort_statesL            =  ['aborted','broken','failed']
Request_update_statesL              =  ['registered','running','done','finished','failed']

MIN_DEFT_TASK_ID           = 4000000

#synch intervals (hours)
REQUEST_SYNCH_INTERVAL = 12000
TASK_SYNCH_INTERVAL    = 12000
DATASET_SYNCH_INTERVAL =    72

#
TASK_RECOVERY_STEP     = '.recov.'

class DEFTClient(object):
#
# author D.Golubkov
#
    auth_url = 'https://atlas-info-mon.cern.ch/api/deft'
    # kerberos
    try :
        sso_cookies = cernsso.Cookies(auth_url).get()
    except Exception, e :
        raise Exception("SSO authentication error: %s" % str(e))

    def __init__(self):
        self.ssocookies = cernsso.Cookies(self._getAPIScope()).get()

    def _getAPIScope(self):
        return 'https://atlas-info-mon.cern.ch/api/deft'

    def _sendRequest(self, request):
        message = base64.b64encode(json.dumps(request))
        payload = {'message': message}
        return requests.post(self._getAPIScope(), data=payload, cookies=self.ssocookies, verify=False).json()

    def getUserInfo(self):
        request = {'method': 'getUserInfo'}
        return self._sendRequest(request)
    
    def createDEFTTask(self, dataset, taskId):
        request = {'method': 'createDEFTTask', 'dataset': dataset, 'taskId': taskId}
        return self._sendRequest(request)
    
    def createProdsysListTask(self, dataset):
        request = {'method': 'createProdsysListTask', 'dataset': dataset}
        return self._sendRequest(request)


def usage() :

 """
  Usage python deft_handling.py cmd

Initialization options :
-h[elp]        display this help message and exit
-v[erbose]     run in verbose mode  
-u[update]     run in update mode (update database)

Action options :

--change-task-state -t[id] TaskID -s[state] State          set task state to 'State'
--check-aborted-tasks                                      check aborted tasks and set datasets state accordingly
--finish-task   -t[id] TaskID                              end task; running jobs will be finished;    
--kill-task -t[id]                                         end task and kill all running jobs
--obsolete-task -t[id]                                     obsolete task
--change-task-priority -t[id] TaskID -p[riority] Priority  set task priority to 'Priority'
--synchronizeJediDeft                                      synchronize DEFT task tables with JEDI ones
            
 """

 print usage.__doc__

def execCmd(cmd,flag) :
   (s,o) = commands.getstatusoutput(cmd)
   if s != 0 and flag == 1:
        print cmd
        print s
        print o
   if verbose :
        print s
        print o
   return s,o


def findProcess(task,command,option):
  status = 0
  ret    = 0
  if option != '' and task != '' :
   cmd = "ps -ef | grep %s | grep -c %s"%(task,command)
   if verbose   > 2 : print cmd
   (status,ret) = execCmd(cmd,0)
   if status == 0 :
    if int(ret) > 2 :
     print "./deft_handling.py -INFO- There is an active process %s %s. %s"%(task,command,time.ctime())
     if option == 'Quit' :
         print "Quit."
         sys.exit(1)
  return 


def connectDEFT(flag) :

#
# connect in RO mode if flag = 'R'
# connect in Update mode     = 'W'

    error = 0
    dbname = deft_conf.daemon['deftDB_INTR']
    deftDB = deft_conf.daemon['deftDB_host']
    if flag == 'W' :
        dbuser = deft_conf.daemon['deftDB_writer']
        dbpwd  = deft_pass.deft_pass_intr['atlas_deft_w']
    else :
        dbuser = deft_conf.daemon['deftDB_reader']
        dbpwd  = deft_pass.deft_pass_intr['atlas_deft_r']
    (pdb,dbcur) = DButils.connectDEFT(dbname,dbuser,dbpwd)

    return pdb, dbcur, deftDB

def connectJEDI(flag) :
#
# connect in RO mode if flag = 'R'
# connect in Update mode     = 'W'
    error = 0
    dbname = deft_conf.daemon['deftDB_ADCR']
    deftDB = deft_conf.daemon['deftDB_host']
    if flag == 'W' :
        dbuser = deft_conf.daemon['deftDB_writer']
        dbpwd  = deft_pass.deft_pass_intr['atlas_jedi_w']
    else :
        dbuser = deft_conf.daemon['deftDB_reader']
        dbpwd  = deft_pass.deft_pass_intr['atlas_jedi_r']
    (pdb,dbcur) = DButils.connectDEFT(dbname,dbuser,dbpwd)

    return pdb, dbcur, deftDB

def connectPandaJEDI(flag) :
#
# connect in RO mode if flag = 'R'
# connect in Update mode     = 'W'
    error = 0
    dbname = deft_conf.daemon['jediDB_ADCR']
    deftDB = deft_conf.daemon['jediDB_host']
    if flag == 'W' :
        dbuser = deft_conf.daemon['deftDB_writer']
        dbpwd  = deft_pass.deft_pass_intr['atlas_jedi_w']
    else :
        dbuser = deft_conf.daemon['deftDB_reader']
        dbpwd  = deft_pass.deft_pass_intr['atlas_jedi_r']
    (pdb,dbcur) = DButils.connectDEFT(dbname,dbuser,dbpwd)

    return pdb, dbcur, deftDB



def JediTaskCmd(cmd,task_id,priority) :
#
    status  = 0
    timenow = time.ctime()
    msg    = "INFO. %s %s at %s"%(cmd, task_id,timenow)
    # find task with rask_id and check its status
    #connect to Oracle
    (pdb,dbcur,deftDB) = connectJEDI('R')
    #
    t_table_JEDI = "%s.%s"%(deftDB,deft_conf.daemon['t_task'])
    sql = "SELECT taskid,status FROM %s WHERE taskid=%s"%(t_table_JEDI,task_id)
    if verbose : print sql
    tasks = DButils.QueryAll(pdb,sql)
    DButils.closeDB(pdb,dbcur)
    tid     = -1
    tstatus = 'unknown'
    for t in tasks :
        tid     = t[0]
        tstatus = t[1]
    if tid == task_id :
     # check status
     if task_finish_states.find(tstatus) < 0 :   
      # check command 
      if cmd == 'finishTask' :
       (status,output) = jedi.client.finishTask(task_id)
      elif cmd == 'killTask' :
       (status,output) = jedi.client.killTask(task_id)    
      elif cmd == 'changeTaskPriority' :
       (status,output) = jedi.client.changeTaskPriority(task_id,priority)   
      else :
        status = -1
        msg = "WARNING. Unknown command : %s"%(cmd)
     else :
         status = -1
         msg = "WARNING. Task : %s State : %s (in %s). Cmd : %s CANNOT BE EXECUTED"%(task_id,tstatus,t_table_JEDI,cmd)
    else :
     status = -1
     msg = "WARNING. Task %s NOT FOUND in %s"%(task_id,t_table_JEDI)
    if status != 0 and status != -1 :
        msg = 'ERROR. jedi.client.finisheTask(%s)'%(task_id)
    print msg,' (Return Status : ',status,')'
    return status

def obsoleteTaskState(task_id, dbupdate) :
# set task state to 'obsolete' and update datasets states accordingly
    error   = 0
    status  = 'unknown'
    project = 'unknown'

    obsoleteFlag = 0

    # find task and check its state
    #connect to Oracle
    (pdb,dbcur,deftDB) = connectDEFT('R')
    #
    t_table_DEFT = "%s.%s"%(deftDB,deft_conf.daemon['t_production_task'])
    t_table_JEDI = "%s.%s"%(deftDB,deft_conf.daemon['t_task'])
    t_dataset_DEFT = "%s.%s"%(deftDB,deft_conf.daemon['t_production_dataset'])
    t_input_dataset= "%s.%s"%(deftDB,deft_conf.daemon['t_input_dataset'])
    #
    sql = "SELECT taskid,status,project FROM %s WHERE taskid=%s"%(t_table_DEFT,task_id)
    if verbose : print sql
    tasks = DButils.QueryAll(pdb,sql)
    DButils.closeDB(pdb,dbcur)
    if len(tasks) > 0 :
     for t in tasks :
         tid     = t[0]
         status  = t[1]
         project = t[2]
         if task_finish_states.find(status) < 0 or status=='obsolete' : error = 1
         if project == 'user' : error = 1
         if error == 0 :
             obsoleteFlag = 1
    else :
        error =1
    if error == 1 :
        print "ERROR. obsoleteTaskState. Task %s NOT FOUND or it has invalid state/project ('%s'/'%s')"%(task_id,status,project)
    else :
        if obsoleteFlag == 1 :
            # update tables
            sql = "update %s SET status = 'obsolete' where task_id=%s"%(t_table_DEFT,task_id)
            sql_update.append(sql)
            sql =  "update %s SET status = 'obsolete' where task_id=%s"%(t_table_JEDI,task_id)
            sql_update.append(sql)
            sql = "update %s SET status = 'waitingErase' where task_id=%s"%(t_dataset_DEFT,task_id)
            sql_update.append(sql)
            sql = "update %s SET status = 'waitingErase' where task_id=%s"%(t_input_dataset,task_id)
            sql_update.append(sql)
            print sql
            if dbupdate == True :
                (pdb,dbcur,deftDB) = connectDEFT('W')
                for sql in sql_update :
                    print sql
                    DButils.QueryUpdate(pdb,sql)
                #DButils.QueryCommit(pdb)
                #DButils.closeDB(pdb,dbcur)
            else :
                print "INFO. obsoleteTaskState : no database update"


def changeTaskState(task_id, task_state,dbupdate) :
# 
   error = 0
   #connect to Oracle
   (pdb,dbcur,deftDB) = connectDEFT('R')
   # get task info
   t_table = "%s.%s"%(deftDB,deft_conf.daemon['t_production_task'])
   sql = "SELECT taskid,status FROM %s WHERE taskid=%s"%(t_table)
   if verbose : print sql
   tasks = DButils.QueryAll(pdb,sql)
   DButils.closeDB(pdb,dbcur)
   if len(tasks) > 0 :
    for t in tasks :
        tid   = t[0]
        state = t[1]
        print "changeTaskState INFO. Task : %s Status : %s"%(tid, status)
        if state in ("done","finished","pending") :
         sql = "UPDATE %s SET status='%s', update_time=current_timestmap "%(t_table,task_state)
         sql+= "WHERE taskid=%s"%(task_id)
         print sql
         # update ADCR database accordingly
        else :
         print "changeTaskState INFO. Task state CANNOT BE CHANGED"
   else :
     print "changeTaskState Error. Can not find info for task %s"%(task_id)
     error = 1
   return error

def checkAbortedTasks() :
#
# check tasks state and set dataset state accordingly
#
  TR_ID_Min = 4000000
  TR_ID_Max = 5000000
  TR_ID_Start_From = 4000000
  dbupdate         = True

  DEFT_tasks_abort_states = ''
  for s in DEFT_tasks_abort_statesL :
      DEFT_tasks_abort_states += "'%s',"%(s)
  DEFT_tasks_abort_states = DEFT_tasks_abort_states[0:(len(DEFT_tasks_abort_states)-1)]
  DEFT_datasets_final_states  = ''
  for s in DEFT_datasets_done_statesL :
        DEFT_datasets_final_states += "'%s',"%(s)
  DEFT_datasets_final_states = DEFT_datasets_final_states[0:(len(DEFT_datasets_final_states)-1)]


  user_project = 'user%'

  timenow = int(time.time())
  findProcess('deft_handling','checkAbortedTasks','Quit')
  # connect to Oracle
  (pdb,dbcur,deftDB) = connectDEFT('R')

  # select tasks
  t_table_DEFT    = "%s.%s"%(deftDB,deft_conf.daemon['t_production_task'])
  sql = "SELECT TASKID FROM %s WHERE "%(t_table_DEFT)
  sql+= "STATUS IN (%s) "%(DEFT_tasks_abort_states)
  sql+= "AND project NOT LIKE '%s' "%(user_project)
  sql+= "AND (taskid>%s AND taskid <%s) ORDER by taskid "%(TR_ID_Min,TR_ID_Max)
  print sql
  tids = DButils.QueryAll(pdb,sql)
  # select datasets
  t_table_datasets = "%s.%s"%(deftDB,deft_conf.daemon['t_production_dataset'])
  sql = "SELECT TASKID, name FROM %s "%(t_table_datasets)
  sql+= "WHERE STATUS IN (%s) "%(DEFT_datasets_final_states)
  sql+= "AND name NOT LIKE '%s' "%(user_project)
  sql+= "AND (taskid>%s AND taskid <%s) ORDER by taskid "%(TR_ID_Min,TR_ID_Max)
  print sql
  dids = DButils.QueryAll(pdb,sql)
  DButils.closeDB(pdb,dbcur)
  T0 = time.time()
  sql_update = []
  print "INFO. checkAbortedTasks. Selection done, start TIDs comparison @",time.ctime(T0)
  for d in dids :
     d_tid = d[0]
     d_name= d[1]
     for t in tids :
       t_tid = t[0]
       if d_tid == t_tid :
        sql_upd = "UPDATE %s SET status='toBeDeleted', TIMESTAMP=CURRENT_TIMESTAMP "%(t_table_datasets)
        sql_upd+= "WHERE name = '%s' "%(d_name)
        sql_update.append(sql_upd)
       elif t_tid > d_tid :
          break
  T1 = time.time()
  print "INFO. checkAbortedTasks. Comparison done @",time.ctime(T1)," (",int(T1-T0+1)," sec)"
  if dbupdate and len(sql_update) > 0 :
   print "INFO. checkAbortedTasks.  Update database"
   (pdb,dbcur,deftDB) = connectDEFT('W')
   for sql in sql_update :
      print sql
      DButils.QueryUpdate(pdb,sql)
   DButils.QueryCommit(pdb)
   DButils.closeDB(pdb,dbcur)
  else :
     msg = "INFO. checkAbortedTasks. NO database update "
     if len(sql_update) < 1 : msg += " (no records to delete)"
     if dbupdate == False : msq += " (dbupdate flag = False)"
     print msg
  
def insertJediTasksJSON(user_task_list):
#
# insert JEDI user tasks into DEFT t_production_task table
#
    user_task_params    = {'taskid' : -1,'total_done_jobs':-1,'status' :'','submit_time' : 'None', 'start_time' : 'None',\
                           'priority' : '-1'}
    user_task_step_id    = deft_conf.daemon['user_task_step_id']
    user_task_request_id = deft_conf.daemon['user_task_request_id']
    deft_task_params = {}
    sql_update       = []
    projects_list    = []
    user_project_name= ''

    dbupdate         = True
    verbose          = False
    for i in range(0,len(user_task_list)) :
     jedi_taskid = user_task_list[i]
     deft_task_params[i] = {
      'TASKID'     : -1,
      'STEP_ID'    : user_task_step_id,
      'PR_ID'      : user_task_request_id,
      'PARENT_TID' : -1,
      'TASKNAME'   : '',
      'PROJECT'    : 'user',
      'DSN'        : '',
      'PHYS_SHORT' : '',
      'SIMULATION_TYPE' : 'anal',
      'PHYS_GROUP'      : 'user',
      'PROVENANCE'      : 'user',
      'STATUS'          : 'TBD',
      'TOTAL_EVENTS'    : -1,
      'TOTAL_REQ_JOBS'  : 0,
      'TOTAL_DONE_JOBS' : 0,
      'SUBMIT_TIME'     : 'None',
      'START_TIME'      : 'None',
      'TIMESTAMP'       : 'None',
      'TASKPRIORITY'    :  -1,
      'INPUTDATASET'    : 'XYZ',
      'PHYSICS_TAG'     : 'None',
      'VO'              : 'XYZ',
      'PRODSOURCELABEL' : '',
      'USERNAME'        : 'XYZ',
      'CURRENT_PRIORITY': -1,
      'CHAIN_TID'       : -1
}
    jedi_task_params = ''
    jedi_task_names  = ['userName','taskName','taskPriority','vo']

    # connect to Oracle
    (pdb,dbcur,deftDB) = connectJEDI('R')

    t_table_DEFT    = "%s.%s"%(deftDB,deft_conf.daemon['t_production_task'])
    t_table_JEDI    = "%s.%s"%(deftDB,deft_conf.daemon['t_task'])
    t_table_projects= "%s.%s"%(deftDB,deft_conf.daemon['t_projects'])
    i                = 0
    jedi_task_params = ''
    for p in user_task_list :

     jedi_tid = p['taskid']

     #--sql = "SELECT dbms_lob.substr(jedi_task_parameters,80000,1) FROM %s WHERE taskid=%s"%(t_table_JEDI,jedi_tid)
     #--sql = "SELECT dbms_lob.substr(jedi_task_parameters) FROM %s WHERE taskid=%s"%(t_table_JEDI,jedi_tid)
     sql = "SELECT jedi_task_parameters FROM %s WHERE taskid=%s"%(t_table_JEDI,jedi_tid)
     if verbose == True : print sql
     tasksJEDI_CLOB = DButils.QueryAll(pdb,sql)
     for t in tasksJEDI_CLOB :
      task_param = t[0]
      task_param_dict = json.loads(str(task_param))
      Skip_Record = False
      for jtn in jedi_task_names :
        param = task_param_dict[jtn]
        jtnC = jtn.upper()
        if jtnC == 'TASKPRIORITY' : 
           deft_task_params[i]['CURRENT_PRIORITY'] = param
        else :
           deft_task_params[i][jtnC] = param
     deft_task_params[i]['TASKID']     = jedi_tid
     deft_task_params[i]['PARENT_TID'] = jedi_tid
     deft_task_params[i]['CHAIN_TID']  = jedi_tid
     deft_task_params[i]['STATUS']     = p['status']
     if p['start_time'] != 'None' : deft_task_params[i]['START_TIME'] = p['start_time']
     if p['submit_time']!= 'None' : deft_task_params[i]['SUBMIT_TIME']= p['submit_time']
     if p['total_done_jobs'] != None : deft_task_params[i]['TOTAL_DONE_JOBS'] = p['total_done_jobs']

     jj = deft_task_params[i]['TASKNAME'].split('.')
     user_project_name = jj[0]
     print user_project_name

     # form insert string
     deft_names_0 = "TASKID,STEP_ID,PR_ID,PARENT_TID,TASKNAME,PROJECT,STATUS,TOTAL_EVENTS,TOTAL_REQ_JOBS,TOTAL_DONE_JOBS,"
     deft_namea_1 = "SUBMIT_TIME, START_TIME,TIMESTAMP" 
       
     if deft_task_params[i]['TOTAL_REQ_JOBS'] == 0 : 
         if deft_task_params[i]['TOTAL_DONE_JOBS'] > 0 :
            deft_task_params[i]['TOTAL_REQ_JOBS'] = deft_task_params[i]['TOTAL_DONE_JOBS']
     sql = "INSERT INTO %s "%(t_table_DEFT)
     sqlN= "(%s "%(deft_names_0)
     sqlV = "VALUES(%s,%s,%s,%s,'%s',"%\
           (deft_task_params[i]['TASKID'],user_task_step_id,user_task_request_id,\
            deft_task_params[i]['TASKID'],deft_task_params[i]['TASKNAME'])
     sqlV+="'%s','%s',%s,%s,%s,"%\
           ('user',deft_task_params[i]['STATUS'],deft_task_params[i]['TOTAL_EVENTS'],\
            deft_task_params[i]['TOTAL_REQ_JOBS'],deft_task_params[i]['TOTAL_DONE_JOBS'])

     if deft_task_params[i]['SUBMIT_TIME'] != 'None' :
      sqlN += "SUBMIT_TIME,"
      sqlV += "TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS'),"%(deft_task_params[i]['SUBMIT_TIME'])

     if deft_task_params[i]['START_TIME'] != 'None' and deft_task_params[i]['START_TIME'] != None:
        sqlN += "START_TIME,"
        sqlV += "TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS'),"%(deft_task_params[i]['START_TIME'])
     sqlN += "TIMESTAMP,"
     sqlV += "current_timestamp,"
     sqlN += "VO,PRODSOURCELABEL,USERNAME,CURRENT_PRIORITY,PRIORITY,CHAIN_TID,BUG_REPORT) "
     sqlV += "'%s','%s','%s', %s,%s,%s,%s)"%\
                                (deft_task_params[i]['VO'],'user',deft_task_params[i]['USERNAME'],\
                                 deft_task_params[i]['CURRENT_PRIORITY'],deft_task_params[i]['TASKPRIORITY'],\
                                 deft_task_params[i]['CHAIN_TID'],-1)
     sql  += sqlN
     sql  += sqlV
#-  
#   # and insert the same string into t_production_task_listpart
#     sqlP = sql.replace(t_table_DEFT,t_table_DEFT_P)
#     print sqlP
#-
     # check project
     project_found = False
     for p in projects_list :
         if p == user_project_name :
             project_found = True
             break
         if project_found : break
     if project_found == False : projects_list.append(user_project_name)
                                 
     sql_update.append(sql)
     i += 1
    DButils.closeDB(pdb,dbcur)
    if dbupdate == True :
     timenow = int(time.time())
     (pdb,dbcur,deftDB) = connectDEFT('W')
     # insert new projects (id any)
     for tp in projects_list :
         sql = "SELECT distinct project FROM %s ORDER by project"%(t_table_projects)
         print sql
         task_projects = DButils.QueryAll(pdb,sql)
         project_found = False
         for td in task_projects :
             t_project = td[0]
             if t_project == tp :
                 project_found = True
             if project_found : break
         if project_found == False :
             print "INFO.SynchronizeJediDeftTasks. New project %s. Insert it into %s"%(tp,t_table_projects)
             sql = "INSERT INTO %s (PROJECT,BEGIN_TIME,END_TIME,STATUS,TIMESTAMP) "
             sql+= "VALUES('%s',%s,%s,'active',%s)"%(tp,timenow,timenow+10*365*24*60*60,timenow)
             print sql
             sql_update.append(sql)
     for sql in sql_update :
      print sql
      DButils.QueryUpdate(pdb,sql)
     DButils.QueryCommit(pdb)
     DButils.closeDB(pdb,dbcur)

def synchronizeJediDeftDatasets () :
#
# get list of all tasks updated in 12h
#   
    timeInterval                = DATASET_SYNCH_INTERVAL # hours
    JEDI_datasets_final_states  = ''
    for s in JEDI_datasets_final_statesL :
        JEDI_datasets_final_states += "'%s',"%(s)
    JEDI_datasets_final_states = JEDI_datasets_final_states[0:(len(JEDI_datasets_final_states)-1)]
 
    DEFT_datasets_final_states  = ''
    for s in DEFT_datasets_final_statesL :
       DEFT_datasets_final_states += "'%s',"%(s)
    DEFT_datasets_final_states = DEFT_datasets_final_states[0:(len(DEFT_datasets_final_states)-1)]

    # connect to Oracle
    (pdb,dbcur,deftDB) = connectDEFT('R')

    t_table_DEFT          = "%s.%s"%(deftDB,deft_conf.daemon['t_production_task'])
    t_table_datasets_DEFT = "%s.%s"%(deftDB,deft_conf.daemon['t_production_dataset'])

    sql = "SELECT taskid, status, phys_group, timestamp, project, username FROM %s "%(t_table_DEFT)
    sql+= "WHERE TIMESTAMP > current_timestamp - %s AND taskid >= %s "%(timeInterval,MIN_DEFT_TASK_ID)
    sql+= "ORDER BY taskid"
    print sql
    tasksDEFT = DButils.QueryAll(pdb,sql)
    print "%s DEFT tasks match to the criteria"%(len(tasksDEFT))
    if len(tasksDEFT) > 0 :
     minTaskID = -1
     maxTaskID = -1
     sql = "SELECT min(taskid),max(taskid) FROM %s  "%(t_table_DEFT)
     sql +="WHERE TIMESTAMP > current_timestamp - %s AND taskid >= %s "%(timeInterval,MIN_DEFT_TASK_ID)
     print sql
     MMtasks = DButils.QueryAll(pdb,sql)
     for t in MMtasks :
         minTaskID = t[0]
         maxTaskID = t[1]
     print "INFO. Check datasets produced by %s - %s tasks"%(minTaskID,maxTaskID)
     sql = "SELECT taskid, name, status, phys_group, timestamp "
     sql+= "FROM %s WHERE taskid >= %s and taskid <= %s "%(t_table_datasets_DEFT,minTaskID,maxTaskID)
     sql += "ORDER BY taskid"
     datasetsDEFT = DButils.QueryAll(pdb,sql)
    DButils.closeDB(pdb,dbcur)

    sql_update = []
    if len(tasksDEFT) > 0 and len(datasetsDEFT) > 0 :
        # step #1 : synchronize DEFT t_production_task and t_production_dataset content
        for t in tasksDEFT :
          t_tid        = t[0]
          t_status     = t[1]
          t_phys_group = t[2]
          t_project    = t[4]
          t_owner      = t[5]
          if verbose : print "INFO. check status %s"%(t_status)
          if task_aborted_states.find(t_status) >= 0 :
           for d in datasetsDEFT :
               d_tid = d[0]
               if d_tid == t_tid :
                   d_status = d[2]
                   if d_status == None or d_status=='None' : d_status='unknown'
                   if datasets_deletion_states.find(d_status) < 0 :
                       sql = "UPDATE %s SET status='toBeDeleted',timestamp=current_timestamp WHERE taskid=%s"%\
                           (t_table_datasets_DEFT,t_tid)
                       sql_update.append(sql)
                       break
                   elif d_status == 'unknown' :
                      sql= "UPDATE %s SET status='%s',TIMESTAMP=current_timestamp WHERE taskid=%s"\
                           (t_table_datasets_DEFT,t_status,t_tid)
                      sql_update.append(sql)
               elif d_tid > t_tid :
                   print "WARNING. Cannot find dataset in %s for task %s (project: %s)"%\
                       (t_table_datasets_DEFT,t_tid,t_project)
                   break
        if len(sql_update) :
            print "INFO. synchronizeJediDeftDatasets. Step1 : Start database update"
            (pdb,dbcur,deftDB) = connectDEFT('W')
            for sql in sql_update :
                if verbose : print sql
                DButils.QueryUpdate(pdb,sql)
            DButils.QueryCommit(pdb)
            DButils.closeDB(pdb,dbcur)
        #step #2. synchronize DEFT t_production_dataset and JEDI atlas_panda.jedi_datasets content
        #connect to JEDI and get list of production datasets
        #
        # form DEFT datasets list
        #(pdb,dbcur,deftDB) = connectDEFT('R')
        #sql = "SELECT taskid,status FROM %s WHERE status IN (%s) "%(t_table_datasets_DEFT, DEFT_datasets_final_states)
        #sql+= "AND (taskid >= %s and taskid <= %s) ORDER BY taskid "%(minTaskID,maxTaskID)
        #if verbose : print sql
        #datasetsDEFT = DButils.QueryAll(pdb,sql)
        #DButils.closeDB(pdb,dbcur)
        # get JEDI datasets list
        sql_update = []
        (pdb,dbcur,jediDB) = connectPandaJEDI('R')
        t_table_datasets_JEDI = "%s.%s"%(jediDB,deft_conf.daemon['t_jedi_datasets'])
        sql = "SELECT jeditaskid, datasetname, status, nfilesfinished, nevents, creationtime, frozentime "
        sql+= "FROM %s "%(t_table_datasets_JEDI)
        sql+= "WHERE jeditaskid >= %s AND jeditaskid <= %s "%(minTaskID,maxTaskID)
        sql+= "AND datasetname NOT LIKE '%s' "%('user%')
        sql+= "AND status IN (%s) "%(JEDI_datasets_final_states)
        sql+= "ORDER BY jeditaskid"
        print sql
        datasetsJEDI = DButils.QueryAll(pdb,sql)
        DButils.closeDB(pdb,dbcur)
        for d in datasetsDEFT :
          d_tid        = d[0]
          d_name       = d[1]
          if d[2] == None : 
              d_status = 'unknown'
          else :
              d_status     = d[2]
          d_phys_group = d[3]
          found        = False
          for j in datasetsJEDI :
              j_tid    = j[0]
              j_name   = j[1]
              j_status = j[2]
              if d_tid == j_tid :
               if d_name == j_name :
                try :
                    j_nfiles = int(j[3])
                except :
                    j_nfiles = 0
                try :
                    j_nevents = int(j[4])
                except :
                    j_nevents = 0
                found    = True
                if j_status != d_status :
                 if DEFT_datasets_final_states.find(d_status) < 0 :
                  if DEFT_datasets_postproduction_states.find(d_status) < 0 :
                   sql = "UPDATE %s "%(t_table_datasets_DEFT)
                   sql+= "SET EVENTS = %s, FILES = %s, STATUS = '%s', "%(j_nevents, j_nfiles, j_status)
                   sql+= "TIMESTAMP = current_timestamp "
                   sql+= "WHERE taskid = %s AND name = '%s' "%(d_tid,d_name)
                   print sql
                   sql_update.append(sql)
                  else :
                     if verbose :
                        print "Task : ",j_tid,d_tid
                        print "DEFT : ",d_name
                        print "JEDI : ",j_name
                 else :
                     print "INFO. dataset : ",d_name
                     print "DEFT state : %s, JEDI state : %s"%(d_status,j_status)
                     print "NO %s update. DEFT dataset state is final"%(t_table_datasets_DEFT)
              elif j_tid > t_tid :
                  print "INFO. Dataset for %s task and states in '(%s)'"%(t_tid,JEDI_datasets_final_states)
                  break
        # update database
        if len(sql_update) :
            (pdb,dbcur,deftDB) = connectDEFT('W')
            print "INFO. synchronizeJediDeftDatasets. Step2 : Start database update"
            for sql in sql_update :
                if verbose : print sql
                DButils.QueryUpdate(pdb,sql)
            DButils.QueryCommit(pdb)
            DButils.closeDB(pdb,dbcur)
    else :
        print "INFO. No tasks or/and datasets match to time interval"

    

def synchronizeJediDeftTasks() :
#
# read task information from t_task and update t_production_tasks accordingly
#
    user_task_label     = 'user'   # JEDI prodsourcelabel parameter
    user_task_list      = []
    user_task_params    = {'taskid' : -1,'total_done_jobs':-1,'status' :'','submit_time' : -1, 'start_time' : 'None',\
                           'priority' : '-1','total_req_jobs':-1}

    updateIntervalHours = TASK_SYNCH_INTERVAL
    timeIntervalOracleHours = "%s/%s"%(updateIntervalHours,24)

    post_production_status = ['aborted','obsolete']
    running_status         = ['running','submitted','submitting','registered','assigned']
    end_status             = ['done','failed','finished','broken']

    # connect to Oracle
    (pdb,dbcur,deftDB) = connectDEFT('R')

    t_table_DEFT = "%s.%s"%(deftDB,deft_conf.daemon['t_production_task'])
    t_table_JEDI = "%s.%s"%(deftDB,deft_conf.daemon['t_task'])

    sql_select = "SELECT taskid, status,total_req_jobs,total_done_jobs,submit_time, start_time, current_priority "
    sql        = sql_select
    sql       += "FROM %s WHERE  timestamp > current_timestamp - %s "%(t_table_DEFT,timeIntervalOracleHours)
    sql       += "AND  taskid > %s "%(MIN_DEFT_TASK_ID)
    sql       += "ORDER by taskid"
    print sql
    tasksDEFT = DButils.QueryAll(pdb,sql)
    DButils.closeDB(pdb,dbcur)
    print "%s DEFT tasks match to the criteria"%(len(tasksDEFT))

    (pdb,dbcur,deftDB) = connectJEDI('R')
    sql_select = "SELECT taskid, status,total_done_jobs,submit_time, start_time, prodsourcelabel,"
    sql_select+= "priority,current_priority, taskname, total_req_jobs "
    sql = sql_select
    sql       += "FROM %s WHERE  timestamp > current_timestamp - %s "%(t_table_JEDI,timeIntervalOracleHours)
    sql       += "AND  taskid > %s "%(MIN_DEFT_TASK_ID)
    sql       += "ORDER by taskid"
    print sql
    tasksJEDI = DButils.QueryAll(pdb,sql)
    print "%s JEDI tasks match to the criteria"%(len(tasksJEDI))
    DButils.closeDB(pdb,dbcur)
    
    sql_update_deft = [] 
    for tj in tasksJEDI :
      tj_tid     = tj[0]
      tj_status  = tj[1]
      tj_done    = tj[2]
      if tj_done == None : tj_done = 0
      tj_submit  = tj[3]
      tj_start   = tj[4]
      tj_prodsourcelabel = tj[5]
      tj_prio    = tj[6]
      tj_curprio = tj[7]
      tj_taskname= tj[8]
      tj_req_jobs= tj[9]
      if tj_req_jobs == None or tj_req_jobs < 0 :
          tj_req_jobs = -1
      found = False
      for td in tasksDEFT :
        td_tid = td[0]
        td_done= td[3]
        td_submit = td[4]
        td_start  = td[5]
        if td_tid == tj_tid :
         # compare records
         print "Compare records for TID = %s"%(tj_tid)
         found = True
         break
        elif td_tid > tj_tid :
         break
      if found == False :
       if tj_prodsourcelabel == user_task_label or tj_taskname.find(TASK_RECOVERY_STEP) > 0 :
           print "synchroniseJediDeft INFO. Task %s NOT FOUND in %s. It is users task"%(tj_tid,t_table_DEFT)
           user_task_params['taskid']          = tj_tid
           user_task_params['status']          = tj_status
           user_task_params['total_done_jobs'] = tj_done
           user_task_params['submit_time']     = tj_submit
           user_task_params['start_time']      = tj_start
           user_task_params['priority']        = tj_prio
           user_task_params['current_priority']= tj_curprio
           user_task_params['prodsourcelabel'] = tj_prodsourcelabel
           user_task_params['total_req_jobs']  = tj_req_jobs
           user_task_list.append(user_task_params.copy())
       else :
           print "synchroniseJediDeft WARNING. Task %s NOT FOUND in %s"%(tj_tid,t_table_DEFT)
      if found == True :
        td_status = td[1]
        if tj_status != td_status :
         print "Status has changed. DEFT, JEDI : %s, %s"%(td_status,tj_status)
         if td_status in post_production_status :
           print "Ignore. DEFT status (in post_production)..."%(td_status)
         else :
           td_status   = tj_status
           td_done     = tj_done
           td_req_jobs = tj_req_jobs
           sql_update  = "UPDATE %s SET status='%s',total_done_jobs=%s,total_req_jobs=%s"%\
               (t_table_DEFT,td_status,td_done,td_req_jobs)
           if tj_start == None :
               print "Warning. Task ID = %s : invalid start time in t_task : %s (%s)"%(td_tid,tj_start,td_start)
           else :
            td_start  = tj_start
            sql_update += ",start_time=to_timestamp('%s','YYYY-MM-DD HH24:MI:SS')"%(td_start) 
           if tj_submit == None or tj_submit == 'None' :
               print "Warning. Task ID = %s : invalid submit time in t_task  : %s (%s)"%(td_tid,tj_submit,td_submit)
           else :
            td_submit = tj_submit
            sql_update += ",submit_time=to_timestamp('%s','YYYY-MM-DD HH24:MI:SS')"%(td_submit)
           sql_update += ",TIMESTAMP = current_timestamp "
           sql_update += "WHERE taskid = %s"%(td_tid)
           print sql_update
           sql_update_deft.append(sql_update)
           
    db_update = True
    if len(sql_update_deft) and db_update == True :
     (pdb,dbcur,deftDB) = connectDEFT('W')
     for sql in sql_update_deft :
         print sql
         DButils.QueryUpdate(pdb,sql)
     DButils.QueryCommit(pdb)
     DButils.closeDB(pdb,dbcur)
    elif db_update == False :
     print "INFO. No database update : db_update = %s"%(db_update)

    if len(user_task_list) :
     print "INFO. process JEDI users tasks"
     insertJediTasksJSON(user_task_list)


def synchronizeDeftRequests():
# update Production request status (caveat do not process user's requests)
    error = 0
    # connect to Oracle
    (pdb,dbcur,deftDB) = connectDEFT('R')
    t_table_Tasks    = "%s.%s"%(deftDB,deft_conf.daemon['t_production_task'])
    t_table_Requests = "%s.%s"%(deftDB,deft_conf.daemon['t_prodmanager_request'])
    t_table_Request_State = "%s.%s"%(deftDB,deft_conf.daemon['t_prodmanager_request_status'])

    request_update_list = ''
    for r in Request_update_statesL :
        request_update_list += "'%s',"%(r)
    request_update_list = request_update_list[0:(len(request_update_list)-1)] 
    sql = "SELECT taskid,pr_id,chain_tid, status,step_id FROM %s "%(t_table_Tasks)
    sql+= "WHERE status IN (%s) "%(request_update_list)
    sql+= "AND taskid > %s "%(MIN_DEFT_TASK_ID)
    sql+= "AND TIMESTAMP > current_timestamp - %s "%(REQUEST_SYNCH_INTERVAL)
    sql+= "AND project NOT LIKE '%s' "%('user%')
    sql+="ORDER BY TASKID, PR_ID, STEP_ID"
    print sql
    tasksDEFT = DButils.QueryAll(pdb,sql)

    requests      = []
    done_requests = []
    final_requests= []
    sql_update    = []

    if len(tasksDEFT) : 
     # select list of requests
     for t in tasksDEFT :
        task_id = t[0]
        req_id  = t[1]
        try :
            req_id = int(req_id)
        except :
            print "WARNING. Unknown request ID : %s (Task ID : %s)"%(req_id,task_id)
        requests.append(req_id)
     requests.sort()

     rold = -1
     for  r in requests :
      if r != rold : 
             final_requests.append(r)
      rold = r
    else :
        print "INFO. NO new tasks in the last % hours"%(REQUEST_TIME_INTERVAL_HOURS)


    for request in final_requests :
      sql = "SELECT req_s_id, pr_id, status FROM %s WHERE PR_ID=%s "%(t_table_Request_State,request)
      reqDEFT = DButils.QueryAll(pdb,sql)
      for r_s_s in reqDEFT :
        r_step_id = r_s_s[0]
        r_req_id  = r_s_s[1]
        r_status  = r_s_s[2]
        status    = r_status
        print "INFO. Process request : %s, Step : %s Current state : %s"%(r_req_id,r_step_id,r_status)
        # now go through list of tasks and find task with for request and step
        for t in tasksDEFT :
            task_id = t[0]
            req_id  = t[1]
            step_id = t[4]
            if req_id == r_req_id and step_id == r_step_id :
                task_status = t[3].lower()
                if task_status == 'registered' :
                    if r_status == 'approved' or r_status == 'registered' or r_status =='waiting' :\
                            r_status = 'processed'
                if task_status == 'running' :
                    if r_status == 'approved' or r_status == 'registered' or r_status =='waiting' or r_status == 'processed':\
                            r_status = 'executing'
                if  task_status == 'done' :
                    # check was it the last task in chain
                    done_requests.append(task_id)
                if r_status != status :
                    status = r_status
                    sql_update.append(sql)
                    # insert new record into t_prodmanager_request_status table
                    sql = "INSERT INTO %s "%(t_table_Request_State)
                    sql+= "(REQ_S_ID,COMMENT,OWNER,STATUS,TIMESTAMP,PR_ID) "
                    sql+= "VALUES(%s,'%s','%s',%s,current_timestamp,'%s'"%\
                        (step,'automatic update','ProdManager',status,request)
                    print sql
                    sys.exit(1)
    DButils.closeDB(pdb,dbcur)
    dbupdate = True
    if dbupdate :
            (pdb,dbcur,deftDB) = connectDEFT('W')
            for sql in sql_update :
                if verbose : print sql
                DButils.QueryUpdate(pdb,sql)
            DButils.QueryCommit(pdb)
            DButils.closeDB(pdb,dbcur)
    elif db_update == False :
            print "INFO. No database update : db_update = %s"%(db_update)


def synchronizeJediDeft() :
#
    T0 = int(time.time())
    print "INFO. synchronizeJediDeftTasks. Started at %s"%(time.ctime(T0))
    synchronizeJediDeftTasks()
    T1 = int(time.time())
    dT10 = int((T1 - T0)/60)
    print "INFO. synchronizeJediDeftTasks. done at %s (%s sec)"%(time.ctime(T0),dT10)
    print "INFO. synchronizeJediDeftDatasetss. Started at %s"%(time.ctime(T1))
    synchronizeJediDeftDatasets()
    T2 = int(time.time())
    dT21 = int((T2-T1)/60)
    print "INFO. synchronizeJediDeftDatasets. done at %s (%s sec)"%(time.ctime(T2),dT21)
    T3 = int(time.time())
    print "INFO. synchronize JediDeftRequests. Started at %s"%(time.ctime(T3))
    synchronizeDeftRequests()
    T4 = int(time.time())
    dT43 = ((T4 - T3)/60)
    print "INFO. synchronizeJediDeftRequests. done at %s (%s sec)"%(time.ctime(T4),dT43)

def main() :


    msg   = ''
    error = 0
    # SSO
    deft_client = DEFTClient()
    sso_info = deft_client.getUserInfo()
    #--print sso_info
    userName =  sso_info['userName']
    userId   =  sso_info['userId']
    print "INFO deft_handling : user ID : ",userId
    # simple authentication, will be replaced by Dmitry's CERN SSO
    # whoami    = os.getlogin()
    #if 'alexei.atlswing.'.find(whoami) < 0 :
    if deft_user_list.deft_users.find(userId) < 0 :
      print "%s : you are not allowed to change Tier and Cloud state "%(whoami)
      sys.exit(0)

    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:f:k:p:r:c:o:t:vu", \
                                                         ["help",\
                                                          "change-task-state",\
                                                          "check-aborted-tasks",\
                                                          "finish-task",\
                                                          "kill-task",\
                                                          "obsolete-task",\
                                                          "change-task-priority",\
                                                          "synchronizeJediDeft",
                                                          "tid",\
                                                          "prio"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    
    dbupdate             = False
    status               = 0      # return status

    changeTaskStateF     = False
    checkAbortedTasksF   = False
    finishTaskF          = False
    killTaskF            = False             
    obsoleteTaskStateF   = False
    changeTaskPriorityF  = False
    synchronizeJediDeftF = False


    task_id   = -1       # Task ID
    task_prio = -1       # Task priority
    task_state='unknown' # Task state

    for o, a in opts:
     if o in ("-h","--help") :
         usage()
         sys.exit(1)
     elif o == "-v" :
         verbose = True
     elif o == "-u"  :
         dbupdate = True
     elif o == "-c" :
         task_state = a.strip()
     elif o == "-t" :
         task_id = int(a.strip())
     elif o == "-p" :
         task_prio = int(a.strip())
     elif o == "--change-task-state" :
         changeTaskStateF = True
     elif o == '--finish-task' :
         finishTaskF = True
     elif o == "--killTask" :
         killTaskF = True
     elif o == "--obsolete-task" :
         obsoleteTaskStateF = True
     elif o == "--change-task-priority" :
         changeTaskPriorityF = True
     elif o == "--check-aborted-tasks":
         checkAbortedTasksF = True
     elif o == "--synchronizeJediDeft" :
         synchronizeJediDeftF = True

    if changeTaskStateF or obsoleteTaskStateF or changeTaskPriorityF :
       # check that other actions are not in progress
         findProcess('deft_handling','Task','Quit')      
         findProcess('deft_handling','synchronizeJediDeft','Quit')

    if changeTaskStateF == True :
        if task_id < 0 or task_state == 'unknown' :
          msg = "ERROR. Check task ID or/and Task State"
          error = 1
        else :
            msg = ("INFO. Change state for task : %s to %s")%(task_id,task_state)
        print msg
        if error == 0 :
         changeTaskState(task_id,task_state,dbupdate)

    if obsoleteTaskStateF == True :
        if task_id < 0  :
          msg = "ERROR. Check task ID"
          error = 1
        else :
            msg = ("INFO. Obsolete task : %s ")%(task_id)
        print msg
        if error == 0 :
          obsoleteTaskState(task_id,dbupdate)

    if changeTaskPriorityF == True :
        if task_id < 0 or task_prio < 0 :
          msg = "ERROR. Check task ID or/and Task Priority"
          error = 1
        else :
            msg = ("INFO. Execute JEDI cmd to change priority for task : %s to %s")%(task_id,task_prio)
        print msg
        if error == 0 : 
           status = JediTaskCmd('changeTaskPriority',task_id,task_prio)

    if checkAbortedTasksF == True :
           findProcess('deft_handling','checkAbortedTasks','Quit')
           status = checkAbortedTasks()

    if finishTaskF == True :
        if task_id < 0  :
          msg = "ERROR. Check task ID "
          error = 1
        else :
            msg = ("INFO. Execute JEDI command to finish task : %s ")%(task_id)
        print msg
        if error == 0 :
           status = JediTaskCmd('finishTask',task_id,task_prio)

    if killTaskF == True :
        if task_id < 0  :
          msg = "ERROR. Check task ID "
          error = 1
        else :
            msg = ("INFO. Execute JEDI cmd to kill task : %s ")%(task_id)
        print msg
        if error == 0 :
           status = JediTaskCmd('killTask',task_id,task_prio)
    
 

    if synchronizeJediDeftF == True :
       error = 0
       findProcess('deft_handling','synchronizeJediDeft','Quit')
       synchronizeJediDeft() 

main()

