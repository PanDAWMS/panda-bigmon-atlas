# A.Klimentov Mar 3, 2014
#
# DEFT tasks handling
# 
# Mar 15, 2014. Add json 
# Mar 19, 2014. Add JEDI Client.py
#
# Last Edit : Mar 19, 2014 ak
#

import re
import sys
import os
import getopt
import datetime
import time
import cx_Oracle
import simplejson as json

# set path and import JEDI client
sys.path.append('/data/atlswing/site-packages')
import jedi.client

import deft_conf
import deft_pass

import DButils

verbose = False

task_finish_states = 'done,finish,failed,obsolete'

def usage() :

 """
  Usage python deft_handling.py cmd

Initialization options :
-h[elp]        display this help message and exit
-v[erbose]     run in verbose mode  
-u[update]     run in update mode (update database)

Action options :
--change-task-state -t[id] TaskID -s[state] State          set task state to 'State'
--finish-task   -t[id] TaskID                              end task; running jobs will be finished;    
--kill-task -t[id]                                         end task and kill all running jobs
--obsolete-task -t[id]                                     obsolete task
--change-task-priority -t[id] TaskID -p[riority] Priority  set task priority to 'Priority'
--synchronizeJediDeft                                      synchronize DEFT task tables with JEDI ones
            
 """

 print usage.__doc__


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

    t_table_DEFT = "%s.%s"%(deftDB,deft_conf.daemon['t_production_task'])
    t_table_JEDI = "%s.%s"%(deftDB,deft_conf.daemon['t_task'])
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
        print jtn   
        param = task_param_dict[jtn]
        print param
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

     # form insert string
     deft_names_0 = "TASKID,STEP_ID,PR_ID,PARENT_TID,TASKNAME,PROJECT,STATUS,TOTAL_EVENTS,TOTAL_REQ_JOBS,TOTAL_DONE_JOBS,"
     deft_namea_1 = "SUBMIT_TIME, START_TIME,TIMESTAMP" 
       
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
     sql_update.append(sql)
     i += 1
    DButils.closeDB(pdb,dbcur)
    if dbupdate == True :
     (pdb,dbcur,deftDB) = connectDEFT('W')
     for sql in sql_update :
      print sql
      DButils.QueryUpdate(pdb,sql)
     DButils.QueryCommit(pdb)
     DButils.closeDB(pdb,dbcur)


def synchronizeJediDeft() :
#
# read task information from t_task and update t_production_tasks accordingly
#
    user_task_label     = 'user'   # JEDI prodsourcelabel parameter
    user_task_list      = []
    user_task_params    = {'taskid' : -1,'total_done_jobs':-1,'status' :'','submit_time' : -1, 'start_time' : 'None',\
                           'priority' : '-1'}

    updateIntervalHours = 12000
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
    sql       += "ORDER by taskid"
    print sql
    tasksDEFT = DButils.QueryAll(pdb,sql)
    DButils.closeDB(pdb,dbcur)
    print "%s DEFT tasks match to the criteria"%(len(tasksDEFT))

    (pdb,dbcur,deftDB) = connectJEDI('R')
    sql_select = "SELECT taskid, status,total_done_jobs,submit_time, start_time, prodsourcelabel,priority,current_priority "
    sql = sql_select
    sql       += "FROM %s WHERE  timestamp > current_timestamp - %s "%(t_table_JEDI,timeIntervalOracleHours)
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
      tj_submit  = tj[3]
      tj_start   = tj[4]
      tj_prodsourcelabel = tj[5]
      tj_prio    = tj[6]
      tj_curprio = tj[7]
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
       if tj_prodsourcelabel == user_task_label :
           print "synchroniseJediDeft INFO. Task %s NOT FOUND in %s. It is users task"%(tj_tid,t_table_DEFT)
           user_task_params['taskid']          = tj_tid
           user_task_params['status']          = tj_status
           user_task_params['total_done_jobs'] = tj_done
           user_task_params['submit_time']     = tj_submit
           user_task_params['start_time']      = tj_start
           user_task_params['priority']        = tj_prio
           user_task_params['current_priority']= tj_curprio
           user_task_params['prodsourcelabel'] = tj_prodsourcelabel
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
           td_status = tj_status
           td_done   = tj_done
           sql_update  = "UPDATE %s SET status='%s',total_done_jobs=%s"%(t_table_DEFT,td_status,td_done)
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
         DButils.QueryUpdate(pdb,sql)
     DButils.QueryCommit(pdb)
     DButils.closeDB(pdb,dbcur)
    elif db_update == False :
     print "INFO. No database update : db_update = %s"%(db_update)

    if len(user_task_list) :
     print "INFO. process JEDI users tasks"
     insertJediTasksJSON(user_task_list)

def main() :

    msg   = ''
    error = 0
    # simple authentication, will be replaced by Dmitry's CERN SSO
    whoami    = os.getlogin()
    if 'alexei.atlswing.'.find(whoami) < 0 :
     print "%s : you are not allowed to change Tier and Cloud state "%(whoami)
     sys.exit(0)

    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:f:k:p:r:c:o:t:vu", \
                                                         ["help",\
                                                          "change-task-state",\
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
     elif o == "--synchronizeJediDeft" :
         synchronizeJediDeftF = True

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
        if error == 0 : status = JediTaskCmd('changeTaskPriority',task_id,task_prio)

    
    if finishTaskF == True :
        if task_id < 0  :
          msg = "ERROR. Check task ID "
          error = 1
        else :
            msg = ("INFO. Execute JEDI command to finish task : %s ")%(task_id)
        print msg
        if error == 0 : status = JediTaskCmd('finishTask',task_id,task_prio)

    if killTaskF == True :
        if task_id < 0  :
          msg = "ERROR. Check task ID "
          error = 1
        else :
            msg = ("INFO. Execute JEDI cmd to kill task : %s ")%(task_id)
        print msg
        if error == 0 : status = JediTaskCmd('killTask',task_id,task_prio)
    
 

    if synchronizeJediDeftF == True :
       error = 0
       synchronizeJediDeft() 

main()

