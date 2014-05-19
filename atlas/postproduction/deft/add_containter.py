# A.Klimentov May 13, 2014
#
# register productiuon containers and add datasets
#
# Last Edit : May 14, 2014 ak
#

import re
import sys
import os
import getopt
import commands
import datetime
import time
import cx_Oracle

import deft_conf
import deft_pass
import deft_user_list

import DButils

#
from dq2.clientapi.DQ2 import DQ2

from dq2.common.DQConstants import DatasetState
from dq2.common.DQException import DQBackendException

from dq2.common.client.DQClientException import DQInternalServerException


from dq2.common.dao.DQDaoException import *

from dq2.common import get_dict_item

from dq2.location.DQLocationConstants import LocationState
from dq2.clientapi.cli.SetMetaDataAttribute import SetMetaDataAttribute

dq2api = DQ2()


verbose = False

task_finish_states         = 'done,finish,failed,obsolete,aborted'
task_aborted_states        =  'aborted,failed,obsolete'
datasets_deletion_states   =  'toBeDeleted,Deleted'

user_task_label            = 'user'   # JEDI prodsourcelabel parameter

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





def addTidDatasetToContainer():
#
# get list of TID datasets
# register container if it isn't registered yet
# add TID dataset to Container
#
 
    timeInterval                = DATASET_SYNCH_INTERVAL # hours
    nDQ2ErrorsInRow             = 0
    nContainers                 = 0
    nDatasets                   = 0
    dbupdate                    = True
    minTaskID                   = MIN_DEFT_TASK_ID
    maxTaskID                   = minTaskID*10

    DEFT_datasets_final_states  = ''
    for s in DEFT_datasets_done_statesL :
       DEFT_datasets_final_states += "'%s',"%(s)
    DEFT_datasets_final_states = DEFT_datasets_final_states[0:(len(DEFT_datasets_final_states)-1)]

    # connect to Oracle
    (pdb,dbcur,deftDB) = connectDEFT('R')

    t_table_datasets_DEFT   = "%s.%s"%(deftDB,deft_conf.daemon['t_production_dataset'])
    t_table_containers_DEFT = "%s.%s"%(deftDB,deft_conf.daemon['t_production_container'])

    # get list of datasets
    sql = "SELECT name, taskid, pr_id, status, phys_group, timestamp FROM %s "%(t_table_datasets_DEFT)
    sql+= "WHERE TIMESTAMP > current_timestamp - %s AND taskid >= %s "%(timeInterval,MIN_DEFT_TASK_ID)
    sql+= "AND (name NOT LIKE '%s' AND name NOT LIKE '%s') "%('user%','%.log.%')
    sql+= "AND status in (%s) "%(DEFT_datasets_final_states)
    sql+= "ORDER BY taskid"
    print sql
    dsets = DButils.QueryAll(pdb,sql)
    # get min and max task ID
    sql = "SELECT min(parent_tid), max(parent_tid) FROM %s "%(t_table_datasets_DEFT)
    sql+= "WHERE TIMESTAMP > current_timestamp - %s AND taskid >= %s "%(timeInterval,MIN_DEFT_TASK_ID)
    sql+= "AND (name NOT LIKE '%s' AND name NOT like '%s') "%('user%','%.log.%')
    sql+= "AND status in (%s) "%(DEFT_datasets_final_states)
    print sql
    mimax = DButils.QueryAll(pdb,sql)
    if len(mimax) :
     for i in mimax :
        minTaskID = i[0]
        maxTaskID = i[1]
    else :
        print "Warning. Cannot find information for query"
    # get list of containers
    sql = "SELECT name, parent_tid, status, c_time FROM %s "%(t_table_containers_DEFT)
    sql+= "WHERE parent_tid >= %s and parent_tid <= %s "%(minTaskID, maxTaskID)
    sql+= "ORDER by parent_tid"
    print sql
    cntrs =  DButils.QueryAll(pdb,sql)

    DButils.closeDB(pdb,dbcur)
    
    # prepare a list of containers to be registered
    sql_update = []
    for ds in dsets :
        dsname   = ds[0]
        d_tid    = ds[1]
        d_rid    = ds[2]
        d_status = ds[3]
        d_phgroup= ds[4]
        if dsname.find('.log') < 0 :
              junk = dsname.split('_tid')
              top_dsname = junk[0].strip()
              cnt_name = "%s/"%(top_dsname)
              print "Check containers list"
              cnt_list_flag = 0 # container registered in database
              ddm_list_flag = 0 # in DDM
              reg_dset_flag = 0 # new dataset(s) added to the container
              for cn in cntrs :
                  cname = cn[0]
                  c_tid = cn[1]
                  c_time= cn[3]
                  if c_tid == d_tid :
                      if cname == cnt_name :
                       print "Container %s found in database (task id = %s, registration time : %s)"%(cname,c_tid,c_time)
                       cnt_list_flag = 1
                  if c_tid > d_tid :
                      break
              if cnt_list_flag != 1 :
                  print "Container %s NOT found in database "%(cnt_name)
              print "Check DDM catalog : %s"%(cnt_name)
              error = 0
              try :
                  ret = dq2api.listDatasets(cnt_name)
                  print ret
                  if len(ret) > 0 : ddm_list_flag = 1
              except :
                  print "ERROR - cannot execute : %s%s%s "%("ret = dq2api.listDatasets(",cnt_name,")")
                  error = 1
              if ddm_list_flag == 1 and error == 0 :
                  msg = "Container %s exists in DDM "%(cnt_name)
                  if cnt_list_flag == 0 : msg += ". Get meta-info and add it to the database"
                  if cnt_list_flag == 1 : msg += "and in database. Do nothing. Proceed to datasets registration in the container"
                  print msg
              if ddm_list_flag == 0 :
                  print "Register container : %s"%(cnt_name)
                  try :
                      dq2api.registerContainer(cnt_name)
                      nContainers += 1
                  except :
                      error = 1
                      print "Error  dq2api.registerContainer(%s)"%(cnt_name)
                      print "do no update database (%s)"%(cnt_name)
              if error == 1 :
                  print "Error in DDM part. quit"
                  sys.exit(1)
              if error == 0 :
               # get creation date
               creationdate = dq2api.getMetaDataAttribute(cnt_name, ['creationdate',])
               c_time = creationdate['creationdate']
               ELEMENTS = []
               print "Register new elements in %s (%s)"%(cnt_name,dsname)
               ELEMENTS.append(dsname)
               try :
                      ret = dq2api.registerDatasetsInContainer(cnt_name,ELEMENTS)
                      nDatasets += len(ELEMENTS)
                      reg_dset_flag = 1
               except (DQException):
                      print "Warning : %s already has dataset : %s"%(cnt_name,dsname)
                      reg_dset_flag = 0
               except : 
                      """fatal error... I increment the error but you can exit if you want"""
                      nDQ2ErrorsInRow +=1
                      error = 1
                      print "Fatal error in registerDatasetsInContainer. Quit  "
                      sys.exit(1)
               # form sql statetment
               sql =''
               if d_rid == None or d_rid == 'None' : d_rid=-1
               if cnt_list_flag == 1 :
                   if reg_dset_flag == 1 :
                          sql = "UPDATE %s SET d_time = current_timestamp WHERE name = '%s'"%(t_table_containers_DEFT,cnt_name)
                          sql_update.append(sql)
               else :
                sql = "INSERT INTO %s VALUES"%(t_table_containers_DEFT)
                if reg_dset_flag == 0 :   
                    sql+= "('%s',%s,%s,'%s','%s',to_timestamp('%s','YYYY-MM-DD HH24:MI:SS'),to_timestamp('%s','YYYY-MM-DD HH24:MI:SS'),current_timestamp)"%(cnt_name,d_tid,d_rid,'registered',d_phgroup,c_time,c_time)
                else :
                    sql+= "('%s',%s,%s,'%s','%s',to_timestamp('%s','YYYY-MM-DD HH24:MI:SS'),current_timestamp,current_timestamp)"%\
                      (cnt_name,d_tid,d_rid,'registered',d_phgroup,c_time)
               if len(sql) : sql_update.append(sql)
    if dbupdate  :
        (pdb,dbcur,deftDB) = connectDEFT('W')
        for sql in sql_update :
            print "SQL update : ",sql
            DButils.QueryUpdate(pdb,sql)
        DButils.QueryCommit(pdb)
        DButils.closeDB(pdb,dbcur)
    else :
        print "No database update"
    print "addTiddatasets. Container registered : %s, Datasets registered : %s"%(nContainers, nDatasets)


def main() :

    addTidDatasetToContainer()

main()

