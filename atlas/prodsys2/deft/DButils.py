import exceptions
import time
import sys
import datetime
import traceback


#
try :
 import cx_Oracle
except :
  print "****ERROR : DButils. Cannot import cx_Oracle"
  pass
#
def connectDEFT(dbname,dbuser,pwd) :
  connect=cx_Oracle.connect(dbuser,pwd,dbname)
  cursor=connect.cursor()

  return connect,cursor


def QueryAll(connection,query) :
    cursor = connection.cursor()
    # print query
    cursor.execute(query)

    dbrows=cursor.fetchall()
    cursor.close()

    return dbrows


def QueryUpdate(connection,query) :
    error = 0

    cursor = connection.cursor()
    
    try :
     cursor.execute(query)
    except  :
      error =1
      print "Error : QueryUpdate - Oracle exception ; query ",query
      sys.exit(1)
    cursor.close()

    return error

def QueryCommit(connection) :
    connection.commit()



def closeDB(pdb,cursor) :
    cursor.close()
    pdb.close()
    
    
