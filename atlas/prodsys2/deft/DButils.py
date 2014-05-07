import exceptions
import time
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
    except DQOracleException, oe :
      error =1
      raise oe
    cursor.close()

    return error

def QueryCommit(connection) :
    connection.commit()



def closeDB(pdb,cursor) :
    cursor.close()
    pdb.close()
    
    
