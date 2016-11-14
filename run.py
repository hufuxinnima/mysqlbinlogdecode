#!/usr/bin/env python
# -*- coding:utf-8 -*-
#Author：hufuxin
import MySQLdb
import func
import sys
import argparse
import re
reload(sys)
sys.path.append(sys.path[0])
sys.setdefaultencoding('utf8')
resultdir=func.getConfig('resultdir')
# 读取
parser = argparse.ArgumentParser()
parser.add_argument("-d","--db", action="store", dest='dbname', help="input the db name", required=True)
parser.add_argument("-t","--table", action="store", dest='tablename',default="*", help="input the table name default all table")
parser.add_argument("-k","--keys", action="store", dest='keys',default=False, help="input primary key")
parser.add_argument("-a","--action", action="store", dest='action', default="*",help="input the convert sql update or delete or insert default all type", required=True)
parser.add_argument("-c","--convert-binlog", action="store", dest='convertbinlog', default=False,help="convert-binlog")
parser.add_argument("-sp","--start-position", action="store", dest='startposition', default="",help="--start-position")
parser.add_argument("-ep","--stop-position", action="store", dest='stopposition', default="",help="--stop-position")
parser.add_argument("-sd","--start-datetime", action="store", dest='startdatetime', default="",help="--start-datetime")
parser.add_argument("-ed","--stop-datetime", action="store", dest='stopdatetime', default="",help="--stop-datetime")
args = parser.parse_args()
args.dbname = args.dbname.strip()
if __name__ == '__main__':
    binlogfile = func.convertMysqlbinlog(args.startposition,args.stopposition,args.startdatetime,args.stopdatetime,args.convertbinlog)
    try:
        conns = MySQLdb.connect(host=func.getConfig('host'), user=func.getConfig('user'), passwd=func.getConfig('passwd'), db=args.dbname,port=int(func.getConfig('port')), connect_timeout=5)
        cursor = conns.cursor()
    except Exception,e:
        print e
        exit()
    if binlogfile:
        if args.tablename == "*":
            tablename=[]
        else:
            tablename = re.sub(r'\s+', ' ', args.tablename).split(' ')
        tablefield = func.getTableField(cursor, args.dbname,tablename)
        if tablefield:
            param = {}
            resultfilelist = func.fileList(resultdir)
            func.delAllFile(resultdir,resultfilelist)
            if args.keys:
                param['keys']=args.keys
            func.converRun(args.action, binlogfile, str(args.dbname), tablefield, param)
        else:
            print "Error: table error"
    else:
        print "Error: binlog convert file is null please check dir '" + func.getConfig('sqlfiledir')+"'"
