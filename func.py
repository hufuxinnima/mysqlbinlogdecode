#!/usr/bin/env python
# -*- coding:utf-8 -*-
#Author：hufuxin
import logging
import ConfigParser
import re
from multiprocessing import Pool,Manager
import os,sys
import hashlib

countdict = Manager().dict()
#获取配置文件选项
def getConfig(config_name):
    config = ConfigParser.ConfigParser()
    config.read("./etc/config")
    config_values = config.get('configs', config_name)
    return config_values

def md5(str):
    m = hashlib.md5()
    m.update(str)
    return m.hexdigest()
# 日志记录模块
def logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler("../logs/PYbinlog.log")
    fh.setLevel(logging.INFO)
    datefrm = "%y-%m-%d %H:%M:%S"
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefrm)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
def fileList(path):
    filelist = []
    try:
        list=os.listdir(path)
        orderfile=getConfig('orderfile')
        if orderfile == 'desc':
            list.sort(reverse=True)
        else:
            list.sort()

        for filename in list:
            if filename == ".gitkeep":
                continue
            filelist.append(filename)
    except:
        pass
    return filelist
def delAllFile(fdir,filelist):
    try:
        if filelist:
            for f in filelist:
                os.remove(fdir + f)
    except:
        print 'delete error'
        pass
# 获取数据表结构模块
def getTableField(cursor,dbname,tablename=[]):
    returndata = {}
    try:
        if  len(tablename) == 0:
            cursor.execute("show tables")
            tdata = cursor.fetchall()
            if tdata:
                for row in tdata:
                    tablename.append(row[0])
        if len(tablename) > 0:
            sql = "select TABLE_NAME,COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='" + str(dbname) + "' and  TABLE_NAME in ('" + str("','".join(tablename)) + "')"
        else:
            sql = "select TABLE_NAME,COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='" + str(dbname) + "'"
        cursor.execute(sql)
        data = cursor.fetchall()
        if data:
            for row in data:
                if returndata.has_key(row[0]):
                    returndata[row[0]].append(row[1])
                else:
                    returndata[row[0]] = [row[1]]
        else:
            print "Error: table " + "','".join(tablename) + " not find in db " + dbname
            exit()
        print returndata
        return returndata
    except Exception, e:
        print "getTableField:"
        print e
        return returndata
def convertMysqlbinlog(startposition,stopposition,startdatetime,stopdatetime,convertbinlog):
    sqlfiledir=getConfig('sqlfiledir')
    binglogdir=getConfig('binglogdir')
    processnum = getConfig('binlogprocess')
    returnfile=[]
    sysdir=sys.path[0]+"/"
    if convertbinlog:
        filelist = fileList(binglogdir)
        command="/usr/bin/mysqlbinlog --no-defaults -v -v  --base64-output=decode-rows "
        if startposition:
            command = command + " --start-position=" + startposition
        if stopposition:
            command = command + " --stop-position=" + stopposition
        if startdatetime:
            command = command + " --start-datetime='" + startdatetime + "' "
        if stopdatetime:
            command = command + " --stop-datetime='" + stopdatetime + "' "
        if len(filelist) > 0:
            sqlfilelist = fileList(sqlfiledir)
            delAllFile(sqlfiledir,sqlfilelist)
            p = Pool(int(processnum))
            for fl in filelist:
                print "Info: convering "+ fl +" ..."
                p.apply_async(os.system, args=(command + sysdir + binglogdir + fl + " > " + sysdir + sqlfiledir + "sql-"+fl,))
            p.close()
            p.join()
        else:
            print "Error: binlog file is null"
            exit()
        print "Info: conver all binlog is success"
    returnfile = fileList(sqlfiledir)
    return returnfile
def addslashes(s):
    #d = {'"':'\\"', "'":"\\'", "\0":"\\\0", "\\":"\\\\"}
    d = {"'": "\\'"}
    return ''.join(d.get(c, c) for c in s)
def converRun(action,binlogfile,dbname,tablefield,param={}):
    args={}
    processnum = getConfig('sqlprocess')
    sqlfiledir = getConfig('sqlfiledir')
    if 'keys' in param:
        keysdict = {}
        keys = re.sub(r'\s+', ' ', param['keys']).split(' ')
        for i in keys:
            keysdict[i] = 1
        args['keysdict'] = keysdict
    # update
    if action=='update':
        funcname="converUpdate"
    elif action == 'updatekey':
        funcname = "converKeyUpdate"
        if not args.has_key('keysdict'):
            print "keys is null input keys"
            exit()
    # insert
    elif action == 'insert':
        funcname = "converInsert"
        args['maxnum'] = int(getConfig('insertmaxnum'))
    elif action == 'insertkey':
        funcname = "converKeyInsert"
        args['maxnum'] = int(getConfig('insertmaxnum'))
        if not args.has_key('keysdict'):
            print "keys is null input keys"
            exit()
    elif action == 'insertuniquekey':
        funcname = "converUniqueKeyInsert"
        args['maxnum'] = int(getConfig('insertmaxnum'))
    # delete
    elif action == 'delete':
        funcname = "converDelete"
        args['deletewhere'] = int(getConfig('deletewhere'))
    elif action == 'deletekey':
        funcname = "converKeyDelete"
        args['deletewhere'] = int(getConfig('deletewhere'))
        if not args.has_key('keysdict'):
            print "keys is null input keys"
            exit()
    elif action == 'deletetoinsert':
        funcname = "converDeleteToInsert"

    # all
    elif action == 'all':
        funcname = "converAll"
        args['deletewhere'] = int(getConfig('deletewhere'))
    elif action == 'allkey':
        funcname = "converKeyAll"
        args['deletewhere'] = int(getConfig('deletewhere'))
        if not args.has_key('keysdict'):
            print "keys is null input keys"
            exit()
    # count
    elif action == 'countall':
        funcname = "countAll"

    if len(binlogfile) > 0:
        p = Pool(int(processnum))
        for filename in binlogfile:
            print filename + " convering..."
            sqlfile = sqlfiledir + filename
            p.apply_async(eval(funcname),args=(filename,sqlfile, dbname, tablefield,args))
        p.close()
        p.join()
        if len(countdict) >0:
            alldic = {}
            for ck in countdict.keys():
                if countdict.has_key(ck):
                    for k in countdict[ck].keys():
                        if alldic.has_key(k):
                            alldic[k] = alldic[k] + countdict[ck][k]
                        else:
                            alldic[k] = countdict[ck][k]
            print alldic
    else:
        print "Error: conver binlog is null "
        exit()
    print "Info: success !"
#解析所有update语句
def converUpdate(filename,sqlfile,dbname,tablefield,args={}):

    f = open(sqlfile,'r')
    fieldsnum = where = set = 0
    fields = []
    wherestr = sqlstr = ''
    result = list()
    for line in f.readlines():
        if fieldsnum > 0:
            if line.startswith('### WHERE'):
                where=1
                continue
            else:
                if where and line.startswith('###   @1='):
                    slen = line.rfind("/*")
                    if slen > 0:
                        wherestr = " WHERE"+ line[:slen].strip().replace("@1",fields[0],1).replace('### ', '',1) + ";"
                    else:
                        print "[Error] " + line
                        exit()
                    where = 0
                    continue
            if line.startswith('### SET'):
                sqlstr = sqlstr + ' SET '
                set=1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    vs = line.find("=")
                    ve = line.rfind("/*")
                    if vs > 0 and ve >0 :
                        sqlstr = sqlstr + " " + "`"+fields[set-1]+"`='"+ addslashes(line[vs + 1:ve].strip().strip("'")) +"',"
                    else:
                        print "[Error] " + line
                        exit()
                    if set == num:
                        set = fieldsnum = 0

                        result.append(sqlstr.strip(",") + wherestr)
                        sqlstr = ''
                        wherestr=''
                        fields = []
                        continue
                    else:
                        set = set + 1
                        continue
        if line.startswith('### UPDATE `'+ dbname +'`'):
            t = re.search("`(.*)`\.`(.*)`", line)
            tablename = t.group(2)
            if tablename and tablefield.has_key(tablename):
                fields = tablefield[tablename]
                fieldsnum = 1
                sqlstr = line.split("/*")[0].strip('\n').replace('### ', '',1)
    f.close()
    fileDump(result,filename)


#解析含有指定主键的update
def converKeyUpdate(filename,sqlfile,dbname,tablefield,args={}):
    f = open(sqlfile, 'r')
    fieldsnum = where = set = 0
    fields = []
    wherestr = sqlstr = ''
    result = list()
    for line in f.readlines():

        if fieldsnum > 0:
            if line.startswith('### WHERE'):
                where = 1
                continue
            else:
                if where and line.startswith('###   @1='):
                    slen = line.rfind("/*")
                    keystr = line[:slen].strip()
                    keylen = keystr.find("=")
                    getkey = keystr[keylen+1:].strip("'")
                    if args['keysdict'].has_key(getkey):
                        if slen > 0:
                            wherestr = " WHERE" + line[:slen].strip().replace("@1", fields[0],1).replace('### ', '',1) + ";"
                        else:
                            print "[Error] " + line
                            exit()
                        where = 0
                        continue
                    else:
                        fieldsnum = 0
                        sqlstr = ''
                        fields = []
                        continue
            if line.startswith('### SET'):
                sqlstr = sqlstr + ' SET '
                set = 1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    vs = line.find("=")
                    ve = line.rfind("/*")
                    if vs > 0 and ve > 0:
                        sqlstr = sqlstr + " " + "`" + fields[set - 1] + "`='" + addslashes(line[vs + 1:ve].strip().strip("'")) + "',"
                    else:
                        print "[Error] " + line
                        exit()
                    if set == num:
                        set = fieldsnum = 0
                        result.append(sqlstr.strip(",") + wherestr)
                        sqlstr = ''
                        wherestr = ''
                        fields = []
                        continue
                    else:
                        set = set + 1
                        continue
        if line.startswith('### UPDATE `' + dbname + '`'):
            t = re.search("`(.*)`\.`(.*)`", line)
            tablename = t.group(2)
            if tablename and tablefield.has_key(tablename):
                fields = tablefield[tablename]
                fieldsnum = 1
                sqlstr = line.split("/*")[0].strip('\n').replace('### ', '',1)
    f.close()
    fileDump(result, filename)
#解析所有inser语句
def converInsert(filename,sqlfile,dbname,tablefield,args={}):
    f = open(sqlfile, 'r')
    fieldsnum  = set = 0
    fields = inserdata = []
    loopdata ={}
    result = list()
    for line in f.readlines():
        if fieldsnum > 0:
            if line.startswith('### SET'):
                set = 1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    slen = line.rfind("/*")
                    keystr = line[:slen].strip()
                    keylen = keystr.find("=")
                    inserdata.append("'"+ addslashes(keystr[keylen + 1:].strip().strip("'")) +"'")
                    if set == num:
                        set = fieldsnum = 0
                        fields = []
                        if len(inserdata) == num:
                            loopdata[tablename].append("("+ ",".join(inserdata) + ")")
                        else:
                            print "Error: " + tablename +" "+ inserdata + "insert field num error";
                        inserdata = []
                        continue
                    else:
                        set = set + 1
                        continue
        if line.startswith('### INSERT INTO `' + dbname + '`'):
            t = re.search("`(.*)`\.`(.*)`", line)
            tablename = t.group(2)
            if tablename and tablefield.has_key(tablename):
                fields = tablefield[tablename]
                if loopdata.has_key(tablename):
                    if len(loopdata[tablename]) == args["maxnum"]:
                        result.append(line.split("/*")[0].strip('\n').replace('### ', '',1) + " VALUES \n" + ",\n".join(loopdata[tablename]) + ";")
                        loopdata[tablename] = []
                else:
                    loopdata[tablename] = []
                fieldsnum = 1

    #清理残余数据
    for k,v in loopdata.items():
        if v:
            result.append("INSERT INTO `" + dbname + "`.`" + k + "` VALUES \n"+ ",\n".join(v) + ";")
    f.close()
    fileDump(result, filename)

#查找指定主键的insert语句
def converKeyInsert(filename,sqlfile,dbname,tablefield,args={}):
    f = open(sqlfile, 'r')
    fieldsnum  = set  = 0
    fields = inserdata = []
    loopdata ={}
    result = list()
    for line in f.readlines():
        if fieldsnum > 0:
            if line.startswith('### SET'):
                set = 1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    slen = line.rfind("/*")
                    keystr = line[:slen].strip()
                    keylen = keystr.find("=")
                    if line.startswith('###   @1='):
                        idkey = keystr[keylen + 1:].strip("'")
                        if not args['keysdict'].has_key(idkey):
                            set = fieldsnum = 0
                            fields = inserdata = []
                            continue
                    inserdata.append("'" + addslashes(keystr[keylen + 1:].strip().strip("'")) + "'")
                    if set == num:
                        set = fieldsnum = 0
                        if len(inserdata) == num:
                            loopdata[tablename].append("("+ ",".join(inserdata) + ")")
                        else:
                            print "Error: " + tablename +" "+ inserdata + "insert field num error";
                        fields=inserdata = []
                        continue
                    else:
                        set = set + 1
                        continue
        if line.startswith('### INSERT INTO `' + dbname + '`'):
            t = re.search("`(.*)`\.`(.*)`", line)
            tablename = t.group(2)
            if tablename and tablefield.has_key(tablename):
                fields = tablefield[tablename]
                if loopdata.has_key(tablename):
                    if len(loopdata[tablename]) == args["maxnum"]:
                        result.append(line.split("/*")[0].strip('\n').replace('### ', '',1) + " VALUES \n" + ",\n".join(loopdata[tablename]) + ";")
                        loopdata[tablename] = []
                else:
                    loopdata[tablename] = []
                fieldsnum = 1

    #清理残余数据
    for k,v in loopdata.items():
        if v:
            result.append("INSERT INTO `" + dbname + "`.`" + k + "` VALUES \n"+ ",\n".join(v) + ";")
    f.close()
    fileDump(result, filename)

#查找去重复主键的insert语句
def converUniqueKeyInsert(filename,sqlfile,dbname,tablefield,args={}):
    f = open(sqlfile, 'r')
    fieldsnum  = set = 0
    fields = inserdata = []
    loopdata ={}
    result = list()
    for line in f.readlines():
        if fieldsnum > 0:
            if line.startswith('### SET'):
                set = 1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    slen = line.rfind("/*")
                    keystr = line[:slen].strip()
                    keylen = keystr.find("=")
                    if line.startswith('###   @1='):
                        idkey = keystr[keylen + 1:].strip("'")
                    inserdata.append("'" + addslashes(keystr[keylen + 1:].strip().strip("'")) + "'")
                    if set == num:

                        fields = []
                        if len(inserdata) == num:
                            loopdata[tablename][str(idkey)] = "("+ ",".join(inserdata) + ")"
                        else:
                            print "Error: " + tablename +" "+ inserdata + "insert field num error";
                        set = fieldsnum = idkey = 0
                        inserdata = []
                        continue
                    else:
                        set = set + 1
                        continue
        if line.startswith('### INSERT INTO `' + dbname + '`'):
            t = re.search("`(.*)`\.`(.*)`", line)
            tablename = t.group(2)
            if tablename and tablefield.has_key(tablename):
                fields = tablefield[tablename]
                if loopdata.has_key(tablename):
                    if len(loopdata[tablename]) == args["maxnum"]:
                        result.append(line.split("/*")[0].strip('\n').replace('### ', '',1) + " VALUES \n" + ",\n".join(list(loopdata[tablename].values())) + ";")
                        loopdata[tablename] = {}
                else:
                    loopdata[tablename] = {}
                fieldsnum = 1

    #清理残余数据
    for k,v in loopdata.items():
        if v:
            result.append("INSERT INTO `" + dbname + "`.`" + k + "` VALUES \n"+ ",\n".join(list(v.values())) + ";")
    f.close()
    fileDump(result, filename)

def converDelete(filename,sqlfile,dbname,tablefield,args={}):
    f = open(sqlfile, 'r')
    fieldsnum  = set = 0
    fields = deletedata = []
    result = list()
    for line in f.readlines():
        if fieldsnum > 0:
            if line.startswith('### WHERE'):
                set = 1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    slen = line.rfind("/*")
                    keystr = line[:slen].strip()
                    keylen = keystr.find("=")

                    if args["deletewhere"] == 1:
                        deletedata.append("`"+fields[set-1] + "`=" + keystr[keylen + 1:].strip())
                        if set == num:
                            if len(deletedata) == num:
                                result.append("DELETE FROM `" + dbname + "`.`" + tablename + "` WHERE " + " AND ".join(deletedata) + ";")
                            else:
                                print "Error: " + tablename + " " + deletedata + "insert field num error";
                            set = fieldsnum  = 0
                            deletedata =fields= []
                            continue
                        else:
                            set = set + 1
                            continue
                    else:
                        if line.startswith('###   @1='):
                            result.append("DELETE FROM `" + dbname + "`.`" + tablename +"` WHERE `" + fields[0] + "`="+ keystr[keylen + 1:].strip() + ";")
                            set = fieldsnum = 0
                            deletedata = fields = []
                            continue

        if line.startswith('### DELETE FROM `' + dbname + '`'):
            t = re.search("`(.*)`\.`(.*)`", line)
            tablename = t.group(2)
            if tablename and tablefield.has_key(tablename):
                fields = tablefield[tablename]
                fieldsnum = 1
    f.close()
    fileDump(result, filename)

def converKeyDelete(filename,sqlfile,dbname,tablefield,args={}):
    f = open(sqlfile, 'r')
    fieldsnum  = set  = 0
    fields = deletedata = []
    result = list()
    for line in f.readlines():
        if fieldsnum > 0:
            if line.startswith('### WHERE'):
                set = 1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    slen = line.rfind("/*")
                    keystr = line[:slen].strip()
                    keylen = keystr.find("=")

                    if args["deletewhere"] == 1:
                        if line.startswith('###   @1='):
                            idkey = keystr[keylen + 1:].strip("'")
                            if not args['keysdict'].has_key(idkey):
                                set = fieldsnum  = 0
                                deletedata = fields = []
                                continue

                        deletedata.append("`"+fields[set-1] + "`=" + keystr[keylen + 1:].strip())
                        if set == num:
                            if len(deletedata) == num:
                                result.append("DELETE FROM `" + dbname + "`.`" + tablename + "` WHERE " + " AND ".join(deletedata) + ";")
                            else:
                                print "Error: " + tablename + " " + deletedata + "insert field num error";
                            set = fieldsnum   = 0
                            deletedata =fields= []
                            continue
                        else:
                            set = set + 1
                            continue
                    else:
                        if line.startswith('###   @1='):
                            idkey = keystr[keylen + 1:].strip("'")
                            if  args['keysdict'].has_key(idkey):
                                result.append("DELETE FROM `" + dbname + "`.`" + tablename +"` WHERE `" + fields[0] + "`="+ keystr[keylen + 1:].strip() + ";")
                            set = fieldsnum  = 0
                            deletedata = fields = []
                            continue

        if line.startswith('### DELETE FROM `' + dbname + '`'):
            t = re.search("`(.*)`\.`(.*)`", line)
            tablename = t.group(2)
            if tablename and tablefield.has_key(tablename):
                fields = tablefield[tablename]
                fieldsnum = 1
    f.close()
    fileDump(result, filename)

def converDeleteToInsert(filename,sqlfile,dbname,tablefield,args={}):
    f = open(sqlfile, 'r')
    fieldsnum  = set = 0
    fields = deletedata = []
    result = list()
    for line in f.readlines():
        if fieldsnum > 0:
            if line.startswith('### WHERE'):
                set = 1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    slen = line.rfind("/*")
                    keystr = line[:slen].strip()
                    keylen = keystr.find("=")
                    deletedata.append("'"+addslashes(keystr[keylen + 1:].strip().strip("'"))+"'")
                    if set == num:
                        if len(deletedata) == num:
                            result.append("INSERT INTO `" + dbname + "`.`" + tablename + "` VALUES (" + ",".join(deletedata) + ");")
                        else:
                            print "Error: " + tablename + " " + deletedata + "insert field num error";
                        set = fieldsnum  = 0
                        deletedata =fields= []
                        continue
                    else:
                        set = set + 1
                        continue
        if line.startswith('### DELETE FROM `' + dbname + '`'):
            t = re.search("`(.*)`\.`(.*)`", line)
            tablename = t.group(2)
            if tablename and tablefield.has_key(tablename):
                fields = tablefield[tablename]
                fieldsnum = 1
    f.close()
    fileDump(result, filename)

def converAll(filename,sqlfile,dbname,tablefield,args={}):
    f = open(sqlfile, 'r')
    fieldsnum  = set = where = 0
    wherestr = sqlstr = ''
    fields = tmpdata = []
    result = list()
    for line in f.readlines():
        # update
        if fieldsnum == 1:
            if line.startswith('### WHERE'):
                where = 1
                continue
            else:
                if where and line.startswith('###   @1='):
                    slen = line.rfind("/*")
                    if slen > 0:
                        wherestr = " WHERE" + line[:slen].strip().replace("@1", fields[0], 1).replace('### ', '',1) + ";"
                    else:
                        print "[Error] " + line
                        exit()
                    where = 0
                    continue
            if line.startswith('### SET'):
                sqlstr = sqlstr + ' SET '
                set = 1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    vs = line.find("=")
                    ve = line.rfind("/*")
                    if vs > 0 and ve > 0:
                        sqlstr = sqlstr + " " + "`" + fields[set - 1] + "`='" + addslashes(line[vs + 1:ve].strip().strip("'")) + "',"
                    else:
                        print "[Error] " + line
                        exit()
                    if set == num:
                        set = fieldsnum = 0
                        result.append(sqlstr.strip(",") + wherestr)
                        sqlstr = wherestr =''
                        fields = []
                        continue
                    else:
                        set = set + 1
                        continue
        # insert
        elif fieldsnum == 2:
            if line.startswith('### SET'):
                set = 1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    slen = line.rfind("/*")
                    keystr = line[:slen].strip()
                    keylen = keystr.find("=")
                    tmpdata.append("'"+addslashes(keystr[keylen + 1:].strip().strip("'"))+"'")
                    if set == num:
                        if len(tmpdata) == num:
                            result.append("INSERT INTO `" + dbname + "`.`" + tablename + "` VALUES (" + ",".join(tmpdata) + ");")
                        else:
                            print "Error: " + tablename + " " + tmpdata + "insert field num error";
                        tmpdata = fields = []
                        set = fieldsnum = 0
                        continue
                    else:
                        set = set + 1
                        continue
        # delete
        elif fieldsnum == 3:
            if line.startswith('### WHERE'):
                set = 1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    slen = line.rfind("/*")
                    keystr = line[:slen].strip()
                    keylen = keystr.find("=")

                    if args["deletewhere"] == 1:
                        tmpdata.append("`" + fields[set - 1] + "`='" + addslashes(keystr[keylen + 1:].strip().strip("'"))+ "'" )
                        if set == num:
                            if len(tmpdata) == num:
                                result.append("DELETE FROM `" + dbname + "`.`" + tablename + "` WHERE " + " AND ".join(
                                    tmpdata) + ";")
                            else:
                                print "Error: " + tablename + " " + tmpdata + "insert field num error";
                            set = fieldsnum = 0
                            tmpdata = fields = []
                            continue
                        else:
                            set = set + 1
                            continue
                    else:
                        if line.startswith('###   @1='):
                            result.append(
                                "DELETE FROM `" + dbname + "`.`" + tablename + "` WHERE `" + fields[0] + "`=" + keystr[
                                                                                                                keylen + 1:].strip() + ";")
                            set = fieldsnum = 0
                            tmpdata = fields = []
                            continue
        if line.startswith('### UPDATE `' + dbname + '`') or line.startswith('### INSERT INTO `' + dbname + '`') or line.startswith('### DELETE FROM `' + dbname + '`'):
            t = re.search("`(.*)`\.`(.*)`", line)
            action = line.split(" ")[1].strip()
            tablename = t.group(2)
            if tablename and tablefield.has_key(tablename):
                fields = tablefield[tablename]
                if action == "UPDATE":
                    sqlstr = line.split("/*")[0].strip('\n').replace('### ', '', 1)
                    fieldsnum = 1
                elif action == "INSERT":
                    fieldsnum = 2
                elif action == "DELETE":
                    fieldsnum = 3
    f.close()
    fileDump(result, filename)




def converKeyAll(filename,sqlfile,dbname,tablefield,args={}):
    f = open(sqlfile, 'r')
    fieldsnum  = set = where = 0
    wherestr = sqlstr = ''
    fields = tmpdata = []
    result = list()
    for line in f.readlines():
        # update
        if fieldsnum == 1:
            if line.startswith('### WHERE'):
                where = 1
                continue
            else:
                if where and line.startswith('###   @1='):
                    slen = line.rfind("/*")
                    keystr = line[:slen].strip()
                    keylen = keystr.find("=")
                    getkey = keystr[keylen + 1:].strip("'")
                    if args['keysdict'].has_key(getkey):
                        if slen > 0:
                            wherestr = " WHERE" + line[:slen].strip().replace("@1", fields[0], 1).replace('### ', '',1) + ";"
                        else:
                            print "[Error] " + line
                            exit()
                        where = 0
                        continue
                    else:
                        set = fieldsnum = 0
                        sqlstr = wherestr = ''
                        fields = []
                        continue
            if line.startswith('### SET'):
                sqlstr = sqlstr + ' SET '
                set = 1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    vs = line.find("=")
                    ve = line.rfind("/*")
                    if vs > 0 and ve > 0:
                        sqlstr = sqlstr + " " + "`" + fields[set - 1] + "`='" + addslashes(
                            line[vs + 1:ve].strip().strip("'")) + "',"
                    else:
                        print "[Error] " + line
                        exit()
                    if set == num:
                        set = fieldsnum = 0
                        result.append(sqlstr.strip(",") + wherestr)
                        sqlstr = wherestr =''
                        fields = []
                        continue
                    else:
                        set = set + 1
                        continue
        # insert
        elif fieldsnum == 2:
            if line.startswith('### SET'):
                set = 1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    slen = line.rfind("/*")
                    keystr = line[:slen].strip()
                    keylen = keystr.find("=")
                    if line.startswith('###   @1='):
                        idkey = keystr[keylen + 1:].strip("'")
                        if not args['keysdict'].has_key(idkey):
                            set = fieldsnum = 0
                            fields = tmpdata = []
                            continue
                    tmpdata.append("'" + addslashes(keystr[keylen + 1:].strip().strip("'")) + "'")
                    if set == num:
                        if len(tmpdata) == num:
                            result.append("INSERT INTO `" + dbname + "`.`" + tablename + "` VALUES (" + ",".join(tmpdata) + ");")
                        else:
                            print "Error: " + tablename + " " + tmpdata + "insert field num error";
                        tmpdata = fields = []
                        set = fieldsnum = 0
                        continue
                    else:
                        set = set + 1
                        continue
        # delete
        elif fieldsnum == 3:
            if line.startswith('### WHERE'):
                set = 1
                continue
            else:
                num = len(fields)
                if set and set <= num:
                    slen = line.rfind("/*")
                    keystr = line[:slen].strip()
                    keylen = keystr.find("=")

                    if args["deletewhere"] == 1:

                        if line.startswith('###   @1='):
                            idkey = keystr[keylen + 1:].strip("'")
                            if not args['keysdict'].has_key(idkey):
                                set = fieldsnum = 0
                                tmpdata = fields = []
                                continue
                        tmpdata.append("`" + fields[set - 1] + "`='" + addslashes(keystr[keylen + 1:].strip().strip("'")) + "'")
                        if set == num:
                            if len(tmpdata) == num:
                                result.append("DELETE FROM `" + dbname + "`.`" + tablename + "` WHERE " + " AND ".join(
                                    tmpdata) + ";")
                            else:
                                print "Error: " + tablename + " " + tmpdata + "insert field num error";
                            set = fieldsnum = 0
                            tmpdata = fields = []
                            continue
                        else:
                            set = set + 1
                            continue
                    else:
                        if line.startswith('###   @1='):
                            idkey = keystr[keylen + 1:].strip("'")
                            if args['keysdict'].has_key(idkey):
                                result.append("DELETE FROM `" + dbname + "`.`" + tablename + "` WHERE `" + fields[0] + "`=" + keystr[keylen + 1:].strip() + ";")
                            set = fieldsnum = 0
                            tmpdata = fields = []
                            continue



        if line.startswith('### UPDATE `' + dbname + '`') or line.startswith('### INSERT INTO `' + dbname + '`') or line.startswith('### DELETE FROM `' + dbname + '`'):
            t = re.search("`(.*)`\.`(.*)`", line)
            action = line.split(" ")[1].strip()
            tablename = t.group(2)
            if tablename and tablefield.has_key(tablename):
                fields = tablefield[tablename]
                if action == "UPDATE":
                    sqlstr = line.split("/*")[0].strip('\n').replace('### ', '', 1)
                    fieldsnum = 1
                elif action == "INSERT":
                    fieldsnum = 2
                elif action == "DELETE":
                    fieldsnum = 3
    f.close()
    fileDump(result, filename)
def countAll(filename,sqlfile,dbname,tablefield,args={}):
    f = open(sqlfile, 'r')
    result = {}
    for line in f.readlines():
        if line.startswith('### UPDATE `') or line.startswith('### INSERT INTO `') or line.startswith('### DELETE FROM `'):
            t = re.search("`(.*)`\.`(.*)`", line)
            action = line.split(" ")[1].strip()
            rekey = t.group(1)+"-"+t.group(2)+":" + action
            if result.has_key(rekey):
                result[rekey] = result[rekey] + 1
            else:
                result[rekey] = 1
    countdict[md5(sqlfile)] = result

def fileDump(data,filename):
    resultdir = getConfig('resultdir')
    bin_file = open(resultdir+filename, "a")
    for line in data:
        bin_file.write(line + "\n")
    bin_file.close()
def mergeSql():
    redir = getConfig('resultdir')
    resultfilelist = fileList(redir)
    if len(resultfilelist) > 0:
        allsql = open(redir + "all.sql", "a")
        for re in resultfilelist:
            f = open(redir + re, 'r')
            for line in f.readlines():
                allsql.write(line)
            f.close()
        allsql.close()
