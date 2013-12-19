#
# author: jinnan.qjn@alibaba-inc.com
#
# vimtab = 4 shiftstop = 4
#

import os
import sys
import commands
import subprocess
import socket
import signal
import random
import re
import time
import math

#import ob_detect_gui
import utils

server_types = {"rs":"rootserver", "ms":"mergeserver", "ups":"updateserver", "cs":"chunkserver", "a":"all"}

try:
    import MySQLdb as ob_client 
except:
    os_info = os.uname()
    os_rpm_dict = {"Linux":"apt-get install mysqldb-python", "RedHat":"yum mysqldb-python"}
    print >> sys.stderr, ("""MySQLdb package should be installed
    you could install it with these steps
    %s 
    """, os_rpm_dict[os_info[0]], )
    exit(signal.SIGHUP)


class mysql_proxy:
    """
    ob mysql client which follow mysql protocol
    """
    def __init__(self, ms_ipaddr, ms_port, ms_user, ms_passwd):
        self.ms_ipaddr = ms_ipaddr
        self.ms_port = ms_port
        self.ms_user = ms_user
        self.ms_passwd = ms_passwd
        self.conn = ob_client.connect(host = ms_ipaddr, port = ms_port, user = ms_user, passwd = ms_passwd) 

    def get_cluster_info(self):
        get_sql = "select cluster_id from __all_cluster"
        cur = self.conn.cursor()
        cur.execute(get_sql)
        res = []
        for rec in cur.fetchall():
            if rec in res: continue
            else:
                res_rec = (int(rec[0]))
                res.append(res_rec)
        return res

    def get_all_ups(self):
        get_sql = ""
        res = []
        cur = self.conn.cursor()
        cur.execute(get_sql)
        for rec in cur.fetchall():
            #form the mss array data and return
            res.append(tuple(res[0], res[1]))
        return res

    def get_all_svrs(self, svr_type, c_id):
        global server_types
        get_sql = ""
        if "all" == svr_type: get_sql = "".join(["select svr_ip, svr_port from __all_server where cluster_id = ",
            str(c_id) , " order by svr_ip"])
        else: get_sql = "".join(["select svr_ip, svr_port from __all_server where svr_type = '", 
            svr_type, "' and cluster_id = ", str(c_id) , " order by svr_ip"])
        
        if svr_type not in server_types.values(): return
        res = []
        cur = self.conn.cursor()
        cur.execute(get_sql)
        for rec in cur.fetchall():
            res_rec = (str(rec[0]), int(rec[1]))
            res.append(res_rec)
        return res
    
    def retry_connect(self, ms_ipaddr, ms_port, ms_user, ms_passwd):
        self.conn = ob_client.connect(host = ms_ipaddr, port = ms_port, user = ms_user, passwd = ms_passwd)

    def get_cs_info(self):
        pass

    def get_ups_info(self):
        pass

    def get_rs_info(self):
        pass

    def get_ms_info(self):
        pass
    
    def do_simple_ms_test(self, mss):
        """parameters: \
                mss  [array]:keeps all merge server host information \ 
           returns: \
                res_cache [dict]:result for check sql executed \ 
        """
        if 0 == len(mss): return
        ms_host_info = mss[random.randint(0, len(mss) - 1)]
#FIXME:
# use hashlib is better
        #text = hashlib.md5(str(time.time())).digest()
        text = str(time.time())
#FIXME:
# when create a table through mysql client, MySQLdb will raise a exception which name is InterfaceError:
# see more: http://www.mikusa.com/python-mysql-docs/MySQLdb.cursors.html
        #test_sql ="".join([" insert into ob_detect_test values (now(), '",text ,"');"])
        test_sql_1 = "create table if not exists ob_detect_test (n_date datetime primary key, text varchar(256));"
        test_sql_2 ="".join([" insert into ob_detect_test values (now(), '",text ,"');"])

        check_sql = "select text from ob_detect_test where text = " + text 
       
        if -1 == utils.check_ip_addr(ms_host_info[0]):
            print >> sys.stdout, "[ERROR: MS ip address is not good]"
        conn = ob_client.connect(host = ms_host_info[0], port = int(ms_host_info[1]), user = "admin", passwd = "admin")     
        #if not conn.ping(): print >> sys.stderr, "connection could not build"
#FIXME:
# I know this is very obtume, but if cursor do not close, the insert job goes wrong
        cur = conn.cursor()
        cur.execute(test_sql_1)
        cur.close()
        
        cur = conn.cursor()
        cur.execute(test_sql_2)
        conn.close() 

        count = 0; page_num = int(math.ceil(len(mss) / 2.0)) 
        res_cache = {}; ms_mat = [[] for x in range(0, int(len(mss)) / page_num + 1)] 
        for i in range(0, int(len(mss) / page_num + 1)):
            for j in range(i * page_num , (i + 1) * page_num):
                try:
                    if mss[j]: ms_mat[i].append(mss[j])
                except IndexError as index_e: continue
        flag = False        
        for i in range(page_num):
            pid = os.fork()
            if pid:
                conn = ob_client.connect(host = ms_mat[0][i][0], port = int(ms_mat[0][i][1]), user = "admin", passwd = "admin") 
                cur = conn.cursor()
                cur.execute(check_sql)
                recs = cur.fetchall()
                if 1 == len(recs) and text == recs[0]:
                    print >> sys.stdout, "[INFO ", ms_mat[0][i][0], " sync data good]" 
                else: print >> sys.stdout, "[WARN ", ms_mat[0][i][0], " sync data bad]"; flag = False 
                cur.close(); conn.close()
                signal.signal(signal.SIGHUP, signal.SIG_IGN)
            else:
                try:
                    conn = ob_client.connect(host = ms_mat[1][i][0], port = int(ms_mat[1][i][1]), user = "admin", passwd = "admin")  
                    cur = conn.cursor()
                    cur.execute(check_sql)
                    recs = cur.fetchall()
                    if 1 == len(recs) and text == recs[0]:
                        print >> sys.stdout, "[INFO ",ms_mat[1][i][0], " sync data good]" 
                    else: print >> sys.stdout, "[WARN ", ms_mat[1][i][0], " sync data bad]"; flag = False 
                except IndexError as index_e: pass
                finally: exit(signal.SIGHUP)
        return flag

def check_inner_port(conn, cluster_id):
    global server_types
    for t in [server_types["ms"], server_types["ups"]]:
        get_inner_port_sql = "".join(["select svr_ip,  inner_port from __all_server where cluster_id = ", 
            str(cluster_id), " and svr_type = '", t, "'"])
        if not conn: print >> sys.stderr, "connection error"
        cur = conn.cursor()
        cur.execute(get_inner_port_sql)
        tmp_rec = int(cur.fetchone()[1]) 
        for rec in cur.fetchall():
            if tmp_rec != int(rec[1]): 
                print >> sys.stdout, "server ", rec[0], " inner port ", rec[1] ," is not matched"
                return
        cur.close()
    print >> sys.stdout, "[INFO: all server inner port matched...]"

def check_master_cluster(self):
    """
    check if there is only one master cluster \
    check if the whether vip and master cluster ip is matched \
    """
    get_sql = "select cluster_id from __all_cluster where "        
    cur = self.conn.cursor()
    cur.execute(get_sql)
    for rec in cur.fetchall():
        #rec is the waht we need
        pass
    #if checking steps all okay
    print >> stdout, "[INFO: cluster is good]"

def check_svr_type(conn, c_id):
    global server_types
    get_type_sql = "select distinct svr_type from __all_server where cluster_id = " + str(c_id)
    cur = conn.cursor()
    cur.execute(get_type_sql) 
    recs = cur.fetchall() 
    if 4 != len(recs): 
        print >> sys.stdout, "[WARN: server types is not completed]"
        return
    for r in recs:
        if not reduce(lambda x, y: x in y, [str(r[0]) ,server_types.values()]): 
            print >> sys.stdout, "[WARN: server types is not right", str(r[0]) ,"]"
            return
    print >> sys.stdout, "[INFO: server types complete]"

def check_ups_master(conn, c_id):
    get_ups_master_sql = "select svr_role from __all_server where svr_type = 'updateserver' and cluster_id = " + str(c_id)
    cur = conn.cursor()
    cur.execute(get_ups_master_sql) 
    recs = cur.fetchall() 
    for r in recs:
        if reduce(lambda x, y: x in y, [int(r[0]), [1, 2]]):pass
        else: 
            print >> sys.stdout, "[WARN: master role in update server is not good]"
            return
    print >> sys.stdout, "[INFO: master role in update server okay]"

def check_cluster_role(conn):
    flag = False
    get_cluster_role_sql = "select cluster_role from __all_cluster"
    cur = conn.cursor()
    cur.execute(get_cluster_role_sql) 
    recs = cur.fetchall() 
    for r in recs:
        if reduce(lambda x, y: x in y, [int(r[0]), [1, 2]]): flag = True 

    get_cluster_role_sql = """select __all_cluster.cluster_vip, __all_cluster.cluster_id, __all_server.svr_ip 
            from __all_cluster, __all_server 
            where 
            __all_cluster.cluster_vip = __all_server.svr_ip and svr_role = 1"""
    cur = conn.cursor()
    cur.execute(get_cluster_role_sql) 
    if 1 == len(cur.fetchall()) and flag is True:  
        print >> sys.stdout, "[INFO: master role in clusters okay]"
    cur.close()

def check_single_point(conn, c_id):
    global server_types
    get_type_sql = "select count(svr_ip) from __all_server where svr_type = 'chunkserver' and cluster_id = " + str(c_id)
    cur = conn.cursor()
    cur.execute(get_type_sql) 
    if 3 > len(cur.fetchall()): print >> sys.stdout, "[WARN: chunkserver single point defect, which cluster id: ", str(c_id) ,"]"
    else: print >> sys.stdout, "[INFO: chunkserver number is good]"

def test_insert_rate(conn, c_id, mss):
    repeat_time = 1000 
    start_t = time.time()  
    test_sql = "".join(["insert into ob_detect_test values (now(), '", str(time.time()),"')"])
    cur = conn.cursor()
    try:
        for rp_t in range(repeat_time):
            cur.execute(test_sql)     
        end_t = time.time()
        print >> sys.stdout, "[INFO: insert 1000 records takes ", str(end_t - start_t) ," seconds]"
    except Exception as e: raise e
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    print >> sys.stderr, "this mod could not be treated as a single one" 

