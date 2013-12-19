#
# author: jinnan.qjn@alibaba-inc.com
#
# vimtab = 4 shiftstop = 4
#

import os
import sys
import commands
import subprocess
import signal
import re
import socket
import math
import optparse

import utils
import ob_mysql_proxy

if __name__ == "__main__":
    #check master cluster
    opt_parser = optparse.OptionParser()
    opt_parser.add_option("--ip-address", "-i", dest = "ms_ipaddr") 
    opt_parser.add_option("--server-port", "-P", dest = "ms_port") 
    opt_parser.add_option("--user", "-u", dest = "ms_user", default = "admin") 
    opt_parser.add_option("--password", "-p", dest = "ms_passwd", default = "admin") 
    (options, args) = opt_parser.parse_args()
    ms_ipaddr = options.ms_ipaddr
    ms_port = options.ms_port 
    ms_user = options.ms_user 
    ms_passwd = options.ms_passwd 
    ob_proxy = ob_mysql_proxy.mysql_proxy(ms_ipaddr, int(ms_port), ms_user, ms_passwd)
    ob_mysql_proxy.check_cluster_role(ob_proxy.conn)

    cluster_ids = ob_proxy.get_cluster_info()
    for c_id in cluster_ids:
        print >> sys.stdout, "[cluster ", c_id ,": ]"
        ob_mysql_proxy.check_inner_port(ob_proxy.conn, c_id)
        
        all_servers = ob_proxy.get_all_svrs("all", c_id)  
        utils.check_all_svrs_online(all_servers)

        if not ob_proxy.conn.ping(): ob_proxy.retry_connect(ms_ipaddr, int(ms_port), ms_user, ms_passwd)
        ob_mysql_proxy.check_svr_type(ob_proxy.conn, c_id)

        if not ob_proxy.conn.ping(): ob_proxy.retry_connect(ms_ipaddr, int(ms_port), ms_user, ms_passwd)
        ob_mysql_proxy.check_ups_master(ob_proxy.conn, c_id)

        if not ob_proxy.conn.ping(): ob_proxy.retry_connect(ms_ipaddr, int(ms_port), ms_user, ms_passwd)
        ob_mysql_proxy.check_single_point(ob_proxy.conn, c_id)

        mss = []
        if not ob_proxy.conn.ping(): ob_proxy.retry_connect(ms_ipaddr, int(ms_port), ms_user, ms_passwd)
        mss = ob_proxy.get_all_svrs("mergeserver", c_id) 
        flag = ob_proxy.do_simple_ms_test(mss)
        
        #if flag and ob_proxy.conn.ping():
        #    ob_mysql_proxy.test_insert_rate(ob_proxy.conn, c_id, mss)
        ob_mysql_proxy.test_insert_rate(ob_proxy.conn, c_id, mss)
