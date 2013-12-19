#
# author: jinnan.qjn@alibaba-inc.com
#
# vimtab = 4 shiftstop = 4
#

import os
import sys
import curses
import signal
import commands
import time
import optparse

import ob_mysql_proxy
import utils


def draw_main_scr(main_scr):
    height, width = main_scr.getmaxyx()
    if height < 15 or width < 10:
        print >> sys.stderr, "please use a bigger screen"
        curses.endwin()
        exit(signal.SIGHUP)

    fd_logo = file("./ob_logo", "r")
    logo = fd_logo.read()
    fd_logo.close()

    main_scr.border(0)

    try:
        user_input = ""
        while "2" != user_input:
            main_scr.erase()
            main_scr.addstr(0, 0, logo, curses.color_pair(2)) 

            main_scr.addstr(6, 1,  "+===============================================================+", curses.color_pair(1)) 
            main_scr.addstr(7, 1,  "|                                                               |", curses.color_pair(1))
            main_scr.addstr(8, 1,  "|                                                               |", curses.color_pair(1))
            main_scr.addstr(9, 1,  "|                       0 detect clusters                       |", curses.color_pair(1))
            main_scr.addstr(10, 1, "|                       1 about OceanBase                       |", curses.color_pair(1))
            main_scr.addstr(11, 1, "|                       2 exit                                  |", curses.color_pair(1))
            main_scr.addstr(12, 1, "|                                                               |", curses.color_pair(1))
            main_scr.addstr(13, 1, "|                                                               |", curses.color_pair(1))
            main_scr.addstr(14, 1, "|                                                               |", curses.color_pair(1))
            main_scr.addstr(15, 1, "|                                        please input index...  |", curses.color_pair(1))
            main_scr.addstr(16, 1, "+===============================================================+", curses.color_pair(1))
            main_scr.refresh()

            user_input = main_scr.getstr(15, 63, 5)
            main_scr.addstr(15, 63, user_input)

            if "0" == user_input:
                draw_sub_scr(main_scr, logo)
            elif "1" == user_input:
                draw_about_scr(main_scr, logo)
            else: pass 
    except Exception as e:
        raise e
    finally:
        curses.endwin()

def show_process(main_scr, count, stop_condition):
    if None == count: count = 0
    while count < stop_condition:
        count += 1
        prog_str = "".join(["[", count * "#", ((stop_condition - count) * " "), "]", ])
        main_scr.addstr(6, 1, prog_str, curses.color_pair(2))     

def draw_sub_scr(main_scr, logo):
    global ERROR, WARN, INFO
    global options
    main_scr.erase()
    if logo: main_scr.addstr(0, 0, logo, curses.color_pair(2))
    else:
        fd_logo = file("./ob_logo", "r")
        logo = fd_logo.read()
        fd_logo.close()
    batch_cmd = "".join(["python ./ob_detect.py -i", options.ms_ipaddr, " -P ", options.ms_port, " -u ", 
        options.ms_user, " -p ", options.ms_passwd])

#FIXME:
# i print all detect result into a file, but subprocess does not lock the file, when it wrote string into file
# i did this because, i want to use more subporcess to save detecting time, if i lock the file when subprocess write,
#   this makes no different between parallel and serial programing

    batch_output_rep = commands.getoutput(batch_cmd)
    rep_lines = batch_output_rep.splitlines() 
    batch_output = {} 
    for line in rep_lines: 
        batch_output[line] = 0
    
    try:
        line_num = 7
        main_scr.addstr(6, 1,         "+===============================================================+", curses.color_pair(1)) 
        main_scr.refresh()
        for line in batch_output.keys():
            if "ERROR" in line: main_scr.addstr(line_num, 1, line, curses.color_pair(ERROR))
            elif "WARN" in line: main_scr.addstr(line_num, 1, line, curses.color_pair(WARN))
            elif "INFO" in line: main_scr.addstr(line_num, 1, line, curses.color_pair(INFO))
            else: main_scr.addstr(line_num, 1, str(line), curses.color_pair(1))
            line_num += 1
        main_scr.addstr(line_num, 1,  "                                     pess any key to continue... ", curses.color_pair(1)) 
        main_scr.addstr(line_num, 1,  "+===============================================================+", curses.color_pair(1)) 

        while True:
            if main_scr.getch(): return
    except Exception as e:
        raise e
    finally:
        curses.endwin()

def draw_about_scr(main_scr, logo):
    main_scr.erase()
    if logo: main_scr.addstr(0, 0, logo, curses.color_pair(2))
    else:
        fd_logo = file("./ob_logo", "r")
        logo = fd_logo.read()
        fd_logo.close()
    try:
        main_scr.addstr(0, 0, logo, curses.color_pair(2))
        main_scr.addstr(6, 1,  "+===============================================================+", curses.color_pair(1)) 
        main_scr.addstr(7, 1,  "|                                                               |", curses.color_pair(1))
        main_scr.addstr(8, 1,  "|                                                               |", curses.color_pair(1))
        main_scr.addstr(9, 1,  "|                                                               |", curses.color_pair(1))
        main_scr.addstr(10, 1, "|                                                               |", curses.color_pair(1))
        main_scr.addstr(11, 1, "|                                                               |", curses.color_pair(1))
        main_scr.addstr(12, 1, "|                                                               |", curses.color_pair(1))
        main_scr.addstr(13, 1, "|                                                               |", curses.color_pair(1))
        main_scr.addstr(14, 1, "|                                                               |", curses.color_pair(1))
        main_scr.addstr(15, 1, "|                                  press any key to continue... |", curses.color_pair(1))
        main_scr.addstr(16, 1, "+===============================================================+", curses.color_pair(1)) 
        if main_scr.getch(): return
        #main_scr.refresh()
    except Exception as e:
        raise e 
    finally:
        curses.endwin() 

def draw_final_result(main_scr, logo):
    pass

ERROR = 3
WARN = 4
INFO = 5

opt_parser = optparse.OptionParser()
opt_parser.add_option("--ip-address", "-i", dest = "ms_ipaddr") 
opt_parser.add_option("--server-port", "-P", dest = "ms_port") 
opt_parser.add_option("--user", "-u", dest = "ms_user", default = "admin") 
opt_parser.add_option("--password", "-p", dest = "ms_passwd", default = "admin") 
(options, args)= opt_parser.parse_args()

if "__main__" == __name__:
    main_scr = curses.initscr()
    try:
        curses.start_color()
        curses.init_pair(1, curses.COLOR_BLUE, curses.COLOR_BLACK)
        curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(ERROR, curses.COLOR_RED, curses.COLOR_BLACK)
        curses.init_pair(WARN, curses.COLOR_YELLOW, curses.COLOR_BLACK)
        curses.init_pair(INFO, curses.COLOR_CYAN, curses.COLOR_BLACK)

        draw_main_scr(main_scr)
    finally:
        curses.endwin()

