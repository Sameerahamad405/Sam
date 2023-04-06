from __future__ import print_function
import sys
import commands
import argparse
import time
import os
import logging
import verdadclient
from pprint import pprint
import re
import socket
from Queue import Queue
from threading import Thread

def prod_to_staging(host,status):
        service = commands.getoutput("/usr/local/bin/vd print %s | grep -i service"%host)
        print("\n Host service is\n")
        print(service)
        manu = commands.getoutput("/usr/local/bin/vd expand  %s | grep -i manu"%host)
        print("\n Host manufacture is \n")
        print(manu)
        err='staging'
        if err in status:
            print("Host is already in staging please select correct option\n")
            check_nagios(host)
        else:   
            comment = raw_input(" Enter your comment\n")
            out=commands.getoutput("/usr/local/bin/vd edit %s 'nagios.status' -u 'nagios.status = staging' -m '%s' -b"%(host,comment))
        err='production'
        changed=commands.getoutput("/usr/local/bin/vd print %s -T nagios.status"%host)
        if err in changed:
            print("\n %s could not make vd changes.please look into it ")
        else:
            print("\n %s"%changed)
            print("\n %sHost moved  to staging")
            print("""\n completed""")
        print("\n___________***********************************_______\n")
def  staging_to_prod(host,status):
        err='production'
        if err in status:
            print("Host is already in prodcution please select the correct option\n")
            check_nagios(host)
        else:
            comment = raw_input("\nEnter your comment\n")
            out=commands.getoutput("/usr/local/bin/vd edit %s 'nagios.status' -u 'nagios.status = production' -m '%s' -b"%(host,comment))    
        err='staging'
        changed=commands.getoutput("/usr/local/bin/vd print %s -T nagios.status"%host)  
        if 'staging' in changed:
            print("\n %s could not make vd changes.please look into it ") 
        else:
            print("\n %s"%changed)
            print("\n %sHost is pulled back to production")
            print("""\n  completed""")
        print("\n___________***********************************_______\n")
def check_nagios(host):
        check=commands.getoutput("/usr/local/bin/vd print %s -T nagios.status"%host)
        print(check)
        if 'staging' in check or 'production' in check:
            if 'staging' in check:
                        print("host %s is in staging" %host)
            elif 'production' in check:
                print("host %s is in production" %host)
            print("\n if you want to move further please select either option\n1.\n \ Move host to Staging \n2. Move host to production \n3.Exit")
            opt = int(raw_input("\ntype [ 1 / 2 / 3] to continue :"))
            if opt == 1:
                print("\nMoving host to staging")
                prod_to_staging(host,check)
                pass
            elif opt == 2:
                print("\nMoving host to production")
                staging_to_prod(host,check)
                pass
            elif opt == 3:
                pass
            else:
                print("\nType either 1/2/3")
                check_nagios(host)
            # sys.exit()
        elif 'not-monitored' in check:
            print("Host is in not-monitored state please check manually \n")
            sys.exit()
        else:
            print("\nHost is not having status")    
def getIP(hostname):
        try :
            ip = socket.gethostbyname(hostname)
        except Exception as e:
            command = 'ssh st11p00im-adminm002 "%s/get_ip.py %s"' %(parent_dir, hostname)
            p = commands.Popen ( command, stdout=commands.PIPE, shell=True )
            (output, err) = p.communicate ()
            ip = output.strip ()
        try:
            socket.inet_aton (ip)
        except socket.error:
            command = 'ssh st11p00im-adminm002 "%s/get_ip.py %s"' %(parent_dir, hostname)
            p = commands.Popen ( command, stdout=commands.PIPE, shell=True )
            (output, err) = p.communicate ()
            ip = output.strip ()
            try:
                socket.inet_aton (ip)
            except socket.error:
                return None
        if ip.count('.') == 3:
            return ip
        else :
            return None
def verdadwrite(host, action, itemflag):
        with verdadclient.Client ( verdadclient.PRODUCTION ) as vd:
        if args.item :
            items = args.item
        else:
            items = vd.items_find ({'name_lk': '%im.service.dlb.app%', 'tag_lk': '%upstream.server%', 'val_lk': '%%%s:%%' %host},form='simple' )
        if len(items) < 1:
            print("\033[91mNo Item found !!!\033[0m")
            sys.exit()
        for item in items:
            query = [{'name_eq': item, 'tag_lk': '%upstream.server%', 'val_lk': '%%%s:%%' %host}]
            out = [{'tag_lk': '%upstream.server%'}]
            data = vd.items_fetch ( query, out=out )
            if len(data) < 1:
                print("\033[91mNo upstream found !!!\033[0m")
                sys.exit()
            for i in data[item].keys ():
                index = 0
                for j in data[item][i][:]:
                    if j[0].startswith (host):  
                        if args.status:
                            replacement = None
                            action = "Status"
                            if re.search('down', j[0]) :
                                print("%s : \033[31mDISABLED\033[0m" %item)
                            else:
                                print("%s : \033[32mENABLED\033[0m" %item)
                        elif args.enable:
                            action = 'Enabling'
                            replacement = j[0].replace ( 'down', 'max_fails=0' )
                        elif args.disable:
                            action = 'Disabling'
                            replacement = j[0].replace ( 'max_fails=0', 'down' )
                        elif args.delete:
                            replacement = []
                            action = "Deleting"
                            if re.search('down', j[0]) :
                                data[item][i].remove(j)
                            else :
                                print("\033[91m ERROR: We can only delete disabled IP\033[0m")
                                sys.exit()
                        else:
                            print("\033[91m ERROR: Please specify enable or disable or delete\033[0m")
                            parser.parse_args(['-h'])
                            sys.exit()
                        if action != "Deleting":
                            data[item][i][index][0] = replacement
                    index = index + 1                    
            if action == "Status" :
                continue
            else :                
                vd.item_lock ( item )
                print('%s %s on dlb %s\n' %(action, host, item))
                txn = vd.begin ( '%s %s on dlb\n' %(action, host) )
                print("Commited transaction %s\n" %txn)
                vd.items_update ( data )
                vd.commit ()
                vd.item_unlock ( item ) 
        if action == "Enabling":
            ssh = commands.Popen(["ssh", "-A", str(host)],
                        stdin =commands.PIPE,
                        stdout=commands.PIPE,
                        stderr=commands.PIPE,
                        universal_newlines=True,
                        bufsize=0)
            ssh.stdin.write("sudo -i\n")
            ssh.stdin.write("puppet agent -t\n")
            ssh.stdin.close()
            for line in ssh.stdout:
                print(line.strip())
        if action == "Status":
            print("If you want to enable or disable host in dlb \n please select appropriate options\n 1. To enable host in dlb\n 2.To disable host in dlb\n 3. Make nagios status change\n")
            choice = int(raw_input("select [ 1 / 2 / 3 ]     :  " ))
            if choice == 1:
                args.status = False
                args.disable = False
                args.enable = True
                print("Please wait, Enabling host in dlb\n")
                verdadwrite(host, action, itemflag)
            elif choice == 2:
                args.status = False
                args.enable = False
                args.disable = True
                print("Please wait, Disabling host in dlb\n")
                verdadwrite(host, action, itemflag)
            else:
                return
        else:
            args.status = True
            args.enable = False
            args.disable = False
            print("Please wait checking status\n")
            verdadwrite(host, action, itemflag)
def threadWorker(action, itemflag, q):
        while True:
            host = q.get()
            verdadwrite(host, action, itemflag)
            q.task_done()

def mainfunction():
        parser = argparse.ArgumentParser(description='To Enable or disable complete ip or specific port for an ip in dlb')
        parser.add_argument('-enable',action='store_true', help='To Enable IP')
        parser.add_argument('-disable',action='store_true', help='To Disable IP')
        parser.add_argument('-delete',action='store_true', help='To remove entry')
        parser.add_argument('-status',action='store_true', help='To get the current status')
        parser.add_argument('-ip', metavar='<upstream IP>', nargs=1, help='specify the <IP> to enable/disable')
        parser.add_argument('-host', metavar='<upstream host>', nargs=1, help='specify the <hostname> or <hostname:PORT> to enable/disable')
        parser.add_argument('-f', metavar='<file>', nargs=1, help='specify the <hostname> or <hostname:PORT> to enable/disable')
        parser.add_argument('-item', metavar='<verdad item>', nargs=1, help='<OPTIONAL> only want to enable disable on a specific item')
        args = parser.parse_args()
        parent_dir = os.getcwd()
        host = None
        action = None
        itemflag = False 
        if args.ip is not None:
            host = ''.join(args.ip[0].strip())
            print("\n\n\t******** Checking status of host *********")
            print("\nHOST -> %s"%hostname) 
            check_nagios(hostname)
            print("############# dlb ##################\n")
            if args.status:
                print("\n dlb status of host --> %s"%hostname)
            elif args.enable or args.disable:
                print("\n %s host from dlb\n"%hostname)
            verdadwrite(host, action, itemflag)
        elif args.host is not None:
            hostname = ''.join(args.host[0].strip())
            opt = 1
            print("Please select your preferred action to perform\n \
            1. Enable or disable host in dlb\n \
            2. Move host to staging or production")           
            opt = int(raw_input("Select your option[1/2]   :    "))
            if opt == 1: 
                print("############# dlb ##################\n") 
                if args.status:
                    print("\n dlb status of host --> %s"%hostname)
                elif args.enable or args.disable:
                    print("\n %s host from dlb\n"%hostname)
                print("Getting IP for Host : %s" %hostname)
                host = getIP(hostname)
                verdadwrite(host, action, itemflag)
                print("\n\n\t********* Checking status of host *****")
                print("\nHOST -> %s"%hostname) 
                check_nagios(hostname)         
            elif opt == 2:
                print("\n\n\t*********** Checking status of host ***")
                print("\nHOST -> %s"%hostname) 
                check_nagios(hostname)
        elif args.f is not None:
            file = ''.join(args.f[0].strip())
            host_queue = Queue()
            for i in range(10):
                worker = Thread(target=threadWorker, args=(action, itemflag, host_queue,))
                worker.setDaemon(True)
                worker.start()
            with open ( file, 'r') as f :
                for line in f:
                    host_queue.put(line.strip())
            host_queue.join()
            sys.exit()
        else :
            print("\033[91m ERROR: Please specify <IP> or <IP:PORT>\033[0m")
            parser.parse_args(['-h'])
            sys.exit()
            
if __name__ == '__main__':

    mainfunction()
