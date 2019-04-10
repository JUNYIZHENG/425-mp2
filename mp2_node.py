from socket import *
import threading
import os
import psutil
import time
import random


# ---------------------------------------------BANDWIDTH------------------------------------------------
def log_bandwidth():
    while True:
        bandwidth = psutil.net_io_counters()
        bytes_sent = bandwidth[0] / 1024 / 1024
        current_time = time.time()
        #print(my_name, bytes_sent, 'M', current_time)
        output_file = open('bandwidth/bandwidth_{nodename}.txt'.format(nodename=my_name), 'a')
        output_file.write(str(current_time) + ',' + str(bytes_sent) + '\n')
        output_file.close()
        time.sleep(1)


# ---------------------------------------------CLIENT---------------------------------------------------
def rec_intro(msg_list):
    # if len(address_list) < 5:
    # print('msg',msg,msg_list)
    prefix = msg_list[0]
    server_ip = msg_list[2]
    if msg_list[3] != '':
        server_port = int(msg_list[3])
        # print('port',server_port)
        server_address = (server_ip, server_port)
        # output_file = open('intro_{nodename}.txt'.format(nodename=my_name), 'a')
        # output_file.write(str(server_address) + '\n')
        # output_file.close()
        if server_address not in address_list:
            if prefix == 'INTRODUCE':
                x = 1
            else:  # prefix == 'SERVERINTRO':
                if len(address_list) < 8:
                    x = random.randint(1,6)
                else:
                    x = 0
            if x == 1:
                # create a new socket
                socket_as_c = socket(AF_INET, SOCK_STREAM)
                try:# if server alive
                    # create s,introduce myself,send request
                    socket_as_c.connect(server_address)
                    address_list.append(server_address)
                    lock_s_as_c.acquire()
                    socket_as_c_list.append(socket_as_c)
                    lock_s_as_c.release()
                    # introduce myself
                    str_l = ['INTRODUCE', 'node', str(my_ip), str(my_port), '\n']
                    msg_send = ' '.join(str_l)
                    # msg_send = 'INTRODUCE node {ip} {port} \n'.format(ip=my_ip, port=str(my_port))
                    try:
                        # send intro to server
                        socket_as_c.send(bytes(msg_send, encoding='utf-8'))
                        # print(my_name, len(address_list))
                        if len(address_list) < 5:
                            try:
                                socket_as_c.send(bytes('REQUEST', encoding='utf-8'))
                            except BrokenPipeError:
                                handle_server_die(socket_as_c,server_address)
                        if server_address in address_list:
                            threading.Thread(target=rec, args=(socket_as_c, server_address)).start()
                    except BrokenPipeError:
                        handle_server_die(socket_as_c, server_address)
                except ConnectionRefusedError: # server dies
                    # print('node is killed by service')
                    socket_as_c.close()


def rec(s, address):
    # while s in socket_as_c_list:
    while True:
        # keep receiving messages
        # if s in socket_as_c_list:
        try:
            msgin = s.recv(1024).decode('utf-8')
            #print('original ',msgin)
            msgin = msgin[:-1]  # string,might with \n
            #print('-2 ',msgin)
            if len(msgin) != 0:
                msgin_pool = msgin.split('\n')
                for msg in msgin_pool:  # string,single trans
                    # print('in pool split by n',msg)
                    msg_list = str.split(msg, ' ')
                    # print('list ',msg_list)
                    prefix = msg_list[0]
                    if prefix == 'INTRODUCE' or prefix == 'SERVERINTRO':
                        # print(msg)
                        # print(msg_list)
                        if len(msg_list) == 4 or len(msg_list) == 5:  #
                            lock_address.acquire()
                            rec_intro(msg_list)
                            lock_address.release()
                    elif prefix == 'TRANSACTION':
                        # print(msg)
                        # print(msg_list)
                        if len(msg_list) == 6:
                            # print(msg)
                            lock_msgin.acquire()
                            rec_time = time.time()
                            timestamp = msg_list[1]
                            if timestamp not in recv_dic:
                                # log,add to dic
                                delay = rec_time - float(timestamp)
                                recv_dic[timestamp] = msg
                                # add to file
                                output_file = open('trans/transactions_{nodename}.txt'.format(nodename=my_name), 'a')
                                string = '{},{},{}\n'
                                output_file.write(string.format(msg, timestamp,delay))
                                output_file.close()
                                # forward via server
                                # lock_c_conn.acquire()
                                # die_socket_list = []
                                for c in c_conn:
                                    try:
                                        # forward to client
                                        c.send((msg+'\n').encode('utf-8'))
                                    except:
                                        continue
                                #         die_socket_list.append(c)
                                # if len(die_socket_list) != 0:
                                #     for die_s in die_socket_list:
                                #         c_conn.remove(die_s)
                                # print(len(c_conn))
                                # lock_c_conn.release()
                            lock_msgin.release()
                    elif prefix == 'DIE' or prefix == 'QUIT':
                        # quit?
                        print(my_name, ' is killed by service')
                        os._exit(0)
            else: # server/service dies
                # print('it is dead but not except!')
                # s.close()
                if address != service_addr:
                    handle_server_die(s, address)
                    break
                else:  # service dies
                    print(my_name, 'quited since service is dead')
                    os._exit(0)
        except ConnectionResetError:
            # s.close()
            if address != service_addr:
                handle_server_die(s,address)
                break
            else: # service dies
                print(my_name, 'quited since service is dead')
                os._exit(0)

def handle_server_die(s,address):
    # remove server,remove my socket
    lock_address.acquire()
    address_list.remove(address)
    lock_address.release()

    lock_s_as_c.acquire()

    if s in socket_as_c_list:
        s.close()
        socket_as_c_list.remove(s)
    lock_s_as_c.release()

    lock_address.acquire()
    lock_s_as_c.acquire()
    if len(address_list) < 5: # if server not enough,req
        # useless_socket_list = []
        for other_socket in socket_as_c_list:
            try:
                # send to server
                other_socket.send(bytes('REQUEST', encoding='utf-8'))
            # except BrokenPipeError or ConnectionResetError:
            except:
                # other_socket.close()
                # useless_socket_list.append(other_socket)
                continue
        # for useless_socket in useless_socket_list:
        #     socket_as_c_list.remove(useless_socket)
    lock_address.release()
    lock_s_as_c.release()

def get_my_port(s_server):
    my_port = random.randint(1025, 60000)
    # try:
    #     s.connect(('127.0.0.1', my_port))
    #     s.shutdown(2)
    #     # print('not',my_port)
    #     get_my_port()
    # except:
    #     # print('is',my_port)
    try:
        s_server.bind((my_ip, my_port))
        return my_port
    except OSError:
        return get_my_port(s_server)


# ----------------------------------------------- SERVER --------------------------------------------------
# send
# As a server socket
# open a threading with accepting clients
def sendtoclient(c):
    while True:
        try:
            msgin = c.recv(1024).decode('utf-8')
            if 'REQUEST' in msgin:
                if len(address_list) > 0:
                    #addr_index = random.sample(range(0,len(address_list)),random.randint(1,len(address_list)))
                    for i in range(0,len(address_list)):
                        # send all servers I know to the one requested for it
                        ip = address_list[i][0]
                        port = address_list[i][1]
                        str_l = ['SERVERINTRO', 'node', str(address_list[i][0]), str(address_list[i][1]), '\n']
                        msg = ' '.join(str_l)
                        try:
                            c.send(msg.encode('utf-8'))
                        except BrokenPipeError:
                            # if can not send,node die,ignore,cause
                            continue
            if 'INTRODUCE' in msgin:
                msgin = msgin[:-1]
                msg_list = str.split(msgin, ' ')
                #print('list ',msg_list)
                rec_intro(msg_list)
        except ConnectionResetError:
            lock_c_conn.acquire()
            c_conn.remove(c)
            lock_c_conn.release()
            break

def serveraccept(s):
    while True:
        # accept connection from other clients
        conn, addr = s.accept()
        lock_c_conn.acquire()
        c_conn.append(conn)
        # print(len(c_conn))
        lock_c_conn.release()
        threading.Thread(target=sendtoclient, args=(conn,)).start()



socket_list = []
address_list = []
recv_dic = {}
c_conn = []
socket_as_c_list = []
lock_msgin = threading.Lock()
lock_address = threading.Lock()
lock_s_as_c = threading.Lock()
lock_c_conn = threading.Lock()

my_ip = gethostbyname(gethostname())
my_name = input('')

# clear the files
file_bandwidth = open('bandwidth/bandwidth_{nodename}.txt'.format(nodename=my_name), 'w')
file_bandwidth.write('')
file_bandwidth.close()
file_trans = open('trans/transactions_{nodename}.txt'.format(nodename=my_name), 'w')
file_trans.write('')
file_trans.close()

# server socket
s_server = socket(AF_INET, SOCK_STREAM)
# s_server.bind((my_ip, my_port))
my_port = get_my_port(s_server)
# hardcoded to vm's IP & port
s_server.listen()

# threading of accepting clients
threading.Thread(target=log_bandwidth).start()
threading.Thread(target=serveraccept, args=(s_server,)).start()

# file_intro = open('intro_{nodename}.txt'.format(nodename=my_name), 'w')
# file_intro.write('')
# # file_bandwidth.write(my_name)
# file_intro.close()

# connect to service
# client socket
service_addr = ('172.22.158.157', 4444)
s_client = socket(AF_INET, SOCK_STREAM)
try:
    s_client.connect(service_addr)
    print(my_name,' has successfully connected to the service')
except ConnectionRefusedError:
    print('Service refused the connection')
    os._exit(0)

msg = 'CONNECT' + ' ' + my_name + ' ' + str(my_ip) + ' ' + str(my_port) + '\n'
try:
    s_client.send(msg.encode('utf-8'))
    threading.Thread(target=rec, args=(s_client, service_addr,)).start()
except BrokenPipeError:
    print('service dies')
    os._exit(0)
# port_as_s = input('Enter the port:')
# port_as_s = 2333


