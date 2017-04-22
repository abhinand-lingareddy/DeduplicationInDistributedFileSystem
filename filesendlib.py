import sendlib
import os
import socket

def no_to_bytes(no):
    b=str(no)
    non_zeroscounter=len(b)
    zeros=[]
    for i in range(4-non_zeroscounter):
        zeros.append("0")
    return "".join(zeros)+b


def bytes_to_no(b):
    return int(b)

def recvfile(storage_path,filename,s):
    length = s.recv(4)
    length = bytes_to_no(length)
    if storage_path is not None:
        if not os.path.exists(storage_path):
            os.makedirs(storage_path)
        f = open(storage_path + '/' +filename , 'wb')
    else:
        f = open(filename, 'wb')

    while (length>0):
        if length>1024:
            size=1024
        else:
            size=length
        print "Receiving..."
        l = s.recv(size)
        f.write(l)
        length=length-size


    f.close()
    print "Done Receiving"

def recvfileredirect(storage_path,filename,s,slaves):
    print slaves
    length_str = s.recv(4)
    length = bytes_to_no(length_str)
    request = {}
    request["file_name"] = filename
    request["operation"] = "CREATE"
    request=str(request)

    if storage_path is not None:
        if not os.path.exists(storage_path):
            os.makedirs(storage_path)
        f = open(storage_path + '/' +filename , 'wb')
    else:
        f = open(filename, 'wb')

    print "printing slaves"
    for slave in slaves:
        print "slave is ",slave
        sendlib.write_socket(slaves[slave].s, request)
        slaves[slave].s.send(length_str)

    while (length>0):
        if length>1024:
            size=1024
        else:
            size=length
        print "Receiving..."
        l = s.recv(size)
        f.write(l)
        for slave in slaves:
            slaves[slave].s.send(l)
        length=length-size

    for slave in slaves:
        response = sendlib.read_socket(slaves[slave].s)
        if "200" not in response:
            print "failed to replicate"


    f.close()
    print "Done Receiving and redirecting"



def sendfile(file_path,s,ack):


    if not os.path.isfile(file_path):
        return False
    sendlib.write_socket(s, ack)
    length=os.stat(file_path).st_size
    length_str = no_to_bytes(length)
    s.send(length_str)

    f = open(file_path, 'rb')

    while (length>0):
        if length>1024:
            size=1024
        else:
            size=length
        l = f.read(size)
        print 'Sending...'
        s.send(l)
        length=length-size
    f.close()
    print "Done Sending"
    #s.shutdown(socket.SHUT_WR)
    return True

