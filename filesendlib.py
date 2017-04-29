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

def storagepathprefix(storage_path):
    if storage_path is not None:
        return storage_path + '/'
    else:
        return ""

def recvfile(storage_path,filename,s):
    length = s.recv(4)
    length = bytes_to_no(length)
    storage_path=storagepathprefix(storage_path)
    if len(storage_path)>0:
        if not os.path.exists(storage_path):
            os.makedirs(storage_path)
        f = open(storage_path +filename , 'wb')
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

