def no_to_bytes(no):
    b=str(no)
    non_zeroscounter=len(b)
    zeros=[]
    for i in range(4-non_zeroscounter):
        zeros.append("0")
    return "".join(zeros)+b


def bytes_to_no(b):
    return int(b)


def read_socket(soc):
    length=soc.recv(4)
    if(len(length)==0):
        return None
    length=bytes_to_no(length)

    i=0
    request=[]
    while(i<length):
        if(length-i>512):
            bufflength=512
        else:
            bufflength=length-i
        request.append(soc.recv(bufflength))
        i=i+bufflength

    st="".join(request)
    st=st.encode("utf-8")
    return st

def write_socket(soc, response):
    length=len(response)
    length_bytes=no_to_bytes(length)

    soc.send(length_bytes)
    i=0
    while(i<length):
        if length-i>512:
            bufflength=512
        else:
            bufflength=length-i
        soc.send(response[i:i+bufflength])
        i=i+bufflength