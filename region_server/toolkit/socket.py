import socket   #for sockets
import sys  #for exit

#create an INET, STREAMing socket
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
except socket.error:
    print(1)
    sys.exit()

print(1)

host = 'oschina.net'
port = 80

try:
    remote_ip = socket.gethostbyname( host )

except socket.gaierror:
    #could not resolve
    print (1)
    sys.exit()

#Connect to remote server
s.connect((remote_ip , port))

print ("Socket Connected to " + host + ' on ip ' + remote_ip)

#Send some data to remote server
message = "GET / HTTP/1.1\r\nHost: oschina.net\r\n\r\n"

try :
    #Set the whole string
    s.sendall(message)
except socket.error:
    #Send failed
    print ('Send failed')
    sys.exit()

print ('Message send successfully')

#Now receive data
reply = s.recv(4096)

print (reply)