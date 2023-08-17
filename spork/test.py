import sys
import socket
import getpass as gt


print('==============================================')
print('Starting App')
print('==============================================')
print('User login:', gt.getuser())

hostname = socket.gethostname() 
ipaddress = socket.gethostbyname(hostname)
print(ipaddress)


print(sys.version)

print('User login:', gt.getuser())

print('======================= ALL ARGS ==========================')
print(sys.argv)

print('======================= INDIVIDUAL ARGS START ==========================')

for v in sys.argv:
    print(v)

print('======================= INDIVIDUAL ARGS END ==========================')

