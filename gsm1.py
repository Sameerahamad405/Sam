import serial
#import RPi,GPIO as GPIO
import os,time
import json
import urllib
#GPIO.setmode(GPIO.BOARD)
global ime
global port
global flag
flag=0
global data
global a
a="hi"
var_id=2
data={"id":var_id}
port = serial.Serial(
              
               port='/dev/ttyUSB0',
               baudrate = 9600,
               parity=serial.PARITY_NONE,
               stopbits=serial.STOPBITS_ONE,
               bytesize=serial.EIGHTBITS,
               timeout=1
           )


	
port.write('AT'+'\r\n')
rcv=port.read(10)
print rcv
time.sleep(2)

port.write("AT+CPIN?\r")
msg=port.read(128)
#rcv=port.read(10)

print msg
time.sleep(2)

port.write('AT+CGSN\r')
rcv=port.read(10)


#print rcv
time.sleep(2)



port.write("AT+CIPSHUT\r")
rcv=port.read(128)
print rcv
time.sleep(2)

port.write('AT+CIPMUX=0\r')
rcv=port.read(10)
print rcv
time.sleep(2)

port.write('AT+CGATT?\r')
rcv=port.read(10)
print rcv
time.sleep(2)


port.write('AT+CSTT="airtelgprs.com","",""\r')
rcv=port.read(128)
print rcv
time.sleep(2)



port.write('AT+CIICR\r')
rcv=port.read(10)
print rcv
time.sleep(2)


port.write('AT+CIFSR\r')
rcv=port.read(120)
print rcv
time.sleep(2)

port.write('AT+SAPBR=3,1,"CONTYPE","GPRS"\r')
rcv=port.read(120)
print rcv
time.sleep(2)


port.write('AT+SAPBR=3,1,"APN","airtelgprs.com"\r')
rcv=port.read(120)
print rcv
time.sleep(2)


port.write('AT+SAPBR=1,1\r')
rcv=port.read(120)
print rcv
time.sleep(2)


port.write('AT+SAPBR=2,1\r')
rcv=port.read(120)
print rcv
time.sleep(2)

port.write('AT+HTTPINIT\r')
rcv=port.read(120)
print rcv
time.sleep(4)


port.write('AT+HTTPPARA="CID",1\r')
rcv=port.read(120)
print rcv
time.sleep(5)

print var_id
port.write('AT+HTTPPARA="URL","http://137.59.201.80:9000/RRT/rest/Billing_info?id='+str(var_id)+'"  '+'\r\n')
#port.write(str(data))

rcv=port.read(300)
print rcv
#port.write(a)
time.sleep(10)



port.write('AT+HTTPACTION=0'+'\r\n')
rcv=port.read(128)
print rcv
time.sleep(5)




port.write('AT+HTTPREAD \r\n')
rcv=port.read(128)
print rcv
time.sleep(5)



port.write('AT+HTTPTERM')
rcv=port.read(128)
print rcv
time.sleep(5)


port.write('AT+SAPBR=0,1\r')
rcv=port.read(120)
print rcv
time.sleep(2)





