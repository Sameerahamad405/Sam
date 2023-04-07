from datetime import datetime #for read date and time
import os # read for os.system
import subprocess # subprocess for to call another file 
import json #json for read json files
import logging # for log create
import logging.config # for log config
import threading # calling thread function
import time # time function wait the process
import paho.mqtt.client as mqtt # mqtt client for subscribe
import paho.mqtt.publish as publish # mqtt client for publish
import glob # function for read number of folders
import socket #to read the camera ip
from paramiko import SSHClient,AutoAddPolicy #to connect ssh client
from scp import SCPClient #to connect scp client
import psutil #to read cpu usage

#subprocess call clearcache file to clear cache file
process = subprocess.Popen(['/home/pi/Envy/clearcache.sh'])

logging.config.fileConfig('/home/pi/Envy/logging.conf')
logger = logging.getLogger()

with open('/home/pi/Envy/videoset.json') as server:
        data = json.load(server)
server.close()

with open('/home/pi/Envy/path.json') as server:
        data_path = json.load(server)
server.close()


data_val = (json.dumps(data['FRAMERATE']))
fps = data_val.replace('"','')
logger.info("fps %s" % fps) 

data_val = (json.dumps(data['DURATION']))
duration = data_val.replace('"','')
logger.info("duration %s" % duration) 

data_val = (json.dumps(data['WIDTH']))
width = data_val.replace('"','')
logger.info("width %s" % width) 

data_val = (json.dumps(data['HEIGHT']))
height = data_val.replace('"','')
logger.info("height %s" % height) 

data_val = (json.dumps(data['BROKER']))
broker_data= data_val.replace('"','')
logger.info("BROKER %s" % broker_data) 

data_val = (json.dumps(data['TOPIC']))
topic= data_val.replace('"','')
logger.debug("topic %s" % topic) 

data_val = (json.dumps(data['USER']))
user= data_val.replace('"','')
logger.info("user %s" % user) 

data_val = (json.dumps(data['PWD']))
password= data_val.replace('"','')
logger.info("password %s" % password) 

data_val = (json.dumps(data['CAMERA_NAME']))
cam_name= data_val.replace('"','')
logger.info("cam_name %s" % cam_name) 

data_val = (json.dumps(data['FILE_SIZE']))
file_size_fifo= data_val.replace('"','')
logger.info("file_size_fifo %s" % file_size_fifo)

file_size_fifo = file_size_fifo.split("GB")
file_size_fifo = int(file_size_fifo[0])

data_val = (json.dumps(data_path['CAMERA_PATH']))
camera_path = data_val.replace('"','')
logger.info("camera_path %s" % camera_path) 
print(camera_path)

data_val = (json.dumps(data_path['AGGRI_PATH']))
aggri_path = data_val.replace('"','')
logger.info("aggri_path %s" % aggri_path) 

data_val = (json.dumps(data_path['EVENT_PATH']))
event_path = data_val.replace('"','')
logger.info("event_path %s" % event_path) 

data_val = (json.dumps(data_path['AGGRI_EVENT_PATH']))
aggri_event_path = data_val.replace('"','')
logger.info("aggri_event_path %s" % aggri_event_path) 



try :
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(("8.8.8.8", 80))
	camera_ip = (s.getsockname()[0])
	logger.info("camera_ip %s" % camera_ip)
	logger.debug("camera_ip %s" % camera_ip)
	s.close()
except Exception as e:
	logger.error(e)

Broker = broker_data
pub_topic = topic
flag_event = 2

camera_dict =	{
  "camera1": "0704",
  "camera2": "0705",
  "camera3": "0706",
  "camera4": "0707" 
}

# scp credentials details
camera_folder = camera_dict[cam_name]
ssh = SSHClient()
ssh.load_system_host_keys()
ssh.set_missing_host_key_policy(AutoAddPolicy())
ssh.connect(Broker,username=user,password=password)


# function for get camera status 
def camera_status():
	camera_st = os.popen('vcgencmd get_camera').readline().rstrip('\n')
	camera_status = camera_st.split(" ")
	val =(camera_status)
	val_camera = val[1].split("=")
	final_val = (val_camera[1])
	if(int(final_val) == 1):
		return (1)	
	else :
		return (0)

# function for get ram free data		
def getFree():
	free = os.popen("free -h")
	i = 0
	while True:
		i = i + 1
		line = free.readline()
		if i==2:
    			return(line.split()[0:7])
    			
# function for get pi CPU data
def getCPUuse():
	for i, percentage in enumerate(psutil.cpu_percent(percpu=True, interval=1)):
		pass
	return(psutil.cpu_percent())
	
# function for get video file count
def file_count():
	dateTimeObj = datetime.now()
	datefolder = dateTimeObj.strftime("%Y%m%d")
	cpt = sum([len(files) for r, d, files in os.walk("/home/pi/Envy/Media/{}".format(datefolder))])
	return (cpt)
	
# function for get sd card data
def getsddata():
	free = os.popen("df -h")
	i = 0
	while True:
		i = i + 1
		line = free.readline()
		#print(line)
		if i==2:
			return(line.split()[0:7])
# function for FIFO ( delete the old file)			
def fifo():
	while 1:
		try :   
			size = subprocess.check_output(['du','-sh', camera_path]).split()[0].decode('utf-8')
			logging.info("Directory size:  %s"%str(size))
			try :
				limit_size = size.split("G")
				limit_size = int(float(limit_size[0]))
				if ( limit_size >= file_size_fifo):
					list_of_files = glob.glob('{}*'.format(camera_path)) 
					oldest_file = min(list_of_files, key=os.path.getctime)
					os.system(' sudo rm -r {}'.format(oldest_file))
					logging.info("Old folder delete: %s"% str(oldest_file))
			except Exception as e: 
				logger.error(e)
				pass
			time.sleep(60)
		except Exception as e: 
			logger.error(e)
			time.sleep(60)
			pass
			
# function for start video stroage			
def Stroage_video():
	global flag_event
	while 1:
		try :
			dateTimeObj = datetime.now()
			datefolder = dateTimeObj.strftime("%Y%m%d")
			timestampStr = dateTimeObj.strftime("%H%M%S")
			try :
				os.system('mkdir -p {}{}'.format(camera_path,datefolder))
			except Exception as e: 
				logger.error(e)
				pass
			logging.info("Record Started time: %s" % timestampStr)
			result = os.system('raspivid -mm matrix -qp 30 -a 1036 -a " {} %Y-%m-%d %X" -ae +20+20 -t {} -w {} -h {} -hf -ih -fps {} -o - | tee {}{}/{}.h264 | ffmpeg  -r {} -i -  -vcodec copy  -an -t 00:03:00 -f rtsp rtsp://{}:80/live/{} '.format(cam_name,duration,width,height,fps,camera_path,datefolder,timestampStr,fps,camera_ip,cam_name))
			val = timestampStr
			try :
				thread1 = threading.Thread(target=storage_video_to_aggri,args=(datefolder,val)).start()
			except Exception as e:
				logger.error("storage_video_to_aggri : {}".format(e)) 
			if 0 == result:
				dateTimeObj = datetime.now()
				timestampStr = dateTimeObj.strftime("%H%M%S")
				logging.info("Record Completed time: %s"% timestampStr)
			else:
				logging.error("Record Not Completed: %d" % result)
		except Exception as e:
			logger.error(e)
			result = os.system('raspivid -mm matrix -qp 30 -a 1036 -a " Envy %Y-%m-%d %X" -ae +20+20 -t {} -w {} -h {} -hf -ih -fps {} -o  {]{}/{}.h264'.format(duration,width,height,fps,camera_path,datefolder,timestampStr))
			if 0 == result:
				dateTimeObj = datetime.now()
				timestampStr = dateTimeObj.strftime("%H%M%S")
				logging.info("Record offline Completed time: %s"% timestampStr)
			else:
				logging.error("Record offline Not Completed: %d" % result)
			pass
		
		if(flag_event == 1):
			try :
				#event_msg(val)
				thread1 = threading.Thread(target=event_msg,args=(val,)).start()
				flag_event = 0
			except Exception as e:
				logger.error(e) 
				

# function for send storage video to aggrigator				
def storage_video_to_aggri(datefolder,val):
	try :
		stdin, stdout, stderr = ssh.exec_command('mkdir -p {}{}/{} '.format(aggri_path,camera_folder,datefolder))
		logging.info("{},{},{}".format( stdin, stdout, stderr))
		logging.debug("Make Dir : {},{},{}".format( stdin, stdout, stderr))
		time.sleep(0.05)
		with SCPClient(ssh.get_transport()) as scp:
			scp.put('{}{}/{}.h264'.format(camera_path,datefolder,val), '{}{}/{}'.format(aggri_path,camera_folder,datefolder))
			scp.close()
			logging.info("video send to the aggriand folder : {} and filename {} ".format(datefolder,val))
	except Exception as e:
		logger.error(e)
		#scp.close()
		pass
# function for store event video and send event video to aggrigator	
def event_msg(val):
	try :
		dateTimeObj = datetime.now()
		datefolder = dateTimeObj.strftime("%Y%m%d")
		
		result = os.system(' cp /home/pi/Envy/Media/{}/{}.h264 /home/pi/Envy/Event/{}/{}.h264'.format(datefolder,val,datefolder,val))
		if 0 == result:
			logging.info("Event Completed time: %s"% result)
		else:
			logging.error(" Event Not Completed: %d" % result)
			
		with SCPClient(ssh.get_transport()) as scp:
			scp.put('{}{}/{}.h264'.format(event_path,datefolder,val), '{}{}/{}'.format(aggri_event_path,camera_folder,datefolder))
			scp.close()	
			logging.info("Event video sent to the aggri and floder : {} and filename {} ".format(datefolder,val))
	
	except Exception as e:
		logger.error(e)
		pass
		
		
"""def storage_snaps_to_aggri():
	file_val = 0
	snap_sec = '00'
	while 1:
		try :
			dateTimeObj = datetime.now()
			datefolder = dateTimeObj.strftime("%d-%m-%Y")
			timestampStr = dateTimeObj.strftime("%H-%M-%S")
			timestampStr_split = timestampStr.split("-")
			if(snap_sec == '60'):
				snap_sec = '00'
			if(timestampStr_split[2] == snap_sec):
				with SCPClient(ssh.get_transport()) as scp:
					stdin, stdout, stderr = ssh.exec_command('mkdir {}snaps/{}/{} '.format(aggri_path,cam_name,datefolder))
					logging.debug("Make Dir : {},{},{}", stdin, stdout, stderr)
					file_val = file_val + 1
					file_name = datefolder+"_"+timestampStr+"_"+"Envy_"+camera_folder+"_"+str(file_val).zfill(4)+".jpeg"
					os.system('ffmpeg -rtsp_transport tcp -i rtsp://{}:80/live/{} -vframes 1 -r 1 -s 640x480 {}{}'.format(camera_ip,cam_name,camera_path,file_name))
					scp.put('{}{}'.format(camera_path,file_name), '{}snaps/{}/{}'.format(aggri_path,cam_name,datefolder))
					os.system('rm {}{}'.format(camera_path,file_name))
					scp.close()
					snap_sec = int(snap_sec) + int(snap_time)
					snap_sec = str(snap_sec)
					time.sleep(2)
			else :
				time.sleep(0.05)
		except Exception as e:
			logger.error(e)
			scp.close()
			pass"""
			
# function for publish camera status to the aggri				
def camera_data_publish():
	
	while 1:
		try :
			return_val = camera_status()
			mem = getFree()
			file_count_rd=file_count()
			sd_data = getsddata()
			dateTimeObj = datetime.now()
			timestampStr = dateTimeObj.strftime("%Y-%m-%d %H:%M:%S")
			datefolder = dateTimeObj.strftime("%Y%m%d")
			dev_val = os.popen('/opt/vc/bin/vcgencmd measure_temp')
			cpu_temp = dev_val.read()[5:-3]
			cpu_use = getCPUuse()
			path = '{}{}'.format(camera_path,datefolder)
			size = subprocess.check_output(['du','-sh', path]).split()[0].decode('utf-8')
			#path_media = '/home/pi//Envy/Media/'
			size_media = subprocess.check_output(['du','-sh', camera_path]).split()[0].decode('utf-8')
			#path_event = '/home/pi//Envy/Event/'
			size_event = subprocess.check_output(['du','-sh', event_path]).split()[0].decode('utf-8')
			mess = {'HEARTBEAT':1,'CAMERA':int(return_val),'TIMESTAMP':timestampStr,'TEMPERATURE': float(cpu_temp),'CPU' :cpu_use,"RAM TOTAL" :mem[1],"RAM USED" :mem[2],"RAM FREE" :mem[3],"RAM AVAILABLE " :mem[6],"SD SIZE" : sd_data[1],"SD USED" : sd_data[2],"SD AVAILABLE" : sd_data[3],"SD USE%" : sd_data[4],'FILE COUNT' : int(file_count_rd),'CURRENT DIRECTORY SIZE' : size,'MEDIA DIRECTORY SIZE' : size_media,'EVENT DIRECTORY SIZE' : size_event} 
			#print (mess
			#mess = 10
			logger.info('Published msg: {}'.format(mess))
			logger.debug('Published msg: {}'.format(mess)) 
			publish.single(pub_topic, str(mess) ,hostname=Broker)
			logging.info("Publish Successfully")
				
			time.sleep(60)
		except Exception as e:
			logger.error(e)
			time.sleep(30)
			pass
			
# function for connect mqtt subscriber 			
def on_connect(client, userdata, flags, rc):
	logging.info("Connected with result code %s "%str(rc))
	client.subscribe(pub_topic)
	
# function for reconnect mqtt subscriber when client disconect 
def on_disconnect(client, userdata, rc):
	if rc != 0:
        	logging.info("Unexpected MQTT disconnection. Will auto-reconnect")

# function for subscriber message  
def on_message(client, userdata, msg):
	global flag_event
	if(str(msg.payload.decode('utf-8')) == "event"):
		try :
			dateTimeObj = datetime.now()
			datefolder = dateTimeObj.strftime("%Y%m%d")
			timestampStr = dateTimeObj.strftime("%H%M%S")
			os.system('mkdir -p {}{}'.format(event_path,datefolder))
			logging.info("Event Completed time: %s"% str(msg.payload))
			flag_event = 1
			try :
				os.system('ffmpeg -i rtsp://{}:80/live/{} -vframes 1 -r 1 {}{}/{}.jpg'.format(camera_ip,cam_name,event_path,datefolder,timestampStr))
				logging.info("Event Snapshot Completed")
			except Exception as e:
				logger.error(e)
				pass 
		except Exception as e: 
			logger.error(e)
			pass
		
			
if __name__ == "__main__":
	client = mqtt.Client()
	client.on_connect = on_connect
	client.on_message = on_message
	client.on_disconnect = on_disconnect

	try :
		client.connect(Broker, 1883, 60)
	except Exception as e:
		logger.error(e)
		pass
	# start thread functions
	thread1 = threading.Thread(target=Stroage_video).start()
	logging.info("Stroage_video thread1 startd")
	#thread2 = threading.Thread(target=storage_snaps_to_aggri).start()
	#logging.info("Stroage_video thread2 startd")
	thread2 = threading.Thread(target=camera_data_publish).start()
	logging.info("camera_data_publish thread2 startd")
	thread3 = threading.Thread(target=fifo).start()
	logging.info("fifo startd")
	client.loop_forever()
		
