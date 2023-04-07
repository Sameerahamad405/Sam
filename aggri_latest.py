from datetime import datetime #for read date and time
import os # read for os.system
import logging #for log create
import logging.config # for log config
import json #json for read json files
import paho.mqtt.client as mqtt # mqtt client for subscribe
import paho.mqtt.publish as publish # mqtt client for publish
import threading  # calling thread function
import time # time function wait the process
import subprocess # subprocess for to call another file
import requests # for url request
import base64 # convert image in base64 format
import ast # to read array files
import glob # function for read number of folders
import psutil #to read cpu usage

logging.config.fileConfig('/home/pi/Envy/logging.conf')
logger = logging.getLogger()

with open('/home/pi/Envy/aggri.json') as aggri_data:
        data = json.load(aggri_data)
aggri_data.close()

with open('/home/pi/Envy/videoset.json') as server:
        data_value = json.load(server)
server.close()

with open('/home/pi/Envy/path.json') as server:
        data_path = json.load(server)
server.close()

data_val = (json.dumps(data['FRAMERATE']))
fps = data_val.replace('"','')
logger.info("fps %s" % fps)

data_val = (json.dumps(data['SNAP_TIME']))
snap_time = data_val.replace('"','')
logger.info("snap_time %s" % snap_time)


data_val = (json.dumps(data['NUMBER_OF_CAMERA']))
num_of_camera = data_val.replace('"','')
logger.info("num_of_camera %s" % num_of_camera)

emg_call_num = (json.dumps(data['CAMERA_NAMES']))
camera_name_list = ast.literal_eval(emg_call_num)
logger.info("camera_name_list %s" % camera_name_list)

emg_call_num = (json.dumps(data['CAMERA_IP']))
camera_ip_list = ast.literal_eval(emg_call_num)
logger.info("camera_ip_list %s" % camera_ip_list)

data_val = (json.dumps(data_value['TOPIC']))
topic= data_val.replace('"','')
logger.info("topic %s" % topic)

data_val = (json.dumps(data_value['BROKER']))
broker_data= data_val.replace('"','')
logger.info("BROKER %s" % broker_data)

data_val = (json.dumps(data_path['CAMERA_PATH']))
camera_path = data_val.replace('"','')
logger.info("camera_path %s" % camera_path) 

data_val = (json.dumps(data_path['SNAP_PATH']))
snap_path = data_val.replace('"','')
logger.info("snap_path %s" % snap_path) 

data_val = (json.dumps(data_path['EVENT_PATH']))
event_path = data_val.replace('"','')
logger.info("event_path %s" % event_path) 


Broker = broker_data
pub_topic = topic
camera_folder = "0701"
camera_folder_append = [camera_folder]
flag_event = 2
#loop for create folders for video,event and snap
for i in range(int(num_of_camera)):
	os.system('mkdir -p {}{}'.format(camera_path,str(camera_folder).zfill(4)))
	os.system('mkdir -p {}{}'.format(snap_path,camera_name_list[i]))
	os.system('mkdir -p {}{}'.format(event_path,str(camera_folder).zfill(4)))
	camera_folder = int(camera_folder) + 1
	camera_folder_append.append(str(camera_folder).zfill(4))
	
	
url = 'http://188.42.97.98:4440/api/FileUpload/UploadPicture'



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

# function for get video and image file count	
def file_count():
	count = []
	count_snap =[]
	dateTimeObj = datetime.now()
	datefolder = dateTimeObj.strftime("%Y%m%d")
	datefolder_snap = dateTimeObj.strftime("%d-%m-%Y")
	for i in range(int(num_of_camera)):
		count_val = sum([len(files) for r, d, files in os.walk("{}{}/{}".format(camera_path,str(camera_folder_append[i]),datefolder))])
		count_val_snap = sum([len(files) for r, d, files in os.walk("{}{}/{}".format(snap_path,str(camera_name_list[i]),datefolder_snap))])
		#count_val = camera_folder_append[i]+ ":" + str(count_val)
		count.append(count_val)
		count_snap.append(count_val_snap)
		
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

"""			
def storage_video_camera(camera_ip,camera_name,camera_folder):

	while 1:
		try :
			dateTimeObj = datetime.now()
			datefolder = dateTimeObj.strftime("%Y%m%d")
			timestampStr = dateTimeObj.strftime("%H%M%S")
			try :
				os.system('mkdir -p /media/pi/HDD/{}/{}'.format(str(camera_folder),datefolder))
			except Exception as e: 
				logger.error(e)
			logging.info("Record Started {} time: {}".format(camera_folder,timestampStr))
			result = os.system('ffmpeg -rtsp_transport tcp  -y  -i rtsp://{}:80/live/{}  -vcodec copy   -an -f mp4 -t 00:03:00 /media/pi/HDD/{}/{}/{}.mp4 '.format(camera_ip,camera_name,str(camera_folder),datefolder,timestampStr))
			val = timestampStr
			
			if 0 == result:
				dateTimeObj = datetime.now()
				timestampStr = dateTimeObj.strftime("%H%M%S")
				logging.info("Record Completed {} time: {}".format(camera_folder,timestampStr))
			else:
				logging.error("Record Not Completed {}: {}".format(camera_folder,result))
		except Exception as e:
			logger.error("Error in {} : {}".format(camera_folder,str(e)))
			pass

		
"""	

# function for snaps form camera		
def snapshot(camera_ip,camera_name,camera_folder):
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
				file_val = file_val + 1
				#for i in range(int(num_of_camera)):
				file_name = datefolder+"_"+timestampStr+"_"+"Envy_"+camera_folder+"_"+str(file_val).zfill(4)+".jpeg"
				os.system('mkdir -p {}{}/{}'.format(snap_path,camera_name,datefolder))
				result = os.system('ffmpeg -rtsp_transport tcp -i rtsp://{}:80/live/{} -vframes 1 -r 1 -s 640x480 {}{}/{}/{} '.format(camera_ip,camera_name,snap_path,camera_name,datefolder,file_name))
				if 0 == result:
					logging.info("snap taken and file name {}".format(file_name))
				else:
					logging.error("snap is not takenand file name {}".format(file_name))
				snap_to_server(camera_name,file_name,camera_folder)
				print(timestampStr_split)	
				snap_sec = int(snap_sec) + int(snap_time)
				snap_sec = str(snap_sec)
				time.sleep(5)
			else :
				#continue
				time.sleep(0.05) 
				
		except Exception as e:
			logger.error(e)
			#print(e)
			pass
			
# function for send snaps to server
def snap_to_server(camera_name,file_name,camera_folder):
	try :
		dateTimeObj = datetime.now()
		datefolder = dateTimeObj.strftime("%d-%m-%Y")
		#if(flag == 1):
		with open(snap_path+camera_name+"/"+str(datefolder)+"/"+str(file_name), "rb") as img_file:
			my_string = base64.b64encode(img_file.read())
		    
		folder_upload = "22663\\{}\\".format(camera_folder)
		myobj = {"ByteArray":my_string,"FileName":file_name,"UploadPath":folder_upload,"FolderName":"null","EventId":"null"}

		x = requests.post(url, data = myobj)
		res = x.status_code
		val1 = x.text
		#print(x.text, x.status_code)
		logging.info("camera name : {},post image Completed : {} and post image value : {} ".format(camera_name,res,val1))
		#logging.info("post image value : %s " % val1)
		#flag = 0
	except Exception as e:
		logger.error(e)
		pass
		
# function for compress the video file size		
def convert_video_file():
	while 1 :
		try :
			for i in range(int(num_of_camera)):
				
				list_of_files = glob.glob('{}{}/*'.format(camera_path,camera_folder_append[i]))
				oldest_file = min(list_of_files, key=os.path.getctime)
				split_file = oldest_file.split("/")
				datefolder = split_file[5]
				if(len(list_of_files) > 3):
					os.system('mkdir -p {}convertfile'.format(camera_path))
					os.system('mkdir -p {}convertfile/{}'.format(camera_path,camera_folder_append[i]))
					os.system('mkdir -p {}convertfile/{}/{}'.format(camera_path,camera_folder_append[i],datefolder))
					for r, d,f in os.walk(oldest_file):
						for file in f:
							if '.h264' in file:
								result = os.system('ffmpeg  -r {} -i {}/{} -s 640x480 -c:v h264_omx -r 15 -b:v 150k -an -t 00:03:00 -f mp4 {}convertfile/{}/{}/{}'.format(fps,oldest_file,file,camera_path,camera_folder_append[i],datefolder,file))
					os.system('sudo rm -r {}'.format(oldest_file))
					logging.info("convert video file completed for : {}".format(camera_folder_append[i]))
				else:
					break
					
		except Exception as e:
			logger.error(e)
			pass
		time.sleep(5)	

# function for publish aggri data		
def aggri_data_publish():
	while 1:
		try :
			mem = getFree()
			file_count_rd=file_count()
			sd_data = getsddata()
			
			dateTimeObj = datetime.now()
			timestampStr = dateTimeObj.strftime("%d%m%Y %H%M%S")
			datefolder = dateTimeObj.strftime("%Y%m%d")
			dev_val = os.popen('/opt/vc/bin/vcgencmd measure_temp')
			cpu_temp = dev_val.read()[5:-3]
			cpu_use = getCPUuse()
			
			mess = {'HEARTBEAT':'alive','TIMESTAMP':timestampStr,'TEMPERATURE': cpu_temp,"CPU": float(cpu_use),"RAM TOTAL" :mem[1],"RAM USED" :mem[2],"RAM FREE" :mem[3],"RAM AVAILABLE " :mem[6],"SD SIZE" : sd_data[1],"SD USED" : sd_data[2],"SD AVAILABLE" : sd_data[3],"SD USE%" : sd_data[4],'TOTAL_CORES': int(psutil.cpu_count(logical=False)),'CAM_FILE COUNT' : file_count_rd} 
			#print (mess
			#mess = 10
			logger.debug('Published msg: {}'.format(mess)) 
			publish.single(pub_topic, str(mess) ,hostname=Broker)
			logging.info("Publish Successfully")
			time.sleep(60)
			
				
		except Exception as e:
			logger.error(e)
			pass

# function for connect mqtt subscriber		
def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))
	client.subscribe("Event")
	for i in range(int(num_of_camera)):
		client.subscribe(camera_name_list[i])	
		
 #function for subscriber message
def on_message(client, userdata, msg):
	
	path = "/home/pi/Envy/{}_status.txt".format(msg.topic)
	file1_panic_write = open(path,"a")#write mode 
		
	file1_panic_write.write(msg.topic+" "+str(msg.payload.decode('utf-8'))+"\n"+"\n"+"\n") 
	file1_panic_write.close()
	
# function for subscriber event message 
def on_message_event(client, userdata, msg):	
	if(str(msg.payload.decode('utf-8')) == "event"):
		dateTimeObj = datetime.now()
		datefolder = dateTimeObj.strftime("%Y%m%d")
		timestampStr = dateTimeObj.strftime("%H%M%S")
		try :
			for i in range(int(num_of_camera)):
				os.system('mkdir -p {}{}/{}'.format(event_path,camera_folder_append[i],datefolder))
				os.system('ffmpeg -rtsp_transport tcp -i rtsp://{}:80/live/{} -vframes 1 -r 1 -s 640x480 {}{}/{}/{}.jpg'.format(camera_ip_list[i],camera_name_list[i],event_path,camera_folder_append[i],datefolder,timestampStr))
				result = os.system('mosquitto_pub -d -t {} -m "event"'.format(camera_name_list[i]))
				logging.info("Event Snapshot Completed : {}".format(camera_name_list[i]))
		except Exception as e: 
			logger.error(e)
			pass
	
		
	
# function for reconnect mqtt subscriber when client disconect 	    
def on_disconnect(client, userdata, rc):
	if rc != 0:
        	logging.info("Unexpected MQTT disconnection. Will auto-reconnect")	
        	
if __name__ == "__main__":
	client = mqtt.Client()
	client.on_connect = on_connect
	#client.on_message = on_message
	client.on_disconnect = on_disconnect

	try :
		client.connect(Broker, 1883, 60)
		for i in range(int(num_of_camera)):
			client.message_callback_add(camera_name_list[i],on_message)
		client.message_callback_add("Event",on_message_event)
	except Exception as e:
		logger.error(e)
		pass

	thread4 = threading.Thread(target=convert_video_file).start()		
	"""for i in range(int(num_of_camera)):
		thread = threading.Thread(target=storage_video_camera,args=(camera_ip_list[i],camera_name_list[i],camera_folder_append[i])).start()
		logging.info("storage_video_camera thread started for : {} ".format(camera_name_list[i])) """
	
	#loop start the thread function	
	for i in range(int(num_of_camera)):
		thread = threading.Thread(target=snapshot,args=(camera_ip_list[i],camera_name_list[i],camera_folder_append[i])).start()
		logging.info("snapshot camera thread started for : {} ".format(camera_name_list[i]))
	
		
	thread4 = threading.Thread(target=aggri_data_publish).start()
		
	client.loop_forever()
