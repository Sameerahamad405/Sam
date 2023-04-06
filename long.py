#!/usr/bin/python3
import socket,json,re,logging,time,random,pymysql,sys,os,ftplib,datetime,csv
from ws4py.client.threadedclient import WebSocketClient
from urllib.request import urlopen
from threading import Thread
from server_messages import changeConfiguration,changeAvailability,remoteStartTransaction,remoteStopTransaction,getConfiguration,updateFirmware,getDiagnostics,reset,unlockConnector,clearCache,getLocalListVersion,sendlocallist,dataTransfer,cancelReservation,setchargingprofile,triggerMessage,reserveNow,clearchargingprofile,GetCompositeSchedule
from ftplib import FTP
import serial
from ast import literal_eval
from ssl import SSLError

try:
	serial_rrt = serial.Serial('/dev/ttyUSB0', 9600, timeout=3)
	#serial_rrt1 = serial.Serial('/dev/ttyUSB1', 9600, timeout=3)
except:
	print ("usb0 not available")

try:
	#serial_rrt = serial.Serial('/dev/ttyUSB0', 9600, timeout=3)
	serial_rrt1 = serial.Serial('/dev/ttyUSB1', 9600, timeout=3)
except:
	print ("usb1 not available")

def database():
	return pymysql.connect(host="localhost", user="rrt", passwd="coovum123", db="ocpp_rrt", cursorclass=pymysql.cursors.DictCursor)

db = database()
with db.cursor() as mycursor:
	values = mycursor.execute("SELECT authorization FROM standalone")
	result = mycursor.fetchone()
if result["authorization"]=="standalone":
  sys.exit()
db.close()
stop_dic = {"1": "Local", "2": "Remote", "3": "SoftReset", "4": "HardReset", "5": "UnlockCommand","6": "DeAuthorized", "7": "EmergencyStop", "8": "EVDisconnected", "9": "PowerLoss", "10": "UnlockCommand"}
socket_reconnect_flag=flag_pro=1
status_temp=0
charging_profile_stop={"1":0,"2":0}
t=k=b=u=trans_flag=off_on_start=check_connectivity=transaction_id=0
file_path="/home/swathi/Documents/restructure"
#file_path="/home/pi/Documents/rrt/rrt_screen"
firm_path="/home/swathi/Documents/restructure"
#firm_path="/home/pi"
logging.basicConfig(format='%(message)s',level=logging.INFO)

with open(file_path+'/rrt_text/process_start.txt', 'w+') as ch:
	ch.write("0")

with open(file_path+'/rrt_text/boot_notification.txt', 'w') as boot_not:
    write_boot_not=boot_not.write("")

file_ip=open(file_path+'/text/pingip.txt', 'r')
ip=(file_ip.read().split('\n')[0])
file_ip.close()
try:
	ipp=ip.split(':')
	ipadd=socket.gethostbyname(ipp[0])
	port=int(ipp[1])
except:
	pass
print (ip)
#to check connectivity of client using websocket ping
def checkNetworkConnectivity():
	while True:
		global flag_pro
		global check_connectivity
		try:
			if flag_pro==2:
				print (ip)
				try:
					ipp=ip.split(':')
					ipadd=socket.gethostbyname(ipp[0])
					port=int(ipp[1])
				except:
					try:
						ipadd=socket.gethostbyname("ip.coovum.com")
						port=5006     
					except:
						pass
			connect=socket.create_connection((ipadd,port),5)
			connect.shutdown(1)
			connect.close()
			check_connectivity="1"
			with open(file_path+'/rrt_text/check_connectivity.txt', 'w') as write_check_connectivity:
			   write_check_connectivity.write(check_connectivity)
			write_check_connectivity.close()
			global trans_flag
			trans_flag=0
			flag_pro=0
			time.sleep(0.1)
		except socket.timeout:
			pass 
		except:
			transid()
			time.sleep(10)
			if flag_pro==1:
				Thread(target=offlineprocess).start()
				flag_pro=2
			try:
				if check_connectivity=="1":
					ws.close_connection()
					#ws.terminate()
					logging.info("connection closed")
			except:
				#logging.info("No connection available to close")
				pass
			check_connectivity="0"
			with open(file_path+'/rrt_text/check_connectivity.txt', 'w') as write_check_connectivity:
			    write_check_connectivity.write(str(check_connectivity))
			write_check_connectivity.close()
			time.sleep(0.3)

#to call online process seperately without disturbing check_connectivity
def call_online():
	time.sleep(5)
	onlineprocess()
	logging.info ("dne1")
	onlineprocess()
	logging.info ("dne2")

#to send any offline stopped transaction to server when network is retrieved
def onlineprocess():
	dbd = database()
	with dbd.cursor() as mycursor:
		mycursor.execute("SELECT connector_id,tag_id,stop_reason,trans_id,cur_time,meter_value,cost FROM on_off WHERE stop_reason IS NOT NULL LIMIT 1")
		result = mycursor.fetchall()
		#logging.info(f"on off res {result}{check_connectivity}")
		if (check_connectivity=="1") and (result!=()):
			ws.send(json.dumps([2,"offline_stop-"+str(result[0]["connector_id"]),"StopTransaction",{"reason":stop_dic[result[0]["stop_reason"]],"meterStop":result[0]["meter_value"],"timestamp":result[0]["cur_time"],"transactionId":result[0]["trans_id"]}]))
			mycursor.execute("DELETE FROM on_off WHERE trans_id=%s",[result[0]["trans_id"]])
			dbd.commit()
		else:
			mycursor.execute("DELETE FROM on_off LIMIT 1")
			dbd.commit()
	dbd.close()

#to clear db and start process when boot is initiated in offline
def offlineprocess():
	time.sleep(20)
	global k
	if  (k==0) and (flag_pro==1):
		print ("offlineprocess")
		dbd = database()
		with dbd.cursor() as mycursor:
			mycursor.execute("UPDATE conn_strt_stp SET remote_start='0',tag_id_stop='', tag_auth_stop='',tag_id='',tag_auth='',trans_id=NULL, st_status='', transaction_progress='', start_transaction='',stop_transaction='',flow='0'")
		dbd.commit()
		dbd.close()
		Thread(target=process,args=("1",)).start()#start the auhorization thread and wait for a valid authorization
		Thread(target=process,args=("2",)).start()#start the auhorization thread and wait for a valid authorization
		Thread(target=status_notification,args=("1")).start()
		Thread(target=status_notification,args=("2")).start()
		with open(file_path+'/rrt_text/process_start.txt', 'w+') as ch:
			  ch.write("1")
		k=1
     
#to save stop transaction of a transaction when goes offline during a transaction
def transid():
	global trans_flag
	if trans_flag==0:
		dba = database()
		with dba.cursor() as mycursor:
			mycursor.execute("SELECT trans_id FROM conn_strt_stp ")
			result = mycursor.fetchall()
			if result[0]["trans_id"]!=None and result[1]["trans_id"]==None:
				tr=result[0]["trans_id"]
				mycursor.execute("INSERT INTO on_off (trans_id) VALUES (%s)",[tr])
			if result[0]["trans_id"]==None and result[1]["trans_id"]!=None:
				tr=result[1]["trans_id"]
				mycursor.execute("INSERT INTO on_off (trans_id) VALUES (%s)",[tr])
			if result[0]["trans_id"]!=None and result[1]["trans_id"]!=None:
				for i in range (0,2):
					tr=result[i]["trans_id"]
					mycursor.execute("INSERT INTO on_off (trans_id) VALUES (%s)",[tr])
			trans_flag=1
		dba.commit()

#to unlock connector. Currently not used for any DC chargers 
def unlockConnectorThread(unique_id,connector_id):
	while True:
		if connector_id=="1":
			file_start=open(file_path+'/rrt_text/unlock_conn1.txt', 'r')
			unlock_conn_params=(file_start.read().split('\n')[0])
			file_start.close()
		elif connector_id=="2":
			file_start=open(file_path+'/rrt_text/unlock_conn2.txt', 'r')
			unlock_conn_params=(file_start.read().split('\n')[0])
			file_start.close()
		if(unlock_conn_params != ""):
			if connector_id=="1":
				with open(file_path+'/rrt_text/unlock_conn1.txt', 'w') as file_start:
					file_start.write("")
			elif connector_id=="2":
				with open(file_path+'/rrt_text/unlock_conn2.txt', 'w') as file_start:
					file_start.write("")
			if(unlock_conn_params == "1"):
				logging.info ("Unlocked")
				status = "Unlocked"
			elif(unlock_conn_params == "0"):
				logging.info ("UnlockFailed")
				status = "UnlockFailed"
			else:
				logging.info ("Invalid!!!!")
			ws.send(json.dumps([3,unique_id,{"status":status}]))
			break
		else:
			logging.info ("Waiting for response from RRT")

#to send heartbeat to server when online
def sendHeartbeat():
	while True:
		heartbeat_interval_value=config("HeartbeatInterval")
		if(check_connectivity == "1"):
			try:
				ws.send(json.dumps([2,"heart_beat","Heartbeat",{}]))
			except Exception as e:
				logging.info (e)
		elif(check_connectivity == "0"):
			logging.info ("No network connection to send Heartbeat!!!!!!!!")
		time.sleep(float(heartbeat_interval_value))

#to get server initiated configuration values from db
def config(param):
	dba = database()
	with dba.cursor() as mycursor:
		mycursor.execute("SELECT %s FROM imp_files"%(param))
		result = mycursor.fetchone()
	dba.close()
	return result[param]

#to send boot notification to server when booted online
def bootNotificationThread(dos):
	dbd = database()
	with dbd.cursor() as mycursor:
		mycursor.execute("UPDATE imp_files SET diagnostic_detail='Idle',firmware_detail='Idle'")
	dbd.commit()
	dbd.close()
	logging.info("BootNotification Thread started")
	boot_notification_read=open(file_path+'/rrt_text/boot_notification.txt', 'r')
	boot_notification_params=(boot_notification_read.read().split('\n')[0])
	if(boot_notification_params != ""):
		split = boot_notification_params.split(",")
		boot_not_status =  split[0]
		boot_not_interval =  split[2]
		if(boot_not_status != "Accepted"):
			time.sleep(float(boot_not_interval))
			boot_notification='[2,"boot_notification","BootNotification",{"chargeBoxSerialNumber":"XXXXXXX","chargePointModel":"BCP15kW","chargePointSerialNumber":"XXXXXXX","chargePointVendor":"RRT","firmwareVersion":"XXXXXXX","iccid":"XXXXXXX","imsi":"XXXXXXX","meterSerialNumber":"XXXXXXX","meterType":"XXXXXXX"}]'
	else:
		boot_notification='[2,"boot_notification","BootNotification",{"chargeBoxSerialNumber":"XXXXXXX","chargePointModel":"BCP15kW","chargePointSerialNumber":"XXXXXXX","chargePointVendor":"RRT","firmwareVersion":"XXXXXXX","iccid":"XXXXXXX","imsi":"XXXXXXX","meterSerialNumber":"XXXXXXX","meterType":"XXXXXXX"}]'
	ws.send(boot_notification)
	if dos==0:
		dbd = database()
		with dbd.cursor() as mycursor:
			mycursor.execute("SELECT trans_id, connector_id FROM conn_strt_stp WHERE trans_id IS NOT NULL and transaction_progress='1'")
			result = mycursor.fetchall()
			h=len(result)
			cur_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
			f= open(file_path+'/rrt_text/getenergy_metervalue.txt', 'r')    
			meter1=f.read().split('\n')[0]
			f.close()
			f= open(file_path+'/rrt_text/getenergy_metervalue_2.txt', 'r')    
			meter2=f.read().split('\n')[0]
			f.close()
			if h==1:
				if result[0]["connector_id"]==1:
					meter=meter1
					fil = open(file_path+"/rrt_text/getstartenergy_occp.txt","w") 
					fil.write(str(meter1))
					fil.close()
				elif result[0]["connector_id"]==2:
					meter=meter2
					fil = open(file_path+"/rrt_text/getstartenergy_occp_2.txt","w") 
					fil.write(str(meter2))
					fil.close()
				ws.send(json.dumps([2,"machine_off_stop-"+str(result[0]["connector_id"]),"StopTransaction",{"reason":stop_dic["9"],"meterStop":meter,"timestamp":cur_time,"transactionId":result[0]["trans_id"] }]))
			elif h==2:
					fil = open(file_path+"/rrt_text/getstartenergy_occp.txt","w") 
					fil.write(str(meter1))
					fil.close()
					fil = open(file_path+"/rrt_text/getstartenergy_occp_2.txt","w") 
					fil.write(str(meter2))
					fil.close()
					ws.send(json.dumps([2,"machine_off_stop-"+str(result[0]["connector_id"]),"StopTransaction",{"reason":stop_dic["9"],"meterStop":meter1,"timestamp":cur_time,"transactionId":result[0]["trans_id"] }]))
					ws.send(json.dumps([2,"machine_off_stop-"+str(result[1]["connector_id"]),"StopTransaction",{"reason":stop_dic["9"],"meterStop":meter2,"timestamp":cur_time,"transactionId":result[1]["trans_id"] }]))
			global k
			if check_connectivity=="1" and k==0:
					logging.info ("emptying db")
					mycursor.execute("UPDATE conn_strt_stp SET remote_start='0',tag_id_stop='', tag_auth_stop='',tag_id='',tag_auth='',trans_id=NULL, st_status='', transaction_progress='', start_transaction='',stop_transaction='',flow='0'")
					dbd.commit()
		dbd.close()
       
#to revert when authorization sent online but goes offline before response
def maintainflow(conn,flow):
	time.sleep(10)
	dba = database()
	with dba.cursor() as mycursor:
		mycursor.execute("SELECT flow FROM conn_strt_stp WHERE connector_id=%s",[conn])
		result1 = mycursor.fetchone()
		if flow=='1' and result1["flow"]=='1':
			mycursor.execute("UPDATE conn_strt_stp SET tag_auth='' WHERE connector_id=%s ",[conn])
			dba.commit()
		elif flow=='3' and result1["flow"]=='3':
			mycursor.execute("UPDATE conn_strt_stp SET start_transaction='1' , st_status='', transaction_progress='' WHERE connector_id=%s ",[conn])
			dba.commit()
	dba.close()
		
#to add tag_ids in authorization cache
def callAuthorizationFunction(authorization_status,auth_expiry_date,tag_id,parent_id):
	db = database()
	with db.cursor() as cur:
		sql_select = cur.execute("SELECT * from authorization_cache WHERE tag_id = (%s)",(tag_id,))
		if(sql_select == 0):
			cur.execute("INSERT INTO authorization_cache (tag_id,expiry_date,status,parent_id) VALUES (%s,%s,%s,%s)",(tag_id,auth_expiry_date,authorization_status,parent_id))
			db.commit()     
		else:
			cur.execute("UPDATE authorization_cache SET expiry_date=%s,status=%s, parent_id=%s WHERE tag_id=%s",(auth_expiry_date,authorization_status,tag_id,parent_id))
			db.commit()
	db.close()

#to do offline authorization of ids
def offlineAuthorization(tag_id):
	db = database()
	with db.cursor() as cur:
		sql_select = cur.execute("SELECT * from authorization_cache WHERE status='Accepted' AND tag_id = (%s)",(tag_id,))
		if(sql_select > 0):
			response = "Accepted"
		else:
			logging.info ("TagId unavailable in Authorization cache!!!!!")
			sql_select_l = cur.execute("SELECT * from  local_authorization_list WHERE tag_id = (%s) ANd status='Accepted'",(tag_id,))
			if(sql_select_l > 0):
				logging.info ("TagId available in local_authorization_list!!!")
				response = "Accepted"
			else:
				logging.info ("TagId unavailable in  Authorization cache and local_authorization_list!!!!!")
				response = "Rejected"
	db.close()
	return response

#to send status notification for two scenarios
def status_notification(connector_id):
	sc='0,0'
	while True:
		if connector_id=="1":
			with open(file_path+'/rrt_text/screenNo.txt', 'r') as f:
				sn=f.read().split('\n')[0]
		elif connector_id=="2":
			with open(file_path+'/rrt_text/screenNo_2.txt', 'r') as f:
				sn=f.read().split('\n')[0]
		if sc!=sn and sn!='' and sn!='1,1' and sn!='2,1':
			sc=sn
			current_date_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
			if sn=='1,2' or sn=='2,2':
				dba = database()
				with dba.cursor() as mycursor:
					mycursor.execute("SELECT availability FROM conn_strt_stp WHERE connector_id=%s",[connector_id])
					result = mycursor.fetchone()
				dba.close()
				if result["availability"]=="Operative":
					stat="Available"
				elif result["availability"]=="Inoperative":
					stat="Unavailable"
				try:
					ws.send(json.dumps([2,"status_notification-"+str(connector_id),"StatusNotification",{"connectorId":connector_id,"errorCode":"NoError","status":stat,"timestamp":current_date_time}]))
				except:
					pass
			elif (sn=='1,4' or sn=='2,4'):
				try:
					ws.send(json.dumps([2,"status_notification-"+str(connector_id),"StatusNotification",{"connectorId":connector_id,"errorCode":"NoError","status":"Preparing","timestamp":current_date_time}]))
				except:
					pass
			elif (sn=='1,7' or sn=='2,7'):
				try:
					ws.send(json.dumps([2,"status_notification-"+str(connector_id),"StatusNotification",{"connectorId":connector_id,"errorCode":"NoError","status":"Charging","timestamp":current_date_time}]))
				except:
					pass
			elif sn=='1,8' or sn=='1,10' or sn=='2,8' or sn=='2,10':
				try:
					ws.send(json.dumps([2,"status_notification-"+str(connector_id),"StatusNotification",{"connectorId":connector_id,"errorCode":"NoError","status":"Finishing","timestamp":current_date_time}]))
				except:
					pass
			else:
                                logging.info("screenNo:"+sn+"!")

#to check parent_id for stop authorization
def parentid_check(tag_id,tag_id_stop):
	dba = database()
	with dba.cursor() as mycursor:
		mycursor.execute("SELECT parent_id FROM authorization_cache WHERE tag_id=%s OR tag_id=%s",[tag_id,tag_id_stop])
		result = mycursor.fetchall()
		if result[0]["parent_id"]==result[1]["parent_id"]:
			auth = "authorized"
		else:
			auth = "rejected"	
	dba.close()
	return auth

#to do whole process of a transaction
def process(connector_id):
	time.sleep(5)
	connector_id=int(connector_id)
	offline_online=0
	a=0
	meter,cur_time=0,0
	global charging_profile_stop
	while True:
		if offline_online!=1 and a!=6:
				a=0
				unknown_id=0
		time.sleep(3)
		start_transaction_id=0
		#Authorize
		while True:                  
			dba = database()
			with dba.cursor() as mycursor:
				mycursor.execute("SELECT tag_id,reservation_status FROM conn_strt_stp WHERE availability='Operative' and tag_auth='' and connector_id=%s",[connector_id])
				result = mycursor.fetchone()
				mycursor.execute("SELECT remote_start FROM conn_strt_stp WHERE availability='Operative' and connector_id=%s ",[connector_id])
				rem = mycursor.fetchone()
			dba.close()
			if result!=None:
				if result["tag_id"]!='' and result["reservation_status"]=='':
					a=0
				elif result["tag_id"]!='' and result["reservation_status"]!='':
					logging.info ("reserve started")
					dba = database()
					with dba.cursor() as mycursor:
						mycursor.execute("SELECT expiry_date,tag_id,parent_id_tag FROM reservation WHERE reservation_id=%s and connector_id=%s",[result["reservation_status"],connector_id])
						resu = mycursor.fetchone()
						respon=reservenow(resu["expiry_date"])
						logging.info ("response"+respon)
						if respon=="Accepted":
							if result["tag_id"]==resu["tag_id"] or result["tag_id"]==resu["parent_id_tag"]:
								auth="authorized"
								a=1
								tag_id=result["tag_id"]
							else:
								auth="rejected"
								a=10
							logging.info ("auth"+auth)
							mycursor.execute("UPDATE conn_strt_stp SET tag_auth=%s,flow='2' WHERE connector_id=%s",[auth,connector_id])
							dba.commit()
						else:
							mycursor.execute("UPDATE conn_strt_stp SET reservation_status='' WHERE connector_id = %s and reservation_status = %s ",[connector_id, result["reservation_status"]] )
							dba.commit()
							mycursor.execute("UPDATE reservation SET expiry_date=NULL,tag_id=NULL,parent_id_tag=NULL,reservation_id=NULL WHERE reservation_id=%s",[result["reservation_status"]])
							dba.commit()
							a=0
					dba.close()
			elif (result==None) and (rem!=None):
				if (rem["remote_start"]==1):
					a=1
					#logging.info ("remote started")
					dba = database()
					with dba.cursor() as mycursor:
						mycursor.execute("SELECT tag_id FROM conn_strt_stp WHERE connector_id=%s",[connector_id])
						resul = mycursor.fetchone()
						tag_id=resul["tag_id"]
					dba.close()	
			if a==0:#authorize
				if result!=None:
					if result["tag_id"]!='':
						tag_id=result["tag_id"]
						dba = database()
						with dba.cursor() as mycursor:
							mycursor.execute("UPDATE conn_strt_stp SET tag_auth='authorizing',flow='1' WHERE connector_id=%s",[connector_id])
						dba.commit()
						dba.close()
						Thread(target=maintainflow,args=(connector_id,"1",)).start() 
					else:
						tag_id=''  	  
				else:
					tag_id=''          
				if(tag_id == ""):
					logging.info ("Waiting for TagId!!!")
					time.sleep(6)
				elif(tag_id != ""):
					logging.info ("TagId received:"+tag_id)
					time.sleep(5)
					if check_connectivity=="1":
						ws.send(json.dumps([2,"auth-"+str(connector_id),"Authorize",{"idTag":tag_id}]))
						a=1
					elif check_connectivity=="0":
						local_auth_offline_status=config("LocalAuthorizeOffline")
						if (bool(local_auth_offline_status) == False):
							logging.info ("Authorize using Local Authorization List/Authorization cache DISABLED!!!!!")
						else:
							logging.info ("Authorize using Local Authorization List/Authorization cache ENABLED!!!!")
							offline_unknown=config("AllowOfflineTxForUnknownId")  
							if bool(offline_unknown)==True:
								local_auth = offlineAuthorization(tag_id)
								if (local_auth != "Accepted" ): 
									unknown_id=1    
								local_auth_status="Accepted"
							else:    
								local_auth_status = offlineAuthorization(tag_id)	
							if (local_auth_status == "Accepted"):
								logging.info ("Authorization Accepted!!!!")
								auth="authorized"  
								a=1
							else:
								logging.info ("Authorization Rejected!!!!")
								auth="rejected"
							dba = database()
							with dba.cursor() as mycursor:
								mycursor.execute("UPDATE conn_strt_stp SET tag_auth=%s,flow='2' WHERE connector_id=%s",[auth,connector_id])
							dba.commit()    
							dba.close()
					time.sleep(2)		    
			elif a==1:
				dba = database()
				with dba.cursor() as mycursor:
					mycursor.execute("SELECT start_transaction FROM conn_strt_stp WHERE availability='Operative' and tag_auth='authorized' and connector_id=%s",[connector_id])
					result = mycursor.fetchone()
					if result!=None:
						if result["start_transaction"]=="1":
							mycursor.execute("UPDATE conn_strt_stp SET start_transaction='', st_status='started', flow='3' WHERE connector_id=%s",[connector_id])
							dba.commit()
							a=2
							break
				dba.close()
				
		if a==2:#send start trans
			if offline_online==1:
				meter_start_data_value=meter
				current_date_time=cur_time
				meter,cur_time=0,0
			else:
				if connector_id==1:
					f= open(file_path+'/rrt_text/getstartenergy_occp.txt', 'r')
					meter_start_data_value=f.read().split('\n')[0]
					f.close()
				elif connector_id==2:
					f= open(file_path+'/rrt_text/getstartenergy_occp_2.txt', 'r')
					meter_start_data_value=f.read().split('\n')[0]
					f.close() 
				current_date_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
			dba = database()
			with dba.cursor() as mycursor:
				mycursor.execute("SELECT reservation_status FROM conn_strt_stp WHERE connector_id=%s",[connector_id])
				res = mycursor.fetchone()
				if res["reservation_status"]!='':
					reservation_id=int(res["reservation_status"])
					mycursor.execute("Update conn_strt_stp SET reservation_status='' WHERE connector_id=%s",[connector_id])
					mycursor.execute("DELETE FROM reservation WHERE reservation_id=%s",(res["reservation_status"],))
					dba.commit()
				else:
					reservation_id=0
			dba.close()
			connectivity=check_connectivity    
			if(check_connectivity == "1"):
				ws.send(json.dumps([2,"start_trans-"+str(connector_id),"StartTransaction",{"connectorId":connector_id,"idTag":tag_id,"meterStart":meter_start_data_value,"reservationId":reservation_id,"timestamp":current_date_time}]))
			elif(check_connectivity == "0"):
				with open(file_path+'/rrt_text/uniqueID.txt', 'r') as id_file:
				   a=id_file.read().split('\n')[0]
				id_file.close()
				start_transaction_id=uniqueID = int(a)+1
				with open(file_path+'/rrt_text/uniqueID.txt', 'w') as id_file:
				   id_file.write(str(uniqueID))
				id_file.close()
				current_date_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
				db1 = database()
				with db1.cursor() as cur:
					cur.execute("INSERT INTO  offline_start_transaction(unique_id,tag_id,connector_id,reservation_id,time_stamp) VALUES (%s,%s,%s,%s,%s)",(uniqueID,tag_id,connector_id,reservation_id,current_date_time))
					db1.commit()
				logging.info ("Offline start Transaction saved successfully!!!")
				db1.close()
			a=3

		time.sleep(3)
		meter_interval_value=config("MeterValueSampleInterval")
		st_end=time.time()+(float(meter_interval_value))
		i=0
		while a==3 or a==4:
			if start_transaction_id==0:
				dba = database()
				with dba.cursor() as mycursor:
					mycursor.execute("SELECT trans_id FROM conn_strt_stp WHERE connector_id=%s",[connector_id])
					result = mycursor.fetchone()
				start_transaction_id=result["trans_id"]
				dba.close()
				if (offline_online==1) and (unknown_id==1):
					Thread(target=offtoon_unknown,args=(connector_id,start_transaction_id,)).start()
					offline_online=0
					unknown_id=0
					a=6
					break
				elif (offline_online==1) and (unknown_id==0):
					offline_online=0
			if a==3:
				if rem["remote_start"]==1:
					dba = database()
					with dba.cursor() as mycursor:
						mycursor.execute("UPDATE charging_profile SET transactionId=%s WHERE connector_id=%s AND transactionId='0'",[start_transaction_id,connector_id])
						dba.commit()
					dba.close()
				#Thread(target=status_notification,args=(connector_id,)).start()
				Thread(target=charging_profile_process,args=(connector_id,start_transaction_id,)).start()
				Thread(target=mvt,args=(connector_id,i,start_transaction_id,)).start() #To start the start transaction function in the class process
				a=4
			if (connectivity == "0") and (check_connectivity== "1"):
				dbd = database()
				with dbd.cursor() as mycursor:
					mycursor.execute("SELECT tag_id, time_stamp FROM offline_start_transaction WHERE unique_id = %s",[start_transaction_id])
					result = mycursor.fetchone()
					msv = "start"
					mycursor.execute("SELECT meter_start_data_value FROM offline_meter WHERE unique_id = %s AND meter_value_status = %s ",[uniqueID,msv])
					result1 = mycursor.fetchone()
					mycursor.execute("DELETE a.*, b.* FROM offline_start_transaction a INNER JOIN offline_meter b ON a.unique_id=b.unique_id WHERE a.unique_id = %s",[uniqueID])
					dbd.commit()
					mycursor.execute("UPDATE conn_strt_stp SET start_transaction='1', st_status='', flow='2',transaction_progress='' WHERE connector_id=%s",[connector_id])
					dbd.commit()
				tag_id=result["tag_id"]
				cur_time=result["time_stamp"]
				meter=result1["meter_start_data_value"]
				charging_profile_stop[str(connector_id)]=1
				offline_online=1
				a=1
				dbd.close()
				break
			try:
				db = database()
				with db.cursor() as mycursor:
					mycursor.execute("SELECT stop_transaction FROM conn_strt_stp WHERE st_status = 'ongoing' AND connector_id=%s",[connector_id])
					myres = mycursor.fetchone()
				stop=myres["stop_transaction"]
				cost="250"
				stp=int(stop)
				db.close()
			except:
				stp=0
				pass
			if (stp<=10 ) and (stp!=0): 
				a=5
				break
			if time.time() > st_end :
				meter_interval_value=config("MeterValueSampleInterval")
				st_end=time.time()+(float(meter_interval_value))
				if i<=1:
					i=i+1
				Thread(target=mvt,args=(connector_id,i,start_transaction_id,)).start() #To start the start transaction function in the class process
			dbd = database()
			with dbd.cursor() as mycursor:
				mycursor.execute("SELECT tag_id_stop FROM conn_strt_stp WHERE connector_id=%s and tag_auth_stop=''",[connector_id])
				result = mycursor.fetchone()
				if result!=None:
					if result["tag_id_stop"]==tag_id and tag_id!='':
						logging.info ("stopped auth for same tag")
						mycursor.execute("UPDATE conn_strt_stp SET tag_auth_stop='authorized' WHERE connector_id=%s",[connector_id])
						dbd.commit()
					elif  result["tag_id_stop"]!='':
						if check_connectivity=="1":
							ws.send(json.dumps([2,"stop_auth-"+str(connector_id),"Authorize",{"idTag":result["tag_id_stop"]}]))
						elif check_connectivity=="0":
							chk=parentid_check(tag_id,result["tag_id_stop"])
							mycursor.execute("UPDATE conn_strt_stp SET tag_auth_stop=%s WHERE connector_id=%s",[chk,connector_id])
							dbd.commit()    
					time.sleep(3)
			dbd.close()

		if a==5:
			if connector_id==1:
					meter_start_rrt= open(file_path+'/rrt_text/getstartenergy_occp.txt', 'r')
					meter_start_data= (meter_start_rrt.read().split('\n')[0])
					meter_start_rrt.close()
			elif connector_id==2:
					meter_start_rrt= open(file_path+'/rrt_text/getstartenergy_occp_2.txt', 'r')
					meter_start_data= (meter_start_rrt.read().split('\n')[0])
					meter_start_rrt.close()
			current_date_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
			if(check_connectivity == "1"):
				while True:
					try:
						ws.send(json.dumps([2,"stop_trans-"+str(connector_id),"StopTransaction",{"reason":stop_dic[stop],"meterStop":meter_start_data,"timestamp":current_date_time,"transactionId":start_transaction_id}]))
						#ws.send(json.dumps([2,"status_notification-"+str(connector_id),"StatusNotification",{"connectorId":connector_id,"errorCode":"NoError","status":"Finishing","timestamp":current_date_time}]))
						break
					except:
						continue
				#ws.send(json.dumps([2,"data_transfer_01","DataTransfer",{"transactionId":start_transaction_id,"cost":cost}]))
				dba = database()
				with dba.cursor() as mycursor:
					mycursor.execute("UPDATE conn_strt_stp SET remote_start='0', tag_id_stop='', tag_auth_stop='', stop_transaction='', st_status='', flow='0',trans_id=NULL, transaction_progress='',  tag_auth='' , tag_id='' WHERE st_status='ongoing' AND connector_id=%s",[connector_id])
				dba.commit()
				dba.close()     
			elif(check_connectivity == "0"):
				dba = database()
				with dba.cursor() as mycursor:
					mycursor.execute("SELECT trans_id FROM conn_strt_stp WHERE connector_id=%s",[connector_id])
					result = mycursor.fetchone()
					if result["trans_id"]==None :
						stop_reason=stop_dic[stop]
						mycursor.execute("UPDATE offline_meter SET meter_start_data_value=%s WHERE meter_value_status='stop' AND unique_id=%s",[meter_start_data,start_transaction_id])
						dba.commit()
						mycursor.execute("INSERT INTO  offline_stop_transaction(unique_id,connector_id,stop_transaction_read,stop_reason,start_transaction_id,current_date_time,cost) VALUES (%s,%s,%s,%s,%s,%s,%s) ",(start_transaction_id,connector_id,stop,stop_reason,start_transaction_id,current_date_time,cost))
						dba.commit()
						logging.info ("Offline stop Transaction saved successfully!!!")
					else:
						mycursor.execute("UPDATE on_off SET meter_value=%s, stop_reason=%s, cur_time=%s, cost=%s WHERE trans_id=%s",(meter_start_data,stop, current_date_time, cost, start_transaction_id))
						dba.commit()
					mycursor.execute("UPDATE conn_strt_stp SET remote_start='0',tag_id_stop='', tag_auth_stop='', stop_transaction='', st_status='', flow='0',trans_id=NULL, transaction_progress='',tag_auth='',tag_id=''WHERE st_status='ongoing' AND connector_id=%s",[connector_id])
					dba.commit()
				dba.close()
				logging.info ("transaction stopped for connector_id offline:"+str(connector_id))  
			charging_profile_stop[str(connector_id)]=1
			     
#to send or save meter values of a transaction 
def mvt(connector_id,h,start_transaction_id):
	logging.info("into metervalue function")
	if connector_id==1:
		data101_76=open(file_path+'/rrt_text/data_78_1.txt', 'r')
		data101_read=(data101_76.read().split('\n')[0])
		data101_76.close()

		meter_start_rrt= open(file_path+'/rrt_text/getenergy_metervalue.txt', 'r')
		meter_start_data_value= (meter_start_rrt.read().split('\n')[0])
		meter_start_rrt.close()

		rrt= open(file_path+'/rrt_text/soc1.txt', 'r')
		soc = (rrt.read().split('\n')[0])
		rrt.close()
	elif connector_id==2:
		data101_76=open(file_path+'/rrt_text/data_78_2.txt', 'r')
		data101_read=(data101_76.read().split('\n')[0])

		meter_start_rrt= open(file_path+'/rrt_text/getenergy_metervalue_2.txt', 'r')
		meter_start_data_value= (meter_start_rrt.read().split('\n')[0])
		meter_start_rrt.close()

		rrt= open(file_path+'/rrt_text/soc2.txt', 'r')
		soc = (rrt.read().split('\n')[0])
		rrt.close()
	try:
		data101_strip=data101_read.strip('[]')
		data101_replace=data101_strip.replace("'","")
		data101=data101_replace.split(',')
		vr_l_split = (data101[17]).split("x")
		vr_l = (vr_l_split[1])
		vr_h_split = (data101[18]).split("x")
		vr_h = (vr_h_split[1])
		vr_join = (vr_h,vr_l)
		vr_int = "".join(vr_join)
		voltage_r = int(vr_int,16)
		vy_l_split = (data101[19]).split("x")
		vy_l = (vy_l_split[1])
		vy_h_split = (data101[20]).split("x")
		vy_h = (vy_h_split[1])
		vy_join = (vy_h,vy_l)
		vy_int = "".join(vy_join)
		voltage_y = int(vy_int,16)
		vb_l_split = (data101[21]).split("x")
		vb_l = (vb_l_split[1])        
		vb_h_split = (data101[22]).split("x")
		vb_h = (vb_h_split[1])
		vb_join = (vb_h,vb_l)
		vb_int = "".join(vb_join)
		voltage_b = int(vb_int,16)
		v1=int(data101[47],16)
		v2=int(data101[48],16)
		v3=int(data101[49],16)
		v4=int(data101[50],16)
		v5=int(data101[51],16)
		v6=int(data101[52],16)
		v7=int(data101[53],16)
		v8=int(data101[54],16)
		v9=int(data101[55],16)
		v10=int(data101[56],16)
		v11=int(data101[57],16)
		v12=int(data101[58],16)
		v13=int(data101[59],16)
		v14=int(data101[60],16)
		v15=int(data101[61],16)
		v16=int(data101[62],16)
		v17=int(data101[63],16)
		v1=str(chr(v1))
		v2=str(chr(v2))
		v3=str(chr(v3))
		v4=str(chr(v4))
		v5=str(chr(v5))
		v6=str(chr(v6))
		v7=str(chr(v7))
		v8=str(chr(v8))
		v9=str(chr(v9))
		v10=str(chr(v10))
		v11=str(chr(v11))
		v12=str(chr(v12))
		v13=str(chr(v13))
		v14=str(chr(v14))
		v15=str(chr(v15))
		v16=str(chr(v16))
		v17=str(chr(v17))
		vin=v1+v2+v3+v4+v6+v7+v8+v9+v10+v11+v12+v13+v14+v15+v16+v17
		if connector_id==1:
			with open(file_path+'/rrt_text/vid1.txt', 'w') as date_rrt_status:
				date_rrt_status.write('%s'% vin)
			date_rrt_status.close()
		elif connector_id==2:
			with open(file_path+'/rrt_text/vid2.txt', 'w') as date_rrt_status:
				date_rrt_status.write('%s'% vin)
			date_rrt_status.close()
		current_r=int(data101[23],16)
		current_y=int(data101[24],16)
		current_b=int(data101[25],16)
		meter_temp=int(data101[46],16)
		with open(file_path+'/rrt_text/get_temp_meter.txt', 'w') as date_rrt_status:
			date_rrt_status.write('%s'% meter_temp)
		date_rrt_status.close()
		fc_st_date=datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
	except:
		pass   
	cur_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
	if(check_connectivity == "1"):
		with open(file_path+'/rrt_text/update_time_stop.txt', 'w') as date_tt_dd:
			date_tt_dd.write('%s'% fc_st_date)
		date_tt_dd.close()
		if h==0:
			chargebox= open(file_path+'/text/cbid.txt', 'r')
			cb = (chargebox.read().split('\n')[0])
			chargebox.close()
			d=[2,"data_transfer-"+str(connector_id),"DataTransfer",{"vendorId":cb,"messageId":"VIN_"+str(start_transaction_id),"data":vin}]
			ws.send(json.dumps(d))
			logging.info (d)
		try:
			ws.send(json.dumps([2,"meter_value-"+str(connector_id),"MeterValues",{"connectorId":connector_id,"transactionId":start_transaction_id,"meterValue":[{"sampledValue":[{"value":meter_start_data_value,"context":"Sample.Periodic","format":"Raw","measurand":"Energy.Active.Import.Interval","location":"Outlet","unit":"Wh"}],"timestamp":fc_st_date},{"sampledValue":[{"value":voltage_r,"context":"Sample.Periodic","format":"Raw","measurand":"Voltage","unit":"V","phase":"L1-N"}],"timestamp":fc_st_date},{"sampledValue":[{"value":voltage_y,"context":"Sample.Periodic","format":"Raw","measurand":"Voltage","unit":"V","phase":"L2-N"}],"timestamp":fc_st_date},{"sampledValue":[{"value":voltage_b,"context":"Sample.Periodic","format":"Raw","measurand":"Voltage","unit":"V","phase":"L3-N"}],"timestamp":fc_st_date},{"sampledValue":[{"value":current_r,"context":"Sample.Periodic","format":"Raw","measurand":"Current.Import","unit":"A","phase":"L1"}],"timestamp":fc_st_date},{"sampledValue":[{"value":current_y,"context":"Sample.Periodic","format":"Raw","measurand":"Current.Import","unit":"A","phase":"L2"}],"timestamp":fc_st_date},{"sampledValue":[{"value":current_b,"context":"Sample.Periodic","format":"Raw","measurand":"Current.Import","unit":"A","phase":"L3"}],"timestamp":fc_st_date}, {"sampledValue":[{"value":soc,"context":"Sample.Periodic","format":"Raw","measurand":"SoC","unit":"%","phase":""}],"timestamp":fc_st_date},{"sampledValue":[{"value":meter_temp,"context":"Sample.Periodic","format":"Raw","measurand":"Temperature","unit":"Celsius"}],"timestamp":fc_st_date}] }]))
		except Exception as e:
			logging.info (e)
		logging.info ("meter value sent for connecor ID:"+str(connector_id))
	elif(check_connectivity == "0"):
		dba = database()
		with dba.cursor() as mycursor:
			mycursor.execute("SELECT trans_id FROM conn_strt_stp Where connector_id=%s",[connector_id])
			result = mycursor.fetchone()
			if result["trans_id"]==None: 
				mycursor.execute("UPDATE conn_strt_stp SET st_status='ongoing', flow='4', transaction_progress='1' WHERE connector_id=%s",connector_id)
				dba.commit()      
				cur_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
				if h==0:
					if connector_id==1:
						f= open(file_path+'/rrt_text/getstartenergy_occp.txt', 'r')
						meter_start_data_value=f.read().split('\n')[0]
						f.close()
					elif connector_id==2:
						f= open(file_path+'/rrt_text/getstartenergy_occp_2.txt', 'r')
						meter_start_data_value=f.read().split('\n')[0]
						f.close()
				
				db2 = database()
				with db2.cursor() as cur:
					if h==0:
						meter_val_stat = "start" 
						cur.execute("INSERT INTO  offline_meter(connector_id,unique_id,meter_value_status,meter_start_data_value,voltage_r,voltage_y,voltage_b,current_r,current_y,current_b,meter_temp,soc,time,vin) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(connector_id,start_transaction_id,meter_val_stat,meter_start_data_value,voltage_r,voltage_y,voltage_b,current_r,current_y,current_b,meter_temp,soc,cur_time,vin))
						db2.commit()
						logging.info ("Offline strt_meter_value Transaction saved successfully for CN!!!"+str(connector_id))
						h=1
					elif h==1:
						meter_val_stat = "stop" 
						cur.execute("INSERT INTO  offline_meter(connector_id,unique_id,meter_value_status,meter_start_data_value,voltage_r,voltage_y,voltage_b,current_r,current_y,current_b,meter_temp,soc,time,vin) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(connector_id,start_transaction_id,meter_val_stat,meter_start_data_value,voltage_r,voltage_y,voltage_b,current_r,current_y,current_b,meter_temp,soc,cur_time,vin))
						db2.commit()
						logging.info ("Offline stop_meter_value Transaction saved successfully for CN!!!"+str(connector_id))
						h=2
					elif h==2:
						meter_val_stat = "stop" 
						cur.execute("UPDATE offline_meter SET meter_start_data_value=%s, voltage_r=%s, voltage_y=%s, voltage_b=%s, current_r=%s,current_y=%s, current_b=%s, meter_temp=%s,soc=%s, time=%s, vin=%s WHERE unique_id = %s AND meter_value_status = %s" , (meter_start_data_value,voltage_r,voltage_y,voltage_b,current_r,current_y,current_b,meter_temp,soc,cur_time,vin,start_transaction_id,meter_val_stat))
						db2.commit()
						logging.info ("Offline meter Transaction updated successfully for CN!!!"+str(connector_id))
				db2.close()
			else:
				mycursor.execute("SELECT tag_id FROM conn_strt_stp WHERE connector_id=%s",[connector_id])
				value=mycursor.fetchone()
				mycursor.execute("UPDATE on_off SET connector_id=%s, tag_id=%s, meter_value=%s WHERE trans_id=%s",(connector_id, value["tag_id"], meter_start_data_value,start_transaction_id))
				dba.commit()
		dba.close()   

#to give charging profiles to machine 
def charging_profile_process(conn_id,trans_id):
    global charging_profile_stop
    while 1:
        if charging_profile_stop[str(conn_id)]:
            charging_profile_stop[str(conn_id)]=0
            break
        logging.debug("process initiated")
        con=database()
        with con.cursor() as cur: # selecting only the maximum stacklevel
            cur.execute("select chargingProfileId,stackLevel,chargingRateUnit,chargingSchedulePeriod from charging_profile where transactionId=%s ORDER BY stackLevel DESC LIMIT 1",(trans_id))
            res=cur.fetchone()
        # logging.debug(res)
        con.close()
        if res: # TxProfile
            logging.debug("TxProfile available for transaction_id:{0}".format(trans_id))
            logging.debug("Maximun stack:{0}".format(res))
            a=datetime.datetime.now()
            cur_time_in_sec=(a.hour*60*60)+(a.minute*60)+a.second
            logging.debug("current time in seconds:{0}".format(cur_time_in_sec))
            chargingSchedulePeriod=literal_eval(res['chargingSchedulePeriod'])
            logging.debug(chargingSchedulePeriod)
            arr=[]
            for i in chargingSchedulePeriod:
                arr.append(i['startPeriod'])
            arr.append(86400)
            logging.debug(arr)
            for i in range(len(arr)):
                if cur_time_in_sec in range(arr[i-1],arr[i]):
                    logging.debug("Period-"+str(i))
                    logging.debug("Unit:{0}".format(res['chargingRateUnit']))
                    logging.debug("Limit:{0}".format(chargingSchedulePeriod[i-1]['limit']))
                    if res['chargingRateUnit']=='A':
                        b_data='{0:08b}'.format(chargingSchedulePeriod[i-1]["limit"])
                        logging.debug(b_data)
                        if len(b_data)==8:
                            try:
                                if conn_id==1:
                                    serial_rrt.write(chr(36).encode())
                                    serial_rrt.write(chr(103).encode())
                                    serial_rrt.write(chr(conn_id).encode())
                                    serial_rrt.write(chr(87).encode())
                                    serial_rrt.write(chr(0).encode())
                                    serial_rrt.write(chr(0).encode())
                                    serial_rrt.write(chr(0).encode())
                                    serial_rrt.write(chr(65).encode())
                                    serial_rrt.write(bytes([int(b_data,2)]))
                                    serial_rrt.write(chr(35).encode())
                                else:
                                    serial_rrt1.write(chr(36).encode())
                                    serial_rrt1.write(chr(103).encode())
                                    serial_rrt1.write(chr(conn_id).encode())
                                    serial_rrt1.write(chr(87).encode())
                                    serial_rrt1.write(chr(0).encode())
                                    serial_rrt1.write(chr(0).encode())
                                    serial_rrt.write(chr(0).encode())
                                    serial_rrt1.write(chr(65).encode())
                                    serial_rrt1.write(bytes([int(b_data,2)]))
                                    serial_rrt1.write(chr(35).encode())
                                logging.debug('serial write sucessful: {0}'.format(int(b_data[0:8],2)))
                            except Exception as e:
                                logging.debug(str(e))
                                logging.debug('Value {0}'.format(int(b_data[0:8],2)))
                        else:
                            logging.debug("Value exceeded 255")
                    else:
                        b_data='{0:024b}'.format(chargingSchedulePeriod[i-1]["limit"])
                        logging.debug(b_data)
                        if len(b_data)==24:
                            try:
                                if conn_id==1:
                                    serial_rrt.write(chr(36).encode())
                                    serial_rrt.write(chr(103).encode())
                                    serial_rrt.write(chr(conn_id).encode())
                                    serial_rrt.write(chr(87).encode())
                                    serial_rrt.write(bytes([int(b_data[0:8],2)]))
                                    serial_rrt.write(bytes([int(b_data[8:16],2)]))
                                    serial_rrt.write(bytes([int(b_data[16:24],2)]))
                                    serial_rrt.write(chr(65).encode())
                                    serial_rrt.write(chr(0).encode())
                                    serial_rrt.write(chr(35).encode())
                                else:
                                    serial_rrt1.write(chr(36).encode())
                                    serial_rrt1.write(chr(103).encode())
                                    serial_rrt1.write(chr(conn_id).encode())
                                    serial_rrt1.write(chr(87).encode())
                                    serial_rrt1.write(bytes([int(b_data[0:8],2)]))
                                    serial_rrt1.write(bytes([int(b_data[8:16],2)]))
                                    serial_rrt1.write(bytes([int(b_data[16:24],2)]))
                                    serial_rrt1.write(chr(65).encode())
                                    serial_rrt1.write(chr(0).encode())
                                    serial_rrt1.write(chr(35).encode())
                                logging.debug('serial write sucessful: {0} {1} {2}'.format(int(b_data[0:8],2),int(b_data[8:16],2),int(b_data[16:24],2)))
                            except Exception as e:
                                logging.debug(str(e))
                                logging.debug('Value {0} {1} {2}'.format(int(b_data[0:8],2),int(b_data[8:16],2),int(b_data[16:24],2)))
                        else:
                            logging.debug("Value exceeded 16777216")
                    break
        else: # TxDefaultProfile
            logging.debug("No TxProfile available for transaction_id:{0}".format(trans_id))
            con=database()
            with con.cursor() as cur: # selecting all stacklevel
                cur.execute("select chargingProfileId,stackLevel,charging_profile_kind,recurrency_kind,validFrom,validTo,chargingRateUnit,chargingSchedulePeriod from charging_profile where charging_profile_type='TxDefaultProfile' and connector_id=%s ORDER BY stackLevel DESC",(conn_id))
                res1=cur.fetchall()
            con.close()
            if res1:
                logging.debug("TxDefaultProfile available for connector_id:{0}".format(conn_id))
                for i in res1: # check validity from maximum to minimum stacklevel
                    if not i['validFrom'] and not i['validTo']: # validFrom and validTo are NULL
                        logging.debug('No validFrom and validTo')
                        logging.debug('Stack {0} Profile valid'.format(i["stackLevel"]))
                        res=i
                        break
                    elif i['validFrom'] and i['validTo']: # validFrom and validTo are available
                        validFrom=datetime.datetime.strptime(i['validFrom'],'%Y-%m-%dT%H:%M:%SZ')
                        validTo=datetime.datetime.strptime(i['validTo'],'%Y-%m-%dT%H:%M:%SZ')
                        cur_datetime=datetime.datetime.now()
                        if cur_datetime >= validFrom and cur_datetime <= validTo:
                            logging.debug('Stack {0} Profile valid'.format(i["stackLevel"]))
                            res=i
                            break
                        else:
                            logging.debug('Stack {0} Profile Invalid. Checking the next stack'.format(i["stackLevel"]))
                            res=None
                    elif not i['validFrom']: # validFrom is NULL
                        logging.debug('No validFrom')
                        validTo=datetime.datetime.strptime(i['validTo'],'%Y-%m-%dT%H:%M:%SZ')
                        cur_datetime=datetime.datetime.now()
                        if cur_datetime <= validTo:
                            logging.debug('Stack {0} Profile valid'.format(i["stackLevel"]))
                            res=i
                            break
                        else:
                            logging.debug('Stack {0} Profile Invalid. Checking the next stack'.format(i["stackLevel"]))
                            res=None
                    elif not i['validTo']: # validTo is NULL
                        logging.debug('No validTo')
                        validFrom=datetime.datetime.strptime(i['validFrom'],'%Y-%m-%dT%H:%M:%SZ')
                        cur_datetime=datetime.datetime.now()
                        if cur_datetime >= validFrom:
                            logging.debug('Stack {0} Profile valid'.format(i["stackLevel"]))
                            res=i
                            break
                        else:
                            logging.debug('Stack {0} Profile Invalid. Checking the next stack'.format(i["stackLevel"]))
                            res=None
            if res: # TxDefaultProfile for given connector id
                logging.debug("Maximun stack:{0}".format(res))
                if res['charging_profile_kind'] == 'Recurring': # Recurring
                    logging.debug('cp_kind: Recurring')
                    if res['recurrency_kind'] or not res['recurrency_kind']: # Daily or Weekly or NULL
                        logging.debug("recurrency_kind: {0}".format(res['recurrency_kind']))
                        a=datetime.datetime.now()
                        cur_time_in_sec=(a.hour*60*60)+(a.minute*60)+a.second
                        logging.debug("current time in seconds:{0}".format(cur_time_in_sec))
                        chargingSchedulePeriod=literal_eval(res['chargingSchedulePeriod'])
                        logging.debug(chargingSchedulePeriod)
                        arr=[]
                        for i in chargingSchedulePeriod:
                            arr.append(i['startPeriod'])
                        arr.append(86400)
                        logging.debug(arr)
                        for i in range(len(arr)):
                            if cur_time_in_sec in range(arr[i-1],arr[i]):
                                logging.debug("Period-"+str(i))
                                logging.debug("Unit:{0}".format(res['chargingRateUnit']))
                                logging.debug("Limit:{0}".format(chargingSchedulePeriod[i-1]['limit']))
                                if res['chargingRateUnit']=='A':
                                    b_data='{0:08b}'.format(chargingSchedulePeriod[i-1]["limit"])
                                    logging.debug(b_data)
                                    if len(b_data)==8:
                                        try:
                                            if conn_id==1:
                                                serial_rrt.write(chr(36).encode())
                                                serial_rrt.write(chr(103).encode())
                                                serial_rrt.write(chr(conn_id).encode())
                                                serial_rrt.write(chr(87).encode())
                                                serial_rrt.write(chr(0).encode())
                                                serial_rrt.write(chr(0).encode())
                                                serial_rrt.write(chr(0).encode())
                                                serial_rrt.write(chr(65).encode())
                                                serial_rrt.write(bytes([int(b_data,2)]))
                                                serial_rrt.write(chr(35).encode())
                                            else:
                                                serial_rrt1.write(chr(36).encode())
                                                serial_rrt1.write(chr(103).encode())
                                                serial_rrt1.write(chr(conn_id).encode())
                                                serial_rrt1.write(chr(87).encode())
                                                serial_rrt1.write(chr(0).encode())
                                                serial_rrt1.write(chr(0).encode())
                                                serial_rrt.write(chr(0).encode())
                                                serial_rrt1.write(chr(65).encode())
                                                serial_rrt1.write(bytes([int(b_data,2)]))
                                                serial_rrt1.write(chr(35).encode())
                                            logging.debug('serial write sucessful: {0}'.format(int(b_data[0:8],2)))
                                        except Exception as e:
                                            logging.debug(str(e))
                                            logging.debug('Value {0}'.format(int(b_data[0:8],2)))
                                    else:
                                        logging.debug("Value exceeded 255")
                                else:
                                    b_data='{0:024b}'.format(chargingSchedulePeriod[i-1]["limit"])
                                    logging.debug(b_data)
                                    if len(b_data)==24:
                                        try:
                                            if conn_id==1:
                                                serial_rrt.write(chr(36).encode())
                                                serial_rrt.write(chr(103).encode())
                                                serial_rrt.write(chr(conn_id).encode())
                                                serial_rrt.write(chr(87).encode())
                                                serial_rrt.write(bytes([int(b_data[0:8],2)]))
                                                serial_rrt.write(bytes([int(b_data[8:16],2)]))
                                                serial_rrt.write(bytes([int(b_data[16:24],2)]))
                                                serial_rrt.write(chr(65).encode())
                                                serial_rrt.write(chr(0).encode())
                                                serial_rrt.write(chr(35).encode())
                                            else:
                                                serial_rrt1.write(chr(36).encode())
                                                serial_rrt1.write(chr(103).encode())
                                                serial_rrt1.write(chr(conn_id).encode())
                                                serial_rrt1.write(chr(87).encode())
                                                serial_rrt1.write(bytes([int(b_data[0:8],2)]))
                                                serial_rrt1.write(bytes([int(b_data[8:16],2)]))
                                                serial_rrt1.write(bytes([int(b_data[16:24],2)]))
                                                serial_rrt1.write(chr(65).encode())
                                                serial_rrt1.write(chr(0).encode())
                                                serial_rrt1.write(chr(35).encode())
                                            logging.debug('serial write sucessful: {0} {1} {2}'.format(int(b_data[0:8],2),int(b_data[8:16],2),int(b_data[16:24],2)))
                                        except Exception as e:
                                            logging.debug(str(e))
                                            logging.debug('Value {0} {1} {2}'.format(int(b_data[0:8],2),int(b_data[8:16],2),int(b_data[16:24],2)))
                                    else:
                                        logging.debug("Value exceeded 16777216")
                                break
            else: # TxDefaultProfile connector id 0
                con=database()
                with con.cursor() as cur: # selecting all stacklevel
                    cur.execute("select chargingProfileId,stackLevel,charging_profile_kind,recurrency_kind,validFrom,validTo,chargingRateUnit,chargingSchedulePeriod from charging_profile where charging_profile_type='TxDefaultProfile' and connector_id=0")
                    res1=cur.fetchall()
                # logging.debug(res)
                con.close()
                if res1:
                    logging.debug("TxDefaultProfile available for connector_id: 0")
                    for i in res1: # check validity from maximum to minimum stacklevel
                        if not i['validFrom'] and not i['validTo']: # validFrom and validTo are NULL
                            logging.debug('No validFrom and validTo')
                            logging.debug('Stack {0} Profile valid'.format(i["stackLevel"]))
                            res=i
                            break
                        elif i['validFrom'] and i['validTo']: # validFrom and validTo are available
                            validFrom=datetime.datetime.strptime(i['validFrom'],'%Y-%m-%dT%H:%M:%SZ')
                            validTo=datetime.datetime.strptime(i['validTo'],'%Y-%m-%dT%H:%M:%SZ')
                            cur_datetime=datetime.datetime.now()
                            if cur_datetime >= validFrom and cur_datetime <= validTo:
                                logging.debug('Stack {0} Profile valid'.format(i["stackLevel"]))
                                res=i
                                break
                            else:
                                logging.debug('Stack {0} Profile Invalid. Checking the next stack'.format(i["stackLevel"]))
                                res=None
                        elif not i['validFrom']: # validFrom is NULL
                            logging.debug('No validFrom')
                            validTo=datetime.datetime.strptime(i['validTo'],'%Y-%m-%dT%H:%M:%SZ')
                            cur_datetime=datetime.datetime.now()
                            if cur_datetime <= validTo:
                                logging.debug('Stack {0} Profile valid'.format(i["stackLevel"]))
                                res=i
                                break
                            else:
                                logging.debug('Stack {0} Profile Invalid. Checking the next stack'.format(i["stackLevel"]))
                                res=None
                        elif not i['validTo']: # validTo is NULL
                            logging.debug('No validTo')
                            validFrom=datetime.datetime.strptime(i['validFrom'],'%Y-%m-%dT%H:%M:%SZ')
                            cur_datetime=datetime.datetime.now()
                            if cur_datetime >= validFrom:
                                logging.debug('Stack {0} Profile valid'.format(i["stackLevel"]))
                                res=i
                                break
                            else:
                                logging.debug('Stack {0} Profile Invalid. Checking the next stack'.format(i["stackLevel"]))
                                res=None
                if res:
                    logging.debug("Maximun stack:{0}".format(res))
                    if res['charging_profile_kind'] == 'Recurring': # Recurring
                        logging.debug('cp_kind: Recurring')
                        if res['recurrency_kind'] or not res['recurrency_kind']: # Daily or Weekly or NULL
                            logging.debug("recurrency_kind: {0}".format(res['recurrency_kind']))
                            a=datetime.datetime.now()
                            cur_time_in_sec=(a.hour*60*60)+(a.minute*60)+a.second
                            logging.debug("current time in seconds:{0}".format(cur_time_in_sec))
                            chargingSchedulePeriod=literal_eval(res['chargingSchedulePeriod'])
                            logging.debug(chargingSchedulePeriod)
                            arr=[]
                            for i in chargingSchedulePeriod:
                                arr.append(i['startPeriod'])
                            arr.append(86400)
                            logging.debug(arr)
                            for i in range(len(arr)):
                                if cur_time_in_sec in range(arr[i-1],arr[i]):
                                    logging.debug("Period-"+str(i))
                                    logging.debug("Unit:{0}".format(res['chargingRateUnit']))
                                    logging.debug("Limit:{0}".format(chargingSchedulePeriod[i-1]['limit']))
                                    if res['chargingRateUnit']=='A':
                                        b_data='{0:08b}'.format(chargingSchedulePeriod[i-1]["limit"])
                                        logging.debug(b_data)
                                        if len(b_data)==8:
                                            try:
                                                if conn_id==1:
                                                    serial_rrt.write(chr(36).encode())
                                                    serial_rrt.write(chr(103).encode())
                                                    serial_rrt.write(chr(conn_id).encode())
                                                    serial_rrt.write(chr(87).encode())
                                                    serial_rrt.write(chr(0).encode())
                                                    serial_rrt.write(chr(0).encode())
                                                    serial_rrt.write(chr(0).encode())
                                                    serial_rrt.write(chr(65).encode())
                                                    serial_rrt.write(bytes([int(b_data,2)]))
                                                    serial_rrt.write(chr(35).encode())
                                                else:
                                                    serial_rrt1.write(chr(36).encode())
                                                    serial_rrt1.write(chr(103).encode())
                                                    serial_rrt1.write(chr(conn_id).encode())
                                                    serial_rrt1.write(chr(87).encode())
                                                    serial_rrt1.write(chr(0).encode())
                                                    serial_rrt1.write(chr(0).encode())
                                                    serial_rrt.write(chr(0).encode())
                                                    serial_rrt1.write(chr(65).encode())
                                                    serial_rrt1.write(bytes([int(b_data,2)]))
                                                    serial_rrt1.write(chr(35).encode())
                                                logging.debug('serial write sucessful: {0}'.format(int(b_data[0:8],2)))
                                            except Exception as e:
                                                logging.debug(str(e))
                                                logging.debug('Value {0}'.format(int(b_data[0:8],2)))
                                        else:
                                            logging.debug("Value exceeded 255")
                                    else:
                                        b_data='{0:024b}'.format(chargingSchedulePeriod[i-1]["limit"])
                                        logging.debug(b_data)
                                        if len(b_data)==24:
                                            try:
                                                if conn_id==1:
                                                    serial_rrt.write(chr(36).encode())
                                                    serial_rrt.write(chr(103).encode())
                                                    serial_rrt.write(chr(conn_id).encode())
                                                    serial_rrt.write(chr(87).encode())
                                                    serial_rrt.write(bytes([int(b_data[0:8],2)]))
                                                    serial_rrt.write(bytes([int(b_data[8:16],2)]))
                                                    serial_rrt.write(bytes([int(b_data[16:24],2)]))
                                                    serial_rrt.write(chr(65).encode())
                                                    serial_rrt.write(chr(0).encode())
                                                    serial_rrt.write(chr(35).encode())
                                                else:
                                                    serial_rrt1.write(chr(36).encode())
                                                    serial_rrt1.write(chr(103).encode())
                                                    serial_rrt1.write(chr(conn_id).encode())
                                                    serial_rrt1.write(chr(87).encode())
                                                    serial_rrt1.write(bytes([int(b_data[0:8],2)]))
                                                    serial_rrt1.write(bytes([int(b_data[8:16],2)]))
                                                    serial_rrt1.write(bytes([int(b_data[16:24],2)]))
                                                    serial_rrt1.write(chr(65).encode())
                                                    serial_rrt1.write(chr(0).encode())
                                                    serial_rrt1.write(chr(35).encode())
                                                logging.debug('serial write sucessful: {0} {1} {2}'.format(int(b_data[0:8],2),int(b_data[8:16],2),int(b_data[16:24],2)))
                                            except Exception as e:
                                                logging.debug(str(e))
                                                logging.debug('Value {0} {1} {2}'.format(int(b_data[0:8],2),int(b_data[8:16],2),int(b_data[16:24],2)))
                                        else:
                                            logging.debug("Value exceeded 16777216")
                                    break
                else: # ChargePointMaxProfile
                    logging.debug("No TxDefaultProfile available")
                    con=database()
                    with con.cursor() as cur: # selecting all stacklevel
                        cur.execute("select stackLevel,validFrom,validTo,chargingRateUnit,chargingSchedulePeriod from charging_profile where charging_profile_type='ChargePointMaxProfile'")
                        res1=cur.fetchall()
                    # logging.debug(res)
                    con.close()
                    if res1:
                        logging.debug("ChargePointMaxProfile available")
                        for i in res1: # check validity from maximum to minimum stacklevel
                            if not i['validFrom'] and not i['validTo']: # validFrom and validTo are NULL
                                logging.debug('No validFrom and validTo')
                                logging.debug('Stack {0} Profile valid'.format(i["stackLevel"]))
                                res=i
                                break
                            elif i['validFrom'] and i['validTo']: # validFrom and validTo are available
                                validFrom=datetime.datetime.strptime(i['validFrom'],'%Y-%m-%dT%H:%M:%SZ')
                                validTo=datetime.datetime.strptime(i['validTo'],'%Y-%m-%dT%H:%M:%SZ')
                                cur_datetime=datetime.datetime.now()
                                if cur_datetime >= validFrom and cur_datetime <= validTo:
                                    logging.debug('Stack {0} Profile valid'.format(i["stackLevel"]))
                                    res=i
                                    break
                                else:
                                    logging.debug('Stack {0} Profile Invalid. Checking the next stack'.format(i["stackLevel"]))
                                    res=None
                            elif not i['validFrom']: # validFrom is NULL
                                logging.debug('No validFrom')
                                validTo=datetime.datetime.strptime(i['validTo'],'%Y-%m-%dT%H:%M:%SZ')
                                cur_datetime=datetime.datetime.now()
                                if cur_datetime <= validTo:
                                    logging.debug('Stack {0} Profile valid'.format(i["stackLevel"]))
                                    res=i
                                    break
                                else:
                                    logging.debug('Stack {0} Profile Invalid. Checking the next stack'.format(i["stackLevel"]))
                                    res=None
                            elif not i['validTo']: # validTo is NULL
                                logging.debug('No validTo')
                                validFrom=datetime.datetime.strptime(i['validFrom'],'%Y-%m-%dT%H:%M:%SZ')
                                cur_datetime=datetime.datetime.now()
                                if cur_datetime >= validFrom:
                                    logging.debug('Stack {0} Profile valid'.format(i["stackLevel"]))
                                    res=i
                                    break
                                else:
                                    logging.debug('Stack {0} Profile Invalid. Checking the next stack'.format(i["stackLevel"]))
                                    res=None

                    if res:
                        logging.debug("Maximun stack:{0}".format(res))
                        a=datetime.datetime.now()
                        cur_time_in_sec=(a.hour*60*60)+(a.minute*60)+a.second
                        logging.debug("current time in seconds:{0}".format(cur_time_in_sec))
                        chargingSchedulePeriod=literal_eval(res['chargingSchedulePeriod'])
                        logging.debug(chargingSchedulePeriod)
                        arr=[]
                        for i in chargingSchedulePeriod:
                            arr.append(i['startPeriod'])
                        arr.append(86400)
                        logging.debug(arr)
                        for i in range(len(arr)):
                            if cur_time_in_sec in range(arr[i-1],arr[i]):
                                logging.debug("Period-"+str(i))
                                logging.debug("Unit:{0}".format(res['chargingRateUnit']))
                                logging.debug("Limit:{0}".format(chargingSchedulePeriod[i-1]['limit']))
                                if res['chargingRateUnit']=='A':
                                    b_data='{0:08b}'.format(chargingSchedulePeriod[i-1]["limit"])
                                    logging.debug(b_data)
                                    if len(b_data)==8:
                                        try:
                                            if conn_id==1:
                                                serial_rrt.write(chr(36).encode())
                                                serial_rrt.write(chr(103).encode())
                                                serial_rrt.write(chr(conn_id).encode())
                                                serial_rrt.write(chr(87).encode())
                                                serial_rrt.write(chr(0).encode())
                                                serial_rrt.write(chr(0).encode())
                                                serial_rrt.write(chr(0).encode())
                                                serial_rrt.write(chr(65).encode())
                                                serial_rrt.write(bytes([int(b_data,2)]))
                                                serial_rrt.write(chr(35).encode())
                                            else:
                                                serial_rrt1.write(chr(36).encode())
                                                serial_rrt1.write(chr(103).encode())
                                                serial_rrt1.write(chr(conn_id).encode())
                                                serial_rrt1.write(chr(87).encode())
                                                serial_rrt1.write(chr(0).encode())
                                                serial_rrt1.write(chr(0).encode())
                                                serial_rrt.write(chr(0).encode())
                                                serial_rrt1.write(chr(65).encode())
                                                serial_rrt1.write(bytes([int(b_data,2)]))
                                                serial_rrt1.write(chr(35).encode())
                                            logging.debug('serial write sucessful: {0}'.format(int(b_data[0:8],2)))
                                        except Exception as e:
                                            logging.debug(str(e))
                                            logging.debug('Value {0}'.format(int(b_data[0:8],2)))
                                    else:
                                        logging.debug("Value exceeded 255")
                                else:
                                    b_data='{0:024b}'.format(chargingSchedulePeriod[i-1]["limit"])
                                    logging.debug(b_data)
                                    if len(b_data)==24:
                                        try:
                                            if conn_id==1:
                                                serial_rrt.write(chr(36).encode())
                                                serial_rrt.write(chr(103).encode())
                                                serial_rrt.write(chr(conn_id).encode())
                                                serial_rrt.write(chr(87).encode())
                                                serial_rrt.write(bytes([int(b_data[0:8],2)]))
                                                serial_rrt.write(bytes([int(b_data[8:16],2)]))
                                                serial_rrt.write(bytes([int(b_data[16:24],2)]))
                                                serial_rrt.write(chr(65).encode())
                                                serial_rrt.write(chr(0).encode())
                                                serial_rrt.write(chr(35).encode())
                                            else:
                                                serial_rrt1.write(chr(36).encode())
                                                serial_rrt1.write(chr(103).encode())
                                                serial_rrt1.write(chr(conn_id).encode())
                                                serial_rrt1.write(chr(87).encode())
                                                serial_rrt1.write(bytes([int(b_data[0:8],2)]))
                                                serial_rrt1.write(bytes([int(b_data[8:16],2)]))
                                                serial_rrt1.write(bytes([int(b_data[16:24],2)]))
                                                serial_rrt1.write(chr(65).encode())
                                                serial_rrt1.write(chr(0).encode())
                                                serial_rrt1.write(chr(35).encode())
                                            logging.debug('serial write sucessful: {0} {1} {2}'.format(int(b_data[0:8],2),int(b_data[8:16],2),int(b_data[16:24],2)))
                                        except Exception as e:
                                            logging.debug(str(e))
                                            logging.debug('Value {0} {1} {2}'.format(int(b_data[0:8],2),int(b_data[8:16],2),int(b_data[16:24],2)))
                                    else:
                                        logging.debug("Value exceeded 16777216")
                                break
                    else:
                        logging.debug("No ChargePointMaxProfile available")
        time.sleep(5)

#to send offline tranaction data to server when online 
def offline_data():
	time.sleep(10)
	b=0
	global transaction_id
	while True:
		if b==0 :
			dbd = database()
			with dbd.cursor() as mycursor:
				mycursor.execute("SELECT tag_id,connector_id,unique_id,time_stamp FROM offline_start_transaction LIMIT 1")
				result = mycursor.fetchone()
				if result:
					msv = "start"
					logging.info (result["unique_id"])
					mycursor.execute("SELECT meter_start_data_value FROM offline_meter WHERE unique_id = %s AND meter_value_status = %s ",(result["unique_id"],msv))
					result1 = mycursor.fetchone()	
					logging.info (result1)
					while True:
							try:
								ws.send(json.dumps([2,"offline_start","StartTransaction",{"connectorId":result["connector_id"],"idTag":result["tag_id"],"meterStart":result1["meter_start_data_value"],"reservationId":"1","timestamp":result["time_stamp"]}]))
								b=1
								break
							except Exception as e:
								logging.info (e)
								continue
				else:
					break
			dbd.close()
			time.sleep(3)
		if b==1 and transaction_id!=0:
			try:
				dba = database()
				with dba.cursor() as cur:
					m_v="stop"
					cur.execute("SELECT meter_start_data_value, voltage_r, voltage_y, voltage_b, current_r, current_y, current_b, meter_temp ,time, vin,soc FROM offline_meter WHERE unique_id = %s AND meter_value_status=%s ",(result["unique_id"],m_v))
					vresult =cur.fetchone()
					if vresult==None:	
						m_v="start"
						cur.execute("SELECT meter_start_data_value, voltage_r, voltage_y, voltage_b, current_r, current_y, current_b, meter_temp ,time, vin,soc FROM offline_meter WHERE unique_id = %s AND meter_value_status=%s ",(result["unique_id"],m_v))
						vresult =cur.fetchone()
					# vin=vresult["vin"]     	
				dba.close() 
				ws.send(json.dumps([2,"offline_meter","MeterValues",{"connectorId":result["connector_id"],"transactionId":transaction_id,"meterValue":[{"sampledValue":[{"value":vresult["meter_start_data_value"],"context":"Sample.Periodic","format":"Raw","measurand":"Energy.Active.Import.Interval","location":"Outlet","unit":"Wh"}],"timestamp":vresult["time"]},{"sampledValue":[{"value":vresult["voltage_r"],"context":"Sample.Periodic","format":"Raw","measurand":"Voltage","unit":"V","phase":"L1-N"}],"timestamp":vresult["time"]},{"sampledValue":[{"value":vresult["voltage_y"],"context":"Sample.Periodic","format":"Raw","measurand":"Voltage","unit":"V","phase":"L2-N"}],"timestamp":vresult["time"]},{"sampledValue":[{"value":vresult["voltage_b"],"context":"Sample.Periodic","format":"Raw","measurand":"Voltage","unit":"V","phase":"L3-N"}],"timestamp":vresult["time"]},{"sampledValue":[{"value":vresult["current_r"],"context":"Sample.Periodic","format":"Raw","measurand":"Current.Import","unit":"A","phase":"L1"}],"timestamp":vresult["time"]},{"sampledValue":[{"value":vresult["current_y"],"context":"Sample.Periodic","format":"Raw","measurand":"Current.Import","unit":"A","phase":"L2"}],"timestamp":vresult["time"]},{"sampledValue":[{"value":vresult["current_b"],"context":"Sample.Periodic","format":"Raw","measurand":"Current.Import","unit":"A","phase":"L3"}],"timestamp":vresult["time"]}, {"sampledValue":[{"value":vresult["soc"],"context":"Sample.Periodic","format":"Raw","measurand":"SoC","unit":"%","phase":""}],"timestamp":vresult["time"]},{"sampledValue":[{"value":vresult["meter_temp"],"context":"Sample.Periodic","format":"Raw","measurand":"Temperature","unit":"Celsius"}],"timestamp":vresult["time"]}] }]))
			except Exception as e:
				logging.info ("meter"+str(e))
				pass
			try:
				db = database()
				with db.cursor() as mycursor:
					mycursor.execute("SELECT stop_transaction_read, current_date_time, cost FROM offline_stop_transaction WHERE unique_id = %s",[result["unique_id"]])
					myresult = mycursor.fetchone()
					# cost=myresult["cost"]
					mycursor.execute("DELETE FROM offline_start_transaction WHERE unique_id =%s",[result["unique_id"]])
					mycursor.execute("DELETE FROM offline_meter WHERE unique_id =%s",[result["unique_id"]])
					mycursor.execute("DELETE FROM offline_stop_transaction WHERE unique_id=%s",[result["unique_id"]])
					db.commit()
				db.close()
				ws.send(json.dumps([2,"offline_stop","StopTransaction",{"reason":stop_dic[myresult["stop_transaction_read"]],"meterStop":vresult["meter_start_data_value"],"timestamp":myresult["current_date_time"],"transactionId":transaction_id}]))					
			except Exception as e:
				logging.info ("meter"+str(e))
				pass
			transaction_id=0
			b=0 

#to stop transaction for unknown id when it gets from offline to online
def offtoon_unknown(connector,start_transaction_id):
	while True:
		max_energy=config("MaxEnergyOnInvalidId")
		if connector==1:
			mete= open(file_path+'/rrt_text/getenergy_occp.txt', 'r')
			meter_energy= (mete.read().split('\n')[0])
			mete.close()
		elif connector==2:
			mete= open(file_path+'/rrt_text/getenergy_occp_2.txt', 'r')
			meter_energy= (mete.read().split('\n')[0])
			mete.close()
		try:
			if int(meter_energy)>=int(max_energy):
				logging.info ("max energy exceeded for"+str(connector))
				stop_unknown=config("StopTransactionOnInvalidId")
				if (bool(stop_unknown)==True):
					if connector==1:
					     f= open(file_path+'/rrt_text/getstartenergy_occp.txt', 'r')
					     meter_start_data_value=f.read().split('\n')[0]
					     f.close()
					elif connector==2:
					     f= open(file_path+'/rrt_text/getstartenergy_occp_2.txt', 'r')
					     meter_start_data_value=f.read().split('\n')[0]
					     f.close()
					cur_time=datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
					ws.send(json.dumps([2,"unknown_stop-"+str(connector),"StopTransaction",{"reason":"DeAuthorized","meterStop":meter_start_data_value,"timestamp":cur_time,"transactionId":start_transaction_id}]))
				if connector==1:
					logging.info ("connection stopped for 1")
					s_byte=36
					f_id=103
					scc_id=7
					dum_id=0
					st_bit=1
					end_byte=35
					try:
						serial_rrt.write(chr(s_byte).encode())
						serial_rrt.write(chr(f_id).encode())
						serial_rrt.write(chr(connector).encode())
						serial_rrt.write(chr(scc_id).encode())
						serial_rrt.write(chr(dum_id).encode())
						serial_rrt.write(chr(st_bit).encode())
						serial_rrt.write(chr(end_byte).encode())
					except:
						pass
					break
				elif connector==2:
					logging.info ("connection stopped for 2")
					s_byte=36
					f_id=103
					scc_id=7
					dum_id=0
					st_bit=1
					end_byte=35
					try:
						serial_rrt1.write(chr(s_byte).encode())
						serial_rrt1.write(chr(f_id).encode())
						serial_rrt1.write(chr(connector).encode())
						serial_rrt1.write(chr(scc_id).encode())
						serial_rrt1.write(chr(dum_id).encode())
						serial_rrt1.write(chr(st_bit).encode())
						serial_rrt1.write(chr(end_byte).encode())
					except:
						pass
					break
		except:
			continue
                
#to check whether a reservation is expired or not
def reservenow(expiry_date):
	d1 = datetime.datetime.utcnow()
	d2 = datetime.datetime.strptime(expiry_date,"%Y-%m-%dT%H:%M:%SZ")
	if d1.date()==d2.date():
		if d1.time()>=d2.time():
			logging.info ("current time is bigger than exp time")
			response = "rejected"
		else:
			response = "Accepted"
	elif d1.date()>d2.date():
		response = "rejected"
		logging.info ("current date is bigger than exp date")
	return response

#to initiate download firmware on time
def updateFirmwareThread(dt,loc,rs,rt):
	while True:
		#logging.info("Update firmware thread started")
		bt=datetime.datetime.utcnow()
		if (bt>=dt):
			Thread(target=downloadFirmwareThread,args=(dt,loc,rs,rt,)).start()
			break

#download firmware from ftp
def downloadFirmwareThread(dt,loc,rs,rt):
	retry=0
	while True:	
		#logging.info("Download firmware thread started")
		with open(file_path+'/rrt_text/check_current_thread.txt', 'w') as write_current_running_thread:
		    write_current_running_thread.write("download_firm")
		write_current_running_thread.close() 
		current_date_time=datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
		try:
			file_down=urlopen(loc)
			file_content=file_down.read()
			ws.send(json.dumps([2,"firmware_status","FirmwareStatusNotification",{"status":"Downloaded","timestamp":current_date_time}]))
			file_zip = open(firm_path+"/updateFirmwareDownloaded/rrt_file.zip","w+b")  
			file_zip.write(file_content) 
			file_zip.close()
			dual_flag = "1"
			fil = open(file_path+"/rrt_text/zip_update_file.txt","w+")  
			fil.write(dual_flag) 
			fil.close()
			ws.send(json.dumps([2,"firmware_status","FirmwareStatusNotification",{"status":"Installed","timestamp":current_date_time}]))
			det="Installed"
			dbd = database()
			with dbd.cursor() as mycursor:
				mycursor.execute("UPDATE imp_files SET firmware_detail=%s",[det])
			dbd.commit()
			dbd.close()
			break
		except Exception as e: 
			det="DownloadFailed" 
			logging.info (det,e)
			ws.send(json.dumps([2,"firmware_status","FirmwareStatusNotification",{"status":"DownloadFailed"}]))
			if (rs!=''):
				retry=retry+1
			else:
				rs=-1
			if (retry>int(rs)):
				dbd = database()
				with dbd.cursor() as mycursor:
					mycursor.execute("UPDATE imp_files SET firmware_detail=%s",[det])
				dbd.commit()
				dbd.close()
				break
			if (rt==''):
				rt=2
			time.sleep(rt)

def remote_start(connector_id):
	tim=config("ConnectionTimeOut")
	tim=int(tim)+5
	time.sleep(tim)
	if connector_id=="1":
		with open(file_path+'/rrt_text/timeout_status.txt', 'r') as f:
					n=f.read().split('\n')[0]
	elif connector_id=="2":
		with open(file_path+'/rrt_text/timeout_status_2.txt', 'r') as f:
					n=f.read().split('\n')[0]
	print ("n",n)
	if n=="1":
		if connector_id=="1":
			with open(file_path+'/rrt_text/timeout_status.txt', 'w+') as w:
			    w.write("0")
		elif connector_id=="2":
			with open(file_path+'/rrt_text/timeout_status_2.txt', 'w+') as w:
			    w.write("0")
		print ("connection timeout")
		current_date_time= datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
		ws.send(json.dumps([2,"status_notification-"+str(connector_id),"StatusNotification",{"connectorId":str(connector_id),"errorCode":"NoError","status":"Finishing","timestamp":current_date_time}]))
		ws.send(json.dumps([2,"status_notification-"+str(connector_id),"StatusNotification",{"connectorId":str(connector_id),"errorCode":"NoError","status":"Available","timestamp":current_date_time}]))

def reboot(typ):
	if typ=="Soft":
		time.sleep(10)
		logging.info ("soft reboots")
		os.system('sudo reboot')
	elif typ=="Hard":
		logging.info ("hard reboots")
		os.system('sudo reboot')

#websocket client to do open, close and receive messages from server
class DummyClient(WebSocketClient):
  
	def closed(self, code, reason=None):
		logging.info("### closed ###")

	def opened(self):
		global t
		logging.info ("into on")
		Thread(target=offline_data).start()
		Thread(target=call_online).start()
		if t==0:
			Thread(target=bootNotificationThread,args=(0,)).start()
			t=1

	def received_message(self,message):
		message=str(message)
		logging.info (message)
		parse_data = json.loads(message)
		message_type = parse_data[0]
		unique_id = parse_data[1]
		action = parse_data[2]
		global b
		if(message_type == 2):
			if(action == "ChangeConfiguration"):
				change_configuration_req = changeConfiguration(message)
				ws.send(json.dumps(change_configuration_req))

			elif(action == "ChangeAvailability"):
				change_availability_req = changeAvailability(message)
				ws.send(json.dumps(change_availability_req))
			    
			elif(action == "RemoteStartTransaction"):
				rem_start_tx_req = remoteStartTransaction(message)
				rm_start_tx_response=rem_start_tx_req[0]
				ws.send(json.dumps(rm_start_tx_response))
				if (rem_start_tx_req[1]!=0) and (rem_start_tx_req[2]!=0):
					authorization_requirement=config("AuthorizeRemoteTxRequests")
					dba = database()
					with dba.cursor() as mycursor:
						if bool(authorization_requirement)==True:
							mycursor.execute("UPDATE conn_strt_stp SET tag_id=%s, remote_start='1' WHERE connector_id = %s",[rem_start_tx_req[2], rem_start_tx_req[1]] )
							dba.commit()
						elif bool(authorization_requirement)==False:
							mycursor.execute("UPDATE conn_strt_stp SET tag_id=%s, tag_auth='authorized',remote_start='1' WHERE connector_id = %s",[rem_start_tx_req[2],rem_start_tx_req[1]] )
							dba.commit()
					if str(rem_start_tx_req[1])=="1":
						fil = open(file_path+"/rrt_text/rfid_tag_id.txt","w") 
						fil.write(str(rem_start_tx_req[2]))
						fil.close()
					elif str(rem_start_tx_req[1])=="2":
						fil = open(file_path+"/rrt_text/rfid_tag_id_2.txt","w") 
						fil.write(str(rem_start_tx_req[2]))
						fil.close()
					dba.close()
					try:
						setchargingprofile(message,1)
					except:
						pass
					current_date_time= datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
					ws.send(json.dumps([2,"status_notification-"+str(rem_start_tx_req[1]),"StatusNotification",{"connectorId":str(rem_start_tx_req[1]),"errorCode":"NoError","status":"Preparing","timestamp":current_date_time}]))
					Thread(target=remote_start,args=(str(rem_start_tx_req[1]),)).start()
						
			elif(action == "RemoteStopTransaction"):
				rem_stop_tx_req = remoteStopTransaction(message)
				ws.send(json.dumps(rem_stop_tx_req[1]))
				dba = database()
				with dba.cursor() as mycursor:
					mycursor.execute("SELECT connector_id FROM conn_strt_stp WHERE trans_id = %s",[rem_stop_tx_req[0]])
					result = mycursor.fetchone()
					if result!=None:
						if result["connector_id"]==1:
							s_byte=36
							f_id=103
							cid=1
							scc_id=7
							dum_id=0
							st_bit=1
							end_byte=35
							try:
								serial_rrt.write(chr(s_byte).encode())
								serial_rrt.write(chr(f_id).encode())
								serial_rrt.write(chr(cid).encode())
								serial_rrt.write(chr(scc_id).encode())
								serial_rrt.write(chr(dum_id).encode())
								serial_rrt.write(chr(st_bit).encode())
								serial_rrt.write(chr(end_byte).encode())
							except:
								pass
						elif result["connector_id"]==2:
							s_byte=36
							f_id=103
							cid=2
							scc_id=7
							dum_id=0
							st_bit=1
							end_byte=35
							try:
								serial_rrt1.write(chr(s_byte).encode())
								serial_rrt1.write(chr(f_id).encode())
								serial_rrt1.write(chr(cid).encode())
								serial_rrt1.write(chr(scc_id).encode())
								serial_rrt1.write(chr(dum_id).encode())
								serial_rrt1.write(chr(st_bit).encode())
								serial_rrt1.write(chr(end_byte).encode())
							except:
								pass

						mycursor.execute("UPDATE conn_strt_stp SET stop_transaction = '2' WHERE trans_id = %s",[rem_stop_tx_req[0]] )
						dba.commit()
					else:
						cur_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
						ws.send(json.dumps([2,"remote_stop","StopTransaction",{"reason":"Remote","meterStop":0,"timestamp":cur_time,"transactionId":rem_stop_tx_req[0]}]))
				dba.close() 
		                               
			elif(action == "GetConfiguration"):
				get_config_req = getConfiguration(message)
				ws.send(json.dumps(get_config_req))

			elif(action == "UpdateFirmware"):
				get_upd_firmware_req = updateFirmware(message)
				cur_dt=datetime.datetime.utcnow()
				if(get_upd_firmware_req[1] > cur_dt):
					ws.send(json.dumps(get_upd_firmware_req[0]))
					Thread(target=updateFirmwareThread,args=(get_upd_firmware_req[1],get_upd_firmware_req[2],get_upd_firmware_req[3],get_upd_firmware_req[4],)).start()
				else:
					logging.info ("Seconds in Negative:::")

			elif(action == "GetDiagnostics"):
				get_diag_firmware_req = getDiagnostics(message)
				ws.send(json.dumps(get_diag_firmware_req))

				dba = database()
				with dba.cursor() as mycursor:
					mycursor.execute("SELECT diagnostic_detail FROM imp_files")
					result = mycursor.fetchone()
				dia_det=result["diagnostic_detail"]
				current_date_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
				ws.send(json.dumps([2,"diagnostic_status-01","DiagnosticStatusNotification",{"status":dia_det,"timestamp":current_date_time}]))
				dba.close()	 

			elif(action == "Reset"):
				reset_req = reset(message)
				ws.send(json.dumps(reset_req[0]))
				try:
					f= open(file_path+'/rrt_text/getenergy_metervalue.txt', 'r')    
					meter1=f.read().split('\n')[0]
					f.close()
					f= open(file_path+'/rrt_text/getenergy_metervalue_2.txt', 'r')    
					meter2=f.read().split('\n')[0]
					f.close()
					dba = database()
					with dba.cursor() as mycursor:
						if(reset_req[1] == "Soft"):
							logging.info ("Transaction in progress(Soft)!!!") 
							mycursor.execute("SELECT connector_id FROM conn_strt_stp WHERE transaction_progress='1' ")
							result = mycursor.fetchall()
							if len(result)==1:	
								s_byte=36
								f_id=103
								scc_id=7
								dum_id=0
								st_bit=1
								end_byte=35	
								if result[0]["connector_id"]==1:
									cid=1
									try:
										print ("trying to stop connector 1")
										serial_rrt.write(chr(s_byte).encode())
										serial_rrt.write(chr(f_id).encode())
										serial_rrt.write(chr(cid).encode())
										serial_rrt.write(chr(scc_id).encode())
										serial_rrt.write(chr(dum_id).encode())
										serial_rrt.write(chr(st_bit).encode())
										serial_rrt.write(chr(end_byte).encode())
									except:
										pass
								elif result[0]["connector_id"]==2:
									cid=2
									try:
										print ("trying to stop connector 2")
										serial_rrt1.write(chr(s_byte).encode())
										serial_rrt1.write(chr(f_id).encode())
										serial_rrt1.write(chr(cid).encode())
										serial_rrt1.write(chr(scc_id).encode())
										serial_rrt1.write(chr(dum_id).encode())
										serial_rrt1.write(chr(st_bit).encode())
										serial_rrt1.write(chr(end_byte).encode())
									except:
										pass
								mycursor.execute("UPDATE conn_strt_stp SET stop_transaction = '3' WHERE connector_id=%s",[result[0]["connector_id"]])
								dba.commit()
							elif len(result)==2:
								print ("trying to stop connector 1,2")
								s_byte=36
								f_id=103
								cid=1
								scc_id=7
								dum_id=0
								st_bit=1
								end_byte=35
								try:
									serial_rrt.write(chr(s_byte).encode())
									serial_rrt.write(chr(f_id).encode())
									serial_rrt.write(chr(cid).encode())
									serial_rrt.write(chr(scc_id).encode())
									serial_rrt.write(chr(dum_id).encode())
									serial_rrt.write(chr(st_bit).encode())
									serial_rrt.write(chr(end_byte).encode())
								except:
									pass
								cid=2
								try:
									serial_rrt1.write(chr(s_byte).encode())
									serial_rrt1.write(chr(f_id).encode())
									serial_rrt1.write(chr(cid).encode())
									serial_rrt1.write(chr(scc_id).encode())
									serial_rrt1.write(chr(dum_id).encode())
									serial_rrt1.write(chr(st_bit).encode())
									serial_rrt1.write(chr(end_byte).encode())
								except:
									pass
								mycursor.execute("UPDATE conn_strt_stp SET stop_transaction = '3'")
								dba.commit()
							Thread(target=reboot,args=("Soft",)).start()
							#time.sleep(10)
							#logging.info ("reboots")
							#os.system('sudo reboot')
						elif(reset_req[1] == "Hard"):
							logging.info ("Transaction in progress(Hard)!!!")
							mycursor.execute("SELECT connector_id FROM conn_strt_stp WHERE transaction_progress='1' ")
							result = mycursor.fetchall()
							if len(result)==1:		
								if result[0]["connector_id"]==1:
									fil = open(file_path+"/rrt_text/getstartenergy_occp.txt","w") 
									fil.write(str(meter1))
									fil.close()
								elif result[0]["connector_id"]==2:
									fil = open(file_path+"/rrt_text/getstartenergy_occp_2.txt","w") 
									fil.write(str(meter2))
									fil.close()
								mycursor.execute("UPDATE conn_strt_stp SET stop_transaction = '4' WHERE connector_id=%s",[result[0]["connector_id"]])
								dba.commit()
							elif len(result)==2:
								fil = open(file_path+"/rrt_text/getstartenergy_occp.txt","w") 
								fil.write(str(meter1))
								fil.close()
								fil = open(file_path+"/rrt_text/getstartenergy_occp_2.txt","w") 
								fil.write(str(meter2))
								fil.close()
								mycursor.execute("UPDATE conn_strt_stp SET stop_transaction = '4'")
								dba.commit()
							
							try:
								serial_rrt.write(chr(36).encode())
								serial_rrt.write(chr(103).encode())
								serial_rrt.write(chr(1).encode())
								serial_rrt.write(chr(7).encode())
								serial_rrt.write(chr(1).encode())
								serial_rrt.write(chr(0).encode())
								serial_rrt.write(chr(35).encode())
								serial_rrt.flushInput()
							except Exception as e:
								print(e)
							try:
								serial_rrt1.write(chr(36).encode())
								serial_rrt1.write(chr(103).encode())
								serial_rrt1.write(chr(2).encode())
								serial_rrt1.write(chr(7).encode())
								serial_rrt1.write(chr(1).encode())
								serial_rrt1.write(chr(0).encode())
								serial_rrt1.write(chr(35).encode())
								serial_rrt1.flushInput()
							except Exception as e:
								print(e)
							while True:
								with open(file_path+'/rrt_text/reset_hard.txt', 'r') as rese:
									d=rese.read()
								#print(d)
								if d=="1":
									with open(file_path+'/rrt_text/reset_hard.txt', 'w+') as rese:
										d=rese.write("0")
									print("reboots")
									Thread(target=reboot,args=("Hard",)).start()
									#os.system('sudo reboot')
					dba.close()
				except:	
					current_date_time= datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")     
					ws.send(json.dumps([2,"status_notification","StatusNotification",{"connectorId":0,"errorCode":"ResetFailure","status":"Faulted","timestamp":current_date_time}]))	                                 
			
			elif(action == "UnlockConnector"):
				unlock_connector_req = unlockConnector(message)
				dba = database()
				with dba.cursor() as mycursor:
					mycursor.execute("SELECT transaction_progress FROM conn_strt_stp WHERE connector_id=%s",[unlock_connector_req[1]])
					result = mycursor.fetchone()
					if result["transaction_progress"]=="1":
						mycursor.execute("UPDATE conn_strt_stp SET stop_transaction = '10' WHERE connector_id=%s",[unlock_connector_req[1]])
						s_byte=36
						f_id=103
						scc_id=7
						dum_id=0
						st_bit=1
						end_byte=35
						if unlock_connector_req[1]=="1":
							cid=1
							try:
								serial_rrt.write(chr(s_byte).encode())
								serial_rrt.write(chr(f_id).encode())
								serial_rrt.write(chr(cid).encode())
								serial_rrt.write(chr(scc_id).encode())
								serial_rrt.write(chr(dum_id).encode())
								serial_rrt.write(chr(st_bit).encode())
								serial_rrt.write(chr(end_byte).encode())
							except:
								pass
						elif unlock_connector_req[1]=="2":
							cid=2
							try:
								serial_rrt1.write(chr(s_byte).encode())
								serial_rrt1.write(chr(f_id).encode())
								serial_rrt1.write(chr(cid).encode())
								serial_rrt1.write(chr(scc_id).encode())
								serial_rrt1.write(chr(dum_id).encode())
								serial_rrt1.write(chr(st_bit).encode())
								serial_rrt1.write(chr(end_byte).encode())
							except:
								pass

				dba.commit()
				dba.close()
				Thread(target=unlockConnectorThread,args=(unlock_connector_req[0],unlock_connector_req[1],)).start()#start the start transaction thread
				s_byte=36
				f_id=103
				mesg_type=8
				request=1
				end_byte=35
				if unlock_connector_req[1]=="1":
					connetor_id=1
					try:
							serial_rrt.write(chr(s_byte).encode())
							serial_rrt.write(chr(f_id).encode())
							serial_rrt.write(chr(connetor_id).encode())
							serial_rrt.write(chr(mesg_type).encode())
							serial_rrt.write(chr(0).encode())
							serial_rrt.write(chr(request).encode())
							#serial_rrt.write(chr(int(0)).encode())
							serial_rrt.write(chr(end_byte).encode())
					except :
							pass
				elif unlock_connector_req[1]=="2":
					connetor_id=2
					try:
						serial_rrt1.write(chr(s_byte).encode())
						serial_rrt1.write(chr(f_id).encode())
						serial_rrt1.write(chr(connetor_id).encode())
						serial_rrt1.write(chr(mesg_type).encode())
						serial_rrt1.write(chr(int(0)).encode())
						serial_rrt1.write(chr(request).encode())
						#serial_rrt1.write(chr(0).encode())
						serial_rrt1.write(chr(end_byte).encode())
					except :
						pass

			elif(action == "ClearCache"):
				clear_cache_req = clearCache(message)
				ws.send(json.dumps(clear_cache_req))

			elif(action == "GetLocalListVersion"):
				local_list_version_req = getLocalListVersion(message)
				ws.send(json.dumps(local_list_version_req))

			elif(action == "SendLocalList"):
				send_local_list_req = sendlocallist(message)
				ws.send(json.dumps(send_local_list_req[0])) 
				if send_local_list_req[1]!="Accepted":
					current_date_time= datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
					ws.send(json.dumps([2,"status_notification","StatusNotification",{"connectorId":0,"errorCode":"LocalListConflict","status":"Faulted","timestamp":current_date_time}]))	

			elif(action == "DataTransfer"):
				data_transfer_req = dataTransfer(message)
				ws.send(json.dumps(data_transfer_req))

			elif(action == "CancelReservation"):
				can_reserv_req = cancelReservation(message)
				ws.send(json.dumps(can_reserv_req[0]))
				dba = database()
				with dba.cursor() as mycursor:
					mycursor.execute("SELECT reservation_status FROM conn_strt_stp")
					result = mycursor.fetchall()
					if (result[0]["reservation_status"]=='') or (result[1]["reservation_status"]==''):             
						if can_reserv_req[1] == "Accepted":
							with open(file_path+'/rrt_text/reserve_now.txt', 'w') as cur_process:
								cur_process.write("0")
							cur_process.close()
				dba.close()
		    
			elif(action == "TriggerMessage"):
				trigger_msg_req = triggerMessage(message)
				ws.send(json.dumps(trigger_msg_req[0]))
				if (trigger_msg_req[1] == "Accepted"):
					if(trigger_msg_req[2] == "BootNotification"):
						with open(file_path+'/rrt_text/boot_notification.txt', 'w') as boot_notification_read:
							boot_notification_read.write("")
						Thread(target=bootNotificationThread,args=(1,)).start()
					elif(trigger_msg_req[2] == "DiagnosticsStatusNotification"):
						dba = database()
						with dba.cursor() as mycursor:
							mycursor.execute("SELECT diagnostic_detail FROM imp_files")
							result = mycursor.fetchone()
						dia_det=result["diagnostic_detail"]
						current_date_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
						ws.send(json.dumps([2,"diagnostic_status-01","DiagnosticStatusNotification",{"status":dia_det,"timestamp":current_date_time}]))
						dba.close()
					elif(trigger_msg_req[2] == "FirmwareStatusNotification"):
						dba = database()
						with dba.cursor() as mycursor:
						 	mycursor.execute("SELECT firmware_detail FROM imp_files")
						 	result = mycursor.fetchone()
						fir_det=result["firmware_detail"]
						current_date_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
						ws.send(json.dumps([2,"firmware_status","FirmwareStatusNotification",{"status":fir_det,"timestamp":current_date_time}]))
						dba.close()
					elif(trigger_msg_req[2] == "Heartbeat"):
						ws.send(json.dumps([2,"heart_beat","Heartbeat",{}]))
					elif(trigger_msg_req[2] == "MeterValues"):
						logging.info ("meter values will be sent")
			
			elif(action == "ReserveNow"):
				reserve_now_req = reserveNow(message)
				ws.send(json.dumps(reserve_now_req[0]))
				if reserve_now_req[1]=="Accepted":
					current_date_time= datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
					ws.send(json.dumps([2,"status_notification","StatusNotification",{"connectorId":reserve_now_req[2],"errorCode":"NoError","status":"Reserved","timestamp":current_date_time}]))
					
			elif(action == "SetChargingProfile"):  
			 	set_charge_pro = setchargingprofile(message,0)
			 	ws.send(json.dumps(set_charge_pro))

			elif(action == "ClearChargingProfile"):
				clear_charge_pro = clearchargingprofile(message)
				ws.send(json.dumps(clear_charge_pro))

			elif(action=="GetCompositeSchedule"):
				get_comp = GetCompositeSchedule(message)
				ws.send(json.dumps(get_comp))

		# Server response for client initiated messages
		elif(message_type == 3):
			global transaction_id
			if(unique_id == 'boot_notification'):
				boot_notification_resp = json.loads(message)
				boot_status = boot_notification_resp[2]['status']
				boot_current_time = boot_notification_resp[2]['currentTime']
				boot_heartbeat = boot_notification_resp[2]['interval']
				boot_result = str(boot_status)+','+str(boot_current_time)+','+str(boot_heartbeat)
				with open(file_path+'/rrt_text/boot_notification.txt', 'w') as boot_not:
					boot_not.write(boot_result)
				boot_not.close()
				if(boot_status == "Accepted"):
					Thread(target=process,args=("1",)).start()#start the auhorization thread and wait for a valid authorization
					Thread(target=process,args=("2",)).start()#start the auhorization thread and wait for a valid authorization
					Thread(target=status_notification,args=("1")).start()
					Thread(target=status_notification,args=("2")).start()
					time.sleep(4)
					with open(file_path+'/rrt_text/process_start.txt', 'w+') as ch:
			  			ch.write("1")
					print ("written 1")
					Thread(target=sendHeartbeat).start()
				elif(boot_status == "Pending"):
					logging.info ("pending plz wait")
				elif(boot_status == "Rejected"):
					Thread(target=bootNotificationThread,args=(0,)).start()
			elif(unique_id == "heart_beat"):
				logging.info ("Heartbeat response received successfully!!!!")
			elif(unique_id == "offline_start"):
				offline_transaction_resp = json.loads(message)
				transaction_id= offline_transaction_resp[2]["transactionId"]
				# offline_transaction_status= offline_transaction_resp[2]["idTagInfo"]["status"]
				logging.info ("offline start sent successfully"+str(transaction_id))
			elif(unique_id == "offline_meter"):
				logging.info ("offline meter sent successfully")
			elif(unique_id == "offline_stop"):
				logging.info ("offline stop sent successfully")
			elif(unique_id=="remote_stop"):
				logging.info ("remote stopped")
			elif(unique_id == "status_notification") or (unique_id == "firmware_status"):
				logging.info ("status response recieved successfully")
			else:
				unique_id = parse_data[1].split('-')
				connector_id=unique_id[1]
				if unique_id[0]=="auth": 
					dba = database()
					with dba.cursor() as mycursor:
						mycursor.execute("SELECT tag_id FROM conn_strt_stp WHERE connector_id=%s",[connector_id])
						result = mycursor.fetchone()
						authorization_resp = json.loads(message)
						auth_status=authorization_resp[2]["idTagInfo"]["status"]
						try:
							auth_expiry_date =  authorization_resp[2]["idTagInfo"]["expiryDate"]
						except:
							auth_expiry_date = ""
						try:
							parent_id= authorization_resp[2]["idTagInfo"]["parentIdTag"]
						except:
							parent_id=""
						check_auth_cache=config("AuthorizationCacheEnabled")
						if(bool(check_auth_cache) == True):
							logging.info ("AuthorizationCacheEnabled functionality is enabled!!!")
							Thread(target=callAuthorizationFunction,args=(auth_status,auth_expiry_date,result["tag_id"],parent_id,)).start()
						else:
							logging.info ("AuthorizationCacheEnabled functionality is disabled!!!")
						if(auth_status == 'Accepted'):
							logging.info ("Authorization accepted. Start Transaction:::")
							mycursor.execute("UPDATE conn_strt_stp SET tag_auth='authorized', flow='2' WHERE connector_id=%s",[connector_id])
							dba.commit()
						else:
							mycursor.execute("UPDATE conn_strt_stp SET tag_auth='rejected',flow='0',remote_start='0' WHERE connector_id=%s",[connector_id])
							dba.commit()
					dba.close()
				elif unique_id[0] == "start_trans":
					start_transaction_resp = json.loads(message)
					start_transaction_id= start_transaction_resp[2]["transactionId"]
					start_transaction_status= start_transaction_resp[2]["idTagInfo"]["status"]
					if(start_transaction_status == "Accepted") or (start_transaction_status == "Blocked"):
						logging.info ("Transaction started!!!!")
						dba = database()
						with dba.cursor() as mycursor:	
							mycursor.execute("UPDATE conn_strt_stp SET transaction_progress='1', st_status='ongoing',flow='4', trans_id=%s WHERE connector_id=%s",(start_transaction_id,connector_id))
						dba.commit()
						dba.close()
						if connector_id=="1":
							   with open(file_path+'/rrt_text/trans_id.txt', 'w') as trns:
					    			trns.write(str(start_transaction_id))
							   trns.close()
						elif connector_id=="2":
							   with open(file_path+'/rrt_text/trans_id_2.txt', 'w') as trns:
					    			trns.write(str(start_transaction_id))
							   trns.close()
				elif unique_id[0] == "meter_value":
					logging.info ("Meter value response received successfully!!!!")
				elif unique_id[0] == "stop_trans" or unique_id[0] == "offline_stop":         
					logging.info ("Transaction stopped successfully!!!")
				elif unique_id[0] == "unknown_stop":
					dba = database()
					with dba.cursor() as mycursor:	
						mycursor.execute("UPDATE conn_strt_stp SET tag_id='',tag_auth='',trans_id=NULL,stop_transaction='',transaction_progress='', st_status='',flow='0' WHERE connector_id=%s",(connector_id))
					dba.commit()
					dba.close()
				elif(unique_id[0] == "status_notification"):
					logging.info ("status response recieved for connector successfully")
				elif(unique_id[0] == "stop_auth"):
					dba = database()
					with dba.cursor() as mycursor:
						mycursor.execute("SELECT tag_id,tag_id_stop FROM conn_strt_stp WHERE connector_id=%s",[connector_id])
						result = mycursor.fetchone()
						authorization_resp = json.loads(message)
						auth_status=authorization_resp[2]["idTagInfo"]["status"]
						try:
							auth_expiry_date =  authorization_resp[2]["idTagInfo"]["expiryDate"]
						except:
							auth_expiry_date = ""
						try:
							parent_id= authorization_resp[2]["idTagInfo"]["parentIdTag"]
						except:
							parent_id=""
						check_auth_cache=config("AuthorizationCacheEnabled")
						if(bool(check_auth_cache) == True):
							logging.info ("AuthorizationCacheEnabled functionality is enabled!!!")
							Thread(target=callAuthorizationFunction,args=(auth_status,auth_expiry_date,result["tag_id_stop"],parent_id,)).start()
						else:
							logging.info ("AuthorizationCacheEnabled functionality is disabled!!!")
						if(auth_status == 'Accepted'):
							chk=parentid_check(result["tag_id"],result["tag_id_stop"])
							mycursor.execute("UPDATE conn_strt_stp SET tag_auth_stop=%s WHERE connector_id=%s",[chk,connector_id])
							dba.commit()
						else:
							mycursor.execute("UPDATE conn_strt_stp SET tag_auth_stop='rejected' WHERE connector_id=%s",[connector_id])
							dba.commit()
					dba.close()
		# Server error response for client initiated messages
		elif(message_type == 4):
			logging.info ("Error message")
		
		else:
			logging.info ("Invalid message type")

if __name__ == "__main__":
	Thread(target=checkNetworkConnectivity).start()

	set_ip = open(file_path+'/text/set_host_add.txt', 'r')
	set_ip_read = (set_ip.read().split('\n')[0])

	set_chargebox = open(file_path+'/text/cbid.txt', 'r')
	set_chargebox_read = (set_chargebox.read().split('\n')[0])
	while 1:
		if check_connectivity=="1":
			try:
				ws = DummyClient("ws://192.168.1.78:8082/RrtWebService/cvm001")
				#ws = DummyClient("wss://electron.coovum.com:8008/RrtWebService/RRT001")
				logging.info("Connecting to SECURE connection......")
				ws.connect()
				logging.info("Connected to SECURE connection")
				ws.run_forever()
				logging.info("run_forever breaks")
			except SSLError:
				try:
					ws = DummyClient('ws://'+str(set_ip_read)+'/'+str(set_chargebox_read)+'')
					# ws = DummyClient("ws://electron.coovum.com:8008/RrtWebService/RRT001")
					logging.info("Connecting to UNSECURE connection......")
					ws.connect()
					logging.info("Connected to UNSECURE connection")
					ws.run_forever()
					logging.info("run_forever breaks")
				except Exception as e1:
					logging.info(e1)
					time.sleep(1)
			except Exception as e2:
				logging.info(e2)
				time.sleep(1)
		else:
		    logging.info("No network available to connect")
		    time.sleep(1)
#'ws://'+str(set_ip_read)+'/'+str(set_chargebox_read)+''
#"ws://192.168.1.78:8082/RrtWebService/css001"
#"ws://electron.coovum.com:8008/RrtWebService/RRT001"
#"ws://ocpp.electreefi.com:80/EVELTXRFIK/RRT010"
