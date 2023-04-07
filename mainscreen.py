import sys
import os
from PySide.QtCore import *         #Access QtCore & QtGui module for GUI functionalities
from PySide.QtGui import * 
import time
from db import*
import subprocess
from screen_single_connector_file import screen_single_connector,intital
from screen_two_connector_file import frame_screen,lcd_set_top,screen_single_connector1,lcd_start_fun,screen_single_connector2,frame_screen_2
from c2_but import frame_data,frame_data2
from standalone_net import online_offline
from c2_standalone_net import standalone_network_fun_2,online_offline1_2
#from zip_update import file_update
#from timestart import lcd_show,start_lcd,stop_lcd
#from standalone_net import standalone_network_fun,stand_hide,online_offline

from ser_flag_network import ser_write_net,ser_write_alone
from c2_but import perparing_1,perparing_1_hide,perparing_2,perparing_2_hide

global LastPressTime
LastPressTime=0
flag_ocpp =0

cur.execute("SELECT authorization FROM  standalone")

for row in cur.fetchall() :

    
        network_standalone =str(row[0])
db.commit()

"""if(network_standalone == "networked"):
        
	subprocess.Popen(['python','/home/pi/Documents/rrt/rrt_screen/long_lived_connection.py'])
	
"""
a = 105
file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/settime.txt","w") 
file.write(str(a)) 
file.close()

file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/settime_2.txt","w") 
file.write(str(a)) 
file.close()

start_c="0"
file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/ser_not_ava.txt","w") 
file.write(start_c) 
file.close()

start_c="0"
file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/ser_not_ava2.txt","w") 
file.write(start_c) 
file.close()

dual_flag = "0"
file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/start_button.txt","w")  
file.write(dual_flag) 
file.close()

dual_flag = "0"
file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/start1_button.txt","w")  
file.write(dual_flag) 
file.close()

dual_flag = "0"
file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/zip_update_file.txt","w")  
file.write(dual_flag) 
file.close()

dual_flag = "00"
file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/cost_charging.txt","w")  
file.write(dual_flag) 
file.close()

dual_flag = "00:00"
file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/time_given_com.txt","w")  
file.write(dual_flag) 
file.close()

dual_flag = "0"
file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/set_time_value.txt","w")  
file.write(dual_flag) 
file.close()

dual_flag = "00"
file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/cost_charging2.txt","w")  
file.write(dual_flag) 
file.close()

dual_flag = "00:00"
file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/time_given_com1.txt","w")  
file.write(dual_flag) 
file.close()

dual_flag = "0"
file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/set_time_value2.txt","w")  
file.write(dual_flag) 
file.close()

cur.execute("SELECT fc_numberconnector FROM  ch_st_location")

for row in cur.fetchall() :

    
        no_of_connector =str(row[0])

db.commit()


flag_th = 1


class Example(QWidget):
    
    def __init__(self):
            super(Example, self).__init__()

            self.initUI()

    def initUI(self):

            global w,no_of_connector,topleft,top,topright,network_standalone,flag_ocpp
            
            w = QWidget(self)
            self.setStyleSheet("background-color:lightskyblue")
            

            if(no_of_connector == "2"):

                hbox = QHBoxLayout(self)
                #hbox = QVBoxLayout(self)

                topleft = QFrame(self)
                topleft.setFrameShape(QFrame.Box)
                topleft.setLineWidth(2)
                topleft.setMidLineWidth(1) 
                topleft.setFixedSize(385,400)

                topright = QFrame(self)
                #topright.setFrameShape(QFrame.StyledPanel)
                topright.setFrameShape(QFrame.Box)
                topright.setLineWidth(2)
                topright.setMidLineWidth(1) 
                topright.setFixedSize(385,400)

                top = QFrame(self)
                #top.setFrameShape(QFrame.StyledPanel)
                top.setFrameShape(QFrame.Box)
                top.setLineWidth(2)
                top.setMidLineWidth(1) 
                top.setFixedSize(777,50)

                splitter2 =QSplitter(Qt.Vertical)

                splitter2.addWidget(top)


                splitter1 =QSplitter(Qt.Horizontal)
                splitter1.addWidget(topleft)
                splitter1.addWidget(topright)
                splitter2.addWidget(splitter1)

                hbox.addWidget(splitter2)
                self.setLayout(hbox)

                frame_screen(topleft)
                frame_data(topleft)
                frame_data2(topright)
                #frame_screen(topright)
                lcd_set_top(top)
                frame_screen_2(topright)

                standalone_network_fun_2(top)
                lcd_start_fun()
                file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/process_start.txt", "r") 
                data_base=file.read()
                if(network_standalone == "networked"):
                        if(data_base == "0"):
                                perparing_1(topleft)
                                perparing_2(topright)
                        else:
                                screen_single_connector1(topleft)
                                screen_single_connector2(topright)
                else :
                        screen_single_connector1(topleft)
                        screen_single_connector2(topright)
                        

            elif(no_of_connector == "1"):
                    intital(self)

            def screen_set():
                    
                global w,no_of_connector,topleft,top,topright,flag_th,network_standalone,flag_ocpp
                #if(flag_th == 1):
                #flag_th = 0
                if(network_standalone == "networked"):
                        ser_write_net()
                else :
                        ser_write_alone()  
                if(no_of_connector == "1"):
                    #start_lcd()
                    
                    online_offline(self)
                    file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/process_start.txt", "r") 
                    data_base=file.read()
                    if(network_standalone == "networked"):
                        if(data_base == "1") or (flag_ocpp == 1):
                            screen_single_connector(self)
                            if(flag_ocpp == 0):
                                    dual_flag = "0"
                                    file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/process_start.txt","w")  
                                    file.write(dual_flag) 
                                    file.close()
                                    flag_ocpp = 1
                    else :
                            screen_single_connector(self)
                    
                elif(no_of_connector == "2"):
                    lcd_start_fun()
                    online_offline1_2(top)
                    file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/process_start.txt", "r") 
                    data_base=file.read()
                    if(network_standalone == "networked"):
                        if(data_base == "1") or (flag_ocpp == 1):
                            try :
                                    perparing_1_hide()
                            except :
                                    pass
                            screen_single_connector1(topleft)
                            #screen_single_connector2(topright)
                            #flag_th =1
                            dual_flag = "0"
                            file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/process_start.txt","w")  
                            file.write(dual_flag) 
                            file.close()
                            flag_ocpp = 1
                            try :
                                    perparing_1_hide()
                            except :
                                    pass
                    else :
                        screen_single_connector1(topleft)
                                
                try :
                        file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/zip_update_file.txt", "r") 
                        data=file.read()
                        file.close()
                        file1 = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/screenNo.txt", "r") 
                        data1=file1.read()
                        file1.close()

                        data_1 = data1.split(",")
                        screen_no=int(data_1[1])

                        file1 = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/screenNo_2.txt", "r") 
                        data1=file1.read()
                        file1.close()
                        data1       = data1.split(",")
                        screen_no_2=int(data1[1])

                        if(data == "1") and (screen_no != 7) and (screen_no_2 != 7):
                                #file_update()
                                subprocess.Popen(['python3','/home/pi/Documents/rrt/rrt_screen/zip_update.py'])
                                print("LLLL")
                                dual_flag = "0"
                                file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/zip_update_file.txt","w")  
                                file.write(dual_flag) 
                                file.close()
                                """#time.sleep(2)
                                os.system('sudo reboot')"""
                except :
                        pass

            def screen_set2():
                    global topright,flag_ocpp
                    #print "con22"
                    file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/process_start.txt", "r") 
                    data_base=file.read()
                    if(network_standalone == "networked"):
                        if(data_base == "1") or (flag_ocpp == 1):
                                try :
                                    perparing_2_hide()
                                except :
                                    pass
                                screen_single_connector2(topright)
                                dual_flag = "0"
                                file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/process_start.txt","w")  
                                file.write(dual_flag) 
                                file.close()
                                flag_ocpp = 1
                                
                    else :
                            screen_single_connector2(topright)

            #self.setGeometry(1, 1, 800, 480)
            self.setWindowTitle('RRT_Coovum')    
            self.showFullScreen()
            #self.show()
            
            timer=QTimer(self)
            timer.setInterval(900)
            timer.timeout.connect(screen_set)

            timer.start()

            if(no_of_connector == "2"):

                    timer=QTimer(self)
                    timer.setInterval(900)
                    timer.timeout.connect(screen_set2)

                    timer.start()


    def paintEvent(Frame,event):
            global  no_of_connector 
            qp = QPainter()
            qp.begin(Frame)
            pen = QPen(Qt.black, 2,Qt.SolidLine)
            qp.setPen(pen)

            if(no_of_connector == "1"):

                Frame.drawLine(event, qp)
            qp.end()

    def drawLine(Frame, event, qp):
            qp.drawLine(2, 60, 800, 60)


    def mousePressEvent(self, QMouseEvent):
            ###print mouse position
            #if(
            global LastPressTime
            ##print QMouseEvent.x()
            ##print QMouseEvent.y()
            #if (QMouseEvent.x()<460  and QMouseEvent.x()>130) and(QMouseEvent.x()<460 and QMouseEvent.x()>180):
            
            LastPressTime=QDateTime.currentMSecsSinceEpoch()
               ##print LastPressTime  
            #if(QMouseEvent.x()>10





    def mouseReleaseEvent(self, QMouseEvent):
             MY_LONG_PRESS_THRESHOLD=2000
             global LastPressTime,availability,no_of_connector
             ###print "hi",QMouseEvent.x()
             ##print QMouseEvent.y()
             file = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/screenNo.txt", "r") 
             data=file.read()
             data       = data.split(",")
            
             screen_no=int(data[1])
            
             file1 = open("/home/pi/Documents/rrt/rrt_screen/rrt_text/screenNo_2.txt", "r") 
             data1=file1.read()
             data1       = data1.split(",")

             screen_no_2=int(data1[1])
             pressTime = QDateTime.currentMSecsSinceEpoch() - LastPressTime
             ##print pressTime,screen_no_2,screen_no
             
             """if (QMouseEvent.x()<460  and QMouseEvent.x()>130) and(QMouseEvent.x()<460 and QMouseEvent.x()>180):
                    
                    ###print pressTime"""
             if( pressTime > MY_LONG_PRESS_THRESHOLD):
                    #print "pressTime",pressTime
                    rrt_status="1"
                    if(((screen_no == 1) or (screen_no == 2))and (no_of_connector == "1")):
                            ##print "lll"
                            #sub_val_init = subprocess.Popen(['python3', '/home/pi/Documents/rrt/Sameer_rrt/rrt_config/mainscreen_admin_service.py'])
                            sub_val_init = subprocess.Popen(['/home/pi/Documents/rrt_file/mainscreen_admin_service'])
                            time.sleep(5)
                            sys.exit()
                            
                    
                    elif(((screen_no == 1) and (screen_no_2 == 1)) or ((screen_no == 2) and (screen_no_2 == 2))and (no_of_connector == "2")):
                            #sub_val_init = subprocess.Popen(['python3', '/home/pi/Documents/rrt/Sameer_rrt/rrt_config/mainscreen_admin_service.py'])
                            sub_val_init = subprocess.Popen(['/home/pi/Documents/rrt_file/mainscreen_admin_service'])
                            ##print "ooo"
                            time.sleep(5)
                            sys.exit()
                    
    
  
        
def main():
    
    app =QApplication(sys.argv)
    ex = Example()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
