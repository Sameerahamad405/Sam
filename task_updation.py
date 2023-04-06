from ise_subtask import *

from collections import OrderedDict
from api import (Incident, ServiceRequest,AssignmentGroupID,DataLogging)

ad, dlog, inc_obj,service,incidents = AssignmentGroupID(), DataLogging(),Incident(), ServiceRequest(), []


def query_subtask(task):
    #print(task)
    #print(type(task))
    subtasks = []
    #time.sleep(2)
    #print (task)
    responses =service.query_service_request(serviceRequest=task)
    #print(responses)
    #time.sleep(5)
    for response in responses:
        #print(response)
        subtask_url = response["relationsWithCount"]["srTask"]["_self"]
        subtask_response = inc_obj.query_url_detail(subtask_url, module="srTask")
        for subtask in subtask_response:
            subtasks.append(subtask["number"]) 
    return subtasks

def subtask_fileinput(inFile):

    subtask_details = OrderedDict()
    with open(inFile, 'r') as fileObject:
        next(fileObject)
        for column in fileObject:
            column = column.strip()
            if column:
                incData = column.split(",")
                ritm = incData[0].strip() if incData[0].lower().strip().startswith("ritm") else ""
                #print(ritm)
                subtask_list = query_subtask(ritm)
                time.sleep(2)
                subtask_details.setdefault("subtasks",[]).append(subtask_list)
                status = incData[1].strip() if incData[1] else ""
                assignmentGrp = incData[2].strip()
                workLog = ",".join(incData[3:]) if incData[3] else ""  
                subtask_details.setdefault("status",[]).append(status)
                subtask_details.setdefault("assignmentGrp",[]).append(assignmentGrp)
                subtask_details.setdefault("workLog",[]).append(workLog)
    
    return subtask_details

def subtask_menu(subtasks=False):

    subtask_details = OrderedDict()
    if not subtasks:
        ritm = raw_input("Please enter RITM ")
        subtask_list = query_subtask(ritm)  
        subtask_details.setdefault("subtasks",[]).append(subtask_list)

    status = raw_input("Please enter status of SubTask ")
    assignmentGrp = raw_input("Please enter Assignment Group ")
    workLog = raw_input("Please enter work notes to be updated")
    subtask_details.setdefault("status",[]).append(status)
    subtask_details.setdefault("assignmentGrp",[]).append(assignmentGrp)
    subtask_details.setdefault("workLog",[]).append(workLog)

    return subtask_details

def udpate_subtask(subtask_details):
    #print(subtask_details)
    subtask_list = subtask_details.get("subtasks", None)
    #print (subtask_list)
    status = subtask_details.get("status", None)
    #print(status)
    assignmentGrps = subtask_details.get("assignmentGrp", None)
    #print (assignmentGrps)
    workLog = subtask_details.get("workLog", None)
    #if len(subtask_list) > 1:
    for index, tasks in enumerate(subtask_details['subtasks']):
            for task in tasks:
                #print(task + " - " + subtask_details['status'][index]  )
                #node = None
                responses = kpi.query_service_request(task=task,module='srTask')
                time.sleep(3)
                #print (responses)
                for response in responses:
                    node = response["title"].strip().split(" ")[-1].strip()
                    time.sleep(3)
                    #print("node is " + node)
                #print ("node is " + node)     
                status = subtask_details['status'][index]
                assignmentGrp = ad(subtask_details['assignmentGrp'][index])
                #print(assignmentGrp)
                workLog = subtask_details['workLog'][index]  
                         #workLog_data = workLog.split("SP")
                        # assignmentGroupId=assignmentGrp ,
                workLog= node + workLog
                output = kpi.update_service_task(number=task,updatedById=user_dsid,state=status,assignmentGroupId=assignmentGrp ,
                workLog=workLog)
                time.sleep(3)

                print("{0} => update {1}".format(task,output if output else "Failed"))
                log("i","dss: DSS Modules",task=task, status="update")

   
        
def subtask(input):

    subtask_details = OrderedDict()
    if input[1].lower() =="update":
        if input[2].lower().startswith("ptask") or input[2].lower().startswith("ritm"):
            #if input[3] and input[3].lower().startswith("file"):
            #print(len(input))
            if len(input)>3:
                inpFile = input[3].split("=")[-1]
                subtask_details = subtask_fileinput(inpFile)
            elif "=" in input[2]:
                inpTask = input[2].split("=")[-1]
                if inpTask.lower().startswith("ritm"):
                # Query subtask store them in list 
                    subtask_list = query_subtask(inpTask)
                    subtask_details = subtask_menu(subtasks=True)
                    subtask_details.setdefault("subtasks",[]).append(subtask_list)

            #elif input[3] and input[3].lower().startswith("file"):
            #    inpFile = input[3].split("=")[-1]
             #   subtask_details = subtask_fileinput(inpFile)
            else:
                subtask_details = subtask_menu()
            udpate_subtask(subtask_details)
        elif input[2].strip().lower().startswith("file"):
            inFile = input[2].split("=")[-1]
            #print(inFile)
            with open(inFile, 'r') as fileObject:
                next(fileObject)
                for column in fileObject:
                    column = column.strip()
                    if column:
                        incData = column.split(",")
                        subTask = incData[0].strip() if incData[0].lower().strip().startswith("task") else ""
                        responses = kpi.query_service_request(task=subTask,module='srTask')
                        time.sleep(2)
                        for response in responses:
                            node = response["title"].strip().split(" ")[-1].strip()
                        # updatedBy = incData[1].strip() if incData[1] else ""
                        status = incData[1].strip() if incData[1] else ""
                        assignmentGrp = ad(incData[2].strip()) if incData[2] else ""
                        workLog = ",".join(incData[3:]) if incData[3] else ""  
                        # workLog_data = workLog.split("SP")
                        # assignmentGroupId=assignmentGrp ,
                        workLog= node+ workLog
                        if subTask.lower().startswith("task"):
                            output = kpi.update_service_task(number=subTask,updatedById=user_dsid,state=status,\
                            workLog=workLog)
                            time.sleep(2)
                            print("{0} => update {1}".format(subTask,output if output else "Failed"))
                            log("i","dss: DSS Modules",task=subTask, status="update")

def subtask_process():
    input = sys.argv
    if input>1:
        subtask(input)
    else:
        # Menu driven option 
        subtask_details = subtask_menu()
        udpate_subtask(subtask_details)


if __name__ == "__main__":
    git_handler()
    subtask_process()
