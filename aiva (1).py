# Apar Innosys Video Analytics - v21.12.18

# 1. Importing packages
import concurrent.futures
import copy
import cv2
import importlib.util
import imutils
import json
import logging
import numpy as np
import os
import schedule
import socket
import sys
import threading
import time
from logging.handlers import RotatingFileHandler
from datetime import datetime
from imutils.video import VideoStream, FileVideoStream
from imutils.video import FPS
from multiprocessing import Queue
from PIL import Image
from queue import Queue
from skimage.metrics import structural_similarity as compare_ssim
from threading import Thread
import os, psutil

NOobject_array = {'NOHELMET': 0, 'NOMASK': 0, 'NOCROWD': 0, 'NOANIMAL': 0,
                  'NODOOR': 0, 'NOHIDDEN': 0, 'NOTILTED': 0, 'NOLOITERING': 0}

# 2. Logging configuration
#logging.basicConfig(filename=log_path, level=logging.DEBUG,format='%(asctime)s %(levelname)s - %(name)s.%(funcName)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# 3. Reading configuration files
with open("/home/pi/edge-controller/config/software.json") as config_file_aiva:
    config_aiva = json.load(config_file_aiva)
config_file_aiva.close()

with open("/home/pi/edge-controller/config/edge-controller.json") as config_file_edge:
    config_edge = json.load(config_file_edge)
config_file_edge.close()

with open("/home/pi/edge-controller/config/hardware.json") as config_file_video:
    config_video = json.load(config_file_video)
config_file_video.close()

create_log_path = json.dumps(config_aiva['LOGS']['AIVA'])
create_log_path = create_log_path.replace('"', '')

# 4. Folder creation
image_dir_helmet = config_aiva["STORAGES"]["AIVA"] + 'Helmet'
image_dir_crowd = config_aiva["STORAGES"]["AIVA"] + 'Crowd'
image_dir_mask = config_aiva["STORAGES"]["AIVA"] + 'Mask'
image_dir_animal = config_aiva["STORAGES"]["AIVA"] + 'Animal'
image_dir_door = config_aiva["STORAGES"]["AIVA"] + 'Door'
image_dir_hidden = config_aiva['STORAGES']['AIVA'] + 'Hidden'
image_dir_tilted = config_aiva['STORAGES']['AIVA'] + 'Tilted'
image_dir_loitering = config_aiva["STORAGES"]["AIVA"] + 'Loitering'
image_dir_lowlight = config_aiva["STORAGES"]["AIVA"] + 'Lowlight'
dir_list = [image_dir_helmet, image_dir_crowd, image_dir_mask, image_dir_animal, image_dir_door,
            image_dir_door, image_dir_hidden, image_dir_tilted, image_dir_loitering, image_dir_lowlight, create_log_path]
for dir in dir_list:
    os.makedirs(dir, mode=0o777, exist_ok=True)
try:
    os.makedirs(create_log_path, mode=0o777, exist_ok=True)
except:
    pass


log_path = json.dumps(config_aiva['LOGS']['AIVA'])
log_path = log_path.replace('"', '')
log_path = log_path + "aiva.log"
log_maxbytes = json.dumps(config_aiva['LOG_ROTATE']['MAXBYTES'])
log_maxbytes = log_maxbytes.replace('"', '')
log_maxbytes = int(log_maxbytes)
log_backupcount = json.dumps(config_aiva['RETENTIONS']['AIVA'])
log_backupcount = log_backupcount.replace('"', '')
log_backupcount = int(log_backupcount)


# 5. Socket connection to edge-controller
host = config_aiva["SOCKETS"]["AIVA"]["SERVER"]
# the port, let's use 3334
port = config_aiva["SOCKETS"]["AIVA"]["PORT"]

logging.basicConfig(handlers=[RotatingFileHandler(log_path, maxBytes=log_maxbytes, backupCount=log_backupcount)],
                    level=logging.DEBUG, format='%(asctime)s %(levelname)s - %(name)s.%(funcName)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
s = socket.socket()
logging.info("Socket successfully created")
logging.info(f"[+] Connecting to {host}:{port}")
s.connect((host, port))
logging.info("[+] Connected.")

for x in NOobject_array:
    try:
        send_data = {"OBJECTS": [x]}
        s.send(json.dumps(send_data).encode())
        time.sleep(0.2)
    except BrokenPipeError:
        pass


# 6. Importing TensorFlow libraries
# If tflite_runtime is installed, import interpreter from tflite_runtime, else import from regular tensorflow
# If using Coral Edge TPU, import the load_delegate library
pkg = importlib.util.find_spec('tflite_runtime')
if pkg:
    from tflite_runtime.interpreter import Interpreter
else:
    from tensorflow.lite.python.interpreter import Interpreter

# 7. Constants
CWD_PATH = os.getcwd()
# In each data folder there is float_model.tflite
GRAPH_NAME_ANIMAL = 'float_model_animal.tflite'
GRAPH_NAME_CROWD = 'float_model_crowd.tflite'
GRAPH_NAME_DOOR = 'float_model_door.tflite'
GRAPH_NAME_HELMET = 'float_model_helmet.tflite'
GRAPH_NAME_MASK = 'float_model_mask.tflite'
CLASSIFIER_GRAPH_NAME = 'lstm_loitering.tflite'
POSE_GRAPH_NAME = 'pose_loitering.tflite'
# In each data folder there is labels.txt
LABELMAP_NAME_ANIMAL = 'labels_animal.txt'
LABELMAP_NAME_CROWD = 'labels_crowd.txt'
LABELMAP_NAME_DOOR = 'labels_door.txt'
LABELMAP_NAME_HELMET = 'labels_helmet.txt'
LABELMAP_NAME_MASK = 'labels_mask.txt'
# Path to .tflite file, which contains the model that is used for object detection
ANIMAL_CKPT = os.path.join(CWD_PATH, GRAPH_NAME_ANIMAL)
CROWD_CKPT = os.path.join(CWD_PATH, GRAPH_NAME_CROWD)
DOOR_CKPT = os.path.join(CWD_PATH, GRAPH_NAME_DOOR)
HELMET_CKPT = os.path.join(CWD_PATH, GRAPH_NAME_HELMET)
MASK_CKPT = os.path.join(CWD_PATH, GRAPH_NAME_MASK)
LOITERING_CKPT = os.path.join(CWD_PATH, POSE_GRAPH_NAME)
CLASSIFIER_CKPT = os.path.join(CWD_PATH, CLASSIFIER_GRAPH_NAME)

ANIMAL_LABELS = os.path.join(CWD_PATH, LABELMAP_NAME_ANIMAL)
CROWD_LABELS = os.path.join(CWD_PATH, LABELMAP_NAME_CROWD)
DOOR_LABELS = os.path.join(CWD_PATH, LABELMAP_NAME_DOOR)
HELMET_LABELS = os.path.join(CWD_PATH, LABELMAP_NAME_HELMET)
MASK_LABELS = os.path.join(CWD_PATH, LABELMAP_NAME_MASK)

# 8. Variables
input_mean = 127.5
input_std = 127.5

# Initialize frame rate calculation
frame_rate_calc = 1
freq = cv2.getTickFrequency()
queueLock = threading.Lock()

# Initialize video stream
inputQueue = Queue()
inputQueueHiddenTilted = Queue()
min_conf_threshold = 0.8
light_thresh = 90
nop_thresh = 2
alert_dict = {'helmet': 0, 'mask': 0, 'animal': 0,
              'door': 0, 'crowd': 0, 'loitering': 0}
vid_path = (config_video["CAMERAS"][1]["URL"])
start_time = time.time()

fps = FPS().start()
check = []

edgeId = config_edge["VBOXID"]

#global THRESH_FRAMES, THRESH_DETECT, initlize, bvalues, hstatus, base_value
#THRESH_FRAMES = 41  # frames
THRESH_DETECT = 6
#initlize = 0
#bvalues = []
#hstatus = "no"
# vs=cv2.VideoCapture(config_video["CAMERAS"][1]["URL"])

# 9. Classes


class helmetMaskCrowdThreadClass (threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q

    def run(self):
        # logging.info("Starting " + self.name)
        process_data_helmet_mask_crowd(self.name, self.q)
        # logging.info("Exiting " + self.name)


class hiddenTiltedThreadClass (threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q

    def run(self):
        # logging.info("Starting " + self.name)
        process_data_hidden_tilted(self.name, self.q)
        # logging.info("Exiting " + self.name)


class loiteringThreadClass (threading.Thread):
    def __init__(self, threadID, name, q, count, check):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.count = count
        self.check = check

    def run(self):
        # logging.info("Starting " + self.name)
        process_data_loitering(self.name, self.q, self.count, self.check)
        # logging.info("Exiting " + self.name)


# 10. Load the label maps
with open(HELMET_LABELS, 'r') as f:
    helmet_labels = [line.strip() for line in f.readlines()]
f.close()
with open(MASK_LABELS, 'r') as f:
    mask_labels = [line.strip() for line in f.readlines()]
f.close()
with open(ANIMAL_LABELS, 'r') as f:
    animal_labels = [line.strip() for line in f.readlines()]
f.close()
with open(CROWD_LABELS, 'r') as f:
    crowd_labels = [line.strip() for line in f.readlines()]
f.close()
with open(DOOR_LABELS, 'r') as f:
    door_labels = [line.strip() for line in f.readlines()]
f.close()

# Have to do a weird fix for label map if using the COCO "starter model" from
# https://www.tensorflow.org/lite/models/object_detection/overview
# First label is '???', which has to be removed.
if helmet_labels[0] == '???':
    del(helmet_labels[0])
if mask_labels[0] == '???':
    del(mask_labels[0])
if animal_labels[0] == '???':
    del(animal_labels[0])
if crowd_labels[0] == '???':
    del(crowd_labels[0])
if door_labels[0] == '???':
    del(door_labels[0])


def scheduler():
    while True:
        try:
            # Checks whether a scheduled task
            # is pending to run or not
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logging.error(e)
            pass


def check_ping(hostname):
    try:
        response = os.system("ping -c 1 {}".format(hostname))
        if response == 0:
            pass
        else:
            logging.error('Ping failed')
            sys.exit()
    except Exception as e:
        logging.error(e)
        pass


def detect_low_light(frame, light_thresh):
    try:
        return 0
        # gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        # is_light = np.mean(gray_frame) > light_thresh
        # if is_light:
        #     return 0
        # else:
        #     return 1
    except Exception as e:
        # logging.error(e)
        return 0


def compare(img1, img2, thresh):
    try:
        count = 0
        total_pix = len(img1.flatten())
        diff = np.subtract(img1, img2)
        # logging.info("diff value :",diff)
        logging.info("diff shape:", diff.shape)
        flat_diff = diff.flatten()
        logging.info("flat_diff shape:", flat_diff.shape)
        logging.info("flat_diff:", flat_diff)
        for i in range(len(flat_diff)):
            if abs(flat_diff[i]) < thresh:
                count += 1
        logging.info("total_pix:", total_pix)
        logging.info("count:", count)
        percent = (count/total_pix)*100
        return percent
    except Exception as e:
        # logging.error(e)
        return 0


def common_infer(interpreter, input_details, input_data, output_details, labels):
    object_names = []
    interpreter.set_tensor(input_details[0]['index'], input_data)
    interpreter.invoke()
    # Comment this line if visualization is not needed
    boxes = interpreter.get_tensor(output_details[0]['index'])[0]
    classes = interpreter.get_tensor(output_details[1]['index'])[0]
    scores = interpreter.get_tensor(output_details[2]['index'])[0]
    for i in range(len(scores)):
        if ((scores[i] > min_conf_threshold) and (scores[i] <= 1.0)):
            object_name = labels[int(classes[i])]
            object_names.append(object_name)
    return object_names
    # return boxes,classes,scores


def crowd_infer(interpreter, input_details, input_data, output_details):
    nop = 0
    interpreter.set_tensor(input_details[0]['index'], input_data)
    interpreter.invoke()
    crowd_output = interpreter.get_tensor(output_details[0]['index'])
    for det in crowd_output[0]:
        if det[5] > 0.5 and det[6] == 1:
            nop += 1
    return nop


def loitering_prediction(interpreter, input_details, output_details, image, CLASSIFIER_CKPT):
    classifier_interpreter = Interpreter(model_path=CLASSIFIER_CKPT)
    classifier_interpreter.allocate_tensors()
    input_details_classifier = classifier_interpreter.get_input_details()
    output_details_classifier = classifier_interpreter.get_output_details()
    _, input_dim, _, _ = input_details[0]['shape']
    _, mp_dim, _, ky_pt_num = output_details[0]['shape']
    image1 = cv2.resize(image, (192, 192))
    image1 = np.reshape(image1, (1, 192, 192, 3))
    # image1 = tf.cast(image1 ,tf.float32)
    image1 = np.asarray(image1, dtype=np.float32)
    interpreter.set_tensor(input_details[0]['index'], image1)
    interpreter.invoke()
    result = interpreter.get_tensor(output_details[0]['index'])
    feat_array = []
    # Process result and create feature array
    res = result.reshape(1, mp_dim**2, ky_pt_num)
    max_idxs = np.argmax(res, axis=1)
    coords = list(map(lambda x: divmod(x, mp_dim), max_idxs))
    feature_vec = np.vstack(coords).T.reshape(2 * ky_pt_num, 1)
    feat_array.append(feature_vec)
    sample = np.array(feat_array).squeeze()
    sample = sample.reshape((1, 28, 1))
    sample = np.asarray(sample, dtype=np.float32)
    classifier_interpreter.set_tensor(
        input_details_classifier[0]['index'], sample)
    classifier_interpreter.invoke()
    # logging.info(output_details_classifier)
    result_classifier = classifier_interpreter.get_tensor(
        output_details_classifier[0]['index'])
    # logging.info(result_classifier)
    return (result_classifier[0][0])


def process_data_helmet_mask_crowd(threadName, inputQueue):
    helmet_interpreter = Interpreter(model_path=HELMET_CKPT)
    mask_interpreter = Interpreter(model_path=MASK_CKPT)
    animal_interpreter = Interpreter(model_path=ANIMAL_CKPT)
    crowd_interpreter = Interpreter(model_path=CROWD_CKPT)
    door_interpreter = Interpreter(model_path=DOOR_CKPT)

    helmet_interpreter.allocate_tensors()
    mask_interpreter.allocate_tensors()
    animal_interpreter.allocate_tensors()
    crowd_interpreter.allocate_tensors()
    door_interpreter.allocate_tensors()
    # Get model details
    # Since mask and helmet input details are same for all model
    input_details = helmet_interpreter.get_input_details()
    crowd_input_details = crowd_interpreter.get_input_details()

    helmet_output_details = helmet_interpreter.get_output_details()
    mask_output_details = mask_interpreter.get_output_details()
    animal_output_details = animal_interpreter.get_output_details()
    crowd_output_details = crowd_interpreter.get_output_details()
    door_output_details = door_interpreter.get_output_details()

    height = input_details[0]['shape'][1]
    width = input_details[0]['shape'][2]
    crowd_height = crowd_input_details[0]['shape'][1]
    crowd_width = crowd_input_details[0]['shape'][2]
    # #uncoment below 3 lines for floating model
    floating_model = (input_details[0]['dtype'] == np.float32)
    crowd_floating_model = (crowd_input_details[0]['dtype'] == np.float32)

    if not inputQueue.empty():
        queueLock.acquire()
        frame1 = inputQueue.get()
        queueLock.release()

        if detect_low_light(frame1, light_thresh) == 1:
            folder = os.path.join(image_dir_lowlight,
                                  datetime.now().strftime("%Y-%m-%d"))
            try:
                if not os.path.exists(folder):
                    os.makedirs(folder)
                filename = os.path.join(folder, datetime.now().strftime(
                    "%Y-%m-%d_%H-%M-%S") + '_' + edgeId + '_lowlight' + '.jpg')
                cv2.imwrite(filename, frame1)
            except Exception as expr:
                logging.error(expr)
            try:
                send_data = {"OBJECTS": ['LOW-LIGHT'], "IMAGE": filename}
                logging.info("LOW-LIGHT")
                s.send(json.dumps(send_data).encode())
            except BrokenPipeError:
                pass
        else:
            imH, imW = frame1.shape[:2]
            h_s, w_s = int(imW/10), int(imW/10)
            # Acquire frame and resize to expected shape [1xHxWx3]
            frame = frame1.copy()
            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            frame_resized = cv2.resize(frame_rgb, (width, height))
            crowd_frame_resized = cv2.resize(
                frame_rgb, (crowd_width, crowd_height))
            # frame_resized =frame_resized.astype(np.uint8)         # added for np.UINT8 quantized model
            input_data = np.expand_dims(frame_resized, axis=0)
            crowd_input_data = np.expand_dims(crowd_frame_resized, axis=0)
            # Normalize pixel values if using a floating model (i.e. if model is non-quantized)
            if floating_model:
                input_data = (np.float32(input_data) - input_mean) / input_std
            if crowd_floating_model:
                crowd_input_data = (np.float32(
                    crowd_input_data) - input_mean) / input_std
            with concurrent.futures.ThreadPoolExecutor() as executor:
                if config_aiva["AIVA"]["MODELS"]["HELMET"]:
                    helmet_future = executor.submit(
                        common_infer, helmet_interpreter, input_details, input_data, helmet_output_details, helmet_labels)
                    helmet_result = helmet_future.result()
                    if NOobject_array['NOHELMET'] == 1:
                        send_data = {"OBJECTS": ['NOHELMET']}
                        s.send(json.dumps(send_data).encode())
                        NOobject_array['NOHELMET'] = 0

                    if 'helmet' in helmet_result:
                        filename = ''
                        folder = os.path.join(
                            image_dir_helmet, datetime.now().strftime("%Y-%m-%d"))
                        try:
                            if not os.path.exists(folder):
                                os.makedirs(folder)
                            filename = os.path.join(folder, datetime.now().strftime(
                                "%Y-%m-%d_%H-%M-%S") + '_' + edgeId + '_Helmet' + '.jpg')
                            cv2.imwrite(filename, frame)
                        except Exception as expr:
                            logging.error(expr)

                        try:
                            send_data = {"OBJECTS": ['HELMET'], "COORDS": [
                                {"X1": 0, "Y1": 0, "X2": 320, "Y2": 240}], "IMAGE": filename}
                            s.send(json.dumps(send_data).encode())
                            NOobject_array['NOHELMET'] = 1
                        except BrokenPipeError:
                            pass

                if NOobject_array['NOMASK'] == 1:
                    send_data = {"OBJECTS": ['NOMASK']}
                    s.send(json.dumps(send_data).encode())
                    NOobject_array['NOMASK'] = 0

                if config_aiva["AIVA"]["MODELS"]["MASK"]:
                    mask_future = executor.submit(
                        common_infer, mask_interpreter, input_details, input_data, mask_output_details, mask_labels)
                    mask_result = mask_future.result()

                    if 'with_mask' in mask_result:
                        filename = ''
                        folder = os.path.join(
                            image_dir_mask, datetime.now().strftime("%Y-%m-%d"))
                        try:
                            if not os.path.exists(folder):
                                os.makedirs(folder)
                            filename = os.path.join(folder, datetime.now().strftime(
                                "%Y-%m-%d_%H-%M-%S") + '_' + edgeId + '_Mask' + '.jpg')
                            cv2.imwrite(filename, frame)
                        except Exception as expr:
                            logging.error(expr)

                        try:
                            send_data = {"OBJECTS": ['MASK'], "COORDS": [
                                {"X1": 0, "Y1": 0, "X2": 320, "Y2": 240}], "IMAGE": filename}
                            s.send(json.dumps(send_data).encode())
                            NOobject_array['NOMASK'] = 1
                        except BrokenPipeError:
                            pass

                if config_aiva["AIVA"]["MODELS"]["ANIMAL"]:
                    animal_future = executor.submit(
                        common_infer, animal_interpreter, input_details, input_data, animal_output_details, animal_labels)
                    animal_result = animal_future.result()
                    if NOobject_array['NOANIMAL'] == 1:
                        send_data = {"OBJECTS": ['NOANIMAL']}
                        s.send(json.dumps(send_data).encode())
                        NOobject_array['NOANIMAL'] = 0

                    if 'animal' in animal_result:
                        filename = ''
                        folder = os.path.join(
                            image_dir_animal, datetime.now().strftime("%Y-%m-%d"))
                        try:
                            if not os.path.exists(folder):
                                os.makedirs(folder)
                            filename = os.path.join(folder, datetime.now().strftime(
                                "%Y-%m-%d_%H-%M-%S") + '_' + edgeId + '_Animal' + '.jpg')
                            cv2.imwrite(filename, frame)
                        except Exception as expr:
                            logging.error(expr)

                        try:
                            send_data = {"OBJECTS": ['ANIMAL'], "COORDS": [
                                {"X1": 0, "Y1": 0, "X2": 320, "Y2": 240}], "IMAGE": filename}
                            s.send(json.dumps(send_data).encode())
                            NOobject_array['NOANIMAL'] = 1
                        except BrokenPipeError:
                            pass

                if config_aiva["AIVA"]["MODELS"]["DOOR"]:
                    door_future = executor.submit(
                        common_infer, door_interpreter, input_details, input_data, door_output_details, door_labels)
                    door_result = door_future.result()

                    if NOobject_array['NODOOR'] == 1:
                        send_data = {"OBJECTS": ['NODOOR']}
                        s.send(json.dumps(send_data).encode())
                        NOobject_array['NODOOR'] = 0

                    if 'open' in door_result:
                        filename = ''
                        folder = os.path.join(
                            image_dir_door, datetime.now().strftime("%Y-%m-%d"))
                        try:
                            if not os.path.exists(folder):
                                os.makedirs(folder)
                            filename = os.path.join(folder, datetime.now().strftime(
                                "%Y-%m-%d_%H-%M-%S") + '_' + edgeId + '_Door' + '.jpg')
                            cv2.imwrite(filename, frame)
                        except Exception as expr:
                            logging.error(expr)

                        try:
                            send_data = {"OBJECTS": ['DOOR'], "COORDS": [
                                {"X1": 0, "Y1": 0, "X2": 320, "Y2": 240}], "IMAGE": filename}
                            s.send(json.dumps(send_data).encode())
                            NOobject_array['NODOOR'] = 1
                        except BrokenPipeError:
                            pass

                if config_aiva["AIVA"]["MODELS"]["CROWD"]:
                    crowd_future = executor.submit(
                        crowd_infer, crowd_interpreter, crowd_input_details, crowd_input_data, crowd_output_details)
                    crowd_result = crowd_future.result()

                    if NOobject_array['NOCROWD'] == 1:
                        send_data = {"OBJECTS": ['NOCROWD']}
                        s.send(json.dumps(send_data).encode())
                        NOobject_array['NOCROWD'] = 0

                    if crowd_result > nop_thresh:
                        filename = ''
                        folder = os.path.join(
                            image_dir_crowd, datetime.now().strftime("%Y-%m-%d"))
                        try:
                            if not os.path.exists(folder):
                                os.makedirs(folder)
                            filename = os.path.join(folder, datetime.now().strftime(
                                "%Y-%m-%d_%H-%M-%S") + '_' + edgeId + '_Crowd' + '.jpg')
                            cv2.imwrite(filename, frame)
                        except Exception as expr:
                            logging.error(expr)

                        try:
                            send_data = {"OBJECTS": ['CROWD'], "COORDS": [
                                {"X1": 0, "Y1": 0, "X2": 320, "Y2": 240}], "IMAGE": filename, "PERSON_COUNT": crowd_result}
                            s.send(json.dumps(send_data).encode())
                            NOobject_array['NOCROWD'] = 1
                        except BrokenPipeError:
                            pass


def process_data_hidden_tilted(threadName, inputQueueHiddenTiltedCopy):

    frame_count = 0
    percent_thresh = 0.75
    thresh_pix = 100
    light_thresh = 90
    maxsize = 25
    N = maxsize - 1
    frames = []
    test_count = 0
    logging.info(1)
    logging.info(inputQueueHiddenTiltedCopy)
    while(not inputQueueHiddenTiltedCopy.empty()):
        queueLock.acquire()
        frame1 = inputQueueHiddenTiltedCopy.get()
        queueLock.release()
        logging.info(2)

        try:
            frame_count += 1

            if detect_low_light(frame1, light_thresh) == 1:
                send_data = {"OBJECTS": ['LOW-LIGHT']}
                logging.info("LOW-LIGHT")
                s.send(json.dumps(send_data).encode())
            else:
                imH, imW = frame1.shape[:2]
                h_s, w_s = int(imW/10), int(imW/10)
                if len(frames) < maxsize:
                    frames.append(frame1)
                    # logging.info("test_count:",test_count)
                    test_count += 1
                    # logging.info("Frames Size before poping:",len(frames))
                if len(frames) == maxsize:
                    last_frame = frames.pop()
                    orig = last_frame.copy()
                    # logging.info("Frames Size after poping:",len(frames))
                    images = np.array(frames)
                    bench_img = np.array(
                        np.mean(images, axis=(0)), dtype=np.uint8)

                    if config_aiva["AIVA"]["MODELS"]["TILTED"]:
                        top_left_roi_bench = bench_img[0:h_s, 0:w_s]
                        top_right_roi_bench = bench_img[0:h_s, imW-w_s:imW]
                        bottom_left_roi_bench = bench_img[imH -
                                                          h_s:imH, 0:w_s]
                        bottom_right_roi_bench = bench_img[imH -
                                                           h_s:imH, imW-w_s:imW]
                        top_left_roi = last_frame[0:h_s, 0:w_s]
                        top_right_roi = last_frame[0:h_s, imW-w_s:imW]
                        bottom_left_roi = last_frame[imH-h_s:imH, 0:w_s]
                        bottom_right_roi = last_frame[imH -
                                                      h_s:imH, imW-w_s:imW]
                        score_top_left, diff1 = compare_ssim(
                            top_left_roi_bench, top_left_roi, full=True, multichannel=True)
                        score_top_right, diff2 = compare_ssim(
                            top_right_roi_bench, top_right_roi, full=True, multichannel=True)
                        score_bottom_left, diff3 = compare_ssim(
                            bottom_left_roi_bench, bottom_left_roi, full=True, multichannel=True)
                        score_bottom_right, diff4 = compare_ssim(
                            bottom_right_roi_bench, bottom_right_roi, full=True, multichannel=True)

                        if (score_top_right > percent_thresh) and (score_bottom_right > percent_thresh):
                            pass
                        if (score_top_left > percent_thresh) and (score_bottom_left > percent_thresh):
                            pass
                        elif (score_top_left > percent_thresh) and (score_top_right > percent_thresh) and (score_bottom_right > percent_thresh):
                            pass
                        elif (score_top_left > percent_thresh) and (score_top_right > percent_thresh) and (score_bottom_left > percent_thresh) and (score_bottom_right > percent_thresh):
                            pass
                        else:
                            filename = ''
                            folder = os.path.join(
                                image_dir_tilted, datetime.now().strftime("%Y-%m-%d"))

                            if NOobject_array['NOTILTED'] == 1:
                                send_data = {"OBJECTS": ['NOTILTED']}
                                s.send(json.dumps(send_data).encode())
                                NOobject_array['NOTILTED'] = 0

                            if NOobject_array['NOTILTED'] == 0:
                                try:
                                    if not os.path.exists(folder):
                                        os.makedirs(folder)
                                    filename = os.path.join(folder, datetime.now().strftime(
                                        "%Y-%m-%d_%H-%M-%S") + '_' + edgeId + '_Tilted' + '.jpg')
                                    cv2.imwrite(filename, frame1)
                                except Exception as expr:
                                    logging.error(expr)

                                try:
                                    send_data = {"OBJECTS": ['TILTED'], "COORDS": [
                                        {"X1": 0, "Y1": 0, "X2": 320, "Y2": 240}], "IMAGE": filename}
                                    s.send(json.dumps(send_data).encode())
                                    NOobject_array['NOTILTED'] = 1
                                except BrokenPipeError:
                                    pass

                    logging.info(3)
                    if config_aiva["AIVA"]["MODELS"]["HIDDEN"]:
                        uniq_pix = np.unique(bench_img)
                        logging.info(uniq_pix.shape[0])
                        if uniq_pix.shape[0] < thresh_pix:
                            filename = ''
                            folder = os.path.join(
                                image_dir_hidden, datetime.now().strftime("%Y-%m-%d"))

                            if NOobject_array['NOHIDDEN'] == 1:
                                send_data = {"OBJECTS": ['NOHIDDEN']}
                                s.send(json.dumps(send_data).encode())
                                NOobject_array['NOHIDDEN'] = 0

                            if(NOobject_array['NOHIDDEN'] == 0):
                                try:
                                    if not os.path.exists(folder):
                                        os.makedirs(folder)
                                    filename = os.path.join(folder, datetime.now().strftime(
                                        "%Y-%m-%d_%H-%M-%S") + '_' + edgeId + '_Hidden' + '.jpg')
                                    cv2.imwrite(filename, frame1)
                                    logging.info("Hidden")
                                except Exception as expr:
                                    logging.error(expr)

                                try:
                                    send_data = {"OBJECTS": ['HIDDEN'], "COORDS": [
                                        {"X1": 0, "Y1": 0, "X2": 320, "Y2": 240}], "IMAGE": filename}
                                    s.send(json.dumps(send_data).encode())
                                    NOobject_array['NOHIDDEN'] = 1
                                except BrokenPipeError:
                                    logging.error(BrokenPipeError)
                                    pass

                    # Updating the frames list
                    frames.pop(0)
                    frames.append(last_frame)

        except Exception as e:
            pass

        time.sleep(1)


def process_data_loitering(threadName, inputQueue, count, check):
    loitering_interpreter = Interpreter(model_path=LOITERING_CKPT)
    check = check
    if(count % 3 == 0):
        check.clear()

    loitering_interpreter.allocate_tensors()
    # Get model details
    loitering_input_details = loitering_interpreter.get_input_details()

    loitering_output_details = loitering_interpreter.get_output_details()

    if not inputQueue.empty():
        queueLock.acquire()
        frame1 = inputQueue.get()
        queueLock.release()

        if detect_low_light(frame1, light_thresh) == 1:
            send_data = {"OBJECTS": ['LOW-LIGHT']}
            logging.info("LOW-LIGHT")
            s.send(json.dumps(send_data).encode())
        else:
            imH, imW = frame1.shape[:2]
            h_s, w_s = int(imW/10), int(imW/10)
            # Acquire frame and resize to expected shape [1xHxWx3]
            frame = frame1.copy()
            loitering = 0
            # frame_resized =frame_resized.astype(np.uint8)         # added for np.UINT8 quantized model
            # Normalize pixel values if using a floating model (i.e. if model is non-quantized)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                if config_aiva["AIVA"]["MODELS"]["LOITERING"]:
                    loitering_future = executor.submit(
                        loitering_prediction, loitering_interpreter, loitering_input_details, loitering_output_details, frame, CLASSIFIER_CKPT)
                    loitering_result = loitering_future.result()

                    if loitering_result > 0.7:
                        loitering = 1
                        check.append(loitering)
                        # logging.info(check)
                        if(np.sum(np.array(check)[:3]) == 2):
                            filename = ''
                            folder = os.path.join(
                                image_dir_loitering, datetime.now().strftime("%Y-%m-%d"))

                            if NOobject_array['NOLOITERING'] == 1:
                                send_data = {"OBJECTS": ['NOLOITERING']}
                                s.send(json.dumps(send_data).encode())
                                NOobject_array['NOLOITERING'] = 0

                            if NOobject_array['NOLOITERING'] == 0:
                                try:
                                    if not os.path.exists(folder):
                                        os.makedirs(folder)
                                    filename = os.path.join(folder, datetime.now().strftime(
                                        "%Y-%m-%d_%H-%M-%S") + '_' + edgeId + '_Loitering' + '.jpg')
                                    cv2.imwrite(filename, frame)
                                except Exception as expr:
                                    logging.error(expr)

                                try:
                                    send_data = {"OBJECTS": ['LOITERING'], "COORDS": [
                                        {"X1": 0, "Y1": 0, "X2": 320, "Y2": 240}], "IMAGE": filename}
                                    s.send(json.dumps(send_data).encode())
                                    NOobject_array['NOLOITERING'] = 1
                                except BrokenPipeError:
                                    pass

                    else:
                        check.append(0)
                        # logging.info(check)

            time.sleep(1)


def sharpness(img):
    img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    lap = cv2.Laplacian(img, cv2.CV_16S)
    mean, stddev = cv2.meanStdDev(lap)
    return stddev[0, 0]


def avg(frames_sharpness):
    return_data = sum(frames_sharpness) / len(frames_sharpness)
    return return_data

def hiddensh(frame, base_value = None):
    try:

        if base_value == None:
            base_value = sharpness(frame)
            logging.info("base_value:{}".format(base_value))
        else:
            val = sharpness(frame)
            if((base_value-val) > THRESH_DETECT):
                hstatus = "HIDDEN"
                filename = ''
                folder = os.path.join(
                    image_dir_hidden, datetime.now().strftime("%Y-%m-%d"))

                if NOobject_array['NOHIDDEN'] == 1:
                    send_data = {"OBJECTS": ['NOHIDDEN']}
                    s.send(json.dumps(send_data).encode())
                    NOobject_array['NOHIDDEN'] = 0
                if(NOobjectarray['NOHIDDEN'] == 0):
                    try:
                        if not os.path.exists(folder):
                            os.makedirs(folder)
                        filename = os.path.join(folder, datetime.now().strftime(
                            "%Y-%m-%d%H-%M-%S") + '_' + edgeId + '_Hidden' + '.jpg')
                        cv2.imwrite(filename, frame)
                    except Exception as expr:
                        logging.error(expr)

                try:
                    send_data = {"OBJECTS": ['HIDDEN'], "COORDS": [
                        {"X1": 0, "Y1": 0, "X2": 320, "Y2": 240}], "IMAGE": filename}
                    s.send(json.dumps(send_data).encode())
                    NOobject_array['NOHIDDEN'] = 1
                except BrokenPipeError:
                    pass
            else:
                hstatus = "NOHIDDEN"
                # pass
            logging.info("{},{},{}".format(base_value, val, hstatus))
            # print(base_value,val,status)
        # reseting frame list
        time.sleep(.25)
        return base_value

    except Exception as e:
        logging.error(e)
        
def _hiddensh(frame):
    global THRESH_FRAMES, THRESH_DETECT, initlize, bvalues, hstatus, base_value, base_value
    try:
        # ret,frame=vs.read()
        #ret,frame = fvs.read()
        """if not ret:
            #vs=cv2.VideoCapture(config_video["CAMERAS"][1]["URL"])

            time.sleep(0.5)"""
        # To identify the baseline threshold which is used to identify the hidden status
        if len(bvalues) <= THRESH_FRAMES:
            bvalue = sharpness(frame)
            # print("bvalue:",bvalue)
            bvalues.append(bvalue)
        else:
            bvalue = sharpness(frame)
            # logging.info("bvalue:{}".format(bvalue))
            bvalues.append(bvalue)  # taking 10th frames

            if initlize == 0:
                base_value = avg(bvalues)
                # logging.info("avg:{}".format(base_value))
                # print("avg:",base_value)
                initlize = 1
            else:
                val = avg(bvalues)
                if((base_value-val) > THRESH_DETECT):
                    hstatus = "HIDDEN"
                    hstatus = "HIDDEN"
                    filename = ''
                    folder = os.path.join(
                        image_dir_hidden, datetime.now().strftime("%Y-%m-%d"))

                    if NOobject_array['NOHIDDEN'] == 1:
                        send_data = {"OBJECTS": ['NOHIDDEN']}
                        s.send(json.dumps(send_data).encode())
                        NOobject_array['NOHIDDEN'] = 0
                    if(NOobject_array['NOHIDDEN'] == 0):
                        try:
                            if not os.path.exists(folder):
                                os.makedirs(folder)
                            filename = os.path.join(folder, datetime.now().strftime(
                                "%Y-%m-%d_%H-%M-%S") + '_' + edgeId + '_Hidden' + '.jpg')
                            cv2.imwrite(filename, frame)
                        except Exception as expr:
                            logging.error(expr)

                    try:
                        send_data = {"OBJECTS": ['HIDDEN'], "COORDS": [
                            {"X1": 0, "Y1": 0, "X2": 320, "Y2": 240}], "IMAGE": filename}
                        s.send(json.dumps(send_data).encode())
                        NOobject_array['NOHIDDEN'] = 1
                    except BrokenPipeError:
                        pass
                else:
                    hstatus = "NOHIDDEN"
                    # pass
                logging.info("{},{},{}".format(base_value, val, hstatus))
                # print(base_value,val,status)
            # reseting frame list
            bvalues = []
            time.sleep(.25)
    except Exception as e:
        logging.error(e)
        pass


def threadVideoGet(source=0):
    base_value=None
    # We need to identify the reason for bringing in the scheduler here when camera was disconnected
    '''try:
        if config_aiva["AIVA"]['SCHEDULER']['ENABLED']:
            val = source.split("@")
            hostname = val[1].split("/")
            logging.info('hostmame {}'.format(hostname[0]))

            schedule_time = json.dumps(config_aiva["AIVA"]['SCHEDULER']['MINUTES'])
            schedule_time = schedule_time.replace('"', '')

            check_ping(hostname[0])
            schedule.every(int(schedule_time)).minutes.do(
                check_ping, hostname[0])
            threading.Thread(target=scheduler).start()
        else:
            pass
        # logging.info("[INFO] starting video file thread...")
    except Exception as e:
        # logging.error(e)
        pass'''

    #fvs = FileVideoStream(source).start()
    fvs = cv2.VideoCapture(source)
    time.sleep(1.0)
    fps = FPS().start()
    frame_count = 0
    count = 0

    # while fvs.more():
    while 1:
        process = psutil.Process(os.getpid())
        logging.info("start:{}".format(process.memory_info().vms))  # in bytes

        # grab the frame from the threaded video file stream, resize
        # it, and convert it to grayscale (while still retaining 3 channels)
        ret, frame = fvs.read()
        if not ret:
            #logging.info('Something wrong with camera frames reading')
            fvs = cv2.VideoCapture(source)
            time.sleep(1.0)
            fps = FPS().start()
        else:
            frame = np.asarray(frame)
            frame_count += 1
            # logging.info(frame_count)
            if(frame.ndim == 0):
                break
            else:

                # Resetting frame_count to avoid integer overflow
                if(frame_count > 42):
                    frame_count = 0
                
                if(frame_count % 42 == 0):
                    queueLock.acquire()
                    inputQueue.put(frame)
                    inputQueueHiddenTilted.put(frame)
                    inputQueueLoitering = copy.copy(inputQueue)
                    queueLock.release()

                    list_threads = []

                    helmetThread = helmetMaskCrowdThreadClass(
                        2, "Thread-Helmet", inputQueue)
                    list_threads.append(helmetThread)
                    time.sleep(5)
                    helmetThread.start()

                    loiteringThread = loiteringThreadClass(
                        4, "Thread-Loitering", inputQueueLoitering, count, check)
                    count = count + 1
                    list_threads.append(loiteringThread)
                    time.sleep(5)
                    loiteringThread.start()
                    
                    base_value=hiddensh(frame,base_value)
                    logging.info("End:{}".format (process.memory_info().vms))                        
                    for t in list_threads:
                        t.join()

                

                cv2.waitKey(1)

            fps.update()
        
          # Calling hidden function
        
    
threadVideoGet(vid_path)
 
# stopping the library
# Why scheduler was introduced when camera disconnected
# Check why scheduler is disabling aiva functionality
# check for network package functionality to ping and not use os.system
# use os.makedirs instead of os.system with mkdir
# AIVA/object
# no object message PA
# include image for low light - updated completed
# when hidden camera low light image is showing
