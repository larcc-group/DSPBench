import os
import time
import datetime;

def start_job(app):
    print("start job")
    # ./bin/dspbench-flink-cluster.sh /home/DSPBench/dspbench-flink/build/libs/dspbench-flink-uber-1.0.jar wordcount /home/DSPBench/dspbench-flink/build/resources/main/config/wordcount.properties
    os.system('./bin/dspbench-flink-cluster.sh /home/DSPBench/dspbench-flink/build/libs/dspbench-flink-uber-1.0.jar ' + app + ' /home/DSPBench/dspbench-flink/build/resources/main/config/' + app + '.properties')

def cancel_job():
    print("cancel job")
    id = os.popen("~/flink-1.17.1/bin/flink list | grep RUNNING | awk -F ':' '{print $4}'").read()
    os.system('~/flink-1.17.1/bin/flink cancel ' + str(id))

init_time = datetime.datetime.now()
start_job("wordcount")
time.sleep(300) #5 min
#cancel_job()