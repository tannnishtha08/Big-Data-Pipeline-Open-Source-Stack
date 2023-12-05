# importing the library to run the commands in the python file
import os
import time

home = os.environ['HOME']
os.chdir(home)
os.chdir("test_airbyte/product_names")
os.getcwd()
os.listdir()

# get the latest file name to create new files and push the files in the data lake
zero_command = '''hadoop fs -ls -R /ecomm_data/data_lake/orders | awk -F" " '{print $6" "$7" "$8}' | sort -nr | head -1 | cut -d" " -f3'''
curr_file = os.popen(zero_command).read()
file_name = int(str(curr_file).split("/")[4].split(".")[0][:-7])+1

# moving order from local to data lake
first_command = "echo 'delhi_110062' | sudo -S mv _airbyte_raw_ordersorders.csv "+str(file_name)+"_orders.csv"
os.system(first_command)
second_command = "hdfs dfs -copyFromLocal /home/hdoop/test_airbyte/product_names/"+str(file_name)+"_orders.csv /ecomm_data/data_lake/orders"
os.system(second_command)
third_command = "echo 'delhi_110062' | sudo rm "+str(file_name)+"_orders.csv"
os.system(third_command)

# moving order from local to data lake
fourth_command = "echo 'delhi_110062' | sudo -S mv _airbyte_raw_orderscompleted_orders.csv "+str(file_name)+"_completed_orders.csv"
os.system(fourth_command)
fifth_command = "hdfs dfs -copyFromLocal /home/hdoop/test_airbyte/product_names/"+str(file_name)+"_completed_orders.csv /ecomm_data/data_lake/completed_orders"
os.system(fifth_command)
sixth_command = "echo 'delhi_110062' | sudo rm "+str(file_name)+"_completed_orders.csv"
os.system(sixth_command)
