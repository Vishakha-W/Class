import elasticsearch
import requests
import base64
import glob
import os
import queue
import sys
from yapsy.PluginManager import PluginManager
import logging
logging.basicConfig(level=logging.DEBUG)
from yapsy.IPlugin import IPlugin
from plugins.plugin1 import PluginOne
import threading

import csv
count=0
import os
global AY ,AN
import xlrd 


class myClass(threading.Thread):
    def __init__(self,thread_id,chunk_size,chunk_size_inc,q):
        threading.Thread.__init__(self)
        self.thread_id=thread_id
        self.chunk_size = chunk_size
        self.chunk_size_inc=chunk_size_inc
        self.q=q
        es = elasticsearch.Elasticsearch() # by default it takes 9200
        print(es.cat.health())


        
    def run(self):
        global data
        print(threading.current_thread().getName(),"Starting")
        fo=f.read(self.chunk_size_inc)
        self.q.put(fo)
        f.seek(self.chunk_size)
        print("contents: ",fo,"\n\n\n")
        return                                 
                                
    def create_pipeline(self):
        body = {
               "description" : "Extract attachment information", 
               "processors" : [
               "attachment" : {
               "field" : "data",
               "properties": [ "AY", "AN" ]
                 }
                }
                  ]
                   }
        
    def load_plugin(self):  
        manager = PluginManager()
        manager.setPluginPlaces(["plugins"])
        manager.collectPlugins()
    # Loop round the plugins and print their names.
        for plugin in manager.getAllPlugins():
            print("==========>  ",format(plugin.plugin_object))
        #p="C:\Python34\directory1"
        #print("path is:======>",p)
            p1=plugin.plugin_object.print_name()
            print(p1) 
            p2=PluginOne()
            p2.g()
                    
                 
    def extract_data_from(self):               
               

def main():   
    global p1
    #  Load the plugins from the plugin directory.
    glob.glob(p1)
    os.chdir(p1)
    for file1 in glob.glob("*.csv"):
           with open(file1, 'rt') as f:
                   reader = csv.reader(f, delimiter=',')
               #header = reader.next()
                  # print(header)
                   for row in reader:
                  # count=count+1
                       fieldcnt=0
                  # print(count)
                       for field in row:             
                           if  field == "Assessment Year":
                               print("is in file")
                               print("hi ",reader.line_num)
                               c=reader.line_num
                               for row in reader:
                                   AY=row[fieldcnt]
                                   print(AY)
                                   break
                           if  field=="Name and address of the Employer":
                               print("its in the file")
                               for row in reader:
                                   AN=row[fieldcnt]
                                   print(AN)
                                   break
                           fieldcnt=fieldcnt+1
           print("************************************",file1)    


       
        
if __name__ == "__main__":
    main()
data1=" "
       for file in glob.glob(file1+".txt"):
              print("========>",file)
              with open(file, 'r') as f:
                      file_path=os.path.dirname(os.path.abspath(file))
                      url=file_path+"\\"+file
                      print(url)
                      file_size = os.path.getsize(file)
                      print('File size: {}'.format(file_size))
                      filesize='File size: {}'.format(file_size)
                      chunk_size =(int)(file_size/10)
                      print("chunk_size is",chunk_size)
                      chunk_size_inc=chunk_size
                      print("==>",f)
                      data=" "
                      data1=" "
                      for i in range(10):
                          t1= myClass(i,chunk_size,chunk_size_inc,my_queue) 
                          t1.start()
                          chunk_size=chunk_size+chunk_size_inc
                          threads.append(t1)
                          data=my_queue.get()
                          data1=data1+data
                    #print("data ",data)
                      for x in threads: 
                          x.join()
              data1 = base64.b64encode(bytes(data1,'utf-8')).decode('ascii');
              result2 = es.index(index='my_index', doc_type='my_type', pipeline='attachment',body={'data': data1,"AY":AY,"AN":AN})    
              print ("Exiting Main Thread")
              f.close()
        
        
       

