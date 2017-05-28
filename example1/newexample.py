

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


class Excel(threading.Thread):
    def __init__(self):
        self.es = elasticsearch.Elasticsearch() # by default it takes 9200
        print(self.es.cat.health())

        
    
    def create_pipeline(self):
        #self.AY=AY
        #self.AN=AN
        body = {
          "description" : "Extract attachment information",
          "processors" : [
                    {
          "attachment" : {
          "field" : "data",
          "properties": [ self.AY, self.AN ]
       
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
            self.p1=plugin.plugin_object.print_name()
            print(self.p1) 
            self.p2=PluginOne()
            self.p2.g()

            

    def getAssesement_year(self,file1):
                with open(file1, 'rt') as self.f:
                        reader = csv.reader(self.f, delimiter=',')
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
                                        self.AY=row[fieldcnt]
                                        print(self.AY)
                                        break
                                fieldcnt=fieldcnt+1    
                print("************************************",file1)



      
    def getAssesse_name(self,file1):      
                with open(file1, 'rt') as self.f:
                        reader = csv.reader(self.f, delimiter=',')
                        #header = reader.next()
                        # print(header)
                        for row in reader:
                            # count=count+1
                            fieldcnt=0
                            # print(count)
                            for field in row:                                        
                                if  field=="Name and address of the Employer":
                                    print("its in the file")
                                    for row in reader:
                                        self.AN=row[fieldcnt]
                                        print(self.AN)
                                        break
                                fieldcnt=fieldcnt+1
                print("************************************",file1)

    def myfunc(self,i,chunk_size,chunk_size_inc,my_queue):
        global data
        print(threading.current_thread().getName(),"Starting")
        self.fo=self.f.read(self.chunk_size_inc)
        self.my_queue.put(self.fo)
        self.f.seek(self.chunk_size)
        print("contents: ",self.fo,"\n\n\n")
        return 

    def indexing(self,file11):
        self.threads=[]
        self.data1=" "
        self.my_queue = queue.Queue()
        #glob.glob(self.p1)
        #os.chdir(self.p1)
        #for self.file11 in glob.glob(self.file1+".txt"):
        print("========>",file11)
        with open(file11, 'r') as self.f:
                      self.file_size = os.path.getsize(file11)
                      print('File size: {}'.format(self.file_size))
                      self.filesize='File size: {}'.format(self.file_size)
                      self.chunk_size =(int)(self.file_size/10)
                      print("chunk_size is",self.chunk_size)
                      self.chunk_size_inc=self.chunk_size
                      print("==>",self.f)
                      self.data=" "
                      self.data1=" "
                      for i in range(10):
                          t1= threading.Thread(target=self.myfunc,args=(i,self.chunk_size,self.chunk_size_inc,self.my_queue,)) 
                          t1.start()
                          self.chunk_size=self.chunk_size+self.chunk_size_inc
                          self.threads.append(t1)
                          self.data=self.my_queue.get()
                          self.data1=self.data1+self.data
                    #print("data ",data)
                      for x in self.threads: 
                          x.join()
        self.data1 = base64.b64encode(bytes(self.data1,'utf-8')).decode('ascii');
        result2 = self.es.index(index='my_index', doc_type='my_type', pipeline='attachment',body={'data': self.data1,"AY":self.AY,"AN":self.AN})    
        print ("Exiting Main Thread")
        self.f.close()



    def getIpaddress(self):
        self.ip=socket.gethostbyname(socket.gethostname())



    

def main():
    obj= Excel()
    obj.load_plugin()
    glob.glob(obj.p1)
    os.chdir(obj.p1)
    for file1 in glob.glob("*.csv"):
        print("*********",file1)
        obj.getAssesement_year(file1)
        obj.getAssesse_name(file1)
        obj.create_pipeline()
        for file11 in glob.glob(file1+".txt"):
            print()
            obj.indexing(file11)
            
    
if __name__ == "__main__":
    main()




        
