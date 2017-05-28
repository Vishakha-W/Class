from yapsy.IPlugin import IPlugin
from configparser import ConfigParser
from xml.etree.ElementTree import ElementTree
import xml.etree.ElementTree as ET
import os,glob,xlrd
import string
import sys
class PluginOne(IPlugin):
    def __init__(self):
        print("This is plugin 1")
        parser = ConfigParser()
        print(parser.read('C:\Python34\example\c1.ini'))
        print(parser.get('path', 'dirpath'))
        path=parser.get('path', 'dirpath')
        self.filename=path    

   
       

    def print_name(self):
        print("This is plugin 1")
        return self.filename
   
  
    def convert_from_xml(self):
  #     filename="C:\Python34\directory1"
        print(self.filename)
        glob.glob(self.filename)
        os.chdir(self.filename)
        for file in glob.glob("*.xml"):
             	        with open(file, 'r') as f:
                                yield file
                                tree=ET.parse(file)
                                doc=ET.tostring(tree.getroot(), encoding='utf-8',method='text')
                                print("result is",doc)
                                print("after ",doc.splitlines())
                                out = open(file + ".txt", "w")
                                out.write(str(doc))
                                return True
     
     
    
    def convert_from_csv(self):
        print(self.filename)
        glob.glob(self.filename)
        os.chdir(self.filename)
        for file in glob.glob("*.csv"):
             	        with open(file, 'r') as f:
                                yield file
                                outFile= open(file+".txt", 'w')
                                for line in f:
                                    elmCellSize = line.split(",")
                                    print(elmCellSize[0:10])
                                    for i in elmCellSize:
                                        outFile.write(i)
                                
                        

    
    
    def convert_from_xlsx(self):
        #arg="C:/python34/conversion"
        #print("args is==>",arg)    
        #filename="C:\Python34\directory1"
        glob.glob(self.filename)
        os.chdir(self.filename)
        print(os)
        for file in glob.glob("*.xlsx"):
            yield file 
            print("filename==>",file)
            workbook=xlrd.open_workbook(file)
            arr=workbook.sheet_names()
            print(arr) 
            out=open(file+".txt","w") 
            for sheet in arr:            
                arr=workbook.sheet_names()
                sh=workbook.sheet_by_name(sheet)
                print("row is",sh.nrows)
                print("column is",sh.ncols)
                print(sheet)
                n=0
                i=0     
                for n in range(sh.nrows):
                    out.write("\t")
                    for i in range(sh.ncols):
                        data =sh.cell_value(n,i)
                        print(data)
                        out.write(str(data))
                        out.write("\t")






    def generator(self):
      #  for i in A.convert_from_xml(self):
       #     print(i)
       # for j in A.convert_from_xlsx(self):
        #    print(j)
        yield from PluginOne.convert_from_xml(self)
        yield from PluginOne.convert_from_xlsx(self)
        yield from PluginOne.convert_from_csv(self)


    def g(self):
        for i in PluginOne.generator(self):
            print(i)   



#a=PluginOne()
#a.g()

   
