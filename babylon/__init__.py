import boto3
import uuid
import json
import os
import time
import calendar
import datetime
import uuid
import requests 
from requests.auth import HTTPBasicAuth
import json



class Config:
    def __init__(self,s3_access, s3_secret , s3_endpoint ,bucket,data_provider,stage='raw',partition_type='a',atlas_username="admin" , atlas_password="admin", atlas_endpoint="localhost:21000" ):
        self.bucket = bucket 
        self.data_provider = data_provider
        self.partition_type = partition_type
        self.stage = stage
        self.s3 = boto3.client(service_name='s3',
                aws_access_key_id=s3_access,
                aws_secret_access_key=s3_secret,
                endpoint_url=s3_endpoint)
        self.atlas_username = atlas_username
        self.atlas_password = atlas_password
        self.atlas_endpoint = atlas_endpoint
    


    def generate_object_metdata(self,partition_type):
    
    ### returns ('s3 path' , 'file name')
    ### partition types
    # h - hourly
    # d - daily
    # w - weekly
    # a - adhoc

        gmtime = time.gmtime(calendar.timegm(time.gmtime()))
        year = gmtime[0]
        month = gmtime[1]
        mday = gmtime[2]
        hour = gmtime[3]

        dt = datetime.date(year, month, mday)
        week = dt.isocalendar()[1]
        epoc = calendar.timegm(time.gmtime())

        if (partition_type == 'h' ): 
            return ( '/'+str(year) +'/' + str(month) + '/' + str(mday) + '/' + str(hour) +"/" , str(epoc) + "_" + str(uuid.uuid4())[-4:] )

        if (partition_type == 'd'):
            return ('/' + str(year) + '/' + str(month) + '/' + str(mday) +"/", str(epoc) + "_" + str(uuid.uuid4())[-4:])
            

        if (partition_type == 'w'):
            return ('/' + str(year) + '/' + str(week)+"/", str(epoc) + "_" + str(uuid.uuid4())[-4:])

        if (partition_type == 'a'):
            return  ('/', str(epoc) + "_" + str(uuid.uuid4())[-4:])

    
    def create_atlas_json(self,metdata= {}):
        (s3_path , file_name) = self.generate_object_metdata(self.partition_type)
        
        if "qualifiedName" in metdata:
            qualifiedName = metdata["qualifiedName"]
        else: 
            qualifiedName  =  "/" + self.bucket + "/"+ self.data_provider +"/" +self.stage  +  s3_path + file_name  

        if "name" in metdata:
            name = metdata["name"]
        else: name = file_name
        
        if "fileFormat" in metdata:
            fileFormat = metdata["fileFormat"]
        else: 
            fileFormat = ""

        if "description" in metdata:
            description = metdata["description"]
        else: 
            description = "S3 object"

        if "createBy" in metdata:
            createBy = metdata["createBy"]
        else: 
            createBy = ""
        
        if "createTime" in metdata:
            createTime = metdata["createTime"]
        else: 
            gmtime = time.gmtime(calendar.timegm(time.gmtime()))
            createTime =  calendar.timegm(time.gmtime())

        if "owner" in metdata:
            owner = metdata["owner"]
        else: 
            owner = ""
        
        if "acl" in metdata:
            acl = metdata["acl"]
        else: 
            acl = ""

        json_string = json.dumps({"entities":[
                                                {
                                                    "typeName" : "custom_s3_object_1",
                                                    "qualifiedName":qualifiedName,
                                                    "name":name,
                                                    "fileFormat":fileFormat,
                                                    "description":description,
                                                    "createBy":createBy,
                                                    "createTime":createTime,
                                                    "owner":owner,
                                                    "acl":acl
                                                }
                                            ]
                                        }
                                    )

        print(json_string)
        return json_string

    def upload_data(self,file,path,object_metadata):
        self.s3.upload_file(file,self.bucket)

    def tag_data(self,object_metadata):
        headers = {'Content-Type' : 'application/json', 'Accept':'application/json'}
        r = requests.post(self.atlas_endpoint,auth=HTTPBasicAuth(self.atlas_username,self.atlas_password),headers=headers, data = object_metadata)
        print(r.text)


    def upload_and_tag(self,file,path,metadata):
        object_metadata = self.create_atlas_json(metadata)
        self.tag_data(object_metadata)

        


        
def main():
    
    babylon = Config(s3_access = "CXAD0ORH0HPU3FJ4IKCQ",
        s3_secret = "bYpeW2byIZ5p7dCMvwL7Mt9nucXmBqsgfngdQtDU",
        s3_endpoint = "http://10.3.178.217:8000",
        bucket = "example-project",
        data_provider = "example-source",
        stage='p1',
        partition_type = 'h',
        atlas_username = "admin",
        atlas_password = "admin",
        atlas_endpoint = "http://127.0.0.1:21000/api/atlas/v2/entity/bulk",
         )
    #babylon.create_atlas_json()
    babylon.upload_and_tag("file","path",{})


main()
