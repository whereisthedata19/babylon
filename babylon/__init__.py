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
    def __init__(self,s3_access, s3_secret , s3_endpoint ,bucket,data_provider,stage='raw',partition_type='a',atlas_username="admin" , atlas_password="admin", atlas_endpoint="localhost:21000",type_name = 'custom_s3_object_v5' ):
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
        self.type_name = type_name
    


    def generate_object_metadata(self,partition_type):
    
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

    
    def create_object_metadata(self,metadata= {}):
        (s3_path , file_name) = self.generate_object_metadata(self.partition_type)

        if "typeName" not in metadata:
            metadata["typeName"] = self.type_name
        
        if "qualifiedName" not in metadata:
            metadata["qualifiedName"]  =  "/" + self.bucket + "/"+ self.data_provider +"/" +self.stage  +  s3_path + file_name  

        if "name" not in metadata:
            metadata["name"] = file_name
        
        if "path" not in metadata:
            metadata["path"] = s3_path
        
        if "fileFormat" not in metadata:
            metadata["fileFormat"] = ""

        if "description" not in metadata:
            metadata["description"] = "S3 object"

        if "createBy" not in metadata:
            metadata["createBy"] = ""
        
        if "createTime" not in metadata:
            gmtime = time.gmtime(calendar.timegm(time.gmtime()))
            metadata["createTime"] =  calendar.timegm(time.gmtime())

        if "owner" not in metadata:
            metadata["owner"] = ""
        
        if "acl" not in metadata:
            metadata["acl"] = ""

        return metadata

    def atlas_entity_format(self,metadata):
        json_string = json.dumps({"entities":[
                                                {
                                                    "typeName" : metadata["typeName"],
                                                    "attributes": {
                                                        "qualifiedName":metadata["qualifiedName"],
                                                        "name":metadata["name"],
                                                        "fileFormat":metadata["fileFormat"],
                                                        "description":metadata["description"],
                                                        "createdBy":metadata["createBy"],
                                                        "createdTime":metadata["createTime"],
                                                        "owner":metadata["owner"],
                                                        "acl":metadata["acl"]
                                                    }
                                                }
                                            ]
                                        }
                                    )

        #print(json_string)
        return json_string
        
    def create_atlas_entity(self, metadata):
        formated_metadata = self.atlas_entity_format(metadata)
        print("")
        print(formated_metadata)
        headers = {'Content-Type' : 'application/json', 'Accept':'application/json'}
        r = requests.post(self.atlas_endpoint,auth=HTTPBasicAuth(self.atlas_username,self.atlas_password),headers=headers, data = formated_metadata)
        print("")
        print(r.text)
        return r


    def upload_file(self,file,updated_metadata = {}):
        metadata = self.create_object_metadata(updated_metadata)
        with open(file, 'rb') as data:
            self.s3.upload_fileobj(data, self.bucket, metadata["path"]+metadata["name"])
            print("Object "+metadata["name"]+" wirtten to "+ metadata["path"]+" succsessfully")
            self.create_atlas_entity(metadata)

def main():
    
    babylon = Config(s3_access = "CXAD0ORH0HPU3FJ4IKCQ",
        s3_secret = "bYpeW2byIZ5p7dCMvwL7Mt9nucXmBqsgfngdQtDU",
        s3_endpoint = "http://10.3.178.217:8000",
        bucket = "example-project",
        data_provider = "example-source",
        stage='w',
        partition_type = 'h',
        atlas_username = "admin",
        atlas_password = "admin",
        atlas_endpoint = "http://127.0.0.1:21000/api/atlas/v2/entity/bulk",
        type_name = 'custom_s3_object_v5'
         )
    #babylon.create_atlas_json()
    #babylon.upload_and_tag("file","path",{})
    babylon.upload_file(file='../tmp/test.txt' )


main()
