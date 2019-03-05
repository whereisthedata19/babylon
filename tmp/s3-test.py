import boto3

s3_access = "CXAD0ORH0HPU3FJ4IKCQ"
secret = "bYpeW2byIZ5p7dCMvwL7Mt9nucXmBqsgfngdQtDU"
endpoint = "http://10.3.178.217:8000"

file = 'test.txt'
bucket = 'test'



# resource 

#s3 = boto3.resource(service_name='s3',
#                            aws_access_key_id=s3_access,
#                            aws_secret_access_key=secret,
#                            endpoint_url=endpoint)
#
#s3.meta.client.upload_file('test.txt', 'test', 'hello.txt')
#
#
##s3.upload_file(file,'test').put(Body=open('test.txt', 'rb'))
##s3.Bucket('test').upload_file('test',secret)


# client 

s3 = boto3.client(service_name='s3',
                            aws_access_key_id=s3_access,
                            aws_secret_access_key=secret,
                            endpoint_url=endpoint)

with open('test2.txt', 'rb') as data:
    s3.upload_fileobj(data, 'test', '/bla/ba/test.txt')
    