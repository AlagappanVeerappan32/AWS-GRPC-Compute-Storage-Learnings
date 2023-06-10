import grpc
from concurrent import futures
import json
import computeandstorage_pb2
import computeandstorage_pb2_grpc
import boto3
from grpc_reflection.v1alpha import reflection
from urllib.parse import urlparse
import os.path



class EC2OperationsServicer(computeandstorage_pb2_grpc.EC2OperationsServicer):    

    def StoreData(self, request, context):
        # Retrieve the data from the request
        data = request.data

        Bucket_Name='adv-cloud-a2-grpc'
        Key_Value='file.txt'
        AWS_Region = 'us-east-1'  

        self.put_object_to_s3(data, Bucket_Name, Key_Value)

        s3uri = "https://"+Bucket_Name+".s3."+AWS_Region+".amazonaws.com/"+Key_Value

        # aws_man_console = boto3.session.Session(profile_name="AlexVersion1")
        # s3 = aws_man_console.client('s3')
        # s3uri = s3.generate_presigned_url(
        #     'get_object',
        #     Params={
        #         'Bucket': Bucket_Name,
        #         'Key': Key_Value
        #     },
        #     ExpiresIn=3600  # Optional: Set the expiration time of the URL (in seconds)
        # )

        return computeandstorage_pb2.StoreReply(s3uri=s3uri)
    

    def put_object_to_s3(self, data, bucket_name, key_value):

        # aws_man_console = boto3.session.Session(profile_name="AlexVersion1")
        # s3 = aws_man_console.client('s3')
        s3 = boto3.client('s3')
        s3.put_object(
            Body=data,
            Bucket=bucket_name,
            Key=key_value
        )

    def AppendData(self, request, context):
        data = request.data

        Bucket_Name='adv-cloud-a2-grpc'
        Key_Value='file.txt'
        AWS_Region = 'us-east-1' 

        # aws_man_console = boto3.session.Session(profile_name="AlexVersion1")
        # s3 = aws_man_console.client('s3')
        s3 = boto3.client('s3')
        
        response = s3.get_object(Bucket=Bucket_Name, Key=Key_Value)
        existing_data = response['Body'].read().decode('utf-8')
        updated_data = existing_data + data
        s3.put_object(Body=updated_data.encode('utf-8'), Bucket=Bucket_Name, Key=Key_Value)

        return computeandstorage_pb2.AppendReply()
    

    def DeleteFile(self, request, context):
        s3uri = request.s3uri

        start_index = len("https://")
        end_index = s3uri.index(".s3")
        Bucket_Name = s3uri[start_index:end_index]
        Key_Value = s3uri.split("/")[-1]

        # parsed_url = urlparse(s3uri)
        # Bucket_Name = parsed_url.netloc.split('.')[0]
        # Key_Value = os.path.basename(parsed_url.path)
            
        # aws_man_console = boto3.session.Session(profile_name="AlexVersion1")
        # s3 = aws_man_console.client('s3')
        s3 = boto3.client('s3')
        s3.delete_object(Bucket=Bucket_Name, Key=Key_Value)

        return computeandstorage_pb2.DeleteReply()



def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    computeandstorage_pb2_grpc.add_EC2OperationsServicer_to_server(EC2OperationsServicer(), server)
    
    reflection.enable_server_reflection('computeandstorage.EC2Operations', server)
    
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()



if __name__ == '__main__':
    serve()
