#!/usr/bin/env python3
import time
import paho.mqtt.client as mqtt
import ssl
import json as js
import rclpy
from rclpy.node import Node
from sensor_msgs.msg import PointCloud2
from sensor_msgs_py import point_cloud2 as pc2
import sys
import math
import pandas as pd
from io import StringIO
import boto3
import csv

def on_connect(client, userdata, flags, rc):
	print("Connected")
	
class PoseSubscriberNode(Node):
	def __init__(self):
		super().__init__("pose_subscriber")
		self.pose_subscriber_ = self.create_subscription(
			PointCloud2, "/velodyne_points", self.pose_callback, 10)
		self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1) 
		self.client.on_connect = on_connect
		self.client.tls_set(ca_certs='/home/team309/Desktop/309_aws_lidar/AmazonRootCA1.pem', certfile='/home/team309/Desktop/309_aws_lidar/certificate.pem.crt', keyfile='/home/team309/Desktop/309_aws_lidar/private.pem.key', tls_version=ssl.PROTOCOL_SSLv23)
		self.client.tls_insecure_set(True)
		self.client.connect("a1z10odhp6hiwe-ats.iot.us-east-1.amazonaws.com", 8883, 60) #Taken from REST API endpoint - Use your own. 
		self.count0 = 0
		
	def pose_callback(self, msg: PointCloud2):
		print("Message sent")
		cloud_data = pc2.read_points_list(msg)	
		csv_file = '/home/team309/ros2_ws/team309/team_309.csv'
		with open(csv_file, 'w', newline='') as file_csv:
			linker = csv.DictWriter(file_csv, fieldnames = ['x','y','z','intensity','time'])
			linker.writeheader()
			for i in range(len(cloud_data)):
				if math.isnan(cloud_data[i]._asdict()["x"]):
					continue
				data = {
				'x': float(cloud_data[i]._asdict()["x"]),
				'y':float(cloud_data[i]._asdict()["y"]),
				'z':float(cloud_data[i]._asdict()["z"]),
				'intensity': float(cloud_data[i]._asdict()["intensity"]),
				'time': float(cloud_data[i]._asdict()["time"])
				}
				json_data = eval(js.dumps(data))
				print(i)
				linker.writerow(json_data)	
		hc = pd.read_csv('/home/team309/ros2_ws/team309/team_309.csv')
		s3 = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='')
		csv_buf = StringIO()
		hc.to_csv(csv_buf, header=True, index=False)
		csv_buf.seek(0)
		s3.put_object(Bucket='309plantdata', Body=csv_buf.getvalue(), Key='test_data.csv')
		
	
		self.count0 += 1
		if self.count0 == 1:
			print('Message finish')
			raise SystemExit
		
def main(args=None):
	rclpy.init(args=args)
	node = PoseSubscriberNode()
	try:
		rclpy.spin(node)
	except SystemExit:
		print('done')	
	rclpy.shutdown()		

if __name__ == '__main__':
	main()
