#!/usr/bin/env python3
import time
import paho.mqtt.client as mqtt
import ssl
import json
import rclpy
from rclpy.node import Node
from sensor_msgs.msg import PointCloud2
from sensor_msgs_py import point_cloud2 as pc2
import sys

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
	
	def pose_callback(self, msg: PointCloud2):
		print("Message sent")
		cloud_data = pc2.read_points_list(msg)
		print(type(cloud_data[1]._asdict()))
		for i in range(len(cloud_data)):
			self.client.publish("device1/data", payload=json.dumps(str(cloud_data[i]._asdict())), qos=0, retain=False)
		
		
def main(args=None):
	
	rclpy.init(args=args)
	node = PoseSubscriberNode()
	rclpy.spin(node)
	rclpy.shutdown()		
