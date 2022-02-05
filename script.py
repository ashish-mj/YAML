#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: ashish
"""
# import libraries

import pymongo
import yaml
import base64

class crud_operations:
    
    def __init__(self):
        
        with open("config.yml", "r") as stream:
            try:
                self.config = yaml.safe_load(stream)
                print(self.config)
            except yaml.YAMLError as exc:
                print(exc)
                
    def connect(self):
        database_config = self.config["application"]["database"]
        base64_bytes = database_config["username"].encode("ascii")
        string_bytes = base64.b64decode(base64_bytes)
        username = string_bytes.decode("ascii")
        base64_bytes = database_config["password"].encode("ascii")
        string_bytes = base64.b64decode(base64_bytes)
        password = string_bytes.decode("ascii")

        conn = pymongo.MongoClient(database_config["connection-string"],database_config["port"],username=username,password=password)
        db = conn[database_config["database-name"]]
        collection = db[database_config["collection-name"]]
        print("Connection Successful")
        return collection
                

if __name__=="__main__":
    obj = crud_operations()
    obj.connect()
                

