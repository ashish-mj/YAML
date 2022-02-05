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
                self.collection = self.connect()
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
    
    def insert(self,id,name,age):
        try:
            data = {"_id":id,"name":name,"age":age}
            self.collection.insert_one(data)
            print(data)
        except pymongo.errors.DuplicateKeyError:
            print("Document exists")
        
    def read(self,id):
        document = self.collection.find_one({'_id':  id})
        if bool(document)==False:
            print("ID doesn't exists")
        else:
            print(dict(document))
            
    def update(self,id,age):
        if self.collection.count_documents({'_id':  id})==0:
            print("ID doesn't exist to be updated")
        else:
            self.collection.update_one({"_id":id},{ "$set": {"age":age }})
            print("Document updated")
        
            
    def delete(self,id):
        if self.collection.count_documents({'_id':  id})==0:
            print("ID doesn't exist to be deleted")
        else:
            data = {"_id":id}
            self.collection.delete_one(data)
            print("ID deleted successfully")
            
    def interact(self):
        while(1):
            print("Select option\n 1 - Insert Document\n2 - Read Document\n3 - Update Document\n4 - Delete Document\nq - Quit\n")
            n = input()
            if n=="1":
                print("Enter ID, Name, Age\n")
                id,name,age = input().split(" ")
                self.insert(int(id),name,int(age))
            elif n=="2":
                print("Enter ID\n")
                id = input()
                self.read(int(id))
            elif n=="3":
                print("Enter ID, Age\n")
                id, age = input().split(" ")
                self.update(int(id), int(age))
            elif n=="4":
                print("Enter ID\n")
                id = input()
                self.delete(int(id))
            elif n=="q":
                return
            else:
                print("Invalid choice")
            
            
        
        
                

if __name__=="__main__":
    obj = crud_operations()
    obj.interact()
    
                

