import pymysql
import json
from random import randint
import os
import csv
import pandas as pd
from io import StringIO, BytesIO
import requests
import boto3
from boxsdk import OAuth2, Client
import PyPDF2
from PyPDF2 import PdfFileReader,PdfFileWriter
from smart_open import smart_open

## **** CONFIGURATION VARIABLES **** ##
bucket = 'empact-test'

# S3 INIT
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
my_bucket = s3.Bucket(bucket)

# box sdk INIT
auth = OAuth2(
    client_id = os.environ['BOX_CLIENT_ID'],
    client_secret = os.environ['BOX_CLIENT_SECRET'],
    access_token = os.environ['BOX_DEV_TOKEN'],
)

client = Client(auth)

user = client.user().get()
print(f'The current user ID is {user.id}')

## ******* FUNCTIONS ****** ##
# AWS LAMBDA SETUP TARGETS THE 'lambda_handler' FUNCTION IN THE 'lambda_function.py' FILE.
## This is the entry point for the API endpoint being called.
def lambda_handler(event, context):
    print("i'm alive!")
    myfiles,myfolders = getAllEntities() #2 lists of tuples (file name, file box id)
    f_list = read_from_s3() #string list of file objects from S3 .key is file name, .bucket is location
    saved_files = []
    for fil in f_list:
        my_file = fil.key.split(".")
        #print("my file:",my_file)
        if my_file[1] == 'pdf':
            try:
                resp = my_bucket.download_file(fil.key,'/tmp/'+fil.key)
                print("download response:?",resp)
                saved_files.append(('/tmp/'+fil.key,fil.key))
                # s3.Object(bucket,fil.key).delete()
            except ValueError as ve:
                print("ERROR:",ve)
                print("download failed for:",fil)
        else:
            print("passing on non-pdf file")
    li = os.listdir("/tmp/")
    print("temp files created:",li)
    print("saved files:",saved_files)
    s_keys = [x[1] for x in saved_files]
    file_names = [x[0] for x in myfiles]
    fold_names = [x[0] for x in myfolders]
    print("box file names:",file_names)
    for ms_file in saved_files:
        f_key = ms_file[1]
        f_path = ms_file[0]
        print("file processing...",ms_file)
        if f_key in file_names:
            ufid = None
            for mfile in myfiles:
                if mfile[0]==f_key:
                    ufid = mfile[1]
            if ufid != None:
                print("update file:",ufid,f_path)
                update_box_pdf(ufid,f_path)
                s3.Object(bucket,f_key).delete()
        else:
            if f_key.split(".")[0] in fold_names:
                fid = '0'
                for folder in myfolders:
                    if folder[0] == f_key.split(".")[0]:
                        fid = folder[1]
                print("write new file to box:",fid,f_key,f_path)
                #write_pdf_to_box(fid, f_key,f_path)
                write_pdf_to_box(fid,f_key,f_path)
                s3.Object(bucket,f_key).delete()
            else:
                print("No matching folder or file found for:",ms_file)

    message = {"message": "Execution started successfully!"}
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(message),
    }

## UTILITY FUNCTIONS
def read_from_s3():
    ret = []
    for buck in my_bucket.objects.all():
        print(buck)
        ret.append(buck)
    return ret

def getAllEntities():
    files = []
    folders = []
    items = client.folder('0').get_items(limit=1000)
    for item in items:
        #print("Item type:",item.type)
        if item.type == 'file':
            files.append((item.name,item.id))
        elif item.type == 'folder':
            folders.append((item.name,item.id))
    for fold in folders:
        items = client.folder(fold[1]).get_items(limit=1000)
        for item in items:
            #print("Item type:",item.type)
            if item.type == 'file':
                files.append((item.name,item.id))
            elif item.type == 'folder':
                #not currently supporting subfolders
                pass
                #folders.append((item.name,item.id))
    return files,folders

def getAllFolders():
    folders = []
    items = client.folder('0').get_items(limit=1000)
    for item in items:
        if item.type == 'folder':
            folders.append((item.name,item.id))
    return folders

def getAllFiles(folder='0'):
    files = []
    print("folder:",folder)
    items = client.folder('0').get_items(limit=1000)
    for item in items:
        if item.type == 'file':
            files.append((item.name,item.id))
    if folder != '0':
        items = client.folder(folder).get(limit=1000).item_collection['entries']
        for item in items:
            print("itype",item.type)
            if item.type == 'file':
                files.append((item.name,item.id))
    print(files)
    return files

def update_box_pdf(fileid,file_path):
    with open(file_path, "rb") as fh:
        bytes_stream = BytesIO(fh.read())
        updated_file = client.file(fileid).update_contents_with_stream(bytes_stream)
        print(f'File "{updated_file.name}" has been updated')
    return updated_file

def write_pdf_to_box(folderid,file_name,file_path):
    with open(file_path, "rb") as fh:
        bytes_stream = BytesIO(fh.read())
        box_file = client.folder(folderid).upload_stream(bytes_stream,file_name)
        print("bytes stream",bytes_stream)
        print("boxfile:",box_file)