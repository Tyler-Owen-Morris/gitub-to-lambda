import pymysql
import json
from random import randint
import os
import csv
import pandas as pd
from io import StringIO
import requests
import boto3
from boxsdk import OAuth2, Client

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
    f_list = read_from_s3() #string list of file names
    for fil in f_list:
        my_file = fil.split(".")
        print("my file:",my_file)
        try:
            s3_client.download_file(bucket,fil,'./tmp/'+fil)
        except:
            print("download failed for:",fil)

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

def write_to_box(fname,df):
    folders = getAllFolders()
    fold_names = [x[0] for x in folders]
    fold_id = None
    # create folder
    if fname in fold_names:
        for folder in folders:
            if folder[0] == fname:
                fold_id = folder[1]
    else:
        fold_id = createFolder(fname)
    # create file in folder
    print("foldID:",fold_id)
    if (fold_id == None or fold_id == ''):
        return
    files = getAllFiles(str(fold_id))
    file_names = [x[0] for x in files]
    print("file names:",file_names)
    if fname + ".csv" in file_names:
        file_id = None
        for file in files:
            if file[0] == fname+".csv":
                file_id = file[1]
        print("updating file", file_id,fname)
        updateFile(file_id,fname,df)
    else:
        createFile(fold_id,fname,df)
    pass

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

def createFolder(new_folder_name, parent_folder = client.folder('0'), ):
    fid = ''
    try:
        new_folder = parent_folder.create_subfolder(new_folder_name)
        fid = new_folder.id
    except:
        folders = getAllFolders()
        for fold in folders:
            print("Matching:",fold,new_folder_name)
            if fold[0] == new_folder_name:
                fid = fold[1]
    return fid

def createFile(folderid, file_name,data):
    stream = StringIO()
    data.to_csv(stream)
    fname = file_name+".csv"
    box_file = client.folder(folderid).upload_stream(stream, fname)

def updateFile(fileid,file_name,data):
    stream = StringIO()
    data.to_csv(stream)
    updated_file = client.file(fileid).update_contents_with_stream(stream)
    print(f'File "{updated_file.name}" has been updated')
    return updated_file