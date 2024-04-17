import azure.functions as func
import requests
import time
import pandas as pd
import pymssql
import os


# Introduce global variables for the normalized data dictionary, as well as the 
# information to connect to the BLOB container

normalized_data = []
CONNECTION_STRING = os.environ.get('xxxx') 
CONTAINER_NAME = "xxxx"
BLOB_NAME = "metaapi.json"  


# Azure functions are wrapped inside a main function, indicating that this function
# is triggered by a HTTP request. Inside the main function are calls to the other 
# functions to support the application 

def main(req: func.HttpRequest) -> func.HttpResponse:
    api_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects" 
    response = requests.get(api_url)

    if response.status_code == 200:
        json_data = response.json()
        
        
        # While we transform some parts of the data into normalized table for loading
        # into an Azure SQL database, we want to preserve the entire JSON response
        # in BLOB storage for further use cases.
        store_json_to_blob(json_data)
        object_ids = json_data.get('objectIDs', [])
        total_objects = json_data.get('total', 0)

    else:
        return func.HttpResponse("API Error", status_code=500)
    
    for object_id in object_ids:
        try:
            object_data = extract_object_details(object_id)
            
            if object_data:
                object_record, artist_record = transform_object(object_data)
                load_object('object', object_record)
                load_object('artist', artist_record)
            else:
                print(f"API Response Invalid for object ID: {object_id}")  
        except requests.exceptions.RequestException as e:
            print(f"Error fetching details for object ID {object_id}: {e}")
            
        time.sleep(0.05) # The Met API is limited to 80 requests per second. This will lower
        # the number of times we request the API for object data to around 20 per second
    
def extract_object_details(object_id):
    api_base_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects/" 
    url = api_base_url + str(object_id)
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        return None

def transform_object(object_data):
    df = pd.json_normalize(object_data)
    
    object_record = df[[ 'objectID','accessionNumber', 'accessionYear', 'classification', 
                        'objectBeginDate', 'objectEndDate', 'medium', 'dimensions']]
    object_record = object_record.fillna('DATA_MISSING')

    artist_record = df[['artistRole', 'artistDisplayName', 'artistSuffix', 
                        'artistNationality', 'artistBeginDate', 'artistEndDate', 'artistGender']]
    artist_record = artist_record[artist_record['artistRole'] == 'Artist'].iloc[0]
    artist_record = artist_record.fillna('DATA_MISSING')

    return object_record.to_dict(), artist_record.to_dict()

def load_object(object_record, artist_record):
    #Using placeholders for security
    conn = pymssql.connect(server, username, password, database)
    cursor = conn.cursor()
    
    object_sql = """
          INSERT INTO object (objectId, accessionNumber, accessionYear, classification,
          objectBeginDate, objectEndDate, medium, dimensions) 
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
          """
    object_values = (object_record['objectId'], object_record['accessionNumber'], object_record['accessionYear'], object_record['classification'],
                     object_record['objectBeginDate'], object_record['objectEndDate'], object_record['medium'], object_record['dimensions'])
    cursor.execute(object_sql, object_values)
    
    artist_sql = """
          INSERT INTO artist (object_id, artistRole, artistDisplayName, artistSuffix, artistNationality, artistBeginDate, artistEndDate, artistGender) 
          VALUES (%s, %s, %s, %s, %s, %s, %s)
          """
    artist_values = (object_id, artist_record['artistRole'], artist_record['artistDisplayName'], artist_record['artistSuffix'], artist_record['artistNationality'],
                     artist_record['artistBeginDate'], artist_record['artistEndDate'], artist_record['artistGender'])
    cursor.execute(artist_sql, artist_values)

    conn.commit()
    conn.close()


def store_json_to_blob(json_data):
    #Using placeholders for security
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

    json_string = json.dumps(json_data)
    blob_client.upload_blob(json_string, overwrite=True)  