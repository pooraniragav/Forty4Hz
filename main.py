import os, json, aiohttp
import pandas as pd
from fastapi import FastAPI, BackgroundTasks, HTTPException


app = FastAPI()

# Local repository to store downloaded dataset
DATASET_FOLDER = "datasets"
os.makedirs(DATASET_FOLDER, exist_ok=True)

# Public URL of the dataset
DATASET_URL = "http://prod1.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"

dataset_path = os.path.join(DATASET_FOLDER, "pp-complete.csv")

# Global variable to store deduplicated JSON
deduplicated_json = None
df_deduplicated = pd.DataFrame([])

download_completed = False
deduplicate_completed = False


# Task 1: Download dataset from public URL asynchronously
async def download_dataset(dataset_url: str):
    """
    Function to download the public dataset asynchronously.
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(dataset_url, timeout=3600) as response:
            if response.status == 200:
                # Write the downloaded data to a file
                filename = os.path.join(DATASET_FOLDER, "pp-complete.csv")
                with open(filename, "wb") as f:
                    while True:
                        chunk = await response.content.read(16384)
                        if not chunk:
                            break
                        f.write(chunk)
                print("Dataset writing completed")
                global download_completed
                download_completed = True


@app.get("/download")
async def trigger_download(background_tasks: BackgroundTasks):
    """
    function to check if dataset already exists, if not triggers the download_dataset method.
    """
    # Check if the dataset already exists 
    if not os.path.exists(dataset_path):
        # Trigger an async job to download the dataset
        # await download_dataset(DATASET_URL)
        background_tasks.add_task(download_dataset, DATASET_URL)
        return {"message": "Dataset download started."}
    elif download_completed:
        return {"message": "Dataset already exists."}
    else:
        return {"message": "Download in progress"}


# Task 2: Deduplicate dataset (remove redundant data using pandas module)
deduplication_columns = ['street', 'locality', 'town', 'district', 'county']

async def deduplicate_chunk(chunk):
    chunk_sorted = chunk.sort_values(by=deduplication_columns)
    duplicates_mask = chunk_sorted.duplicated(subset=deduplication_columns, keep='first')
    deduplicated_chunk = chunk_sorted[~duplicates_mask]
    return deduplicated_chunk

async def deduplicate_data():
    deduplicated_dfs = []
    column_names = ['uuid', 'price', 'date', 'postcode', 'type', 'isNew', 'duration', 'code',
                    'dNo', 'street', 'locality', 'town', 'district', 'county', 'skip1', 'skip2']
    chunksize = 100000  # Adjust chunk size as needed
    
    for chunk in pd.read_csv(dataset_path, names=column_names, chunksize=chunksize):
        deduplicated_chunk = await deduplicate_chunk(chunk)
        deduplicated_dfs.append(deduplicated_chunk)
    print("deduplication completed")
    return pd.concat(deduplicated_dfs)

@app.get("/deduplicate")
async def get_deduplicated_data(background_tasks: BackgroundTasks):
    global deduplicated_json
    # If deduplication has already been performed, return the stored deduplicated JSON
    if deduplicated_json is not None:
        # check if deduplicate is completed
        global deduplicate_completed
        global download_completed
        if download_completed and deduplicate_completed:
            return deduplicated_json
        elif deduplicate_completed:
            deduplicate_completed = False
        else:
            return {"status": "deduplication_in_progress"}
    
    # check if dataset is present before proceeding with deduplication
    if not os.path.exists(dataset_path):
        return {"message": "Data is not downloaded"}
    

    if not deduplicate_completed:
    # Start the deduplication process in the background
        background_tasks.add_task(deduplicate_data_process)
    
    return {"status": "deduplication_in_progress"}

async def deduplicate_data_process():
    """ function to trigger deduplication and json convertion
    """
    global df_deduplicated
    
    
    df_deduplicated = await deduplicate_data()
   
    print(type(df_deduplicated))
    global deduplicated_json
    deduplicated_json = df_deduplicated.to_json(orient='records')
    # Return the deduplicated JSON
    print("deduplication json is being returned")
    global deduplicate_completed
    deduplicate_completed = True
    return deduplicated_json

# Task3 of fetching data using uuid
@app.get("/data/{uuid}")
async def get_data_by_uuid(uuid: str):
    """
    Get data for a specific UUID.
    """
    # Check if deduplicated DataFrame exists
    global df_deduplicated
    if df_deduplicated.empty:
        return {"message": "Dataset not deduplicated yet. Please wait."}
    
    # Check if the UUID exists in the DataFrame
    print(df_deduplicated['uuid'][0])
    
    if uuid in df_deduplicated['uuid'].values:
        # Retrieve the row corresponding to the UUID
        row = df_deduplicated[df_deduplicated['uuid'] == uuid].iloc[0].to_dict()
        print(row)
        return json.dumps(row)

    else:
        return {"message": "UUID not found"}


@app.get("/{path:path}")
async def not_found(path: str):
    raise HTTPException(status_code=404, detail="Endpoint not found")