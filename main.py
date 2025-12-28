import ingest_bq as ib
import os
from dotenv import load_dotenv
import block_data as bd
load_dotenv()

project= os.getenv('project_id')
destination= os.getenv('dataset')+'.'+os.getenv('table')
key_url= os.getenv('key_url')
infura_url= os.getenv('infura_url')
infura_key= os.getenv('infura_key')

def main():
    bd.get_latest_transactions(infura_url)

    ib.create_bq_client(project, key_url)
    ib.create_job_config("write")
    ib.bq_load()
    

if __name__== "__main__":
    main()