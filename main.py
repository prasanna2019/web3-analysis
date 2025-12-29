import ingest_bq as ib
import os
from dotenv import load_dotenv
import block_data as bd
load_dotenv()
import logging

project= os.getenv('project_id')
destination= os.getenv('dataset')+'.'+os.getenv('table')
key_url= os.getenv('key_url')
infura_url= os.getenv('infura_url')
infura_key= os.getenv('infura_key')

logging.basicConfig(filename="logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def main():
    try:
        df= bd.get_latest_transactions(infura_url+infura_key)
        df['value']= df['value']/1e18
    except Exception as e:
        logger.exception(" Error in getting transactions data: %s", str(e))
        return
    
    try:
        client= ib.create_bq_client(project, key_url)
        jobConf= ib.create_job_config("WRITE_APPEND")
    except Exception as e:
        logger.exception("Error in setting up bq client: %s", str(e))
        return
    if(not df.empty):
        try:
            ib.bq_load(df, client, jobConf, destination)
        except Exception as e:
            logger.exception(f'Error in setting up client for bq ingestion')
            return

    else:
        logger.info(f"no transaction details... exiting")
    logger.info(f'Exiting successfully...')

    

if __name__== "__main__":
    main()