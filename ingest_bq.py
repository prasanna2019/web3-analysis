from google.cloud import bigquery
from google.oauth2 import service_account

def create_bq_client(project, key_url) ->bigquery.Client:
    creds = service_account.Credentials.from_service_account_file(
        key_url
    )
    client= bigquery.Client(project=project, credentials=creds)
    
    return client

def create_job_config(write_disposition) -> bigquery.LoadJobConfig:
    return bigquery.LoadJobConfig(
        write_disposition= write_disposition,
        autodetect= True

    )


def bq_load(df, client, jobConfig, destination):
    job = client.load_table_from_dataframe(
        df,
        destination,
        job_config=jobConfig
    )
    return job.result()