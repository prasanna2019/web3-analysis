from web3 import Web3
import pandas as pd


# Access API key directly
INFURA_PROJECT_ID = os.getenv("INFURA_PROJECT_ID")

# Initialize Web3 connection (Using Infura)
INFURA_URL = "https://mainnet.infura.io/v3/"+INFURA_PROJECT_ID


# Fetch latest Ethereum block transactions
def get_latest_transactions(block= 'latest', infura_url):
    try:
        w3 = Web3(Web3.HTTPProvider(infura_url))
        block = w3.eth.get_block(block, full_transactions=True)
        transactions = block.transactions
        tx_dicts= [dict(tx) for tx in transactions]
        required_columns= ['blockHash', 'blockNumber', 'chainId','from', 'gas', 'gasPrice','hash', 'input', 'maxFeePerGas', 'maxPriorityFeePerGas', 'nonce','type', 'value' ]
        df= pd.json_normalize(tx_dicts,sep='_')
        if not (set(required_columns)- set(df.columns)):
            df_selected= df[['blockHash', 'blockNumber', 'chainId','from', 'gas', 'gasPrice','hash', 'input', 'maxFeePerGas', 'maxPriorityFeePerGas', 'nonce','type', 'value' ]]
            return df_selected
    except Exception as e:
        print(f"Error fetching transactions: {e}")
        return []
    
def get_receipts(tx_hash):
    transaction_receipt= w3.eth.get_transaction_receipt(tx_hash)
    print(transaction_receipt)

    return
    
if __name__== '__main__':
    get_latest_transactions()


