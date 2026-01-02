from web3 import Web3
import pandas as pd

# Initialize Web3 connection (Using Infura)


# Fetch latest Ethereum block transactions
def get_latest_transactions( infura_url, block= 'latest'):
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
    
def get_receipts(tx_hash, infura_url):
    w3 = Web3(Web3.HTTPProvider(infura_url))
    transaction_receipt= w3.eth.get_transaction_receipt(tx_hash)
    tx= dict(transaction_receipt)
    logs= [dict(t) for t in tx['logs'] ]
    df= pd.DataFrame(logs)
    return df
    
if __name__== '__main__':
    get_latest_transactions()


