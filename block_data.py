from web3 import Web3
from dotenv import load_dotenv
import os
from decimal import Decimal


# Load environment variables from .env file
load_dotenv()

# Access API key directly
INFURA_PROJECT_ID = os.getenv("INFURA_PROJECT_ID")

# Initialize Web3 connection (Using Infura)
INFURA_URL = "https://mainnet.infura.io/v3/"+INFURA_PROJECT_ID
w3 = Web3(Web3.HTTPProvider(INFURA_URL))

# Fetch latest Ethereum block transactions
def get_latest_transactions():
    try:
        latest_block = w3.eth.get_block('latest', full_transactions=True)
        transactions = latest_block.transactions
        tx_list = []
        
        for tx in transactions:
            tx_list.append({
                "hash": tx.hash.hex(),
                "from": tx['from'],
                "to": tx['to'] if tx['to'] else "Contract Creation",
                "value": Decimal(w3.from_wei(tx["value"], "ether")),  # Convert to Decimal
                "gas": Decimal(tx["gas"]),  # Convert to Decimal
                "gasPrice": Decimal(w3.from_wei(tx["gasPrice"], "gwei"))  # Convert to Decimal
            })
        
        return tx_list
    except Exception as e:
        print(f"Error fetching transactions: {e}")
        return []


