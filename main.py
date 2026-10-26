import asyncio
import aiohttp
import logging
import json
from datetime import datetime

# Configure production logging layout
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MarketDataExtractor:
    """
    Handles asynchronous data extraction from digital asset market APIs.
    Utilizes connection pooling via aiohttp for high-throughput concurrent ingestion.
    """
    def __init__(self, symbols: list):
        # Public REST endpoint for production-grade market asset streaming
        self.base_url = "https://coingecko.com"
        self.symbols = symbols

    async def fetch_symbol_data(self, session: aiohttp.ClientSession, symbol: str) -> dict:
        """
        Fetches real-time price and volume metrics for a specific asset partition.
        """
        params = {
            'ids': symbol, 
            'vs_currencies': 'usd', 
            'include_24hr_vol': 'true', 
            'include_24hr_change': 'true'
        }
        try:
            async with session.get(self.base_url, params=params, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Successfully extracted telemetry data for: {symbol}")
                    return {
                        "symbol": symbol,
                        "timestamp": datetime.utcnow().isoformat(),
                        "price": data[symbol]['usd'],
                        "volume": data[symbol]['usd_24h_vol']
                    }
                logger.error(f"Ingestion anomaly detected for {symbol}: HTTP status {response.status}")
                return {}
        except Exception as e:
            logger.error(f"Network transport timeout or connection error for {symbol}: {str(e)}")
            return {}

    async def extract_all(self) -> list:
        """
        Orchestrates concurrent async workers to execute batch data extraction tasks.
        """
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_symbol_data(session, sym) for sym in self.symbols]
            results = await asyncio.gather(*tasks)
            return [res for res in results if res]

async def main():
    # Targeted asset matrix for operational tracking
    tracked_crypto = ['bitcoin', 'ethereum', 'solana']
    extractor = MarketDataExtractor(tracked_crypto)
    
    logger.info("Initializing asynchronous data extraction pipeline pipeline...")
    extracted_data = await extractor.extract_all()
    
    # Staging mock output representing payload ready for PostgreSQL target database insertion
    print("\n--- Pipeline Operational Data Output (Staging Schema Ready) ---")
    print(json.dumps(extracted_data, indent=4))

if __name__ == "__main__":
    asyncio.run(main())
