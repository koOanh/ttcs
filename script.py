import os
import json
import sys
import logging
from typing import Dict, Any
from colorama import Fore, Style, init
from flask import Flask

init(autoreset=True)

from utils.coin_market import CoinMarketCapAPI
from utils.posgres_pool import PostgresManager, DatabaseError

class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': Fore.BLUE,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Style.BRIGHT
    }

    def format(self, record):
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{Style.RESET_ALL}"
            record.msg = f"{self.COLORS[levelname]}{record.msg}{Style.RESET_ALL}"
        return super().format(record)

logger = logging.getLogger('crypto_job')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = ColoredFormatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)

def insert_crypto_data(api_response: Dict[str, Any]) -> int:
    logger.info("Processing cryptocurrency data for database insertion")

    records_inserted = 0
    data_to_insert = []
    for item in api_response.get("data", []):
        try:
            name = item.get("name")
            symbol = item.get("symbol")
            cmc_rank = item.get("cmc_rank")
            quote_usd = item.get("quote", {}).get("USD", {})
            price = quote_usd.get("price")
            volume_24h = quote_usd.get("volume_24h")
            market_cap = quote_usd.get("market_cap")
            last_updated_str = quote_usd.get("last_updated")

            if all([name, symbol, price, last_updated_str]):
                last_updated = last_updated_str.replace('T', ' ').replace('Z', '')
                data_to_insert.append((name, symbol, cmc_rank, price, volume_24h, market_cap, last_updated))
            else:
                logger.warning(f"Skipping incomplete record: {item.get('name', 'N/A')}")
        except Exception as e:
            logger.error(f"Error processing item: {item.get('name', 'N/A')} - {e}")
            continue

    if data_to_insert:
        insert_query = """
            INSERT INTO cryptocurrency_data (name, symbol, rank, price_usd, volume_24h_usd, market_cap_usd, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (name, symbol, last_updated) DO NOTHING;
        """
        try:
            with PostgresManager() as db_manager:
                db_manager.execute_batch(insert_query, data_to_insert)
            records_inserted = len(data_to_insert)
            logger.info(f"Successfully inserted/updated {records_inserted} records into the database.")
        except DatabaseError as e:
            logger.error(f"Database insertion failed: {e}")
            raise
    else:
        logger.info("No valid data to insert.")
    return records_inserted

def run_data_collection_job():
    job_status = "SUCCESS"
    logger.info("╔══════════════════════════════════════════════════════╗")
    logger.info("║           Starting Cryptocurrency Data Collection Job          ║")
    logger.info("╚══════════════════════════════════════════════════════╝")

    try:
        api_key = os.environ.get("COINMARKETCAP_API_KEY")
        if not api_key:
            logger.critical("COINMARKETCAP_API_KEY environment variable not set. Exiting.")
            sys.exit(1)

        client = CoinMarketCapAPI(api_key)

        logger.info("Connecting to PostgreSQL database...")
        with PostgresManager() as db_manager:
            db_manager.initialize_pool()
            logger.info("Successfully connected to PostgreSQL")

            db_manager.execute_query("""
                CREATE TABLE IF NOT EXISTS cryptocurrency_data (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    symbol VARCHAR(10) NOT NULL,
                    rank INT,
                    price_usd DECIMAL(20, 10),
                    volume_24h_usd DECIMAL(20, 10),
                    market_cap_usd DECIMAL(20, 10),
                    last_updated TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (name, symbol, last_updated)
                );
            """)
            logger.info("Database table 'cryptocurrency_data' ensured.")

            logger.info("Fetching data from CoinMarketCap API")
            api_response = client.get_latest_listings()

            if "error" in api_response:
                logger.error(f"API Error: {api_response['error']}")
                job_status = "FAILED"
                return {"status": "error", "message": api_response["error"]}, 500

            insert_crypto_data(api_response)
            logger.info("✅ Job completed successfully")
            job_status = "SUCCESS"

    except ImportError:
        logger.error("Failed to import required modules. Check that all dependencies are installed")
        job_status = "FAILED"
        return {"status": "error", "message": "Failed to import required modules"}, 500
    except Exception as e:
        logger.error(f"Job failed: {e}")
        job_status = "FAILED"
        return {"status": "error", "message": str(e)}, 500
    finally:
        end_time = logging.Formatter().formatTime(logging.LogRecord('', 0, '', 0, '', (), None), '%Y-%m-%d %H:%M:%S')
        color = Fore.GREEN if job_status == "SUCCESS" else Fore.RED

        end_banner = f"{color}╔{'═' * 54}╗{Style.RESET_ALL}"
        title_banner = f"{color}║{Style.BRIGHT}{Fore.WHITE} {'✅' if job_status == 'SUCCESS' else '❌'} CRYPTOCURRENCY DATA COLLECTION JOB {job_status}{' ' * (9 if job_status == 'SUCCESS' else 11)}{color}║{Style.RESET_ALL}"
        time_banner = f"{color}║{Fore.WHITE} Finished at: {end_time}{' ' * 29}{color}║{Style.RESET_ALL}"
        bottom_banner = f"{color}╚{'═' * 54}╝{Style.RESET_ALL}"

        logger.info(end_banner)
        logger.info(title_banner)
        logger.info(time_banner)
        logger.info(bottom_banner)
    return {"status": "success", "message": "Data collection job completed successfully"}, 200

app = Flask(__name__)

@app.route("/", methods=["GET"])
def index():
    """
    Main entry point for the Cloud Run service.
    Triggers the data collection job.
    """
    logger.info("Received HTTP request to trigger data collection job.")
    result, status_code = run_data_collection_job()
    return result, status_code

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Starting Flask application on port {port}")
    app.run(debug=False, host="0.0.0.0", port=port)
