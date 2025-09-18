#!/usr/bin/env python3
"""
Activity 1: Order Book & Trade Feed data for a single Stock for a single day
Collects maximum possible historical data from IBKR TWS API
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import TickerId
import pandas as pd
import threading
import time
from datetime import datetime, timedelta
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Activity1DataCollector(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.historical_ticks = []
        self.historical_bars = []
        self.data_received = threading.Event()
        self.request_complete = threading.Event()
        
    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        logger.error(f"Error {reqId}: {errorCode} - {errorString}")
        if errorCode == 162:  # Historical data request pacing violation
            logger.warning("Pacing violation - waiting before next request...")
            time.sleep(10)
    
    def historicalTicks(self, reqId: int, ticks, done: bool):
        """Receives historical time & sales data"""
        logger.info(f"Received {len(ticks)} historical ticks for reqId {reqId}")
        for tick in ticks:
            self.historical_ticks.append({
                'time': datetime.fromtimestamp(tick.time),
                'price': tick.price,
                'size': tick.size,
                'exchange': getattr(tick, 'exchange', ''),
                'special_conditions': getattr(tick, 'specialConditions', '')
            })
        
        if done:
            logger.info(f"Historical ticks request {reqId} completed")
            self.request_complete.set()

    def historicalTicksLast(self, reqId: int, ticks, done: bool):
        """Receives historical time & sales data - CORRECT callback name!"""
        logger.info(f"Received {len(ticks)} ticks")
        
        for tick in ticks:
            self.historical_ticks.append({
                'reqId': reqId,
                'time': datetime.fromtimestamp(tick.time),
                'price': tick.price,
                'size': tick.size,
                'exchange': getattr(tick, 'exchange', ''),
            })
        
        if done:
            self.request_complete.set()
    
    def historicalData(self, reqId: int, bar):
        """Receives historical bar data"""
        if bar.date.startswith("finished"):
            logger.info(f"Historical bars request {reqId} completed")
            self.request_complete.set()
            return
            
        self.historical_bars.append({
            'datetime': bar.date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume,
            'wap': bar.wap,
            'count': bar.barCount
        })
    
    def collect_single_stock_data(self, symbol: str, exchange: str, target_date: str):
        """
        Collects all available historical data for a single stock on a specific date
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            exchange: Exchange (e.g., 'SMART', 'NYSE')
            target_date: Date in format 'YYYYMMDD HH:MM:SS'
        """
        # Define contract
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = exchange
        contract.currency = "USD"
        
        logger.info(f"Starting data collection for {symbol} on {target_date}")
        
        # 1. Collect historical Time & Sales data (Trade Feed)
        self._collect_historical_trades(contract, target_date)
        
        # 2. Collect historical bar data at multiple intervals
        self._collect_historical_bars(contract, target_date)
        
        return {
            'trades': pd.DataFrame(self.historical_ticks),
            'bars_1min': pd.DataFrame([b for b in self.historical_bars if '1 min' in str(b)]),
            'bars_5min': pd.DataFrame([b for b in self.historical_bars if '5 min' in str(b)]),
            'bars_15min': pd.DataFrame([b for b in self.historical_bars if '15 min' in str(b)])
        }
    
    def _collect_historical_trades(self, contract: Contract, end_date: str):
        """Collect all historical trades for the day (paginated requests)"""
        logger.info("Collecting historical Time & Sales data...")
        
        # Start from market open and collect in chunks
        current_end = end_date
        req_id = 1000
        
        for chunk in range(2):  # Collect up to 10,000 trades (10 x 1000 limit)
            self.request_complete.clear()
            
            # Request historical ticks (trades)
            self.reqHistoricalTicks(
                reqId=req_id + chunk,
                contract=contract,
                startDateTime="",
                endDateTime=current_end,
                numberOfTicks=1000,
                whatToShow="TRADES",
                useRth=True,  # Regular trading hours only
                ignoreSize=False,
                miscOptions=[]
            )
            
            # Wait for response
            self.request_complete.wait(timeout=5)
            
            # Check if we got less than 1000 ticks (end of data)
            current_chunk_trades = len([t for t in self.historical_ticks if t['reqId'] == req_id + chunk])
            if current_chunk_trades < 1000:
                logger.info(f"Reached end of available data (got {current_chunk_trades} trades)")
                break
                
            # Update end time for next request (get earlier data)
            if self.historical_ticks:
                # Get the earliest timestamp from current chunk
                chunk_trades = [t for t in self.historical_ticks if t['reqId'] == req_id + chunk]
                if chunk_trades:
                    earliest_time = min(chunk_trades, key=lambda x: x['time'])['time']
                    current_end = (earliest_time - timedelta(seconds=1)).strftime("%Y%m%d %H:%M:%S")
                    logger.info(f"Next request will end at: {current_end}")
            
            time.sleep(1)  # Pacing
    
    def _collect_historical_bars(self, contract: Contract, end_date: str):
        """Collect historical bar data at different intervals"""
        logger.info("Collecting historical bar data...")
        
        intervals = ["1 min", "5 mins", "15 mins", "1 hour"]
        req_id = 2000
        
        for i, interval in enumerate(intervals):
            self.request_complete.clear()
            
            self.reqHistoricalData(
                reqId=req_id,
                contract=contract,
                endDateTime=end_date,
                durationStr="1 D",  # One day of data
                barSizeSetting=interval,
                whatToShow="TRADES",
                useRTH=1,  # Regular trading hours
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[]
            )
            
            self.request_complete.wait(timeout=5)
            time.sleep(2)  # Pacing between requests

def main():
    """Example usage"""
    app = Activity1DataCollector()
    
    # Connect to TWS/IB Gateway
    app.connect("172.23.128.1", 7497, clientId=1)  # Use 7496 for TWS, 7497 for IB Gateway
    
    # Start the socket in a separate thread
    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()
    
    time.sleep(2)  # Allow connection to establish
    
    # Collect data for Apple stock on a specific trading day
    target_date = "20250615 16:00:00 US/Eastern"  # Example: March 15, 2024 at market close
    
    try:
        data = app.collect_single_stock_data("AAPL", "SMART", target_date)
        print(data['trades'].head())
        # Save collected data
        for data_type, df in data.items():
            if not df.empty:
                filename = f"AAPL_{data_type}_{target_date[:8]}.csv"
                #df.to_csv(filename, index=False)
                logger.info(f"Saved {len(df)} records to {filename}")
                
    except Exception as e:
        logger.error(f"Data collection failed: {e}")
    
    finally:
        app.disconnect()

if __name__ == "__main__":
    main()


## NOTES:
# Trades wont get the full day, have to paginate in 1000 tick chunks