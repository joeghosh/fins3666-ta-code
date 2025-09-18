#!/usr/bin/env python3
"""
Activity 2: Data for AU listed ETF NDQ and all hedging instruments
Collects historical data for NDQ ETF, NQ Futures, AUD/USD FX, and QQQ ETF
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import TickerId
import pandas as pd
import threading
import time
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Activity2HedgingDataCollector(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data_storage = {}  # Store data by instrument
        self.historical_ticks = []
        self.historical_bars = []
        self.current_instrument = ""
        self.request_complete = threading.Event()
        
    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        logger.error(f"Error {reqId}: {errorCode} - {errorString}")
        if errorCode == 162:  # Pacing violation
            logger.warning("Pacing violation - waiting...")
            time.sleep(10)
    
    def historicalTicksLast(self, reqId: int, ticks, done: bool):
        """Receives historical time & sales data - CORRECT callback name!"""
        logger.info(f"Received {len(ticks)} ticks for {self.current_instrument}")
        
        for tick in ticks:
            self.historical_ticks.append({
                'instrument': self.current_instrument,
                'time': datetime.fromtimestamp(tick.time),
                'price': tick.price,
                'size': tick.size,
                'exchange': getattr(tick, 'exchange', ''),
            })
        
        if done:
            self.request_complete.set()
    
    def historicalTicks(self, reqId: int, ticks, done: bool):
        """Receives historical time & sales data"""
        logger.info(f"Received {len(ticks)} historical ticks for reqId {reqId}")
        for tick in ticks:
            self.historical_ticks.append({
                'instrument': self.current_instrument,
                'time': datetime.fromtimestamp(tick.time),
                'price': tick.price,
                'size': tick.size,
                'exchange': getattr(tick, 'exchange', ''),
                'special_conditions': getattr(tick, 'specialConditions', '')
            })
        
        if done:
            logger.info(f"Historical ticks request {reqId} completed")
            self.request_complete.set()

    def historicalData(self, reqId: int, bar):
        """Receives historical bar data"""
        if bar.date.startswith("finished"):
            self.request_complete.set()
            return
            
        self.historical_bars.append({
            'instrument': self.current_instrument,
            'datetime': bar.date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume,
            'wap': bar.wap,
            'count': bar.barCount
        })
    
    def create_contracts(self) -> Dict[str, Contract]:
        """Define all hedging instruments"""
        contracts = {}
        
        # # 1. AU ETF NDQ (BetaShares NASDAQ 100 ETF)
        contracts['NDQ_AU'] = Contract()
        contracts['NDQ_AU'].symbol = "NDQ"
        contracts['NDQ_AU'].secType = "STK"
        contracts['NDQ_AU'].exchange = "ASX"
        contracts['NDQ_AU'].currency = "AUD"
        
        #2. NQ Futures (E-mini NASDAQ 100)
        # contracts['NQ_FUTURES'] = Contract()
        # contracts['NQ_FUTURES'].symbol = "NQ"
        # contracts['NQ_FUTURES'].secType = "FUT"
        # contracts['NQ_FUTURES'].exchange = "CME"
        # contracts['NQ_FUTURES'].currency = "USD"
        # contracts['NQ_FUTURES'].lastTradeDateOrContractMonth = "202506"

        # USE SP500 Futures as proxy for NQ Futures
        contracts['NQ_FUTURES'] = Contract()
        contracts['NQ_FUTURES'].conId = 495512563
        contracts['NQ_FUTURES'].exchange = "CME"
        
        # 3. AUD/USD FX
        contracts['AUDUSD'] = Contract()
        contracts['AUDUSD'].symbol = "AUD"
        contracts['AUDUSD'].secType = "CASH"
        contracts['AUDUSD'].exchange = "IDEALPRO"
        contracts['AUDUSD'].currency = "USD"
        
        # 4. US ETF QQQ (Invesco QQQ Trust)
        contracts['QQQ'] = Contract()
        contracts['QQQ'].symbol = "QQQ"
        contracts['QQQ'].secType = "STK"
        contracts['QQQ'].exchange = "SMART"
        contracts['QQQ'].currency = "USD"
        
        return contracts
    
    def collect_hedging_portfolio_data(self, target_date: str, duration: str = "1 D"):
        """
        Collect data for all hedging instruments
        
        Args:
            target_date: End date in format 'YYYYMMDD HH:MM:SS'
            duration: Data duration ('1 D' for 1 day, '1 W' for 1 week, etc.)
        """
        contracts = self.create_contracts()
        logger.info(f"Collecting hedging portfolio data for {len(contracts)} instruments")
        
        for instrument_name, contract in contracts.items():
            logger.info(f"Processing {instrument_name}...")
            self.current_instrument = instrument_name
            
            # Collect different types of data for each instrument
            try:
                # 1. Historical trades
                self._collect_trades(contract, target_date)
                
                # 2. Historical bars (multiple timeframes)
                self._collect_bars(contract, target_date, duration)
                
                print(self.data_storage)
                # Store data for this instrument
                self.data_storage[instrument_name] = {
                    'trades': pd.DataFrame([t for t in self.historical_ticks 
                                          if t['instrument'] == instrument_name]),
                    'bars': pd.DataFrame([b for b in self.historical_bars 
                                        if b['instrument'] == instrument_name])
                }
                
                time.sleep(3)  # Pacing between instruments
                
            except Exception as e:
                logger.error(f"Failed to collect data for {instrument_name}: {e}")
                continue
        
        return self.data_storage
    
    def _collect_trades(self, contract: Contract, end_date: str):
        """Collect historical trade data"""
        req_id = hash(self.current_instrument) % 10000
        
        # Collect trades in chunks
        for chunk in range(1):  # Up to 5,000 trades
            self.request_complete.clear()
            
            self.reqHistoricalTicks(
                reqId=req_id + chunk,
                contract=contract,
                startDateTime="",
                endDateTime=end_date,
                numberOfTicks=1000,
                whatToShow="TRADES",
                useRth=True,
                ignoreSize=False,
                miscOptions=[]
            )
            
            if not self.request_complete.wait(timeout=5):
                logger.warning(f"Timeout waiting for trades data for {self.current_instrument}")
                break
                
            time.sleep(1)  # Pacing
    
    def _collect_bars(self, contract: Contract, end_date: str, duration: str):
        """Collect historical bar data at different intervals"""
        intervals = ["1 min", "5 mins", "15 mins", "1 hour"]
        base_req_id = hash(self.current_instrument + "bars") % 10000
        
        for i, interval in enumerate(intervals):
            self.request_complete.clear()
            
            self.reqHistoricalData(
                reqId=base_req_id + i,
                contract=contract,
                endDateTime=end_date,
                durationStr=duration,
                barSizeSetting=interval,
                whatToShow="MIDPOINT", # if contract.secType == "CASH" else "TRADES",
                useRTH=1,
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[]
            )
            
            if not self.request_complete.wait(timeout=30):
                logger.warning(f"Timeout waiting for {interval} bars for {self.current_instrument}")
                
            time.sleep(2)  # Pacing
    
    def analyze_correlations(self) -> pd.DataFrame:
        """Analyze correlations between instruments using 15-minute bars"""
        correlation_data = {}
        
        for instrument, data in self.data_storage.items():
            bars_df = data['bars']
            if not bars_df.empty:
                # Filter for 15-minute bars and get closing prices
                bars_15min = bars_df[bars_df['datetime'].str.contains('15 mins', na=False)]
                if not bars_15min.empty:
                    correlation_data[instrument] = bars_15min['close'].values
        
        if len(correlation_data) >= 2:
            correlation_df = pd.DataFrame(correlation_data)
            return correlation_df.corr()
        else:
            return pd.DataFrame()
    
    def save_all_data(self, base_filename: str):
        """Save all collected data to CSV files"""
        for instrument, data in self.data_storage.items():
            # Save trades
            if not data['trades'].empty:
                trades_file = f"{base_filename}_{instrument}_trades.csv"
                data['trades'].to_csv(trades_file, index=False)
                logger.info(f"Saved {len(data['trades'])} trades to {trades_file}")
            
            # Save bars
            if not data['bars'].empty:
                bars_file = f"{base_filename}_{instrument}_bars.csv"
                data['bars'].to_csv(bars_file, index=False)
                logger.info(f"Saved {len(data['bars'])} bars to {bars_file}")
        
        # Save correlation analysis
        correlations = self.analyze_correlations()
        if not correlations.empty:
            corr_file = f"{base_filename}_correlations.csv"
            correlations.to_csv(corr_file)
            logger.info(f"Saved correlation analysis to {corr_file}")

def main():
    """Example usage"""
    app = Activity2HedgingDataCollector()
    
    # Connect to TWS/IB Gateway
    app.connect("172.23.128.1", 7497, clientId=2)
    
    # Start the socket in a separate thread
    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()
    
    time.sleep(2)  # Allow connection to establish
    
    # Collect hedging portfolio data
    target_date = "20250615 16:00:00"  # March 15, 2024
    
    try:
        data = app.collect_hedging_portfolio_data(target_date, "1 D")
        
        # Save all data
        app.save_all_data(f"hedging_portfolio_{target_date[:8]}")
        
        # Print summary
        logger.info("\n=== DATA COLLECTION SUMMARY ===")
        for instrument, data in app.data_storage.items():
            trades_count = len(data['trades']) if not data['trades'].empty else 0
            bars_count = len(data['bars']) if not data['bars'].empty else 0
            logger.info(f"{instrument}: {trades_count} trades, {bars_count} bars")
            
    except Exception as e:
        logger.error(f"Data collection failed: {e}")
    
    finally:
        app.disconnect()

if __name__ == "__main__":
    main()