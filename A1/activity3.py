#!/usr/bin/env python3
"""
Activity 3: Comprehensive Stock Data Collection
- EOD price data for one year
- Trade Feed data for three months
- Best Bid/Offer (BBO) data for three months at 1-minute intervals
- Best Bid/Offer (BBO) data for one day at 1-second intervals
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

class Activity3DataCollector(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.historical_bars = []
        self.historical_ticks = []
        self.bid_ask_ticks = []
        self.request_complete = threading.Event()
        self.current_request_info = {}
        self.error_messages = []
        
    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        error_msg = f"ReqId {reqId}: Error {errorCode} - {errorString}"
        logger.error(error_msg)
        self.error_messages.append({
            'reqId': reqId,
            'errorCode': errorCode,
            'errorString': errorString,
            'timestamp': datetime.now()
        })
        
        # Set completion for various error conditions
        if errorCode in [162, 200, 354, 10147, 10148, 2104, 2106]:
            self.request_complete.set()
    
    def historicalData(self, reqId: int, bar):
        """Receives historical bar data (EOD and intraday)"""
        if bar.date.startswith("finished"):
            logger.info(f"Historical bars request {reqId} completed")
            self.request_complete.set()
            return
            
        self.historical_bars.append({
            'reqId': reqId,
            'datetime': bar.date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume,
            'wap': bar.wap,
            'count': bar.barCount,
            'request_type': self.current_request_info.get(reqId, {}).get('type', ''),
            'symbol': self.current_request_info.get(reqId, {}).get('symbol', '')
        })

    def historicalDataEnd(self, reqId, start, end):
        """Called when historical data request is complete"""
        print(f"Historical data request {reqId} completed. Period: {start} to {end}")
        #self.request_complete = True
        self.request_complete.set()
    
    def historicalTicksLast(self, reqId: int, ticks, done: bool):
        """Receives historical trade ticks"""
        logger.info(f"Received {len(ticks)} trade ticks for reqId {reqId}")
        
        for tick in ticks:
            self.historical_ticks.append({
                'reqId': reqId,
                'time': datetime.fromtimestamp(tick.time),
                'price': tick.price,
                'size': tick.size,
                'exchange': getattr(tick, 'exchange', ''),
                'tick_type': 'TRADE',
                'symbol': self.current_request_info.get(reqId, {}).get('symbol', '')
            })
        
        if done:
            logger.info(f"Trade ticks request {reqId} completed")
            self.request_complete.set()
    
    def historicalTicksBidAsk(self, reqId: int, ticks, done: bool):
        """Receives historical bid/ask ticks (BBO data)"""
        logger.info(f"Received {len(ticks)} bid/ask ticks for reqId {reqId}")
        
        for tick in ticks:
            self.bid_ask_ticks.append({
                'reqId': reqId,
                'time': datetime.fromtimestamp(tick.time),
                'bid': tick.priceBid,
                'ask': tick.priceAsk,
                'bid_size': tick.sizeBid,
                'ask_size': tick.sizeAsk,
                'tick_type': 'BID_ASK',
                'symbol': self.current_request_info.get(reqId, {}).get('symbol', '')
            })
        
        if done:
            logger.info(f"Bid/Ask ticks request {reqId} completed")
            self.request_complete.set()
    
    def collect_comprehensive_data(self, symbol: str, exchange: str = "SMART"):
        """
        Collect all required data for Activity 3
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            exchange: Exchange (default 'SMART')
        """
        # Create contract
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = exchange
        contract.currency = "USD"
        
        logger.info(f"Starting comprehensive data collection for {symbol}")
        
        # Calculate date ranges
        now = datetime.now() - timedelta(days=1)  # Ensure we don't request future data
        one_year_ago = now - timedelta(days=365)
        three_months_ago = now - timedelta(days=90)
        one_day_ago = now - timedelta(days=1)
        
        results = {}
        
        # 1. EOD price data for one year
        logger.info("=" * 60)
        logger.info("1. Collecting EOD price data for one year...")
        logger.info("=" * 60)
        results['eod_1year'] = None #self._collect_eod_data(contract, symbol, one_year_ago, now)
        time.sleep(3)
        
        # 2. Trade Feed data for three months
        logger.info("=" * 60)
        logger.info("2. Collecting Trade Feed data for three months...")
        logger.info("=" * 60)
        results['trades_3months'] = None #self._collect_trade_data(contract, symbol, three_months_ago, now)
        time.sleep(3)
        
        # 3. BBO data for three months using 1-minute intervals
        logger.info("=" * 60)
        logger.info("3. Collecting BBO data for three months (1-minute bars)...")
        logger.info("=" * 60)
        results['bbo_3months_1min'] = self._collect_bbo_minute_data(contract, symbol, three_months_ago, now)
        time.sleep(3)
        
        # 4. BBO data for one day using 1-second intervals (tick data)
        logger.info("=" * 60)
        logger.info("4. Collecting BBO data for one day (1-second resolution)...")
        logger.info("=" * 60)
        results['bbo_1day_1sec'] =  None #self._collect_bbo_tick_data(contract, symbol, one_day_ago, now)
        
        return results
    
    def _collect_eod_data(self, contract: Contract, symbol: str, start_date: datetime, end_date: datetime):
        """Collect End-of-Day data for one year"""
        logger.info(f"Requesting EOD data from {start_date.date()} to {end_date.date()}")
        
        self.request_complete.clear()
        initial_bar_count = len(self.historical_bars)
        
        req_id = 1000
        self.current_request_info[req_id] = {
            'symbol': symbol,
            'type': 'EOD_1YEAR',
            'start_date': start_date,
            'end_date': end_date
        }
        
        # Request daily bars for 1 year
        self.reqHistoricalData(
            reqId=req_id,
            contract=contract,
            endDateTime=end_date.strftime("%Y%m%d %H:%M:%S") + " US/Eastern",
            durationStr="1 Y",  # 1 year
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=1,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[]
        )
        
        success = self.request_complete.wait(timeout=60)
        if not success:
            logger.error("Timeout collecting EOD data")
            return pd.DataFrame()
        
        # Filter and return EOD data
        eod_bars = [b for b in self.historical_bars[initial_bar_count:] if b['reqId'] == req_id]
        logger.info(f"Collected {len(eod_bars)} EOD bars")
        
        return pd.DataFrame(eod_bars)
    
    def _collect_trade_data(self, contract: Contract, symbol: str, start_date: datetime, end_date: datetime):
        """Collect trade data for three months using multiple requests"""
        logger.info(f"Requesting trade data from {start_date.date()} to {end_date.date()}")
        
        all_trades = []
        
        # Split into monthly chunks to avoid hitting limits
        current_start = start_date
        chunk_num = 0
        
        while current_start < end_date and chunk_num < 3:  # Max 3 months
            chunk_end = min(current_start + timedelta(days=30), end_date)
            
            logger.info(f"Collecting trade chunk {chunk_num + 1}: {current_start.date()} to {chunk_end.date()}")
            
            # Collect trades for this chunk using multiple paginated requests
            chunk_trades = self._collect_trade_chunk(contract, symbol, current_start, chunk_end, chunk_num)
            all_trades.extend(chunk_trades)
            
            current_start = chunk_end + timedelta(days=1)
            chunk_num += 1
            time.sleep(5)  # Pacing between chunks
        
        logger.info(f"Total trade data collected: {len(all_trades)} trades")
        return pd.DataFrame(all_trades)
    
    def _collect_trade_chunk(self, contract: Contract, symbol: str, start_date: datetime, end_date: datetime, chunk_num: int):
        """Collect trade data for a specific time chunk using pagination"""
        chunk_trades = []
        current_end = end_date
        page_num = 0
        max_pages = 10  # Limit pages per chunk
        
        while page_num < max_pages:
            self.request_complete.clear()
            initial_tick_count = len(self.historical_ticks)
            
            req_id = 2000 + (chunk_num * 100) + page_num
            self.current_request_info[req_id] = {
                'symbol': symbol,
                'type': f'TRADES_CHUNK_{chunk_num}_PAGE_{page_num}',
                'start_date': start_date,
                'end_date': current_end
            }
            
            logger.info(f"  Requesting trade page {page_num + 1}, ending at {current_end}")
            
            try:
                self.reqHistoricalTicks(
                    reqId=req_id,
                    contract=contract,
                    startDateTime="",
                    endDateTime=current_end.strftime("%Y%m%d %H:%M:%S") + " US/Eastern",
                    numberOfTicks=1000,
                    whatToShow="TRADES",
                    useRth=True,
                    ignoreSize=False,
                    miscOptions=[]
                )
                
                success = self.request_complete.wait(timeout=20)
                if not success:
                    logger.warning(f"Timeout on trade page {page_num + 1}")
                    break
                
                # Get new ticks from this request
                page_ticks = [t for t in self.historical_ticks[initial_tick_count:] if t['reqId'] == req_id]
                chunk_trades.extend(page_ticks)
                
                logger.info(f"  Page {page_num + 1}: {len(page_ticks)} trades")
                
                if len(page_ticks) < 1000:
                    logger.info(f"  End of data reached (got {len(page_ticks)} < 1000)")
                    break
                
                # Update end time for next page (earlier data)
                if page_ticks:
                    earliest_time = min(page_ticks, key=lambda x: x['time'])['time']
                    current_end = earliest_time - timedelta(seconds=1)
                    
                    if current_end < start_date:
                        logger.info(f"  Reached start date boundary")
                        break
                
            except Exception as e:
                logger.error(f"Error on trade page {page_num + 1}: {e}")
                break
            
            page_num += 1
            time.sleep(2)  # Pacing between requests
        
        logger.info(f"Chunk {chunk_num + 1} completed: {len(chunk_trades)} trades")
        return chunk_trades
    
    # def _collect_bbo_minute_data(self, contract: Contract, symbol: str, start_date: datetime, end_date: datetime):
    #     """Collect BBO data for three months using 1-minute bars with MIDPOINT"""
    #     logger.info(f"Requesting 1-minute BBO data from {start_date.date()} to {end_date.date()}")
        
    #     # Use multiple requests for different time periods to get full 3 months
    #     all_bbo_bars = []
        
    #     # Split into monthly chunks
    #     current_start = start_date
    #     month_num = 0
    #     req_id_change = 0
        
    #     while current_start < end_date and month_num < 3:
    #         chunk_end = min(current_start + timedelta(days=30), end_date)
            
    #         logger.info(f"BBO Month {month_num + 1}: {current_start.date()} to {chunk_end.date()}")
            
    #         self.request_complete.clear()
    #         initial_bar_count = len(self.historical_bars)
            
    #         req_id = 3000 + month_num
    #         self.current_request_info[req_id] = {
    #             'symbol': symbol,
    #             'type': f'BBO_1MIN_MONTH_{month_num + 1}',
    #             'start_date': current_start,
    #             'end_date': chunk_end
    #         }

    #         # BREAK THIS INTO BLOCKS OF 7 DAYS TO AVOID TIMEOUTS / LIMITS ON THE DATA
            
    #         # Request 1-minute bars with BID_ASK data
    #         self.reqHistoricalData(
    #             reqId=req_id + req_id_change,
    #             contract=contract,
    #             endDateTime=chunk_end.strftime("%Y%m%d %H:%M:%S") + " US/Eastern",
    #             durationStr="30 D",  # 30 days per request
    #             barSizeSetting="1 min",
    #             whatToShow="BID_ASK",  # This gives us bid/ask OHLC
    #             useRTH=1,
    #             formatDate=1,
    #             keepUpToDate=False,
    #             chartOptions=[]
    #         )
            
    #         success = self.request_complete.wait(timeout=60)
    #         if success:
    #             month_bars = [b for b in self.historical_bars[initial_bar_count:] if b['reqId'] == req_id]
    #             all_bbo_bars.extend(month_bars)
    #             logger.info(f"  Month {month_num + 1}: {len(month_bars)} 1-minute BBO bars")
    #         else:
    #             logger.warning(f"Timeout on BBO month {month_num + 1}")
            
    #         current_start = chunk_end + timedelta(days=1)
    #         month_num += 1
    #         req_id_change += 1
    #         time.sleep(5)
        
    #     logger.info(f"Total 1-minute BBO bars: {len(all_bbo_bars)}")
    #     return pd.DataFrame(all_bbo_bars)

    def _collect_bbo_minute_data(self, contract: Contract, symbol: str, start_date: datetime, end_date: datetime):
        """Collect BBO data for three months using 1-minute bars with BID_ASK, in 7-day blocks"""
        logger.info(f"Requesting 1-minute BBO data from {start_date.date()} to {end_date.date()}")

        all_bbo_bars = []
        current_start = start_date
        block_num = 0
        req_id_change = 0

        while current_start < end_date:
            chunk_end = min(current_start + timedelta(days=7), end_date)

            logger.info(f"BBO Block {block_num + 1}: {current_start.date()} to {chunk_end.date()}")

            self.request_complete.clear()
            initial_bar_count = len(self.historical_bars)

            req_id = 3000 + block_num
            self.current_request_info[req_id] = {
                'symbol': symbol,
                'type': f'BBO_1MIN_BLOCK_{block_num + 1}',
                'start_date': current_start,
                'end_date': chunk_end
            }

            self.reqHistoricalData(
                reqId=req_id,
                contract=contract,
                endDateTime=chunk_end.strftime("%Y%m%d %H:%M:%S") + " US/Eastern",
                durationStr="7 D",
                barSizeSetting="1 min",
                whatToShow="BID_ASK",
                useRTH=1,
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[]
            )

            success = self.request_complete.wait(timeout=60)
            if success:
                block_bars = [b for b in self.historical_bars[initial_bar_count:] if b['reqId'] == req_id]
                all_bbo_bars.extend(block_bars)
                logger.info(f"  Block {block_num + 1}: {len(block_bars)} 1-minute BBO bars")
            else:
                logger.warning(f"Timeout on BBO block {block_num + 1}")

            current_start = chunk_end + timedelta(days=1)
            block_num += 1
            time.sleep(10)

        logger.info(f"Total 1-minute BBO bars: {len(all_bbo_bars)}")
        return pd.DataFrame(all_bbo_bars)
    
    def _collect_bbo_tick_data(self, contract: Contract, symbol: str, start_date: datetime, end_date: datetime):
        """Collect high-resolution BBO data for one day using bid/ask ticks"""
        logger.info(f"Requesting 1-second BBO tick data for {start_date.date()}")
        
        all_bbo_ticks = []
        
        # Use multiple paginated requests to get full day of bid/ask ticks
        current_end = end_date
        page_num = 0
        max_pages = 20  # Allow more pages for 1 day of high-res data
        
        while page_num < max_pages:
            self.request_complete.clear()
            initial_tick_count = len(self.bid_ask_ticks)
            
            req_id = 4000 + page_num
            self.current_request_info[req_id] = {
                'symbol': symbol,
                'type': f'BBO_1SEC_PAGE_{page_num + 1}',
                'start_date': start_date,
                'end_date': current_end
            }
            
            logger.info(f"  BBO tick page {page_num + 1}, ending at {current_end}")
            
            try:
                self.reqHistoricalTicks(
                    reqId=req_id,
                    contract=contract,
                    startDateTime="",
                    endDateTime=current_end.strftime("%Y%m%d %H:%M:%S"),
                    numberOfTicks=1000,
                    whatToShow="BID_ASK",
                    useRth=True,
                    ignoreSize=False,
                    miscOptions=[]
                )
                
                success = self.request_complete.wait(timeout=20)
                if not success:
                    logger.warning(f"Timeout on BBO page {page_num + 1}")
                    break
                
                # Get new bid/ask ticks from this request
                page_ticks = [t for t in self.bid_ask_ticks[initial_tick_count:] if t['reqId'] == req_id]
                all_bbo_ticks.extend(page_ticks)
                
                logger.info(f"  BBO page {page_num + 1}: {len(page_ticks)} bid/ask ticks")
                
                if len(page_ticks) < 1000:
                    logger.info(f"  End of BBO data (got {len(page_ticks)} < 1000)")
                    break
                
                # Update end time for next page
                if page_ticks:
                    earliest_time = min(page_ticks, key=lambda x: x['time'])['time']
                    current_end = earliest_time - timedelta(seconds=1)
                    
                    if current_end < start_date:
                        logger.info(f"  Reached start date for BBO data")
                        break
                
            except Exception as e:
                logger.error(f"Error on BBO page {page_num + 1}: {e}")
                break
            
            page_num += 1
            time.sleep(2)  # Pacing
        
        logger.info(f"Total BBO ticks collected: {len(all_bbo_ticks)}")
        return pd.DataFrame(all_bbo_ticks)
    
    def save_results(self, results: Dict, symbol: str, base_filename: str):
        """Save all collected data to CSV files"""
        logger.info(f"\n{'='*60}")
        logger.info("SAVING RESULTS")
        logger.info(f"{'='*60}")
        
        saved_files = []
        
        for data_type, df in results.items():
            if not df.empty:
                filename = f"{base_filename}_{symbol}_{data_type}.csv"
                df.to_csv(filename, index=False)
                logger.info(f"Saved {len(df)} records to {filename}")
                saved_files.append((filename, len(df)))
            else:
                logger.warning(f"No data collected for {data_type}")
        
        return saved_files
    
    def print_summary(self, results: Dict, symbol: str):
        """Print a summary of collected data"""
        logger.info(f"\n{'='*80}")
        logger.info(f"ACTIVITY 3 DATA COLLECTION SUMMARY FOR {symbol}")
        logger.info(f"{'='*80}")
        
        for data_type, df in results.items():
            count = len(df) if not df.empty else 0
            
            if data_type == 'eod_1year':
                logger.info(f"1. EOD Data (1 Year):           {count:,} daily bars")
            elif data_type == 'trades_3months':
                logger.info(f"2. Trade Data (3 Months):       {count:,} individual trades")
            elif data_type == 'bbo_3months_1min':
                logger.info(f"3. BBO Data (3 Months, 1-min):  {count:,} minute bars")
            elif data_type == 'bbo_1day_1sec':
                logger.info(f"4. BBO Data (1 Day, 1-sec):     {count:,} bid/ask ticks")
        
        total_data_points = sum(len(df) for df in results.values() if not df.empty)
        logger.info(f"\nTotal Data Points Collected: {total_data_points:,}")

def main():
    """Example usage for Activity 3"""
    app = Activity3DataCollector()
    
    try:
        # Connect to IBKR
        logger.info("Connecting to IBKR...")
        app.connect("172.23.128.1", 7497, clientId=4)
        
        api_thread = threading.Thread(target=app.run, daemon=True)
        api_thread.start()
        time.sleep(3)
        
        if not app.isConnected():
            logger.error("Failed to connect to IBKR")
            return
        
        logger.info("Connected successfully")
        
        # Collect comprehensive data for a single stock
        symbol = "AAPL"  # Can be changed to any stock
        
        results = app.collect_comprehensive_data(symbol, exchange="SMART")
        
        # Save results
        base_filename = f"activity3_{datetime.now().strftime('%Y%m%d')}"
        saved_files = app.save_results(results, symbol, base_filename)
        
        # Print summary
        app.print_summary(results, symbol)
        
        # Additional analysis if data was collected
        if saved_files:
            logger.info(f"\n{'='*60}")
            logger.info("FILES CREATED:")
            for filename, count in saved_files:
                logger.info(f"  - {filename} ({count:,} records)")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        app.disconnect()
        logger.info("Disconnected from IBKR")

if __name__ == "__main__":
    main()