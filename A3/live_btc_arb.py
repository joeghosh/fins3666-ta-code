#!/usr/bin/env python3
"""
BTC-AUD Synthetic Price Monitor
===============================
This script:
1. Connects to IBKR TWS API to get live BTC-USD and AUD-USD rates
2. Calculates synthetic BTC-AUD price
3. Fetches live BTC-AUD prices from CoinSpot
4. Compares synthetic vs actual prices with configurable bid/ask skew
5. Identifies potential arbitrage opportunities

Requirements:
- pip install ibapi requests websocket-client threading
- IBKR TWS or Gateway running with API enabled
"""

import os
import time
import threading
import json
import requests
from datetime import datetime
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import TickerId
from ibapi.ticktype import TickType
from typing import Dict, Optional


class PriceMonitor(EWrapper, EClient):
    def __init__(self, bid_skew: float = 0.002, ask_skew: float = 0.002):
        EClient.__init__(self, self)
        
        # Price storage
        self.btc_usd_bid = None
        self.btc_usd_ask = None
        self.btc_usd_last = None
        self.aud_usd_bid = None
        self.aud_usd_ask = None
        self.aud_usd_last = None
        
        # CoinSpot prices
        self.coinspot_btc_bid = None
        self.coinspot_btc_ask = None
        self.coinspot_btc_last = None
        self.coinspot_last_update = None
        
        # Configuration
        self.bid_skew = bid_skew  # e.g., 0.002 = 0.2% below mid
        self.ask_skew = ask_skew  # e.g., 0.002 = 0.2% above mid
        
        # Request ID tracking
        self.btc_usd_req_id = 1001
        self.aud_usd_req_id = 1002
        
        # Threading
        self.running = True
        self.coinspot_thread = None
        
        print(f"Price Monitor initialized with bid skew: {bid_skew*100:.2f}%, ask skew: {ask_skew*100:.2f}%")
        
    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        """Handle API errors"""
        if errorCode != 2104 and errorCode != 2106 and errorCode != 2158:  # Ignore common info messages
            print(f"Error {errorCode}: {errorString} (Request ID: {reqId})")
        
    def nextValidId(self, orderId: int):
        """Connection established callback"""
        print(f"IBKR connection established. Next valid order ID: {orderId}")
        self.start_market_data()
        
    def connectAck(self):
        """Connection acknowledgment"""
        print("IBKR connection acknowledged")
        
    def create_crypto_contract(self, symbol: str, exchange: str = "PAXOS") -> Contract:
        """Create contract for cryptocurrency"""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "CRYPTO"
        contract.exchange = exchange
        contract.currency = "USD"
        return contract
        
    def create_forex_contract(self, symbol: str) -> Contract:
        """Create contract for forex pair"""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "CASH"
        contract.exchange = "IDEALPRO"
        contract.currency = "USD"
        return contract
        
    def start_market_data(self):
        """Request market data for BTC-USD and AUD-USD"""
        print("Starting market data subscriptions...")
        
        # Request BTC-USD data
        btc_contract = self.create_crypto_contract("BTC")
        self.reqMktData(self.btc_usd_req_id, btc_contract, "", False, False, [])
        print(f"Requested BTC-USD data (req_id: {self.btc_usd_req_id})")
        
        # Request AUD-USD data
        aud_contract = self.create_forex_contract("AUD")
        self.reqMktData(self.aud_usd_req_id, aud_contract, "", False, False, [])
        print(f"Requested AUD-USD data (req_id: {self.aud_usd_req_id})")
        
    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib):
        """Receive tick price updates"""
        now = datetime.now().strftime("%H:%M:%S")

        print(f"[{now}] Tick Price Update - ReqID: {reqId}, Type: {tickType}, Price: {price}")
        
        # 1 = BID, 2 = ASK, 4 = LAST

        if reqId == self.btc_usd_req_id:
            if tickType == 1:
                self.btc_usd_bid = price
                print(f"[{now}] BTC-USD Bid: ${price:,.2f}")
            elif tickType == 2:
                self.btc_usd_ask = price
                print(f"[{now}] BTC-USD Ask: ${price:,.2f}")
            elif tickType == 4:
                self.btc_usd_last = price
                print(f"[{now}] BTC-USD Last: ${price:,.2f}")
                
        elif reqId == self.aud_usd_req_id:
            if tickType == 1:
                self.aud_usd_bid = price
                print(f"[{now}] AUD-USD Bid: ${price:.4f}")
            elif tickType == 2:
                self.aud_usd_ask = price
                print(f"[{now}] AUD-USD Ask: ${price:.4f}")
            elif tickType == 4:
                self.aud_usd_last = price
                print(f"[{now}] AUD-USD Last: ${price:.4f}")
        
        # Calculate synthetic prices whenever we get updates
        self.calculate_and_compare()
        
    def fetch_coinspot_prices(self):
        """Fetch current BTC-AUD prices from CoinSpot API"""
        try:
            print("Fetching CoinSpot BTC-AUD prices...")
            # CoinSpot public API endpoint for latest prices
            url = "https://www.coinspot.com.au/pubapi/v2/latest/btc"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'ok':
                    prices = data.get('prices', {})
                    
                    self.coinspot_btc_bid = float(prices.get('bid', 0))
                    self.coinspot_btc_ask = float(prices.get('ask', 0))
                    self.coinspot_btc_last = float(prices.get('last', 0))
                    self.coinspot_last_update = datetime.now()
                    return True
                else:
                    print(f"CoinSpot API error: {data.get('message', 'Unknown error')}")
            else:
                print(f"CoinSpot API request failed: {response.status_code}")
                
        except Exception as e:
            print(f"Error fetching CoinSpot prices: {e}")
            
        return False
        
    def coinspot_price_updater(self):
        """Continuously update CoinSpot prices"""
        while self.running:
            self.fetch_coinspot_prices()
            time.sleep(10)  # Update every 10 seconds
            
    def calculate_synthetic_btc_aud(self) -> Dict[str, Optional[float]]:
        """Calculate synthetic BTC-AUD prices"""
        result = {
            'bid': None,
            'ask': None,
            'mid': None
        }
        
        # Need both BTC-USD and AUD-USD data
        if (self.btc_usd_bid is not None and self.btc_usd_ask is not None and
            self.aud_usd_bid is not None and self.aud_usd_ask is not None):
            
            # Calculate synthetic BTC-AUD
            # BTC-AUD = BTC-USD / AUD-USD
            synthetic_bid = self.btc_usd_bid / self.aud_usd_ask  # Use worst case for bid
            synthetic_ask = self.btc_usd_ask / self.aud_usd_bid  # Use worst case for ask
            synthetic_mid = (synthetic_bid + synthetic_ask) / 2
            
            # Apply skews to create our quoted prices
            result['mid'] = synthetic_mid
            result['bid'] = synthetic_mid * (1 - self.bid_skew)
            result['ask'] = synthetic_mid * (1 + self.ask_skew)
            
        return result
        
    def calculate_and_compare(self):
        """Calculate synthetic prices and compare with CoinSpot"""
        synthetic = self.calculate_synthetic_btc_aud()
        
        if synthetic['mid'] is None:
            return  # Not enough data yet

        os.system('clear')   
        now = datetime.now().strftime("%H:%M:%S")
        print(f"\n{'='*80}")
        print(f"[{now}] PRICE COMPARISON")
        print(f"{'='*80}")
        
        # Display synthetic prices
        print(f"SYNTHETIC BTC-AUD:")
        print(f"  Mid:     ${synthetic['mid']:,.2f}")
        print(f"  Bid:     ${synthetic['bid']:,.2f} ({-self.bid_skew*100:.2f}% from mid)")
        print(f"  Ask:     ${synthetic['ask']:,.2f} (+{self.ask_skew*100:.2f}% from mid)")
        
        # Display CoinSpot prices if available
        if (self.coinspot_btc_bid and self.coinspot_btc_ask and 
            self.coinspot_last_update):
            
            age = (datetime.now() - self.coinspot_last_update).total_seconds()
            print(f"\nCOINSPOT BTC-AUD (age: {age:.0f}s):")
            print(f"  Bid:     ${self.coinspot_btc_bid:,.2f}")
            print(f"  Ask:     ${self.coinspot_btc_ask:,.2f}")
            print(f"  Last:    ${self.coinspot_btc_last:,.2f}")
            
            # Calculate spreads and opportunities
            coinspot_mid = (self.coinspot_btc_bid + self.coinspot_btc_ask) / 2
            coinspot_spread = self.coinspot_btc_ask - self.coinspot_btc_bid
            synthetic_spread = synthetic['ask'] - synthetic['bid']
            
            print(f"\nSPREAD ANALYSIS:")
            print(f"  CoinSpot spread: ${coinspot_spread:,.2f} ({coinspot_spread/coinspot_mid*100:.3f}%)")
            print(f"  Synthetic spread: ${synthetic_spread:,.2f} ({synthetic_spread/synthetic['mid']*100:.3f}%)")
            
            # Arbitrage opportunities
            print(f"\nARBITRAGE OPPORTUNITIES:")
            
            # Can we buy from CoinSpot and sell synthetic?
            buy_coinspot_profit = synthetic['bid'] - self.coinspot_btc_ask
            if buy_coinspot_profit > 0:
                print(f"  📈 BUY CoinSpot, SELL Synthetic: +${buy_coinspot_profit:,.2f} ({buy_coinspot_profit/self.coinspot_btc_ask*100:.3f}%)")
            else:
                print(f"  📉 Buy CoinSpot, Sell Synthetic: ${buy_coinspot_profit:,.2f} ({buy_coinspot_profit/self.coinspot_btc_ask*100:.3f}%)")
            
            # Can we buy synthetic and sell to CoinSpot?
            buy_synthetic_profit = self.coinspot_btc_bid - synthetic['ask']
            if buy_synthetic_profit > 0:
                print(f"  📈 BUY Synthetic, SELL CoinSpot: +${buy_synthetic_profit:,.2f} ({buy_synthetic_profit/synthetic['ask']*100:.3f}%)")
            else:
                print(f"  📉 Buy Synthetic, Sell CoinSpot: ${buy_synthetic_profit:,.2f} ({buy_synthetic_profit/synthetic['ask']*100:.3f}%)")
                
            # Price differences
            mid_diff = synthetic['mid'] - coinspot_mid
            print(f"\nPRICE DIFFERENCES:")
            print(f"  Synthetic vs CoinSpot mid: ${mid_diff:,.2f} ({mid_diff/coinspot_mid*100:.3f}%)")
            
        else:
            print(f"\nCOINSPOT: No data available")
            
        print(f"{'='*80}\n")
        
    def run_monitor(self, ibkr_host: str = "172.23.128.1", ibkr_port: int = 7497):
        """Main monitoring loop"""
        print("Starting BTC-AUD Synthetic Price Monitor...")
        print(f"Connecting to IBKR at {ibkr_host}:{ibkr_port}")
        
        try:
            # Start CoinSpot price updater thread
            self.coinspot_thread = threading.Thread(target=self.coinspot_price_updater, daemon=True)
            self.coinspot_thread.start()
            print("CoinSpot price updater started")
            
            # Connect to IBKR
            self.connect(ibkr_host, ibkr_port, clientId=1)
            
            # Start IBKR message processing
            ibkr_thread = threading.Thread(target=self.run, daemon=True)
            ibkr_thread.start()
            
            # Wait for connection
            time.sleep(3)
            
            if self.isConnected():
                print("✅ Connected to IBKR successfully!")
                
                # Initial CoinSpot price fetch
                print("Fetching initial CoinSpot prices...")
                self.fetch_coinspot_prices()
                
                # Keep the main thread alive
                try:
                    while True:
                        time.sleep(1)
                        
                except KeyboardInterrupt:
                    print("\nShutting down...")
                    self.running = False
                    
            else:
                print("❌ Failed to connect to IBKR!")
                print("Make sure TWS or IB Gateway is running and API is enabled.")
                
        except Exception as e:
            print(f"Error in monitor: {e}")
            
        finally:
            self.running = False
            if self.isConnected():
                # Cancel market data
                self.cancelMktData(self.btc_usd_req_id)
                self.cancelMktData(self.aud_usd_req_id)
                self.disconnect()
                print("Disconnected from IBKR")


def main():
    """Main function"""
    print("BTC-AUD Synthetic Price Monitor")
    print("=" * 50)
    
    # Configuration
    BID_SKEW = 0.002   # 0.2% below mid for our bid
    ASK_SKEW = 0.002   # 0.2% above mid for our ask
    IBKR_HOST = "172.23.128.1"
    IBKR_PORT = 7497   # 7497 for TWS, 4002 for Gateway
    
    print(f"Configuration:")
    print(f"  IBKR Host: {IBKR_HOST}:{IBKR_PORT}")
    print(f"  Bid Skew: {BID_SKEW*100:.2f}%")
    print(f"  Ask Skew: {ASK_SKEW*100:.2f}%")
    print(f"  CoinSpot Update Interval: 10 seconds")
    print()
    
    # Create and run monitor
    monitor = PriceMonitor(bid_skew=BID_SKEW, ask_skew=ASK_SKEW)
    monitor.run_monitor(ibkr_host=IBKR_HOST, ibkr_port=IBKR_PORT)


if __name__ == "__main__":
    main()