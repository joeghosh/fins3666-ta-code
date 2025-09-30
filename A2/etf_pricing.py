import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import traceback
import pytz

## NOTES
# EOD Nasdq data needed to have first and last line removed

class ETFPricer:
    def __init__(self):
        self.eod_data = None
        self.ndq_data = None
        self.fx_data = None
        self.nq_futures_data = None
        self.pricing_results = None
        self.sydney_tz = 'Australia/Sydney'
    
    def load_data(self, eod_file, ndq_file, fx_file, nq_file):
        """Load all data files into pandas DataFrames and normalize to Sydney time"""
        print("Loading data files...")
        try:
            self.eod_data = pd.read_csv(eod_file)
            # Parse date as naive, set time to 16:00 (4pm), localize to NY, then convert to Sydney
            self.eod_data['DateTime'] = pd.to_datetime(self.eod_data['Trade Date'], format='%d/%m/%Y')
            self.eod_data['DateTime'] = self.eod_data['DateTime'].apply(
                lambda d: pd.Timestamp(d.year, d.month, d.day, 16, 0)
            )
            ny_tz = pytz.timezone('America/New_York')
            self.eod_data['DateTime'] = self.eod_data['DateTime'].dt.tz_localize(ny_tz)
            self.eod_data['DateTime'] = self.eod_data['DateTime'].dt.tz_convert(self.sydney_tz)
            print(f"EOD data loaded: {len(self.eod_data)} records")

            # NDQ intraday: Assume Sydney time
            self.ndq_data = pd.read_csv(ndq_file)
            self.ndq_data['DateTime'] = pd.to_datetime(self.ndq_data['Date-Time'])
            if self.ndq_data['DateTime'].dt.tz is None:
                self.ndq_data['DateTime'] = self.ndq_data['DateTime'].dt.tz_localize(self.sydney_tz)
            else:
                self.ndq_data['DateTime'] = self.ndq_data['DateTime'].dt.tz_convert(self.sydney_tz)
            print(f"NDQ data loaded: {len(self.ndq_data)} records")

            # FX data: Assume UTC-5 (e.g., New York time)
            self.fx_data = pd.read_csv(fx_file)
            self.fx_data['DateTime'] = pd.to_datetime(self.fx_data['Date-Time'])
            ny_tz = pytz.timezone('America/New_York')
            if self.fx_data['DateTime'].dt.tz is None:
                self.fx_data['DateTime'] = self.fx_data['DateTime'].dt.tz_localize(ny_tz)
            else:
                self.fx_data['DateTime'] = self.fx_data['DateTime'].dt.tz_convert(ny_tz)
            self.fx_data['DateTime'] = self.fx_data['DateTime'].dt.tz_convert(self.sydney_tz)
            print(f"FX data loaded: {len(self.fx_data)} records")

            # NQ Futures data: Assume UTC
            self.nq_futures_data = pd.read_csv(nq_file)
            self.nq_futures_data['DateTime'] = pd.to_datetime(self.nq_futures_data['Date-Time'], utc=True)
            self.nq_futures_data['DateTime'] = self.nq_futures_data['DateTime'].dt.tz_convert(self.sydney_tz)
            print(f"NQ Futures data loaded: {len(self.nq_futures_data)} records")
            print("All data loaded and normalized to Sydney time!")

        except Exception as e:
            print(f"Error loading data: {e}")
            raise
    
    def get_latest_eod_nav(self, target_date):
        """Get the most recent EOD NAV for US ETFs tracking Nasdaq 100"""
        # Filter for the most recent trading day on or before target date
        target_date = pd.Timestamp(target_date).tz_convert(self.sydney_tz)
        print("Target date for EOD NAV:", target_date)
        available_dates = self.eod_data[self.eod_data['DateTime'] <= target_date]

        if available_dates.empty:
            raise ValueError("No EOD data available for the target date")
        
        latest_date = available_dates['DateTime'].max()
        latest_eod = available_dates[available_dates['DateTime'] == latest_date]

        base_nav = float(latest_eod['Universal Close Price'].iloc[0])
        
        print(f"Using EOD NAV: ${base_nav} from {latest_date.date()}")
        return base_nav, latest_date

    def price_etf_intraday(self, target_date_str='2025-03-20T10'):
        """Calculate NDQ fair value throughout the trading day using futures and FX multipliers"""
        target_date = pd.Timestamp(target_date_str).tz_localize(self.sydney_tz)
        print("Target date for intraday pricing:", target_date)
        # 1. Establish EOD anchor values (US close)
        base_nav_usd, eod_date = self.get_latest_eod_nav(target_date)
        print(f"EOD Date for reference: {eod_date}")
        # Find EOD NQ futures price (last price before/at EOD date)
        eod_nq_futures = self.nq_futures_data[self.nq_futures_data['DateTime'] <= eod_date]
        if eod_nq_futures.empty:
            print(self.nq_futures_data.head())
            raise ValueError("No NQ futures data available for EOD date")
        eod_nq_price = eod_nq_futures['Close Mid Price'].iloc[-1]

        # Find EOD FX rate (last rate before/at EOD date)
        eod_fx_data = self.fx_data[self.fx_data['DateTime'] <= eod_date]
        if eod_fx_data.empty:
            raise ValueError("No FX data available for EOD date")
        eod_fx_rate = eod_fx_data['Close Mid Price'].iloc[-1]

        ndq_trading_times = self.ndq_data[
            (self.ndq_data['DateTime'].dt.date == target_date.date()) &
            (self.ndq_data['DateTime'].dt.time >= datetime.strptime("10:00", "%H:%M").time()) &
            (self.ndq_data['DateTime'].dt.time <= datetime.strptime("16:00", "%H:%M").time())
        ]['DateTime'].unique()

        results = []

        print(f"Calculating fair values for {len(ndq_trading_times)} time points...")

        i = 0
        for timestamp in ndq_trading_times:
            try:
                # Ensure timestamp is timezone-aware and in Sydney time
                if isinstance(timestamp, pd.Timestamp):
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.tz_localize(self.sydney_tz)
                    else:
                        timestamp = timestamp.tz_convert(self.sydney_tz)
                else:
                    timestamp = pd.Timestamp(timestamp).tz_localize(self.sydney_tz)

                # 2. Get current NQ futures price
                current_nq_data = self.nq_futures_data[self.nq_futures_data['DateTime'] <= timestamp]
                if current_nq_data.empty:
                    current_nq_price = eod_nq_price
                else:
                    current_nq_price = current_nq_data['Close Mid Price'].iloc[-1]

                # 3. Get current FX rate
                current_fx_data = self.fx_data[self.fx_data['DateTime'] <= timestamp]
                if current_fx_data.empty:
                    current_fx_rate = eod_fx_rate
                else:
                    current_fx_rate = current_fx_data['Close Mid Price'].iloc[-1]

                # 4. Calculate multipliers
                nq_multiplier = current_nq_price / eod_nq_price
                fx_multiplier = current_fx_rate # / eod_fx_rate

                # 5. Synthetic fair value calculation
                fair_value_aud = base_nav_usd * nq_multiplier * fx_multiplier
                adjusted_nav_usd = base_nav_usd * nq_multiplier

                # 6. Actual NDQ price
                actual_ndq = self.ndq_data[self.ndq_data['DateTime'] == timestamp]
                close_mid = (actual_ndq['Close Bid'].iloc[0] + actual_ndq['Close Ask'].iloc[0])/2 if not actual_ndq.empty else np.nan

                results.append({
                    'DateTime': timestamp,
                    'Base_NAV_USD': base_nav_usd,
                    'EOD_NQ_Futures': eod_nq_price,
                    'EOD_FX_Rate': eod_fx_rate,
                    'Current_NQ_Futures': current_nq_price,
                    'Current_FX_Rate': current_fx_rate,
                    'NQ_Multiplier': nq_multiplier,
                    'FX_Multiplier': fx_multiplier,
                    'Adjusted_NAV_USD': adjusted_nav_usd,
                    'Fair_Value_AUD': fair_value_aud,
                    'Actual_NDQ_Mid': close_mid,
                    'Pricing_Error': close_mid - fair_value_aud if not np.isnan(close_mid) else np.nan
                })

                i += 1

            except Exception as e:
                print(f"Error calculating price for {timestamp}: {e}")
                print(traceback.format_exc())
                continue

        self.pricing_results = pd.DataFrame(results)
        print(f"Calculated {len(self.pricing_results)} price points")

        return self.pricing_results
    
    def analyze_results(self):
        """Analyze pricing results and print summary statistics"""
        if self.pricing_results is None:
            print("No pricing results available. Run price_etf_intraday() first.")
            return
        
        results = self.pricing_results.dropna(subset=['Pricing_Error'])
        
        print("\n" + "="*50)
        print("PRICING ANALYSIS SUMMARY")
        print("="*50)
        
        print(f"Total observations: {len(results)}")
        print(f"Mean pricing error: ${results['Pricing_Error'].mean():.4f}")
        print(f"Std dev of pricing error: ${results['Pricing_Error'].std():.4f}")
        print(f"Mean absolute error: ${results['Pricing_Error'].abs().mean():.4f}")
        print(f"Max positive error: ${results['Pricing_Error'].max():.4f}")
        print(f"Max negative error: ${results['Pricing_Error'].min():.4f}")
        
        print(f"\nFX Rate Range: {results['Current_FX_Rate'].min():.4f} - {results['Current_FX_Rate'].max():.4f}")
        print(f"Futures Adjustment Range: {results['NQ_Multiplier'].min():.4f} - {results['NQ_Multiplier'].max():.4f}")
        print(f"Fair Value Range: ${results['Fair_Value_AUD'].min():.2f} - ${results['Fair_Value_AUD'].max():.2f}")
    
    def plot_results(self):
        """Create plots showing pricing results"""
        if self.pricing_results is None:
            print("No pricing results available. Run price_etf_intraday() first.")
            return
        
        results = self.pricing_results.dropna()
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('NDQ ETF Pricing Analysis', fontsize=16)
        
        # Plot 1: Fair Value vs Actual Price
        axes[0,0].plot(results['DateTime'], results['Fair_Value_AUD'], 
                       label='Theoretical Fair Value', color='blue', alpha=0.7)
        axes[0,0].plot(results['DateTime'], results['Actual_NDQ_Mid'], 
                       label='Actual Market Price', color='red', alpha=0.7)
        axes[0,0].set_title('Fair Value vs Market Price')
        axes[0,0].set_ylabel('Price (AUD)')
        axes[0,0].legend()
        axes[0,0].tick_params(axis='x', rotation=45)
        
        # Plot 2: Pricing Error
        axes[0,1].plot(results['DateTime'], results['Pricing_Error'], 
                       color='green', alpha=0.7)
        axes[0,1].axhline(y=0, color='black', linestyle='--', alpha=0.5)
        axes[0,1].set_title('Pricing Error (Market - Fair Value)')
        axes[0,1].set_ylabel('Error (AUD)')
        axes[0,1].tick_params(axis='x', rotation=45)
        
        # Plot 3: AUD/USD Rate
        axes[1,0].plot(results['DateTime'], results['Current_FX_Rate'], 
                       color='orange', alpha=0.7)
        axes[1,0].set_title('AUD/USD Exchange Rate')
        axes[1,0].set_ylabel('AUD/USD')
        axes[1,0].tick_params(axis='x', rotation=45)
        
        # Plot 4: Futures Adjustment Factor
        axes[1,1].plot(results['DateTime'], results['NQ_Multiplier'], 
                       color='purple', alpha=0.7)
        axes[1,1].axhline(y=1.0, color='black', linestyle='--', alpha=0.5)
        axes[1,1].set_title('Futures Adjustment Factor')
        axes[1,1].set_ylabel('Adjustment Factor')
        axes[1,1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.show()

# Usage example:
def main():
    # Initialize the pricer
    pricer = ETFPricer()
    
    # Load data files (update these paths to your actual file locations)
    eod_file = "eod_nasdaq_100.csv"
    ndq_file = "ndq_data.csv"
    fx_file = "fx_data.csv"
    nq_file = "nq_data.csv"
    
    try:
        # Load all data
        pricer.load_data(eod_file, ndq_file, fx_file, nq_file)
        
        # Calculate intraday pricing
        results = pricer.price_etf_intraday('2025-03-20T10')
        
        # Analyze results
        pricer.analyze_results()
        
        # Plot results
        pricer.plot_results()
        
        # Save results to CSV
        results.to_csv('ndq_pricing_results.csv', index=False)
        print("\nResults saved to 'ndq_pricing_results.csv'")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()