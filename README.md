# Getting Started with the IBKR TWS API & Jupyter Notebooks

This guide will help you set up your environment to use the Interactive Brokers Trader Workstation (TWS) API with Python and Jupyter Notebooks, and run the provided code for Activities 1-3.

## Prerequisites

- **Interactive Brokers Account** (education one)
- **Windows, macOS, or Linux** machine

## Step 1: Install Trader Workstation (TWS)

1. Download TWS from the [Interactive Brokers website](https://www.interactivebrokers.com/en/index.php?f=16040).
2. Install and launch TWS.
3. Log in with your IBKR credentials.
4. Enable API access:
   - Go to `File > Global Configuration > API > Settings`
   - Check `Enable ActiveX and Socket Clients`
   - (Optional) Set trusted IPs and port (default is 7497 for live, 7496 for paper).

## Step 2: Download and Install the IBKR API Python Package

1. Download the IBKR API from [IBKR API Downloads](https://interactivebrokers.github.io/).
2. Unzip the package.
3. Install the Python API:
   ```bash
   cd ~/Downloads/IBJts/source/pythonclient
   python setup.py install
   ```
   Or, using pip:
   ```bash
   pip install ~/Downloads/IBJts/source/pythonclient
   ```

## Step 3: Set Up a Python Virtual Environment

1. **Install Python** (if not already installed):

   - [Download Python](https://www.python.org/downloads/) and follow your OS instructions.

2. **Create a virtual environment**:

   ```bash
   python3 -m venv ibkr_env
   source ibkr_env/bin/activate  # On Windows: ibkr_env\Scripts\activate
   ```

3. **Install required packages**:
   ```bash
   pip install pandas numpy jupyter
   ```

## Step 4: Launch Jupyter Notebook

1. Start Jupyter Notebook:
   ```bash
   jupyter notebook
   ```
2. Open your browser and navigate to the provided notebook files for Activities 1-3.

## Step 5: Run the Provided Code (Activities 1-3)

- Make sure TWS is running and API access is enabled.
- Open the notebook files for Activities 1-3.
- Run each cell in order.
- If you encounter connection errors, check your TWS API settings and port.

## References

- [IBKR Quant News: Introduction to TWS API with Jupyter Notebooks](https://www.interactivebrokers.com/campus/ibkr-quant-news/an-introduction-to-tws-api-with-jupyter-notebooks/)
- [IBKR API Documentation](https://interactivebrokers.github.io/)

## Troubleshooting

- **Connection Issues**: Ensure TWS is running and API is enabled.
- **Missing Packages**: Install with `pip install <package>`.
- **Python Not Found**: Install Python from [python.org](https://www.python.org/downloads/).

---

Happy coding!
