# ib_trading_module.py
import asyncio
import logging
import pandas as pd
from ib_async import IB, Stock, Forex, Future, Option, Crypto, Contract, Order, MarketOrder, LimitOrder, StopLimitOrder, StopOrder, util, Trade, Position, Fill, Execution, OrderStatus, PnL, PnLSingle, CommissionReport
import datetime
import os
import copy

# Configure logging for ib_async
util.logToConsole(logging.INFO) # Changed to INFO for less verbose default output
# util.logToConsole(logging.DEBUG) # Uncomment for more detailed logs


class IBTradingBot:
    """
    A trading bot for Interactive Brokers using the ib_async library.
    Manages connection, data fetching, order placement, and real-time CSV logging
    of portfolio, orders, and fills.
    """

    def __init__(self, host='127.0.0.1', port=7497, clientId=1,
                 portfolio_csv='portfolio.csv', orders_csv='orders.csv', fills_csv='fills.csv',
                 account_code=None):
        self.ib = IB()
        self.host = host
        self.port = port
        self.clientId = clientId
        self.account_code = account_code
        self.portfolio_csv_path = portfolio_csv
        self.orders_csv_path = orders_csv
        self.fills_csv_path = fills_csv
        self.current_positions = {}
        self.pnl_data = {}
        self.all_orders_log = [] # Stores dicts
        self.all_fills_log = []  # Stores dicts
        self.active_realtime_bars = {} # {conId: bars_object}
        self.open_trades_cache = {} # {orderId: Trade} - for quick access to Trade objects

        self._register_event_handlers()
        self._init_csv_files()

    def _register_event_handlers(self):
        self.ib.connectedEvent += self._on_connected
        self.ib.disconnectedEvent += self._on_disconnected
        self.ib.errorEvent += self._on_error
        self.ib.positionEvent += self._on_position # Streams all positions for the account
        self.ib.pnlSingleEvent += self._on_pnlSingle # Streams PnL for positions
        self.ib.orderStatusEvent += self._on_orderStatus
        self.ib.openOrderEvent += self._on_openOrder # Streams open orders at connection and updates
        self.ib.execDetailsEvent += self._on_execDetails
        self.ib.commissionReportEvent += self._on_commissionReport
        self.ib.accountSummaryEvent += self._on_accountSummary # Streams account summary data

    def _init_csv_files(self):
        self._rewrite_portfolio_csv() # Initialize with headers
        self._rewrite_orders_csv()    # Initialize with headers
        self._rewrite_fills_csv()     # Initialize with headers

    async def connect(self):
        if not self.ib.isConnected():
            try:
                self.ib._logger.info(f"Attempting to connect to {self.host}:{self.port} with clientId {self.clientId}...")
                await self.ib.connectAsync(self.host, self.port, clientId=self.clientId, timeout=20)
                # After connection, ib_async usually starts sending account data if event handlers are registered.
                # Explicit reqAccountUpdates is often not needed for the primary account.
            except asyncio.TimeoutError:
                self.ib._logger.error(f"Connection to {self.host}:{self.port} timed out.")
                raise
            except ConnectionRefusedError:
                self.ib._logger.error(f"Connection to {self.host}:{self.port} refused. Ensure TWS/Gateway is running and API is enabled.")
                raise
            except Exception as e:
                self.ib._logger.error(f"Connection failed: {e}")
                raise
        
        if self.ib.isConnected():
            if not self.account_code: # If no specific account code was provided at init
                managed_accounts = self.ib.managedAccounts()
                if managed_accounts:
                    self.account_code = managed_accounts[0] # Default to the first managed account
                    self.ib._logger.info(f"Automatically selected account: {self.account_code}")
                else:
                    self.ib._logger.warning("No managed accounts found by IB. Account specific features might be limited or use a default determined by TWS/Gateway.")
            
            # Subscribe to account summary data - this is a good practice
            # if self.account_code:
            #     self.ib.reqAccountSummary(True, 'All', self.account_code)
            # else:
            #     self.ib.reqAccountSummary(True, 'All', '') # For all accounts if specific one isn't set

            # Request initial load of open orders and executions
            # self.ib.reqOpenOrders() # Handled by openOrderEvent mostly
            # self.ib.reqExecutions() # Handled by execDetailsEvent

    async def disconnect(self):
        if self.ib.isConnected():
            self.ib._logger.info("Preparing to disconnect...")
            # Stop any active real-time bars
            for con_id in list(self.active_realtime_bars.keys()):
                await self.stop_realtime_bars(con_id) # Pass con_id
                
            # Unsubscribe from account summary
            # if self.account_code:
            #    self.ib.cancelAccountSummary(self.account_code)
            # else:
            #    self.ib.cancelAccountSummary('')

            self.ib.disconnect()
            self.ib._logger.info("Disconnected from IB.")

    def _on_connected(self):
        self.ib._logger.info("Successfully connected to IB.")
        # It's good practice to request positions explicitly after connection
        # if self.account_code:
        #    self.ib.reqPositions() # This will trigger _on_position events

    def _on_disconnected(self):
        self.ib._logger.info("Disconnected from IB.")

    def _on_error(self, reqId, errorCode, errorString, contract=None):
        # Filter out common informational messages
        info_codes = [2104, 2106, 2108, 2158, 2103, 2105, 2107, 2100, 399, 2150, 2168, 2169, 2170, 202, 321, 322]
        if errorCode in info_codes:
            self.ib._logger.info(f"IB Info. ReqId: {reqId}, Code: {errorCode}, Msg: {errorString}")
        else:
            self.ib._logger.error(f"IB Error. ReqId: {reqId}, Code: {errorCode}, Msg: {errorString}, Contract: {contract}")

    async def _on_position(self, position: Position):
        """Handles streaming position updates."""
        self.ib._logger.info(f"Position update: Account {position.account}, {position.contract.symbol} {position.position} @ {position.avgCost}")
        if position.position == 0: # Position closed
            if position.contract.conId in self.current_positions:
                del self.current_positions[position.contract.conId]
            if position.contract.conId in self.pnl_data:
                del self.pnl_data[position.contract.conId]
        else:
            self.current_positions[position.contract.conId] = position
            # Request PnL for this specific position
            if self.ib.isConnected(): # Ensure connected before making new requests
                 try:
                    await self.ib.reqPnLSingleAsync(position.account, '', conId=position.contract.conId)
                 except Exception as e:
                    self.ib._logger.error(f"Error requesting PnLSingle for {position.contract.symbol}: {e}")

        self._rewrite_portfolio_csv()

    async def _on_pnlSingle(self, pnlSingle: PnLSingle):
        """Handles streaming PnL updates for single positions."""
        # self.ib._logger.info(f"PnLSingle for conId {pnlSingle.conId}: DailyPnL={pnlSingle.dailyPnL}, UnRealizedPnL={pnlSingle.unrealizedPnL}")
        if pnlSingle.position != 0: # Only update if there's an active position
            self.pnl_data[pnlSingle.conId] = {
                'timestamp': datetime.datetime.now().isoformat(),
                'account': pnlSingle.account,
                'conId': pnlSingle.conId,
                'symbol': self.current_positions.get(pnlSingle.conId, Position(contract=Contract(symbol='UNKNOWN'))).contract.symbol, # Get symbol
                'position': pnlSingle.position,
                'marketPrice': pnlSingle.marketValue / pnlSingle.position if pnlSingle.position else 0,
                'marketValue': pnlSingle.value, # This is often the market value
                'dailyPnL': pnlSingle.dailyPnL,
                'unrealizedPnL': pnlSingle.unrealizedPnL,
                'realizedPnL': pnlSingle.realizedPnL, # This is for the PnLSingle request, not overall realized
            }
        elif pnlSingle.conId in self.pnl_data: # Position closed, remove PnL entry
            del self.pnl_data[pnlSingle.conId]
        self._rewrite_portfolio_csv()

    def _on_orderStatus(self, trade: Trade):
        """Handles order status updates for all orders."""
        self.ib._logger.info(f"Order Status: ID {trade.order.orderId} ({trade.contract.symbol}), PermID {trade.order.permId}, Status {trade.orderStatus.status}, Filled {trade.orderStatus.filled}, Remaining {trade.orderStatus.remaining}, AvgFillPrice {trade.orderStatus.avgFillPrice}")
        self.open_trades_cache[trade.order.orderId] = trade # Cache the trade object
        
        order_log_entry = next((o for o in self.all_orders_log if o['OrderId'] == trade.order.orderId), None)
        if order_log_entry:
            order_log_entry.update({
                'Status': trade.orderStatus.status,
                'Filled': trade.orderStatus.filled,
                'Remaining': trade.orderStatus.remaining,
                'AvgFillPrice': trade.orderStatus.avgFillPrice,
                'WhyHeld': trade.orderStatus.whyHeld or '',
                'LastUpdateTime': datetime.datetime.now().isoformat()
            })
        else:
            # This can happen if an order was placed before this session started tracking
            self.ib._logger.info(f"OrderStatus for OrderId {trade.order.orderId} not in local log, adding now.")
            self._add_order_to_log(trade.contract, trade.order, trade.orderStatus)
        self._rewrite_orders_csv()

    def _on_openOrder(self, trade: Trade): # trade object is passed here
        """Handles open order updates, typically at connection or for live orders not managed by placeOrder."""
        self.ib._logger.info(f"Open Order Event: ID {trade.order.orderId} ({trade.contract.symbol}), PermID {trade.order.permId}, Status {trade.orderStatus.status}, Action {trade.order.action}, Qty {trade.order.totalQuantity}")
        self.open_trades_cache[trade.order.orderId] = trade # Cache the trade object
        self._add_order_to_log(trade.contract, trade.order, trade.orderStatus)
        self._rewrite_orders_csv()


    def _on_execDetails(self, trade: Trade, fill: Fill):
        """Handles execution details (fills)."""
        self.ib._logger.info(f"Execution: {fill.execution.side} {fill.execution.shares} of {fill.contract.symbol} @ {fill.execution.price}. OrderId: {fill.execution.orderId}, ExecId: {fill.execution.execId}")
        exec_data = {
            'Timestamp': fill.time.isoformat() if isinstance(fill.time, datetime.datetime) else datetime.datetime.now().isoformat(),
            'Symbol': fill.contract.symbol, 'SecType': fill.contract.secType, 'Exchange': fill.contract.exchange,
            'LocalSymbol': fill.contract.localSymbol, 'ConID': fill.contract.conId,
            'OrderId': fill.execution.orderId, 'ExecId': fill.execution.execId,
            'Side': fill.execution.side, 'Shares': fill.execution.shares, 'Price': fill.execution.price,
            'CumQty': fill.execution.cumQty, 'AvgPrice': fill.execution.avgPrice,
            'EvRule': fill.execution.evRule or '', 'EvMultiplier': fill.execution.evMultiplier or 0.0,
            'Commission': None, # Will be updated by commission report
            'RealizedPnLFill': None # Will be updated by commission report
        }
        # Avoid duplicate fill entries
        if not any(f['ExecId'] == exec_data['ExecId'] for f in self.all_fills_log):
            self.all_fills_log.append(exec_data)
        self._rewrite_fills_csv()

    def _on_commissionReport(self, trade: Trade, fill: Fill, commissionReport: CommissionReport):
        """Handles commission reports."""
        self.ib._logger.info(f"Commission Report for ExecId {commissionReport.execId}: Comm {commissionReport.commission} {commissionReport.currency}, PnL {commissionReport.realizedPNL}")
        for f_log in reversed(self.all_fills_log): # Update the corresponding fill log
            if f_log['ExecId'] == commissionReport.execId:
                f_log['Commission'] = commissionReport.commission
                f_log['RealizedPnLFill'] = commissionReport.realizedPNL
                break
        self._rewrite_fills_csv()

    def _on_accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        """Handles account summary updates."""
        if tag in ['NetLiquidation', 'TotalCashValue', 'BuyingPower', 'AvailableFunds', 'RealizedPnL', 'UnrealizedPnL']:
            self.ib._logger.info(f"Account Summary ({account}): {tag} = {value} {currency}")
        # Optional: store for later use
        # self.account_summary_cache[tag] = {'account': account, 'value': value, 'currency': currency}



    def _rewrite_portfolio_csv(self):
        header = ['Timestamp', 'Account', 'ConID', 'Symbol', 'Position', 'AvgCost', 'MarketPrice', 'MarketValue', 'DailyPnL', 'UnrealizedPnL', 'RealizedPnL']
        # Combine position data with PnL data
        combined_data = []
        now_ts = datetime.datetime.now().isoformat()
        for con_id, pos in self.current_positions.items():
            pnl = self.pnl_data.get(con_id, {})
            combined_data.append({
                'Timestamp': now_ts,
                'Account': pos.account,
                'ConID': pos.contract.conId,
                'Symbol': pos.contract.symbol,
                'Position': pos.position,
                'AvgCost': pos.avgCost,
                'MarketPrice': pnl.get('marketPrice', pos.marketPrice if hasattr(pos, 'marketPrice') else 0), # Fallback if PnL not yet received
                'MarketValue': pnl.get('marketValue', pos.marketValue if hasattr(pos, 'marketValue') else 0),
                'DailyPnL': pnl.get('dailyPnL'),
                'UnrealizedPnL': pnl.get('unrealizedPnL'),
                'RealizedPnL': pnl.get('realizedPnL') # This PnL is from PnLSingle, might differ from overall account PnL
            })
        
        try:
            df = pd.DataFrame(combined_data, columns=header)
            if not os.path.exists(self.portfolio_csv_path) or df.empty:
                 df.to_csv(self.portfolio_csv_path, mode='w', header=True, index=False)
            else:
                 df.to_csv(self.portfolio_csv_path, mode='a', header=False, index=False) # Append if file exists
        except Exception as e:
            self.ib._logger.error(f"Error writing portfolio to CSV: {e}")


    def _rewrite_orders_csv(self):
        header = ['Timestamp', 'OrderId', 'PermId', 'ClientId', 'Account', 'Symbol', 'SecType', 'Exchange', 'Currency', 'LocalSymbol', 'ConID',
                  'Action', 'Quantity', 'OrderType', 'LmtPrice', 'AuxPrice', 'TIF',
                  'Status', 'Filled', 'Remaining', 'AvgFillPrice', 'WhyHeld', 'LastUpdateTime']
        try:
            df = pd.DataFrame(self.all_orders_log, columns=header)
            df.to_csv(self.orders_csv_path, mode='w', header=True, index=False) # Always rewrite orders for simplicity
        except Exception as e:
            self.ib._logger.error(f"Error writing orders to CSV: {e}")

    def _rewrite_fills_csv(self):
        header = ['Timestamp', 'Symbol', 'SecType', 'Exchange', 'LocalSymbol', 'ConID',
                  'OrderId', 'ExecId', 'Side', 'Shares', 'Price', 'CumQty', 'AvgPrice',
                  'EvRule', 'EvMultiplier', 'Commission', 'RealizedPnLFill']
        try:
            df = pd.DataFrame(self.all_fills_log, columns=header)
            df.to_csv(self.fills_csv_path, mode='w', header=True, index=False) # Always rewrite fills
        except Exception as e:
            self.ib._logger.error(f"Error writing fills to CSV: {e}")


    async def get_contract_interactive(self):
        """Interactively prompts user for contract details and lets them select from results."""
        print("\n--- Define Contract ---")
        symbol = input("Enter symbol (e.g., AAPL, EURUSD, ES, SPY): ").upper()
        sec_type = input("Enter security type (STK, CASH, FUT, OPT, CRYPTO, IND, CFD): ").upper()
        exchange = input("Enter exchange (SMART, IDEALPRO, CME, NYSE, NASDAQ, PAXOS, etc. or leave blank): ").upper()
        currency = input("Enter currency (USD, EUR, INR, etc.): ").upper()

        contract = Contract(symbol=symbol, secType=sec_type, currency=currency)
        if exchange:
            contract.exchange = exchange
        
        # Add primaryExchange for stocks if SMART is used, helps disambiguation
        if sec_type == 'STK' and exchange == 'SMART':
            if '.' not in symbol: # Simple US stock
                 contract.primaryExchange = "NASDAQ" # Or NYSE, ARCA - NASDAQ is a common default
                 print(f"Hint: For SMART routed US stocks, primaryExchange often helps (e.g., NASDAQ, NYSE). Using NASDAQ as a default hint.")


        if sec_type == 'FUT':
            contract.lastTradeDateOrContractMonth = input("Enter Future expiry (YYYYMM or YYYYMMDD, or blank): ")
        elif sec_type == 'OPT':
            contract.lastTradeDateOrContractMonth = input("Enter Option expiry (YYYYMM or YYYYMMDD, or blank): ")
            strike_str = input("Enter strike price (optional): ")
            if strike_str: contract.strike = float(strike_str)
            right_str = input("Enter right (C or P, optional): ").upper()
            if right_str: contract.right = right_str
        elif sec_type == 'CASH': # Forex
            if len(symbol) == 6 and not contract.exchange: # e.g. EURUSD
                contract.exchange = 'IDEALPRO'
                print(f"Hint: For CASH (Forex) like {symbol}, exchange is often IDEALPRO.")
        
        self.ib._logger.info(f"Requesting contract details for: Symbol={contract.symbol}, SecType={contract.secType}, Exchange={contract.exchange}, Currency={contract.currency}")
        try:
            cds = await self.ib.reqContractDetailsAsync(contract)
        except Exception as e:
            self.ib._logger.error(f"Error requesting contract details: {e}")
            return None

        if not cds:
            print("No matching contracts found with the provided details. Try being more specific or broader (e.g., leave exchange blank).")
            return None

        print(f"\nFound {len(cds)} contract(s):")
        contracts_list = []
        for i, cd_item in enumerate(cds): # Renamed cd to cd_item to avoid conflict
            c = cd_item.contract # Access the contract object from ContractDetails
            contracts_list.append(c)
            print(f"  {i+1}. Symbol: {c.symbol}, LocalSymbol: {c.localSymbol}, SecType: {c.secType}, Exch: {c.exchange}, PrimExch: {c.primaryExchange}, Curr: {c.currency}" +
                  (f", Expiry: {c.lastTradeDateOrContractMonth}" if c.lastTradeDateOrContractMonth else "") +
                  (f", Strike: {c.strike}" if c.secType == 'OPT' and c.strike != 0.0 else "") +
                  (f", Right: {c.right}" if c.secType == 'OPT' and c.right else "") +
                  (f", TradingClass: {c.tradingClass}" if c.tradingClass else "") +
                  (f", Multiplier: {c.multiplier}" if c.multiplier else "") +
                  (f", ConID: {c.conId}" if c.conId else "")
                 )
        
        selected_contract_obj = None
        if len(cds) == 1:
            selected_contract_obj = cds[0].contract
            print(f"Auto-selected contract: {selected_contract_obj.localSymbol or selected_contract_obj.symbol}")
        else:
            while True:
                try:
                    choice = int(input(f"Select contract number (1-{len(cds)}): "))
                    if 1 <= choice <= len(cds):
                        selected_contract_obj = cds[choice-1].contract
                        break
                    else:
                        print("Invalid choice.")
                except ValueError:
                    print("Invalid input. Please enter a number.")
        
        # It's good practice to qualify the specifically chosen contract again,
        # though reqContractDetails often returns a fully qualified one.
        # This ensures we have the conId.
        if selected_contract_obj:
            try:
                qualified_contracts = await self.ib.qualifyContractsAsync(selected_contract_obj)
                if qualified_contracts:
                    final_contract = qualified_contracts[0]
                    self.ib._logger.info(f"Selected and qualified contract: {final_contract}, ConID: {final_contract.conId}")
                    return final_contract
                else:
                    self.ib._logger.error(f"Failed to re-qualify selected contract: {selected_contract_obj}")
                    return None # Or return selected_contract_obj if qualification is not strictly needed here
            except Exception as e:
                self.ib._logger.error(f"Exception during re-qualification of {selected_contract_obj.symbol}: {e}")
                return None
        return None


    async def fetch_historical_data(self, contract: Contract, durationStr: str = '1 M',
                                    barSizeSetting: str = '1 day', whatToShow: str = 'TRADES', useRTH: bool = True):
        if not contract or not contract.conId: # Check for conId
            self.ib._logger.error("Cannot fetch historical data: Contract is not qualified or missing conId.")
            print("Error: Contract not qualified or missing conId. Please select and qualify a contract first.")
            return []
        
        self.ib._logger.info(f"Fetching historical data for {contract.localSymbol or contract.symbol} (ConID: {contract.conId}): {durationStr} of {barSizeSetting} bars")
        try:
            # Ensure TWS has time to process, especially after connection
            await self.ib.reqMarketDataType(3) # Request delayed data if live isn't subscribed
            bars = await self.ib.reqHistoricalDataAsync(
                contract, endDateTime='', durationStr=durationStr, barSizeSetting=barSizeSetting,
                whatToShow=whatToShow, useRTH=useRTH, formatDate=1, keepUpToDate=False ) # keepUpToDate=False for one-time fetch
            
            self.ib._logger.info(f"Fetched {len(bars)} historical bars for {contract.localSymbol or contract.symbol}.")
            if bars:
                print(f"\n--- Historical Data for {contract.localSymbol or contract.symbol} ---")
                df = util.df(bars) # Convert to pandas DataFrame
                print(df)
                # print(f"Last bar: {bars[-1]}")
            else:
                print("No historical data returned. Check parameters and market data permissions.")
            return bars
        except Exception as e:
            self.ib._logger.error(f"Error fetching historical data for {contract.localSymbol or contract.symbol}: {e}")
            print(f"Error fetching historical data: {e}")
            return []

    async def start_realtime_bars(self, contract: Contract, barSize: int = 5, whatToShow: str = 'TRADES', useRTH: bool = True): # Changed useRTH default
        if not contract or not contract.conId:
            self.ib._logger.error("Cannot start real-time bars: Contract is not qualified or missing conId.")
            print("Error: Contract not qualified. Please select and qualify a contract first.")
            return None
        if contract.conId in self.active_realtime_bars:
            self.ib._logger.warning(f"Real-time bars already active for {contract.localSymbol or contract.symbol}. Stop first to restart.")
            print(f"Real-time bars already active for {contract.localSymbol or contract.symbol}.")
            return self.active_realtime_bars[contract.conId]

        self.ib._logger.info(f"Starting real-time {barSize}s bars for {contract.localSymbol or contract.symbol} (ConID: {contract.conId}, Type: {whatToShow})")
        try:
            # It's good practice to set market data type if you expect specific behavior (e.g., delayed)
            await self.ib.reqMarketDataType(3) # Request delayed data if live isn't subscribed
            bars = self.ib.reqRealTimeBars(contract, barSize, whatToShow, useRTH, []) # Pass useRTH
            bars.updateEvent += self._on_default_bar_update # Attach general handler
            self.active_realtime_bars[contract.conId] = bars
            await asyncio.sleep(2) # Allow time for subscription to establish
            
            # Check if subscription was successful (heuristic: bars object exists, no immediate error)
            # Real confirmation comes from receiving bar updates.
            if contract.conId in self.active_realtime_bars:
                 self.ib._logger.info(f"Successfully subscribed to real-time bars for {contract.localSymbol or contract.symbol}")
                 print(f"Subscribed to real-time bars for {contract.localSymbol or contract.symbol}. Updates will appear if data flows.")
            else: # Should not happen if no exception
                 self.ib._logger.warning(f"Subscription to real-time bars for {contract.localSymbol or contract.symbol} might have failed silently.")
            return bars
        except Exception as e:
            self.ib._logger.error(f"Error starting real-time bars for {contract.localSymbol or contract.symbol}: {e}")
            print(f"Error starting real-time bars: {e}")
            return None

    def _on_default_bar_update(self, bars, hasNewBar):
        """Default handler for real-time bar updates."""
        # 'bars' here is the BarDataList object
        contract_symbol = bars.contract.localSymbol or bars.contract.symbol
        if hasNewBar:
            bar = bars[-1] # The newest bar
            self.ib._logger.info(f"RealTimeBar ({contract_symbol}): Time={bar.time}, O={bar.open_}, H={bar.high}, L={bar.low}, C={bar.close}, V={bar.volume}")
            print(f"New Bar ({contract_symbol}): {bar.time} O:{bar.open_} H:{bar.high} L:{bar.low} C:{bar.close} V:{bar.volume}")
        # else:
            # This part is called for every tick that doesn't form a new bar
            # self.ib._logger.debug(f"RealTimeTick Update for {contract_symbol}: Last bar C={bars[-1].close if bars else 'N/A'}")


    async def stop_realtime_bars(self, contract_id_or_symbol):
        """Stops streaming real-time bars for a given contract ID or symbol."""
        bars_obj_to_cancel = None
        con_id_to_cancel = None
        target_symbol = str(contract_id_or_symbol) # For logging

        if isinstance(contract_id_or_symbol, int): # conId provided
            con_id_to_cancel = contract_id_or_symbol
            bars_obj_to_cancel = self.active_realtime_bars.get(con_id_to_cancel)
        elif isinstance(contract_id_or_symbol, str): # Symbol provided
            # Find by symbol - less direct, assumes unique symbol in active_realtime_bars
            for cid, bars_obj in self.active_realtime_bars.items():
                if (bars_obj.contract.symbol == contract_id_or_symbol or
                    bars_obj.contract.localSymbol == contract_id_or_symbol):
                    con_id_to_cancel = cid
                    bars_obj_to_cancel = bars_obj
                    target_symbol = contract_id_or_symbol
                    break
        elif hasattr(contract_id_or_symbol, 'contract'): # bars object itself passed
            bars_obj_to_cancel = contract_id_or_symbol
            con_id_to_cancel = bars_obj_to_cancel.contract.conId
            target_symbol = bars_obj_to_cancel.contract.localSymbol or bars_obj_to_cancel.contract.symbol
        
        if bars_obj_to_cancel and con_id_to_cancel is not None:
            self.ib._logger.info(f"Stopping real-time bars for {target_symbol} (ConID: {con_id_to_cancel})")
            try:
                self.ib.cancelRealTimeBars(bars_obj_to_cancel)
                # Remove from active list
                if con_id_to_cancel in self.active_realtime_bars:
                    del self.active_realtime_bars[con_id_to_cancel]
                print(f"Stopped real-time bars for {target_symbol}.")
            except Exception as e:
                 self.ib._logger.error(f"Error cancelling real-time bars for {target_symbol}: {e}")
                 print(f"Error stopping real-time bars for {target_symbol}: {e}")
        else:
            self.ib._logger.warning(f"No active real-time bars found for '{target_symbol}' to stop.")
            print(f"No active real-time bars found for '{target_symbol}' to stop.")


    def _add_order_to_log(self, contract: Contract, order: Order, orderStatus: OrderStatus = None):
        """Adds or updates an order in the internal log and rewrites CSV."""
        # Ensure we have the most complete order details, especially if it's an update
        existing_entry = next((o for o in self.all_orders_log if o['OrderId'] == order.orderId), None)

        entry_data = {
            'Timestamp': datetime.datetime.now().isoformat(), # Timestamp of this log action
            'OrderId': order.orderId,
            'PermId': order.permId,
            'ClientId': order.clientId, # Use clientId from the order object itself
            'Account': order.account or self.account_code or '', # Ensure account is logged
            'Symbol': contract.symbol,
            'SecType': contract.secType,
            'Exchange': contract.exchange,
            'Currency': contract.currency,
            'LocalSymbol': contract.localSymbol or '',
            'ConID': contract.conId,
            'Action': order.action,
            'Quantity': order.totalQuantity,
            'OrderType': order.orderType,
            'LmtPrice': order.lmtPrice if order.orderType in ['LMT', 'STPLMT', 'LIT'] else None,
            'AuxPrice': order.auxPrice if order.orderType in ['STP', 'STPLMT', 'TRAIL', 'TRAILLMT'] else None,
            'TIF': order.tif,
            'Status': orderStatus.status if orderStatus else 'Unknown', # Default if no status yet
            'Filled': orderStatus.filled if orderStatus else 0,
            'Remaining': orderStatus.remaining if orderStatus else order.totalQuantity,
            'AvgFillPrice': orderStatus.avgFillPrice if orderStatus else 0.0,
            'WhyHeld': orderStatus.whyHeld if orderStatus and orderStatus.whyHeld else '',
            'LastUpdateTime': datetime.datetime.now().isoformat() # Timestamp of this status update
        }
        if existing_entry:
            existing_entry.update(entry_data) # Update existing log entry
        else:
            self.all_orders_log.append(entry_data) # Add new log entry
        
        self.all_orders_log.sort(key=lambda x: x.get('OrderId', 0)) # Keep sorted by OrderId for consistency
        self._rewrite_orders_csv()


    async def place_order_interactive(self, contract: Contract):
        """Interactively prompts user for order details and places the order."""
        if not contract or not contract.conId:
            print("Error: No valid contract selected (missing ConID). Please select and qualify a contract first.")
            return None
        
        print(f"\n--- Place Order for {contract.localSymbol or contract.symbol} (ConID: {contract.conId}) ---")
        action = input("Action (BUY or SELL): ").upper()
        while action not in ['BUY', 'SELL']:
            action = input("Invalid action. Enter BUY or SELL: ").upper()

        order_type = input("Order type (MKT, LMT, STP, STPLMT, TRAIL): ").upper()
        valid_order_types = ['MKT', 'LMT', 'STP', 'STPLMT', 'TRAIL']
        while order_type not in valid_order_types:
            order_type = input(f"Invalid order type. Choose from {', '.join(valid_order_types)}: ").upper()

        try:
            quantity_str = input(f"Quantity (number of contracts/shares for {contract.symbol}): ")
            quantity = float(quantity_str)
            if quantity <= 0: raise ValueError("Quantity must be positive.")
        except ValueError as e:
            print(f"Invalid quantity: {e}")
            return None

        limit_price = None
        stop_price = None # This is auxPrice for STP, STPLMT
        trail_amount_or_percent = None # For TRAIL orders (auxPrice is amount, trailingPercent is percent)
        trail_stop_price = None # For TRAILLMT orders

        if order_type == 'LMT' or order_type == 'STPLMT':
            try:
                limit_price = float(input("Limit Price: "))
            except ValueError: print("Invalid limit price."); return None
        
        if order_type == 'STP' or order_type == 'STPLMT':
            try:
                stop_price = float(input("Stop Price (trigger price): "))
            except ValueError: print("Invalid stop price."); return None

        if order_type == 'TRAIL':
            try:
                trail_input = input("Trailing Amount (e.g., 0.50) or Percent (e.g., 0.5%): ")
                if '%' in trail_input:
                    trail_amount_or_percent = float(trail_input.strip('%'))
                    if trail_amount_or_percent <=0 or trail_amount_or_percent > 100:
                        raise ValueError("Trailing percent must be > 0 and <= 100.")
                else:
                    trail_amount_or_percent = float(trail_input)
                    if trail_amount_or_percent <= 0:
                         raise ValueError("Trailing amount must be positive.")
            except ValueError as e: print(f"Invalid trailing value: {e}"); return None
        
        # Add TRAILLMT if needed, it's more complex

        tif = input("Time in Force (DAY, GTC, IOC, FOK - default DAY): ").upper() or 'DAY'
        valid_tifs = ['DAY', 'GTC', 'IOC', 'FOK', 'GTD'] # GTD needs goodTillDate
        if tif not in valid_tifs:
            print(f"Invalid TIF '{tif}', defaulting to DAY.")
            tif = 'DAY'
        
        # Use the main place_order method
        return await self.place_order(contract, order_type, action, quantity,
                                      limit_price=limit_price, aux_price=stop_price, # aux_price for STP/STPLMT
                                      tif=tif, trailingPercent=trail_amount_or_percent if isinstance(trail_amount_or_percent, float) and '%' in trail_input else None,
                                      trailStopPrice=trail_amount_or_percent if isinstance(trail_amount_or_percent, float) and '%' not in trail_input else None)


    async def place_order(self, contract: Contract, order_type: str, action: str, quantity: float,
                          limit_price: float = None, aux_price: float = None, # aux_price for STP, STPLMT, TRAIL amount
                          tif: str = 'DAY', transmit: bool = True, parentId: int = 0,
                          trailingPercent: float = None, trailStopPrice: float = None, # Specific for TRAIL orders
                          **kwargs): # For other Order fields like goodAfterTime, goodTillDate etc.
        if not contract or not contract.conId:
            self.ib._logger.error("Cannot place order: Contract is not qualified or missing conId.")
            print("Error: Contract not qualified. Please select and qualify a contract first.")
            return None
        if not self.account_code and not order_kwargs.get('account'):
            self.ib._logger.warning("Account code not set for the bot, and not specified in order. Order will use default account from TWS/Gateway.")
            # print("Warning: Account code not set. Order will use TWS/Gateway default.")

        order_type_upper = order_type.upper()
        order_kwargs = {
            'action': action.upper(),
            'totalQuantity': quantity,
            'tif': tif.upper(),
            'transmit': transmit,
            'account': self.account_code or kwargs.get('account') or '' # Use bot's account_code if available
        }
        if parentId: # For child orders in strategies like brackets
            order_kwargs['parentId'] = parentId
        
        order_kwargs.update(kwargs) # Merge any other explicit order fields

        order = None
        if order_type_upper == 'MKT':
            order = Order(orderType='MKT', **order_kwargs)
        elif order_type_upper == 'LMT':
            if limit_price is None: self.ib._logger.error("Limit price required for LMT order."); print("Error: Limit price required."); return None
            order = Order(orderType='LMT', lmtPrice=float(limit_price), **order_kwargs)
        elif order_type_upper == 'STP':
            if aux_price is None: self.ib._logger.error("Stop price (auxPrice) required for STP order."); print("Error: Stop price required."); return None
            order = Order(orderType='STP', auxPrice=float(aux_price), **order_kwargs)
        elif order_type_upper == 'STPLMT':
            if limit_price is None or aux_price is None: self.ib._logger.error("Limit and Stop prices required for STPLMT order."); print("Error: Limit and Stop prices required."); return None
            order = Order(orderType='STPLMT', lmtPrice=float(limit_price), auxPrice=float(aux_price), **order_kwargs)
        elif order_type_upper == 'TRAIL': # Trailing Stop Order
            if trailingPercent is not None and trailStopPrice is not None:
                 self.ib._logger.error("For TRAIL order, specify EITHER trailingPercent OR trailStopPrice (amount), not both."); print("Error: Specify either percent or amount for TRAIL order."); return None
            if trailingPercent is not None: # Trailing percent
                order = Order(orderType='TRAIL', trailingPercent=float(trailingPercent), **order_kwargs)
            elif trailStopPrice is not None: # Trailing amount (passed as auxPrice by some, but trailStopPrice is more explicit for TWS)
                order = Order(orderType='TRAIL', auxPrice=float(trailStopPrice), **order_kwargs) # trailStopPrice often maps to auxPrice for simple TRAIL
            else:
                self.ib._logger.error("Trailing percent or amount required for TRAIL order."); print("Error: Trailing percent or amount required."); return None
        # Add more order types like TRAILLMT if needed
        else:
            self.ib._logger.error(f"Unsupported order type: {order_type_upper}"); print(f"Error: Unsupported order type '{order_type_upper}'."); return None

        self.ib._logger.info(f"Placing Order: Account={order.account}, Contract={contract.localSymbol or contract.symbol}, Action={order.action}, Qty={order.totalQuantity}, Type={order.orderType}, LmtPrc={order.lmtPrice}, AuxPrc={order.auxPrice}")
        try:
            trade = self.ib.placeOrder(contract, order)
            # OrderId and PermId are populated by placeOrder
            self.ib._logger.info(f"Order placed successfully via API. OrderId: {trade.order.orderId}, PermId: {trade.order.permId}. Status: {trade.orderStatus.status}")
            print(f"Order Sent: ID {trade.order.orderId}, Action {trade.order.action}, Type {trade.order.orderType}, Qty {trade.order.totalQuantity} for {contract.symbol}")
            
            self._add_order_to_log(contract, trade.order, trade.orderStatus) # Log initial submission
            self.open_trades_cache[trade.order.orderId] = trade # Cache it
            return trade # Return the Trade object
        except Exception as e:
            self.ib._logger.error(f"Error placing order for {contract.localSymbol or contract.symbol}: {e}")
            print(f"Error placing order: {e}")
            return None

    async def modify_order_interactive(self):
        """Interactively modifies an existing open order."""
        if not self.ib.isConnected():
            print("Not connected. Please connect first.")
            return None

        print("\n--- Modify Order ---")
        open_orders_from_log = [o for o in self.all_orders_log if o['Status'] not in ['Filled', 'Cancelled', 'Inactive', 'ApiCancelled']]
        if not open_orders_from_log:
            print("No active orders found in the log to modify.")
            return None

        print("Current active/pending orders from log:")
        for i, o_log in enumerate(open_orders_from_log):
            print(f"  {i+1}. ID: {o_log['OrderId']}, PermID: {o_log['PermId']}, Symbol: {o_log['Symbol']}, "
                  f"Action: {o_log['Action']}, Qty: {o_log['Quantity']}, Type: {o_log['OrderType']}, "
                  f"Lmt: {o_log['LmtPrice']}, Aux: {o_log['AuxPrice']}, Status: {o_log['Status']}")
        
        try:
            choice = int(input(f"Select order number to modify (1-{len(open_orders_from_log)}): ")) - 1
            if not (0 <= choice < len(open_orders_from_log)):
                print("Invalid selection."); return None
            selected_order_log = open_orders_from_log[choice]
            order_id_to_modify = selected_order_log['OrderId']
        except ValueError:
            print("Invalid input."); return None

        # Retrieve the actual Trade object from cache or request it
        trade_to_modify = self.open_trades_cache.get(order_id_to_modify)
        if not trade_to_modify:
            # Fallback: Try to find it in all open trades from IB if not in cache (might be from previous session)
            # This requires ensuring openOrders are fresh.
            await self.ib.reqAllOpenOrdersAsync() # Refresh open orders from TWS
            all_ib_open_trades = self.ib.openTrades()
            trade_to_modify = next((t for t in all_ib_open_trades if t.order.orderId == order_id_to_modify), None)

        if not trade_to_modify:
            print(f"Could not find active Trade object for OrderID {order_id_to_modify}. Modification might not be possible if order is not managed by this session or already completed.")
            # We can still attempt to construct an Order object for modification if we have permId
            if not selected_order_log.get('PermId'):
                print("PermId not found in log for this order, cannot reliably modify without a live Trade object.")
                return None
            
            # Construct a new order object for modification based on logged data
            # This is less ideal than modifying a live Trade object's order
            print(f"Warning: Modifying based on logged data for OrderID {order_id_to_modify} as live Trade object not cached. Ensure PermID is correct.")
            mod_order = Order()
            mod_order.orderId = selected_order_log['OrderId']
            mod_order.permId = selected_order_log['PermId']
            mod_order.action = selected_order_log['Action']
            mod_order.orderType = selected_order_log['OrderType']
            mod_order.totalQuantity = selected_order_log['Quantity'] # Original quantity
            mod_order.lmtPrice = selected_order_log.get('LmtPrice')
            mod_order.auxPrice = selected_order_log.get('AuxPrice')
            mod_order.account = selected_order_log.get('Account') or self.account_code or ''
            # Contract needs to be fetched or reconstructed
            mod_contract = Contract(conId=selected_order_log['ConID'], symbol=selected_order_log['Symbol'], exchange=selected_order_log['Exchange'], secType=selected_order_log['SecType'], currency=selected_order_log['Currency'])
            await self.ib.qualifyContractsAsync(mod_contract) # Re-qualify
            if not mod_contract.conId: print("Failed to re-qualify contract for modification."); return None

        else: # We have the live Trade object
            mod_order = trade_to_modify.order # Get the order object from the Trade
            mod_contract = trade_to_modify.contract
            print(f"Modifying live order: {mod_order.orderId}, current LmtPrice: {mod_order.lmtPrice}, Qty: {mod_order.totalQuantity}")


        print(f"What do you want to modify for Order ID {mod_order.orderId} ({mod_contract.symbol})?")
        new_qty_str = input(f"New Quantity (current: {mod_order.totalQuantity}, press Enter to keep): ")
        new_lmt_str = input(f"New Limit Price (current: {mod_order.lmtPrice}, press Enter to keep, 'NA' if not LMT): ")
        new_aux_str = input(f"New Stop/Aux Price (current: {mod_order.auxPrice}, press Enter to keep, 'NA' if not STP/TRAIL): ")

        original_order_for_logging = copy.deepcopy(mod_order) # Keep a copy of original for logging changes

        modified = False
        if new_qty_str:
            try:
                new_qty = float(new_qty_str)
                if new_qty > 0: mod_order.totalQuantity = new_qty; modified = True
                else: print("Invalid new quantity.")
            except ValueError: print("Invalid quantity input.")
        
        if new_lmt_str.upper() != 'NA' and new_lmt_str and mod_order.orderType in ['LMT', 'STPLMT', 'LIT']:
            try:
                mod_order.lmtPrice = float(new_lmt_str); modified = True
            except ValueError: print("Invalid limit price input.")
        elif new_lmt_str.upper() == 'NA':
            mod_order.lmtPrice = 0.0 # Or appropriate default if changing order type (more complex)

        if new_aux_str.upper() != 'NA' and new_aux_str and mod_order.orderType in ['STP', 'STPLMT', 'TRAIL', 'TRAILLMT']:
            try:
                mod_order.auxPrice = float(new_aux_str); modified = True
            except ValueError: print("Invalid stop/aux price input.")
        elif new_aux_str.upper() == 'NA':
            mod_order.auxPrice = 0.0

        if not modified:
            print("No changes made to the order.")
            return None

        # IB uses placeOrder to submit modifications. The orderId should remain the same.
        # TWS handles it as a cancel/replace internally if needed.
        self.ib._logger.info(f"Attempting to modify OrderID {mod_order.orderId} for {mod_contract.symbol} to Qty:{mod_order.totalQuantity}, LmtPrc:{mod_order.lmtPrice}, AuxPrc:{mod_order.auxPrice}")
        try:
            # Ensure transmit is True for modifications to go through
            mod_order.transmit = True 
            modified_trade = self.ib.placeOrder(mod_contract, mod_order) # Use the contract from the trade
            
            self.ib._logger.info(f"Modification request sent for OrderID {mod_order.orderId}. New OrderId if replaced: {modified_trade.order.orderId}. Status: {modified_trade.orderStatus.status}")
            print(f"Order modification sent for ID {mod_order.orderId}. New ID if replaced: {modified_trade.order.orderId}")
            
            # Update log for the original orderId to reflect it's being modified,
            # and add new log if OrderId changes (though it usually shouldn't for simple mods)
            self._add_order_to_log(mod_contract, modified_trade.order, modified_trade.orderStatus)
            self.open_trades_cache[modified_trade.order.orderId] = modified_trade # Update cache

            return modified_trade
        except Exception as e:
            self.ib._logger.error(f"Error modifying order {mod_order.orderId}: {e}")
            print(f"Error modifying order: {e}")
            # Revert order object in memory if API call failed, or re-log original status
            self._add_order_to_log(mod_contract, original_order_for_logging, trade_to_modify.orderStatus if trade_to_modify else None)
            return None

    async def cancel_order_interactive(self):
        """Interactively cancels an open order."""
        if not self.ib.isConnected():
            print("Not connected. Please connect first.")
            return False

        print("\n--- Cancel Order ---")
        # Filter orders from the log that are cancellable
        open_orders_from_log = [o for o in self.all_orders_log if o['Status'] not in ['Filled', 'Cancelled', 'Inactive', 'ApiCancelled', 'PendingCancel']]
        
        if not open_orders_from_log:
            print("No active orders found in the log to cancel.")
            return False

        print("Current active/pending orders from log:")
        for i, o_log in enumerate(open_orders_from_log):
            print(f"  {i+1}. ID: {o_log['OrderId']}, PermID: {o_log['PermId']}, Symbol: {o_log['Symbol']}, Action: {o_log['Action']}, Qty: {o_log['Quantity']}, Status: {o_log['Status']}")
        
        try:
            choice = int(input(f"Select order number to cancel (1-{len(open_orders_from_log)}): ")) - 1
            if not (0 <= choice < len(open_orders_from_log)):
                print("Invalid selection."); return False
            order_id_to_cancel = open_orders_from_log[choice]['OrderId']
        except ValueError:
            print("Invalid input."); return False

        # Retrieve the actual Trade object from cache for cancellation
        trade_to_cancel = self.open_trades_cache.get(order_id_to_cancel)
        
        order_obj_for_api = None
        if trade_to_cancel:
            order_obj_for_api = trade_to_cancel.order
            self.ib._logger.info(f"Found live Trade object for cancellation: OrderID {order_id_to_cancel}")
        else:
            # If not in live cache, construct a minimal Order object with orderId
            # This might be less reliable if TWS doesn't recognize just the ID for some reason
            # but is the standard way to cancel by ID.
            self.ib._logger.warning(f"Live Trade object for OrderID {order_id_to_cancel} not in cache. Attempting cancel by ID.")
            order_obj_for_api = Order(orderId=order_id_to_cancel)


        if not order_obj_for_api: # Should not happen if logic above is correct
            print(f"Serious issue: Could not obtain or construct an order object for cancellation (ID: {order_id_to_cancel}).")
            return False

        self.ib._logger.info(f"Attempting to cancel OrderID {order_id_to_cancel}")
        try:
            self.ib.cancelOrder(order_obj_for_api) # Pass the Order object
            print(f"Cancel request sent for OrderID {order_id_to_cancel}. Monitor status updates.")
            # Status will update via _on_orderStatus.
            # Update local log optimistically or wait for callback.
            log_entry = next((o for o in self.all_orders_log if o['OrderId'] == order_id_to_cancel), None)
            if log_entry:
                log_entry['Status'] = 'PendingCancel' # Optimistic update
                log_entry['LastUpdateTime'] = datetime.datetime.now().isoformat()
                self._rewrite_orders_csv()
            return True
        except Exception as e:
            self.ib._logger.error(f"Error cancelling order {order_id_to_cancel}: {e}")
            print(f"Error sending cancel request for order {order_id_to_cancel}: {e}")
            return False

    async def fetch_account_positions(self):
        if not self.ib.isConnected(): print("Not connected."); return pd.DataFrame()
        self.ib._logger.info("Requesting current positions...")
        try:
            # positions = await self.ib.reqPositionsAsync() # This is one way
            # However, relying on the positionEvent stream is often better for continuous updates
            # For an on-demand fetch, reqPositionsAsync is fine, but it won't update self.current_positions directly
            # unless you process the result. The positionEvent handler already updates self.current_positions.
            # So, we can just trigger a refresh if needed or rely on existing data.
            
            # Forcing a refresh of positions through the event stream (if supported this way)
            # Or, more simply, just use the cached data from positionEvent
            if self.account_code:
                self.ib.reqPositions() # This will trigger _on_position events for the default or specified account
            else: # If no account code, it might not target correctly.
                 self.ib._logger.warning("Account code not set, reqPositions might not fetch specific data.")


            await asyncio.sleep(1) # Give time for events if reqPositions was called

            if not self.current_positions:
                print("No current positions reported or cached. Ensure account has positions and events are firing.")
                return pd.DataFrame()

            # Data is already in self.current_positions and self.pnl_data, which _rewrite_portfolio_csv uses
            self._rewrite_portfolio_csv() # Ensure CSV is up-to-date with current cache
            print(f"\n--- Current Portfolio (from cache, see {self.portfolio_csv_path}) ---")
            # For display, load from the CSV we just wrote
            if os.path.exists(self.portfolio_csv_path):
                try:
                    df = pd.read_csv(self.portfolio_csv_path)
                    if not df.empty: print(df)
                    else: print("Portfolio CSV is empty.")
                    return df
                except pd.errors.EmptyDataError:
                    print("Portfolio CSV is empty.")
                    return pd.DataFrame()
            return pd.DataFrame()

        except Exception as e:
            self.ib._logger.error(f"Error fetching/displaying positions: {e}")
            print(f"Error fetching positions: {e}")
            return pd.DataFrame()


    async def fetch_account_summary_interactive(self):
        if not self.ib.isConnected(): print("Not connected."); return pd.DataFrame()
        self.ib._logger.info("Requesting account summary...")
        try:
            # Account summary data streams via _on_accountSummary after subscription.
            # To get a "snapshot", you'd typically check a cache populated by this event.
            # Forcing a request here might not be standard; ib_async manages the stream.
            # Let's assume _on_accountSummary has populated some data or we can request it.
            
            # If you want to explicitly request for particular tags on demand:
            tags = "NetLiquidation,TotalCashValue,BuyingPower,AvailableFunds,RealizedPnL,UnrealizedPnL,EquityWithLoanValue"
            # account_values = await self.ib.reqAccountSummaryAsync(tags=tags, groupName='All', account=self.account_code or '')
            # The above returns AccountValue objects, not a DataFrame directly.
            # The _on_accountSummary event handler is better for continuous updates.
            
            # For this interactive fetch, let's just trigger the subscription if not already active
            # and rely on the event handler to log/display.
            print("Account summary updates are streamed. Check logs/console for updates.")
            print(f"Account used for summary: {self.account_code or 'Default/All'}")
            if not self.ib.accountSummary(): # Check if any summary data has arrived
                self.ib.reqAccountSummary(True, 'All', self.account_code or '') # Subscribe if no data yet
                await asyncio.sleep(1) # Give it time

            # For display, you'd ideally have self.account_summary_cache populated by _on_accountSummary
            # For now, we'll just inform the user that it's event-driven.
            # Example of how you might construct a DF if you cached it:
            # if self.account_summary_cache:
            #     df = pd.DataFrame.from_dict(self.account_summary_cache, orient='index')
            #     print(df)
            #     return df
            return pd.DataFrame() # Placeholder, as summary is event-driven for display

        except Exception as e:
            self.ib._logger.error(f"Error fetching account summary: {e}")
            print(f"Error fetching account summary: {e}")
            return pd.DataFrame()

    async def fetch_trade_log_interactive(self):
        if not self.ib.isConnected(): print("Not connected."); return
        self.ib._logger.info("Displaying trade logs (orders and fills)...")
        
        print(f"\n--- Order Log (see {self.orders_csv_path} for full details) ---")
        if os.path.exists(self.orders_csv_path):
            try:
                orders_df = pd.read_csv(self.orders_csv_path)
                if not orders_df.empty:
                    print(orders_df[['Timestamp', 'OrderId', 'Symbol', 'Action', 'Quantity', 'OrderType', 'LmtPrice', 'Status', 'Filled', 'AvgFillPrice']].tail(10))
                else: print("Order log is empty.")
            except pd.errors.EmptyDataError: print("Order log is empty.")
            except Exception as e: print(f"Error reading order log: {e}")
        else: print("Order log file not found.")

        print(f"\n--- Fill Log (see {self.fills_csv_path} for full details) ---")
        if os.path.exists(self.fills_csv_path):
            try:
                fills_df = pd.read_csv(self.fills_csv_path)
                if not fills_df.empty:
                    print(fills_df[['Timestamp', 'ExecId', 'Symbol', 'Side', 'Shares', 'Price', 'Commission']].tail(10))
                else: print("Fill log is empty.")
            except pd.errors.EmptyDataError: print("Fill log is empty.")
            except Exception as e: print(f"Error reading fill log: {e}")
        else: print("Fill log file not found.")


async def display_menu(contract_selected):
    print("\n-------------------- MENU --------------------")
    print(f"Current Contract: {contract_selected.localSymbol if contract_selected else 'None Selected'}")
    print("1. Select/Change Contract")
    print("2. Start Real-Time Bars for Current Contract")
    print("3. Place New Order for Current Contract")
    print("4. Fetch Historical Data for Current Contract")
    print("5. Stop Real-Time Bars for Current Contract")
    print("6. Modify Existing Open Order")
    print("7. Cancel Existing Open Order")
    print("8. Fetch Account Positions & Portfolio Value")
    print("9. Fetch Account Summary")
    print("10. View Trade Logs (Orders & Fills)")
    print("0. Exit")
    choice = input("Enter your choice: ")
    return choice

async def main_interactive_loop():
    bot = None
    current_contract: Contract = None
    try:
        # Prompt for connection details (optional, could have defaults)
        host = input(f"Enter IB Gateway/TWS host (default: 127.0.0.1): ") or '127.0.0.1'
        port_str = input(f"Enter IB Gateway/TWS port (default: 7497 for paper, 7496 for live TWS): ") or '7497'
        port = int(port_str)
        client_id_str = input(f"Enter Client ID (default: {datetime.datetime.now().second + 100}): ") or str(datetime.datetime.now().second + 100) # More unique default
        client_id = int(client_id_str)
        account_code_input = input(f"Enter specific Account Code (optional, press Enter for default): ") or None

        bot = IBTradingBot(host=host, port=port, clientId=client_id, account_code=account_code_input)
        await bot.connect()

        if not bot.ib.isConnected():
            print("Failed to connect. Exiting.")
            return

        while True:
            choice = await display_menu(current_contract)
            action_result = True # Default to continue loop unless set to False (for exit) or None (for re-selecting contract)

            if choice == '1':
                new_contract = await bot.get_contract_interactive()
                if new_contract:
                    current_contract = new_contract
                else:
                    print("Failed to define a new contract, keeping previous or none.")
            
            elif not current_contract and choice not in ['0', '8', '9', '10', '6', '7']: # Choices that don't strictly need a contract upfront
                print("Please select a contract first (Option 1).")
                continue

            elif choice == '2':
                if current_contract:
                    await bot.start_realtime_bars(current_contract)
                else: print("No contract selected.")
            elif choice == '3':
                if current_contract:
                    await bot.place_order_interactive(current_contract)
                else: print("No contract selected.")
            elif choice == '4':
                if current_contract:
                    # Prompt for historical data params
                    duration = input("Enter duration (e.g., '1 M', '10 D', '1 Y'): ") or '1 M'
                    barsize = input("Enter bar size (e.g., '1 day', '5 mins', '1 hour'): ") or '1 day'
                    whattoshow = input("Enter whatToShow (TRADES, MIDPOINT, BID, ASK - default TRADES): ").upper() or 'TRADES'
                    await bot.fetch_historical_data(current_contract, durationStr=duration, barSizeSetting=barsize, whatToShow=whattoshow)
                else: print("No contract selected.")
            elif choice == '5':
                if current_contract:
                    # Could also prompt for which bars to stop if multiple are supported by symbol/conID
                    await bot.stop_realtime_bars(current_contract.conId) # Stop by conId
                elif bot.active_realtime_bars:
                    print("Active real-time bar subscriptions:")
                    for i, (con_id, bars_obj) in enumerate(bot.active_realtime_bars.items()):
                        print(f"  {i+1}. {bars_obj.contract.localSymbol or bars_obj.contract.symbol} (ConID: {con_id})")
                    stop_choice_str = input("Enter number of subscription to stop, or symbol/ConID: ")
                    if stop_choice_str:
                        try:
                            stop_idx = int(stop_choice_str) -1
                            if 0 <= stop_idx < len(bot.active_realtime_bars):
                                con_id_to_stop = list(bot.active_realtime_bars.keys())[stop_idx]
                                await bot.stop_realtime_bars(con_id_to_stop)
                            else: print("Invalid selection.")
                        except ValueError: # User entered symbol or ConID string
                            await bot.stop_realtime_bars(stop_choice_str) # Try stopping by symbol/ConID string
                else:
                    print("No contract selected and no active real-time bars to stop.")
            elif choice == '6':
                await bot.modify_order_interactive()
            elif choice == '7':
                await bot.cancel_order_interactive()
            elif choice == '8':
                await bot.fetch_account_positions()
            elif choice == '9':
                await bot.fetch_account_summary_interactive()
            elif choice == '10':
                await bot.fetch_trade_log_interactive()
            elif choice == '0':
                action_result = False # Signal to exit main loop
            else:
                print("Invalid choice. Please try again.")

            if not action_result: # If False, break from while loop to exit
                break
            # If action_result is None, it means we want to go back to contract selection,
            # so current_contract might be set to None by the handler if needed.
            # For now, just continue the loop.

            await asyncio.sleep(0.1) # Brief pause

    except ConnectionRefusedError:
        print("Connection refused. Ensure TWS/Gateway is running and API is enabled on the correct port.")
    except asyncio.TimeoutError:
        print("Connection or operation timed out.")
    except KeyboardInterrupt:
        print("\nUser interrupted. Disconnecting...")
    except Exception as e:
        print(f"An unexpected error occurred in main loop: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if bot and bot.ib and bot.ib.isConnected():
            print("Disconnecting from IB...")
            await bot.disconnect()
        print("Program terminated.")

if __name__ == "__main__":
    try:
        # Ensure util.patch_asyncio() is called if running in certain environments like Spyder with IPython
        # However, for standard Python scripts, asyncio.run() is usually sufficient.
        # If you encounter "RuntimeError: Event loop is already running",
        # and you are in an environment like Jupyter or Spyder's IPython console,
        # you might need to use ib_async.util.startLoop() or run things differently.
        # For standalone scripts, asyncio.run() is the standard.
        asyncio.run(main_interactive_loop())
    except RuntimeError as e:
        # A common issue in some IDEs with pre-existing event loops
        if "Event loop is already running" in str(e) and util.LOOP_HAS_STARTED:
             print("Asyncio event loop already running (likely in an IDE like Spyder or a Jupyter notebook).")
             print("If so, you might need to instantiate IBTradingBot and call its methods directly within the existing loop,")
             print("or ensure only one asyncio.run() is called for the application's lifetime.")
             print("Consider using 'await main_interactive_loop()' if in an async context already.")
        else: # Re-raise other RuntimeErrors
            raise
    except KeyboardInterrupt:
        print("Program shutdown initiated by user (Ctrl+C).")


