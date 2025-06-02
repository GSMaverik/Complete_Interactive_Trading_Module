# ib_trading_module.py
import asyncio
import logging
import pandas as pd
from ib_async import IB, Stock, Forex, Future, Option, Crypto, Contract, Order, MarketOrder, LimitOrder, StopLimitOrder, StopOrder, util, Trade, Position, Fill, Execution, OrderStatus, PnL, PnLSingle, CommissionReport
import datetime
import os

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
        self.all_orders_log = []
        self.all_fills_log = []
        self.active_realtime_bars = {} # {conId: bars_object}

        self._register_event_handlers()
        self._init_csv_files()

    def _register_event_handlers(self):
        self.ib.connectedEvent += self._on_connected
        self.ib.disconnectedEvent += self._on_disconnected
        self.ib.errorEvent += self._on_error
        self.ib.positionEvent += self._on_position
        self.ib.pnlSingleEvent += self._on_pnlSingle
        self.ib.orderStatusEvent += self._on_orderStatus
        self.ib.execDetailsEvent += self._on_execDetails
        self.ib.commissionReportEvent += self._on_commissionReport

    def _init_csv_files(self):
        self._rewrite_portfolio_csv()
        self._rewrite_orders_csv()
        self._rewrite_fills_csv()

    async def connect(self):
        if not self.ib.isConnected():
            try:
                self.ib._logger.info(f"Attempting to connect to {self.host}:{self.port} with clientId {self.clientId}...")
                await self.ib.connectAsync(self.host, self.port, clientId=self.clientId, timeout=15)
                #self.ib.reqAccountUpdates(True, acctCode=(self.account_code or ''))
            except asyncio.TimeoutError:
                self.ib._logger.error(f"Connection to {self.host}:{self.port} timed out.")
                raise
            except ConnectionRefusedError:
                self.ib._logger.error(f"Connection to {self.host}:{self.port} refused. Ensure TWS/Gateway is running and API is enabled.")
                raise
            except Exception as e:
                self.ib._logger.error(f"Connection failed: {e}")
                raise
        if self.ib.isConnected() and not self.account_code:
            managed_accounts = self.ib.managedAccounts()
            if managed_accounts:
                self.account_code = managed_accounts[0]
                self.ib._logger.info(f"Using account: {self.account_code}")
                #self.ib.reqAccountUpdates(True, acctCode=self.account_code)
            else:
                self.ib._logger.warning("No managed accounts found. Account specific features might not work.")

    async def disconnect(self):
        if self.ib.isConnected():
            # Stop any active real-time bars
            for con_id, bars_obj in list(self.active_realtime_bars.items()): # list() for safe iteration while modifying
                await self.stop_realtime_bars(bars_obj)
                
            #self.ib.reqAccountUpdates(False, acctCode=(self.account_code or ''))
            self.ib.disconnect()
            self.ib._logger.info("Disconnected from IB.")

    def _on_connected(self):
        self.ib._logger.info("Successfully connected to IB.")

    def _on_disconnected(self):
        self.ib._logger.info("Disconnected from IB.")

    def _on_error(self, reqId, errorCode, errorString, contract=None):
        if errorCode not in [2104, 2106, 2108, 2158, 2103, 2105, 2107, 2100, 399, 2150, 2168, 2169, 2170]: # Added more info codes
            self.ib._logger.error(f"IB Error. ReqId: {reqId}, Code: {errorCode}, Msg: {errorString}, Contract: {contract}")

    async def _on_position(self, position: Position):
        self.ib._logger.info(f"Position update: Account {position.account}, {position.contract.symbol} {position.position} @ {position.avgCost}")
        if position.position == 0:
            if position.contract.conId in self.current_positions:
                del self.current_positions[position.contract.conId]
            if position.contract.conId in self.pnl_data:
                del self.pnl_data[position.contract.conId]
        else:
            self.current_positions[position.contract.conId] = position
            await self.ib.reqPnLSingleAsync(position.account, '', conId=position.contract.conId)
        self._rewrite_portfolio_csv()

    async def _on_pnlSingle(self, pnlSingle: PnLSingle):
        if pnlSingle.position != 0:
            self.pnl_data[pnlSingle.conId] = {
                'timestamp': datetime.datetime.now().isoformat(),
                'account': pnlSingle.account,
                'conId': pnlSingle.conId,
                'position': pnlSingle.position,
                'marketValue': pnlSingle.value,
                'dailyPnL': pnlSingle.dailyPnL,
                'unrealizedPnL': pnlSingle.unrealizedPnL,
                'realizedPnL': pnlSingle.realizedPnL,
            }
        elif pnlSingle.conId in self.pnl_data:
            del self.pnl_data[pnlSingle.conId]
        self._rewrite_portfolio_csv()

    def _on_orderStatus(self, trade: Trade):
        self.ib._logger.info(f"Order Status: {trade.order.orderId} ({trade.contract.symbol}) - {trade.orderStatus.status}, Filled: {trade.orderStatus.filled}, Remaining: {trade.orderStatus.remaining}, AvgFillPrice: {trade.orderStatus.avgFillPrice}")
        order_log_entry = next((o for o in self.all_orders_log if o['OrderId'] == trade.order.orderId), None)
        if order_log_entry:
            order_log_entry.update({
                'Status': trade.orderStatus.status,
                'Filled': trade.orderStatus.filled,
                'Remaining': trade.orderStatus.remaining,
                'AvgFillPrice': trade.orderStatus.avgFillPrice,
                'WhyHeld': trade.orderStatus.whyHeld,
                'LastUpdateTime': datetime.datetime.now().isoformat()
            })
        else:
            self.ib._logger.warning(f"OrderStatus for unknown OrderId {trade.order.orderId}. Logging new.")
            self._add_order_to_log(trade.contract, trade.order, trade.orderStatus)
        self._rewrite_orders_csv()

    def _on_execDetails(self, trade: Trade, fill: Fill):
        self.ib._logger.info(f"Execution: {fill.execution.side} {fill.execution.shares} of {fill.contract.symbol} @ {fill.execution.price}. OrderId: {fill.execution.orderId}, ExecId: {fill.execution.execId}")
        exec_data = {
            'Timestamp': fill.time.isoformat() if fill.time else datetime.datetime.now().isoformat(), # fill.time can be None
            'Symbol': fill.contract.symbol, 'SecType': fill.contract.secType, 'Exchange': fill.contract.exchange,
            'LocalSymbol': fill.contract.localSymbol, 'ConID': fill.contract.conId,
            'OrderId': fill.execution.orderId, 'ExecId': fill.execution.execId,
            'Side': fill.execution.side, 'Shares': fill.execution.shares, 'Price': fill.execution.price,
            'CumQty': fill.execution.cumQty, 'AvgPrice': fill.execution.avgPrice,
            'EvRule': fill.execution.evRule, 'EvMultiplier': fill.execution.evMultiplier,
            'Commission': None, 'RealizedPnLFill': None
        }
        self.all_fills_log.append(exec_data)
        self._rewrite_fills_csv()

    def _on_commissionReport(self, trade: Trade, fill: Fill, commissionReport: CommissionReport):
        self.ib._logger.info(f"Commission Report for ExecId {commissionReport.execId}: Comm {commissionReport.commission}, PnL {commissionReport.realizedPNL}")
        for f_log in reversed(self.all_fills_log):
            if f_log['ExecId'] == commissionReport.execId:
                f_log['Commission'] = commissionReport.commission
                f_log['RealizedPnLFill'] = commissionReport.realizedPNL
                break
        self._rewrite_fills_csv()

    async def get_contract_interactive(self):
        """Interactively prompts user for contract details and lets them select from results."""
        print("\n--- Define Contract ---")
        symbol = input("Enter symbol (e.g., AAPL, EUR, ES, INFY): ").upper()
        sec_type = input("Enter security type (STK, CASH, FUT, OPT, CRYPTO, IND): ").upper()
        exchange = input("Enter exchange (e.g., SMART, IDEALPRO, CME, NSE, PAXOS, or leave blank for broad search): ").upper()
        currency = input("Enter currency (e.g., USD, EUR, INR): ").upper()

        contract = Contract(symbol=symbol, secType=sec_type, currency=currency)
        if exchange: # Only add exchange if user provided it, for broader search otherwise
            contract.exchange = exchange

        if sec_type == 'FUT':
            contract.lastTradeDateOrContractMonth = input("Enter Future expiry (YYYYMM or YYYYMMDD, or leave blank for all): ")
            # multiplier = input("Enter multiplier (optional): ")
            # if multiplier: contract.multiplier = multiplier
        elif sec_type == 'OPT':
            contract.lastTradeDateOrContractMonth = input("Enter Option expiry (YYYYMM or YYYYMMDD, or leave blank for all): ")
            strike_str = input("Enter strike price (optional, leave blank for all): ")
            if strike_str: contract.strike = float(strike_str)
            right_str = input("Enter right (C or P, optional, leave blank for all): ").upper()
            if right_str: contract.right = right_str
            # multiplier = input("Enter multiplier (optional): ")
            # if multiplier: contract.multiplier = multiplier
        elif sec_type == 'FOREX':
            pass
        
        self.ib._logger.info(f"Requesting contract details for: {contract}")
        try:
            cds = await self.ib.reqContractDetailsAsync(contract)
        except Exception as e:
            self.ib._logger.error(f"Error requesting contract details: {e}")
            return None

        if not cds:
            print("No matching contracts found with the provided details.")
            return None

        print(f"\nFound {len(cds)} contract(s):")
        contracts_list = []
        for i, cd in enumerate(cds):
            c = cd.contract
            contracts_list.append(c)
            print(f"  {i+1}. Symbol: {c.symbol}, LocalSymbol: {c.localSymbol}, SecType: {c.secType}, Exch: {c.exchange}, Curr: {c.currency}" +
                  (f", Expiry: {c.lastTradeDateOrContractMonth}" if c.lastTradeDateOrContractMonth else "") +
                  (f", Strike: {c.strike}" if c.secType == 'OPT' and c.strike != 0.0 else "") +
                  (f", Right: {c.right}" if c.secType == 'OPT' and c.right else "") +
                  (f", TradingClass: {c.tradingClass}" if c.tradingClass else "") +
                  (f", Multiplier: {c.multiplier}" if c.multiplier else "")
                 )
        
        if len(cds) == 1:
            selected_contract = cds[0].contract
            print(f"Auto-selected contract: {selected_contract.localSymbol or selected_contract.symbol}")
        else:
            while True:
                try:
                    choice = int(input(f"Select contract number (1-{len(cds)}): "))
                    if 1 <= choice <= len(cds):
                        selected_contract = cds[choice-1].contract
                        break
                    else:
                        print("Invalid choice.")
                except ValueError:
                    print("Invalid input. Please enter a number.")
        
        # Qualify the chosen contract to get its conId (often already populated from reqContractDetails)
        qualified_contracts = await self.ib.qualifyContractsAsync(selected_contract)
        if qualified_contracts:
            final_contract = qualified_contracts[0]
            self.ib._logger.info(f"Selected and qualified contract: {final_contract}")
            return final_contract
        else:
            self.ib._logger.error(f"Failed to qualify selected contract: {selected_contract}")
            return None


    async def fetch_historical_data(self, contract: Contract, durationStr: str = '1 M',
                                    barSizeSetting: str = '1 day', whatToShow: str = 'TRADES', useRTH: bool = True):
        if not contract or not contract.conId:
            self.ib._logger.error("Cannot fetch historical data: Contract is not qualified.")
            return []
        
        self.ib._logger.info(f"Fetching historical data for {contract.localSymbol or contract.symbol}: {durationStr} of {barSizeSetting} bars")
        try:
            bars = await self.ib.reqHistoricalDataAsync(
                contract, endDateTime='', durationStr=durationStr, barSizeSetting=barSizeSetting,
                whatToShow=whatToShow, useRTH=useRTH, formatDate=1 )
            print(bars)
            self.ib._logger.info(f"Fetched {len(bars)} historical bars for {contract.localSymbol or contract.symbol}.")
            if bars:
                print(f"Last bar: {bars[-1]}")
                # Optionally print more bars or save to a file
                # print(util.df(bars)) # If pandas is desired for display
            else:
                print("No historical data returned.")
            return bars
        except Exception as e:
            self.ib._logger.error(f"Error fetching historical data for {contract.localSymbol or contract.symbol}: {e}")
            return []

    async def start_realtime_bars(self, contract: Contract, barSize: int = 5, whatToShow: str = 'TRADES', useRTH: bool = False):
        if not contract or not contract.conId:
            self.ib._logger.error("Cannot start real-time bars: Contract is not qualified.")
            return None
        if contract.conId in self.active_realtime_bars:
            self.ib._logger.warning(f"Real-time bars already active for {contract.localSymbol or contract.symbol}. Stop first to restart.")
            return self.active_realtime_bars[contract.conId]

        self.ib._logger.info(f"Starting real-time {barSize}s bars for {contract.localSymbol or contract.symbol} ({whatToShow})")
        try:
            bars = self.ib.reqRealTimeBars(contract, barSize, whatToShow, useRTH, [])
            bars.updateEvent += self._on_default_bar_update # Attach handler
            self.active_realtime_bars[contract.conId] = bars
            await asyncio.sleep(1) 
            if bars:
                 self.ib._logger.info(f"Successfully subscribed to real-time bars for {contract.localSymbol or contract.symbol}")
                 print(f"Subscribed to real-time bars for {contract.localSymbol or contract.symbol}. Updates will appear in console/log.")
            return bars
        except Exception as e:
            self.ib._logger.error(f"Error starting real-time bars for {contract.localSymbol or contract.symbol}: {e}")
            return None

    def _on_default_bar_update(self, bars, hasNewBar):
        """Default handler for real-time bar updates."""
        if hasNewBar:
            bar = bars[-1]
            self.ib._logger.info(f"RealTimeBar Update ({bars.contract.localSymbol or bars.contract.symbol}): "
                                 f"Time: {bar.time}, O: {bar.open_}, H: {bar.high}, L: {bar.low}, C: {bar.close}, V: {bar.volume}")
            # Here you could also update market_prices and rewrite portfolio CSV if needed

    async def stop_realtime_bars(self, bars_object_or_contract_id):
        """Stops streaming real-time bars for a given contract or bars object."""
        bars_obj_to_cancel = None
        con_id_to_cancel = None

        if isinstance(bars_object_or_contract_id, int): # conId provided
            con_id_to_cancel = bars_object_or_contract_id
            bars_obj_to_cancel = self.active_realtime_bars.get(con_id_to_cancel)
        elif hasattr(bars_object_or_contract_id, 'contract'): # bars object provided
            bars_obj_to_cancel = bars_object_or_contract_id
            con_id_to_cancel = bars_obj_to_cancel.contract.conId
        
        if bars_obj_to_cancel:
            symbol_name = bars_obj_to_cancel.contract.localSymbol or bars_obj_to_cancel.contract.symbol
            self.ib._logger.info(f"Stopping real-time bars for {symbol_name}")
            try:
                self.ib.cancelRealTimeBars(bars_obj_to_cancel)
                if con_id_to_cancel in self.active_realtime_bars:
                    del self.active_realtime_bars[con_id_to_cancel]
                print(f"Stopped real-time bars for {symbol_name}.")
            except Exception as e:
                 self.ib._logger.error(f"Error cancelling real-time bars for {symbol_name}: {e}")
        else:
            self.ib._logger.warning(f"No active real-time bars found for contract ID {con_id_to_cancel} to stop.")
            print(f"No active real-time bars found for contract ID {con_id_to_cancel} to stop.")


    def _add_order_to_log(self, contract, order, orderStatus=None):
        existing_entry = next((o for o in self.all_orders_log if o['OrderId'] == order.orderId), None)
        entry_data = {
            'Timestamp': datetime.datetime.now().isoformat(), 'OrderId': order.orderId, 'PermId': order.permId,
            'ClientId': self.ib.clientId, 'Account': order.account or self.account_code,
            'Symbol': contract.symbol, 'SecType': contract.secType, 'Exchange': contract.exchange,
            'Currency': contract.currency, 'LocalSymbol': contract.localSymbol, 'ConID': contract.conId,
            'Action': order.action, 'Quantity': order.totalQuantity, 'OrderType': order.orderType,
            'LmtPrice': order.lmtPrice if order.orderType in ['LMT', 'STPLMT'] else None,
            'AuxPrice': order.auxPrice if order.orderType in ['STP', 'STPLMT', 'TRAIL'] else None,
            'TIF': order.tif, 'Status': orderStatus.status if orderStatus else 'PendingSubmit',
            'Filled': orderStatus.filled if orderStatus else 0,
            'Remaining': orderStatus.remaining if orderStatus else order.totalQuantity,
            'AvgFillPrice': orderStatus.avgFillPrice if orderStatus else 0.0,
            'WhyHeld': orderStatus.whyHeld if orderStatus else None,
            'LastUpdateTime': datetime.datetime.now().isoformat()
        }
        if existing_entry: existing_entry.update(entry_data)
        else: self.all_orders_log.append(entry_data)
        self._rewrite_orders_csv()

    async def place_order_interactive(self, contract: Contract):
        """Interactively prompts user for order details and places the order."""
        if not contract or not contract.conId:
            print("No valid contract selected to place an order.")
            return None

        print(f"\n--- Place Order for {contract.localSymbol or contract.symbol} ---")
        action = input("Action (BUY or SELL): ").upper()
        while action not in ['BUY', 'SELL']:
            action = input("Invalid action. Enter BUY or SELL: ").upper()

        order_type = input("Order type (MKT, LMT, STP, STPLMT): ").upper()
        while order_type not in ['MKT', 'LMT', 'STP', 'STPLMT']:
            order_type = input("Invalid order type. Enter MKT, LMT, STP, STPLMT: ").upper()
        
        try:
            quantity = float(input("Quantity: "))
        except ValueError:
            print("Invalid quantity.")
            return None

        limit_price = None
        stop_price = None

        if order_type == 'LMT' or order_type == 'STPLMT':
            try:
                limit_price = float(input("Limit Price: "))
            except ValueError:
                print("Invalid limit price.")
                return None
        if order_type == 'STP' or order_type == 'STPLMT':
            try:
                stop_price = float(input("Stop Price: "))
            except ValueError:
                print("Invalid stop price.")
                return None
        
        tif = input("Time in Force (DAY, GTC, IOC, FOK - default DAY): ").upper() or 'DAY'

        return await self.place_order(contract, order_type, action, quantity, limit_price, stop_price, tif)


    async def place_order(self, contract: Contract, order_type: str, action: str, quantity: float,
                          limit_price: float = None, stop_price: float = None, tif: str = 'DAY',
                          transmit: bool = True, **kwargs):
        if not contract or not contract.conId:
            self.ib._logger.error("Cannot place order: Contract is not qualified.")
            return None

        order_type_upper = order_type.upper()
        order_args = {'action': action.upper(), 'totalQuantity': quantity, 'tif': tif.upper(), 'transmit': transmit}
        order_args.update(kwargs)

        order = None
        if order_type_upper == 'MKT':
            order = MarketOrder(**order_args)
        elif order_type_upper == 'LMT':
            if limit_price is None: self.ib._logger.error("Limit price required for LMT order."); return None
            order = LimitOrder(lmtPrice=limit_price, **order_args)
        elif order_type_upper == 'STP':
            if stop_price is None: self.ib._logger.error("Stop price required for STP order."); return None
            order = StopOrder(stopPrice=stop_price, **order_args)
        elif order_type_upper == 'STPLMT':
            if limit_price is None or stop_price is None: self.ib._logger.error("Limit and Stop prices required for STPLMT order."); return None
            order = StopLimitOrder(lmtPrice=limit_price, stopPrice=stop_price, **order_args)
        else:
            self.ib._logger.error(f"Unsupported order type: {order_type}"); return None
        
        if not order.account and self.account_code: order.account = self.account_code

        self.ib._logger.info(f"Placing order: {action} {quantity} {contract.localSymbol or contract.symbol} @ {order_type_upper} " +
                           (f"Lmt:{limit_price} " if limit_price else "") + (f"Stp:{stop_price}" if stop_price else ""))
        try:
            trade = self.ib.placeOrder(contract, order)
            self.ib._logger.info(f"Order placed. OrderId: {trade.order.orderId}, Status: {trade.orderStatus.status}")
            print(f"Order submitted: {action} {quantity} {contract.localSymbol or contract.symbol} @ {order_type_upper}. OrderID: {trade.order.orderId}, Status: {trade.orderStatus.status}")
            self._add_order_to_log(trade.contract, trade.order, trade.orderStatus)
            return trade
        except Exception as e:
            self.ib._logger.error(f"Error placing order for {contract.localSymbol or contract.symbol}: {e}")
            print(f"Error placing order: {e}")
            return None

    async def modify_order(self, orderId: int, new_lmtPrice: float = None, new_totalQuantity: float = None, new_auxPrice: float = None):
        # (Implementation remains largely the same, ensure logging uses localSymbol or symbol)
        trade_to_modify = next((t for t in self.ib.trades() if t.order.orderId == orderId), None)
        if not trade_to_modify: self.ib._logger.error(f"Cannot modify order: OrderId {orderId} not found."); return None
        
        new_order = Order() # Create a new order object for modification
        new_order.orderId = trade_to_modify.order.orderId
        new_order.action = trade_to_modify.order.action
        new_order.totalQuantity = new_totalQuantity if new_totalQuantity is not None else trade_to_modify.order.totalQuantity
        new_order.orderType = trade_to_modify.order.orderType
        new_order.lmtPrice = new_lmtPrice if new_lmtPrice is not None else trade_to_modify.order.lmtPrice
        new_order.auxPrice = new_auxPrice if new_auxPrice is not None else trade_to_modify.order.auxPrice
        new_order.tif = trade_to_modify.order.tif
        new_order.account = trade_to_modify.order.account or self.account_code
        new_order.transmit = trade_to_modify.order.transmit

        symbol_name = trade_to_modify.contract.localSymbol or trade_to_modify.contract.symbol
        self.ib._logger.info(f"Attempting to modify OrderId {orderId} ({symbol_name}): New Qty: {new_order.totalQuantity}, New LmtP: {new_order.lmtPrice}, New AuxP: {new_order.auxPrice}")
        try:
            modified_trade = self.ib.placeOrder(trade_to_modify.contract, new_order)
            self.ib._logger.info(f"Order modification submitted for OrderId: {modified_trade.order.orderId}. Status: {modified_trade.orderStatus.status}")
            self._add_order_to_log(modified_trade.contract, modified_trade.order, modified_trade.orderStatus)
            return modified_trade
        except Exception as e:
            self.ib._logger.error(f"Error modifying order {orderId}: {e}")
            return None

    async def cancel_order_by_id(self, orderId: int):
        # (Implementation remains largely the same)
        order_to_cancel = next((trade.order for trade in self.ib.trades() if trade.order.orderId == orderId), None)
        if not order_to_cancel:
            open_orders_list = await self.ib.reqOpenOrdersAsync()
            order_to_cancel = next((o for o in open_orders_list if o.orderId == orderId), None)
        
        if order_to_cancel:
            self.ib._logger.info(f"Cancelling orderId: {orderId}")
            self.ib.cancelOrder(order_to_cancel)
            print(f"Cancellation request sent for OrderID: {orderId}.")
            return True
        else:
            self.ib._logger.error(f"Order with ID {orderId} not found for cancellation.")
            print(f"Order with ID {orderId} not found for cancellation.")
            return False

    async def get_open_orders_display(self):
        print("\n--- Open Orders ---")
        open_orders = await self.ib.reqOpenOrdersAsync()
        if not open_orders:
            print("No open orders.")
            return
        for o_ord in open_orders:
            contract_desc = f"{o_ord.contract.localSymbol or o_ord.contract.symbol} ({o_ord.contract.secType})"
            print(f"  OrderId: {o_ord.orderId}, Account: {o_ord.account}, Contract: {contract_desc}, Action: {o_ord.action}, Qty: {o_ord.totalQuantity}, "
                  f"Type: {o_ord.orderType}, LmtPx: {o_ord.lmtPrice if o_ord.lmtPrice != 0 and o_ord.lmtPrice != 1.7976931348623157e+308 else 'N/A'}, " # Handle IB's way of no price
                  f"AuxPx: {o_ord.auxPrice if o_ord.auxPrice != 0 and o_ord.auxPrice != 1.7976931348623157e+308 else 'N/A'}, "
                  f"Status: {o_ord.status}") # Status from reqOpenOrders might be general

    async def get_positions_display(self):
        print("\n--- Current Positions ---")
        positions = self.ib.positions() # Uses cached positions
        if not positions:
            print("No current positions.")
            return
        for pos in positions:
            pnl_info = self.pnl_data.get(pos.contract.conId)
            unrealized_pnl = pnl_info['unrealizedPnL'] if pnl_info else 'N/A'
            market_value = pnl_info['marketValue'] if pnl_info else 'N/A'
            print(f"  Account: {pos.account}, Symbol: {pos.contract.localSymbol or pos.contract.symbol}, Qty: {pos.position}, AvgCost: {pos.avgCost:.2f}, "
                  f"MarketValue: {market_value}, UnrealizedPnL: {unrealized_pnl}")

    async def get_account_summary_display(self):
        print("\n--- Account Summary ---")
        tags = "NetLiquidation,TotalCashValue,BuyingPower,EquityWithLoanValue,GrossPositionValue,MaintMarginReq,AvailableFunds,ExcessLiquidity"
        summary = await self.ib.reqAccountSummaryAsync(self.account_code or 'All', tags)
        if not summary:
            print("Could not retrieve account summary.")
            return
        for item in summary:
            print(f"  {item.tag}: {item.value} {item.currency}")
        
    def _rewrite_portfolio_csv(self):
        df_data = []
        processed_con_ids = set()
        for con_id, pnl_info_dict in self.pnl_data.items():
            pos_obj = self.current_positions.get(con_id)
            if not pos_obj: continue
            contract = pos_obj.contract
            avg_cost = pos_obj.avgCost
            market_price = (pnl_info_dict['marketValue'] / pnl_info_dict['position']) if pnl_info_dict['position'] != 0 else 0
            df_data.append({
                'Timestamp': pnl_info_dict['timestamp'], 'Account': pnl_info_dict['account'],
                'Symbol': contract.symbol, 'LocalSymbol': contract.localSymbol, 'SecType': contract.secType, 
                'Currency': contract.currency, 'Exchange': contract.exchange, 'ConID': con_id,
                'Position': pnl_info_dict['position'], 'AvgCost': avg_cost, 'MarketPrice': market_price,
                'MarketValue': pnl_info_dict['marketValue'], 'UnrealizedPnL': pnl_info_dict['unrealizedPnL'],
                'RealizedPnL': pnl_info_dict['realizedPnL'], 'DailyPnL': pnl_info_dict['dailyPnL']
            })
            processed_con_ids.add(con_id)
        for con_id, pos_obj in self.current_positions.items():
            if con_id not in processed_con_ids:
                contract = pos_obj.contract
                df_data.append({
                    'Timestamp': datetime.datetime.now().isoformat(), 'Account': pos_obj.account,
                    'Symbol': contract.symbol, 'LocalSymbol': contract.localSymbol, 'SecType': contract.secType,
                    'Currency': contract.currency, 'Exchange': contract.exchange, 'ConID': con_id,
                    'Position': pos_obj.position, 'AvgCost': pos_obj.avgCost, 'MarketPrice': None,
                    'MarketValue': None, 'UnrealizedPnL': None, 'RealizedPnL': None, 'DailyPnL': None
                })
        portfolio_columns = ['Timestamp', 'Account', 'Symbol', 'LocalSymbol', 'SecType', 'Currency', 'Exchange',
                             'ConID', 'Position', 'AvgCost', 'MarketPrice', 'MarketValue', 'UnrealizedPnL', 'RealizedPnL', 'DailyPnL']
        try:
            pd.DataFrame(df_data if df_data else None, columns=portfolio_columns).to_csv(self.portfolio_csv_path, index=False)
        except Exception as e: self.ib._logger.error(f"Error writing portfolio CSV: {e}")

    def _rewrite_orders_csv(self):
        orders_columns = ['Timestamp', 'OrderId', 'PermId', 'ClientId', 'Account', 'Symbol', 'LocalSymbol', 'SecType', 'Exchange',
                          'Currency', 'ConID', 'Action', 'Quantity', 'OrderType', 'LmtPrice', 'AuxPrice', 'TIF', 
                          'Status', 'Filled', 'Remaining', 'AvgFillPrice', 'WhyHeld', 'LastUpdateTime']
        try:
            pd.DataFrame(self.all_orders_log if self.all_orders_log else None, columns=orders_columns).to_csv(self.orders_csv_path, index=False)
        except Exception as e: self.ib._logger.error(f"Error writing orders CSV: {e}")

    def _rewrite_fills_csv(self):
        fills_columns = ['Timestamp', 'Symbol', 'LocalSymbol', 'SecType', 'Exchange', 'ConID', 'OrderId', 'ExecId', 
                         'Side', 'Shares', 'Price', 'CumQty', 'AvgPrice', 'EvRule', 'EvMultiplier', 'Commission', 'RealizedPnLFill']
        try:
            pd.DataFrame(self.all_fills_log if self.all_fills_log else None, columns=fills_columns).to_csv(self.fills_csv_path, index=False)
        except Exception as e: self.ib._logger.error(f"Error writing fills CSV: {e}")


async def interactive_contract_menu(bot: IBTradingBot, current_contract: Contract):
    """Displays menu for actions on a selected contract."""
    while True:
        print(f"\n--- Actions for {current_contract.localSymbol or current_contract.symbol} ({current_contract.secType}) ---")
        print("1. Fetch Historical Data")
        #print("2. Start Real-Time Bars (5s)")
        #print("3. Stop Real-Time Bars")
        print("4. Place Order")
        #print("5. View Portfolio/Positions")
        #print("6. View Open Orders")
        #print("7. View Account Summary")
        print("8. Select New Contract")
        print("0. Exit Program")
        choice = input("Enter choice: ")

        if choice == '1':
            duration = input("Enter duration (e.g., '30 D', '1 M', '1 Y' - default '7 D'): ") or '7 D'
            bar_size = input("Enter bar size (e.g., '1 min', '15 mins', '1 hour', '1 day' - default '1 day'): ") or '1 day'
            await bot.fetch_historical_data(current_contract, durationStr=duration, barSizeSetting=bar_size)
        elif choice == '2':
            await bot.start_realtime_bars(current_contract)
        elif choice == '3':
            await bot.stop_realtime_bars(current_contract.conId) # Pass conId to stop
        elif choice == '4':
            await bot.place_order_interactive(current_contract)
        elif choice == '5':
            await bot.get_positions_display()
        elif choice == '6':
            await bot.get_open_orders_display()
        elif choice == '7':
            await bot.get_account_summary_display()
        elif choice == '8':
            return # Go back to main loop to select new contract
        elif choice == '0':
            return "exit_program" # Signal to exit program
        else:
            print("Invalid choice. Please try again.")
        await asyncio.sleep(0.1) # Brief pause for IB processing if any


async def main_interactive_loop():
    bot = IBTradingBot(clientId=int(datetime.datetime.now().timestamp()) % 1000 + 100) # More dynamic clientId
    current_contract = None
    try:
        await bot.connect()
        if not bot.ib.isConnected():
            print("Could not connect to IB. Exiting.")
            return

        print(f"Connected to IB. Account: {bot.account_code or 'Default'}")
        print(f"Portfolio, Orders, and Fills will be logged to: "
              f"{bot.portfolio_csv_path}, {bot.orders_csv_path}, {bot.fills_csv_path}")

        while True:
            if not current_contract:
                current_contract = await bot.get_contract_interactive()
                if not current_contract:
                    exit_choice = input("Failed to get a contract. Try again? (y/n): ").lower()
                    if exit_choice != 'y':
                        break # Exit main loop
                    else:
                        continue # Try getting contract again
            
            if current_contract:
                action_result = await interactive_contract_menu(bot, current_contract)
                if action_result == "exit_program":
                    break # Exit main loop
                elif action_result is None: # Returned from menu to select new contract
                    current_contract = None 
                    continue
            else: # Should not happen if loop logic is correct, but as a fallback
                print("No contract selected. Please select a contract first.")
                current_contract = None # Reset to trigger selection

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
        asyncio.run(main_interactive_loop())
    except RuntimeError as e:
        if "Event loop is already running" in str(e) and util.LOOP_HAS_STARTED:
             print("Asyncio event loop already running (likely in Jupyter). "
                   "If so, instantiate IBTradingBot and call methods directly, do not run main_interactive_loop().")
        else:
            raise
