from datetime import *
from dateutil.relativedelta import *
import os,sys
import logging
import pandas as pd
import time
from time import sleep


def set_logging():
    logger = logging.getLogger('Downloads')
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even DEBUG messages
    file_handler = logging.FileHandler('admin/logs/reconciler.log')
    file_handler.setLevel(logging.DEBUG)

    # create a console handler to show errors
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # Create formatter and add it to handlers
    # formatter = logging.Formatter('%(message)s')

    console_formatter = logging.Formatter('{asctime} {name} {levelname:8s} {message}', datefmt='%Y%m%d %I:%M:%S%p',\
                                          style='{')
    file_formatter = logging.Formatter('{asctime},{name},{levelname:8s},{message}', datefmt='%Y%m%d %I:%M:%S%p',\
                                       style='{')
    '''
    console_formatter = logging.Formatter('{asctime} {name} {levelname:8s} {message}', datefmt='%Y%m%d %I:%M:%S%p')


    file_formatter = logging.Formatter('{asctime},{name},{levelname:8s},{message}', datefmt='%Y%m%d %I:%M:%S%p')
    '''
    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

def last_download_date(markets_df, next_markets, path):
    #print(markets_df)
    print(markets_df)
    print(next_markets)
    for tuple in markets_df.itertuples():
        market = tuple.Index
        if tuple.PRICE == tuple.PRICE:
            legacy_price_file = path + market + '_price.csv'
            legacy_price_df = pd.read_csv(legacy_price_file, index_col=0)
            last_price_date = legacy_price_df.iloc[-1:].index[0]
        else:
            legacy_price_file = path + market + 'fx.csv'
            legacy_price_df = pd.read_csv(legacy_price_file, index_col=0)
            last_price_date = legacy_price_df.iloc[-1:].index[0]
            print("---------- Forex")
        string = "{:<12}{:<12}".format(market, last_price_date)
        print(string)
    print()
def main():


    from datetime import timedelta
    from wrapper_v5 import IBWrapper, IBclient
    import pandas as pd
    import numpy as np
    from swigibpy import Contract as IBcontract
    #------------------------------------------------------------
    print("sys.executable: ", sys.executable)
    print("os.get_cwd(): ", os.getcwd())
    print("sys.version: ", sys.version)
    print("sys.path")
    print(sys.path)
    print("--------------------------")
    print("os.path: ", os.path)
    print("os.path.dirname(__file___): ", os.path.dirname(__file__))
    print(os.path.join(os.path.dirname(__file__), '..'))
    print("--------------------------")
    print("Try this: ", os.path.dirname(os.path.abspath(__file__)))
    #-----------------------------------------------------------------



    dir_filename = "admin/directories.csv"
    print("os.get_cwd(): ", os.getcwd())
    if not os.path.isfile(dir_filename):
        print()
        logger.error("The file, {}, does not exist".format(dir_filename))
    else:
        # Get daily system positions
        dir_df = pd.read_csv(dir_filename, index_col=['DIRECTION'], dtype={'PATH': str})
        source_path = dir_df.loc['SOURCE'][0] # /home/pete/Documents/Python Packages/sysIB/private/data/
        legacy_path = dir_df.loc['DESTINATION'][0]
        positions_file = 'admin/positions/system.csv'
        positions_df = pd.read_csv(positions_file, index_col=0, dtype={'MATURITY': str, 'PRICE': str})

        market_data_filename = source_path + 'marketdata.csv'
        market_data_df = pd.read_csv(market_data_filename,  dtype={'CARRY': str, 'PRICE': str})
        # For each market we only need most recent (by DATETIME) row where DONE == 1
        #live_market_df = market_data_df[market_data_df.DONE == 1]
        temp_df = market_data_df[market_data_df.DONE == 1].sort_values(by=['CARVER','DATEFROM'], ascending=[1,1])
        curr_markets_df = temp_df.groupby('CARVER').tail(1)
        curr_markets_df = curr_markets_df.set_index(['CARVER'])
        temp_df = market_data_df[market_data_df.DONE == 0].sort_values(by=['CARVER', 'DATEFROM'], ascending=[1, 1])
        next_markets_df = temp_df.groupby('CARVER').tail(1)
        next_markets_df = next_markets_df.set_index(['CARVER'])
        #print(curr_markets_df)
        curr_markets_df = curr_markets_df[['IB', 'PRICE']]
        next_markets_df = next_markets_df[['IB', 'DATEFROM', 'PRICE']]
        last_download_date(curr_markets_df, next_markets_df, legacy_path)

        curr_markets_df = curr_markets_df[pd.notnull(curr_markets_df['PRICE'])]
        system_positions = pd.concat([curr_markets_df, positions_df], axis=1)[['IB', 'MATURITY', 'QUANTITY']]
        system_positions = system_positions.set_index(['IB'])

        # CONNECT to Interactive Brokers and get current portfolio positions
        callback = IBWrapper()
        client = IBclient(callback)
        (account_value, portfolio_data) = client.get_IB_account_data()
        port_df1 = pd.DataFrame(portfolio_data)
        port_df1 = port_df1.ix[:, 0:2]
        port_df1.columns = ['IB', 'EXPIRY', 'POSITION']
        port_df1['EXPIRY'].replace('', np.nan, inplace=True)
        portfolio_positions = port_df1[pd.notnull(port_df1['EXPIRY'])].copy()
        portfolio_positions['EXPIRY'] = portfolio_positions.EXPIRY.str[:6]
        portfolio_positions = portfolio_positions[portfolio_positions['POSITION'] != 0]
        portfolio_positions = portfolio_positions.set_index(['IB'])

        # Merge system positions and portfolio positions into one df
        merged_positions = system_positions.join(portfolio_positions)
        merged_positions['POSITION'] = merged_positions['POSITION'].fillna(0)
        merged_positions['POSITION'] = merged_positions['POSITION'].astype(int)

        actual_positions = merged_positions[(merged_positions.QUANTITY != 0) | (merged_positions.POSITION != 0)]
        print("Actual Positions")
        print(actual_positions)
        print()
        sleep(1)
        logger.debug("")
        for tuple in actual_positions.itertuples():

            market = tuple.Index
            expiry = tuple.EXPIRY
            if (market == 'NG' or market == 'CL') and expiry == expiry:  # expiry == expiry is true if expiry is not NAN
                # month early expiry so shift expiry month forwad
                date = datetime(int(tuple.EXPIRY[:4]), int(tuple.EXPIRY[4:6]), 1)
                foward_date = date + relativedelta(months=1)
                expiry = "{:%Y%m}".format(foward_date)

            if tuple.MATURITY == expiry and tuple.POSITION == tuple.QUANTITY:
                logger.info("OK        : {:<6}: {:<6}: {:<3}: Position is OK".format(market, expiry, tuple.POSITION))
        logger.debug("")
        trades_df = pd.DataFrame(columns=['Market','Action','Quantity','Notes'])
        for tuple in actual_positions.itertuples():
            market = tuple.Index
            expiry = tuple.EXPIRY
            if (market == 'NG' or market == 'CL') and expiry == expiry:
                date = datetime(int(tuple.EXPIRY[:4]), int(tuple.EXPIRY[4:6]), 1)
                foward_date = date + relativedelta(months=1)
                expiry = "{:%Y%m}".format(foward_date)
            if tuple.MATURITY != expiry and pd.notnull(expiry):
                units = tuple.QUANTITY
                logger.error("ROLL      : {:<10}: from {} to {}".format(market, expiry, tuple.MATURITY))
                add_df = pd.DataFrame([{ 'Market': market,\
                                         'Action':"ROLL", \
                                         'Quantity': units,\
                                         "Notes": "from {} to {}".format(expiry, tuple.MATURITY)}])
                print(add_df)
                trades_df = trades_df.append(add_df)
            if tuple.QUANTITY > tuple.POSITION:
                units = tuple.QUANTITY - tuple.POSITION
                logger.error("BUY       : {:<6}{:<6}: Buy {} contracts".format(market, tuple.MATURITY, units))
                add_df = pd.DataFrame([{'Market': market,\
                                         'Action':"BUY", \
                                         'Quantity': units,\
                                         "Notes": "Buy {} contracts".format(units)}])
                print(add_df)
                trades_df = trades_df.append(add_df)
            if tuple.QUANTITY < tuple.POSITION:
                units = tuple.POSITION - tuple.QUANTITY
                logger.error("SELL      : {:<6}{:<6}: Sell {} contracts".format(market, tuple.MATURITY, units))
                add_df = pd.DataFrame([{'Market': market,\
                                         'Action':"SELL", \
                                         'Quantity': units,\
                                         "Notes": "Sell {} contracts".format(units)}])
                #print(add_df)

                trades_df = trades_df.append(add_df)

        today_date = time.strftime("%Y%m%d")
        today_time = time.strftime("%H:%M:%S")
        print("time", today_time)
        trades_df['Time'] = today_time
        trades_df = trades_df[['Market', 'Action', 'Quantity', 'Notes', 'Time']]
        trades_df.set_index(['Market'], inplace=True)



        trades_file = 'admin/trades/trades.csv'
        trades_history_file = 'admin/trades/history/' + today_date + 'trades.csv'
        trades_df.to_csv(trades_file)
        #print("trades_df-----------------------------------------------")

        #print(trades_df)
        if os.path.isfile(trades_history_file):
            amtrades_df = pd.read_csv(trades_history_file, \
                                      usecols=['Market', 'Action', 'Quantity', 'Notes','Time'] \
                                      ,dtype={'Market': str, 'Action': str,'Notes': str})
            amtrades_df.set_index(['Market'],inplace=True)
            #trades_df.reset_index(inplace=True)
            #print(trades_df)
            #print(amtrades_df)
            addtrades_df = pd.concat([amtrades_df, trades_df])
        else:
            addtrades_df = trades_df
        print("today's merged trades...")
        print(trades_df)

        addtrades_df.to_csv(trades_history_file)
        #print(add_df)



if __name__=="__main__":

    """
    This simple example places an order, checks to see if it is active, and receives fill(s)

    Note: If you are running this on the 'edemo' account it will probably give you back garbage

    Though the mechanics should still work

    This is because you see the orders that everyone demoing the account is trading!!!
    """

    logger = set_logging()
    try:
        main()
    except Exception as e:
        logger.exception(e)


