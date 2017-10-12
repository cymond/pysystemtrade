from datetime import *
import os,sys
import logging
import pandas as pd

from dbutils.capital import get_capital_from_db
from sqlalchemy import create_engine
engine = create_engine('mysql+pymysql://root:admin@0.0.0.0/pkdemo')

def pretty_print(df, desc):
    print("======================================================================")
    print(desc)
    print(df)
    print("======================================================================")

 
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

def get_market_data():
    market_data_df = pd.read_sql_table(table_name='marketdata', con=engine, index_col=['CARVER'])
    return market_data_df

def mat_from_expiry(row):
    # Adjust
    if row['IB'] == 'NG':
        row['EXPIRY'] = int(row['EXPIRY']) + 1
        row['EXPIRY'] = str(row['EXPIRY'])
    return row.EXPIRY


def get_broker_positions(client, df):
    import numpy as np
    # return brokerage account positions from IB
    market_data_df = get_market_data()
    (account_value, portfolio_data) = client.get_IB_account_data()
    port_df1 = pd.DataFrame(portfolio_data)
    port_df1 = port_df1.ix[:,[0,1,2,3,6,8]]
    port_df1.columns = ['IB', 'EXPIRY', 'BROKER_POSITION', 'CURRENT_PRICE', 'GAIN', 'CURRENCY']
    port_df1['EXPIRY'].replace('', np.nan, inplace=True)
    portfolio_positions = port_df1[pd.notnull(port_df1['EXPIRY'])].copy()
    portfolio_positions['EXPIRY'] = portfolio_positions.EXPIRY.str[:6]
    portfolio_positions['EXPIRY'] = portfolio_positions.apply(mat_from_expiry, axis=1)
    portfolio_positions = portfolio_positions[portfolio_positions['BROKER_POSITION'] != 0]
    #portfolio_positions = portfolio_positions.set_index(['IB'])
    portfolio_positions['CARVER'] = ""
    portfolio_positions.reset_index(inplace=True)
    #portfolio_positions.rename({0: 'IB'}, inplace=True)
    for counter, row in enumerate(portfolio_positions.itertuples(), 1):
        carver_value = market_data_df[market_data_df['IB'] == row.IB].index.values[0]
        portfolio_positions.set_value(counter - 1, 'CARVER', carver_value)
    portfolio_positions.set_index(['CARVER'], inplace=True)
    df = df.join(portfolio_positions, how='left', lsuffix='_current', rsuffix='_next')
    df.reset_index(inplace=True)
    for counter, row in enumerate(df.itertuples(), 1):
        if row.IB != row.IB:
            ib_value = market_data_df.loc[row.CARVER]['IB' ]
            price_contract = df[df.CARVER == row.CARVER]['PRICE_CONTRACT_current'].iloc[0]
            df.set_value(counter - 1, 'IB', ib_value)
            df.set_value(counter - 1, 'EXPIRY', price_contract)
        #ib_value
    return df

def get_curr_next_contracts():
    # From the roll-schedule, for each market, determine what markets we should have
    # and the ones we'll next roll into as of today (assume rolls up to yesterBD have happened)

    roll_table = 'roll_schedule'
    market_data = get_market_data()
    roll_df = pd.read_sql_table(table_name=roll_table, \
                                con=engine, index_col=['DATETIME'],
                                parse_dates=['DATETIME'])
    roll_df.reset_index(inplace=True)

    current_rolls = roll_df[roll_df['DATETIME'] < datetime.now()].groupby(['CARVER']).last()
    next_rolls = roll_df[roll_df['DATETIME'] >= datetime.now()].groupby(['CARVER']).first()
    next_rolls['DAYSTOROLL'] = 0
    next_rolls.reset_index(inplace=True)
    for counter, row in enumerate(next_rolls.itertuples(), 1):
        days_left = (row.DATETIME.date() - date.today()).days
        next_rolls.set_value(counter - 1, 'DAYSTOROLL', days_left)
    next_rolls.set_index(['CARVER'], inplace=True)
    contracts_df = current_rolls.join(next_rolls, how='left', lsuffix='_current', rsuffix='_next')
    return contracts_df

def add_latest_donwloads(contracts_df):
    # Return the actual latest rows in the database...
    from private.SystemR.mysqldata import mysqlFuturesData
    yesterBD = pd.datetime.today() - pd.tseries.offsets.BDay(1)
    data = mysqlFuturesData()
    contracts_df['LAST_DOWNLOAD'] = pd.NaT
    dict_list = []
    print("****************************")
    contracts_df.reset_index(inplace=True)
    for counter, row in enumerate(contracts_df.itertuples(),1):
        last_dl_date = data.get_instrument_raw_carry_data(row.CARVER).iloc[-1:].index[0]
        contracts_df.set_value(counter-1, 'LAST_DOWNLOAD', last_dl_date)
    contracts_df.set_index(['CARVER'], inplace=True)
    return contracts_df

def add_system_positions(downloads_df):
    from sysdata.configdata import Config
    from systems.provided.futures_chapter15.estimatedsystem import futures_system
    from private.SystemR.mysqldata import mysqlFuturesData

    pickle_file = "private.SystemR.system.pck"

    capital = get_capital_from_db()
    capital_dict = dict(notional_trading_capital=capital)
    new_config = Config(["private.SystemR.production01.yaml", capital_dict])
    data = mysqlFuturesData()
    system = futures_system(data=data, config=new_config, log_level="on")
    system.unpickle_cache(pickle_file)

    downloads_df['SYSTEM_POSITION'] = 0
    downloads_df.reset_index(inplace=True)
    for counter, row in enumerate(downloads_df.itertuples(),1):
        position =system.accounts.get_buffered_position(row.CARVER, roundpositions=True)[-1:].iloc(0)[0]
        downloads_df.set_value(counter-1, 'SYSTEM_POSITION',position)
    downloads_df.set_index(['CARVER'], inplace=True)

    return downloads_df

def check_roll(row):
    if row['IB_mat'] == row['IB_mat']:
        if row['Sys_mat'][:6] > row['IB_mat'][:6]:
            return 'Roll'
        elif row['Sys_mat'][:6] < row['IB_mat'][:6]:
            return 'Back?'
        elif row['DaysToRoll'] == row['DaysToRoll']:
            return int(row['DaysToRoll'])
        else:
            return ''
    else:
        return ''


def generate_reports(frame):
    # Display latest downloads... CARVER IB PRICE CARRY LAST_DOWNLOAD
    display = frame.loc[:,['CARVER', 'IB','PRICE_CONTRACT_current','CARRY_CONTRACT_current','LAST_DOWNLOAD']]
    pretty_print(display, "LAST DOWNLOAD DATE")
    # Display roll report
    display = frame.loc[:,['CARVER', 'IB','PRICE_CONTRACT_current','CARRY_CONTRACT_current','DATETIME_next','DAYSTOROLL']]
    # Display System Broker discrepancy

    #display = frame.loc[:,['CARVER','IB','PRICE_CONTRACT_current', 'SYSTEM_POSITION','EXPIRY', 'LAST_DOWNLOAD','BROKER_POSITION']].copy()
    display = frame.loc[:, ['CARVER', 'IB', 'PRICE_CONTRACT_current', 'SYSTEM_POSITION', 'EXPIRY', 'LAST_DOWNLOAD',
                            'BROKER_POSITION', 'DAYSTOROLL']].copy()
    display.BROKER_POSITION = display.BROKER_POSITION.apply(lambda x: int(x) if x == x  else 0)
    display['ACTION'] = display['SYSTEM_POSITION'] - display['BROKER_POSITION']

    display.reset_index(inplace=True)
    aList = []
    for row in display.itertuples():

        if row.BROKER_POSITION == row.BROKER_POSITION or row.SYSTEM_POSITION != 0:

            aList.append(row)
    display = pd.DataFrame(aList)  # below causes KeyError!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    display.set_index(['CARVER'],inplace=True)
    #display.BROKER_POSITION = display.BROKER_POSITION.apply(lambda x: int(x) if x == x  else 0)
    #display.BROKER_POSITION = display.BROKER_POSITION.astype(int)


    #display.ACTION = display.ACTION.astype(int)
    display.ACTION = display.ACTION.apply(lambda x: int(x) if x == x  else 0)
    display.rename(columns={'PRICE_CONTRACT_current': 'Sys_mat', 'SYSTEM_POSITION':'Target',
                            'LAST_DOWNLOAD': 'LstDnLoad','EXPIRY':'IB_mat', 'ACTION':'Action',
                            'BROKER_POSITION':'Broker', 'DAYSTOROLL': 'DaysToRoll'},
                   inplace=True)
    display.Action = display.Action.astype(str)
    display.Action.replace(['0'], '', inplace=True)

    display.Action = display.Action.apply(lambda x: 'Buy ' + x[1:] if (len(x) > 0 and x[:1] != '-') else x)
    display.Action = display.Action.apply(lambda x: 'Sell ' + x[1:] if x[:1] == '-' else x)
    display.drop(['Index'], 1, inplace=True)
    display['Roll'] = display.apply(check_roll, axis=1)
    display = display[['IB','LstDnLoad', 'IB_mat', 'Roll','Sys_mat', 'Broker', 'Target', 'Action']]
    display = display[ ( (display['Broker'] != 0) | (display['Target'] != 0))]
    pretty_print(display, "STATUS")
    return display

def main():

    from wrapper_v5 import IBWrapper, IBclient
    import pandas as pd

    from swigibpy import Contract as IBcontract
    callback = IBWrapper()
    client = IBclient(callback)

    dir_filename = "admin/directories.csv"
    print("os.get_cwd(): ", os.getcwd())

    if not os.path.isfile(dir_filename):
        print("file error: ", dir_filename)
        logger.error("The file, {}, does not exist".format(dir_filename))
    else:
        # Must indicate if downloads up to date, if system is up to date and if brokerage account is up to date
        # 1. From roll-schedule, for all markets, get  current 'working' contracts (price and carry),
        #      and the next ones (after subsequent roll...), including roll date i.e. days left to roll.
        # 2. Get system positions (and the date on which they are based)
        # 3. Determine up-to-dateness of system positions. Are there missing BDays?  ******* Task 1
        # 4. Get all current broker positions
        # 5. Determine if broker positions match system positions                    ******** Task 2
        # 6. Flag missing roll-dates, dodgy data, delayed trades, delayed rolls
        # 7. Report current trading rules and P/L

        contracts_df = get_curr_next_contracts()
        pretty_print(contracts_df, "Required Markets...")

        with_downloads_df = add_latest_donwloads(contracts_df)
        pretty_print(with_downloads_df, "Latest loads...")

        with_system_positions_df = add_system_positions(with_downloads_df)
        pretty_print(with_system_positions_df, "System positions...")

        with_broker_positions_df = get_broker_positions(client, with_system_positions_df)
        pretty_print(with_broker_positions_df, "Broker positions...")

        with_broker_positions_df.to_csv("admin/positions/current.csv")

        generate_reports(with_broker_positions_df)


if __name__=="__main__":

    """

    """

    logger = set_logging()
    try:
        main()
    except Exception as e:
        print(e)
        logger.exception(e)

































'''
    dict_list = []
    market_data_df = get_market_data()

    for row in market_data_df.itertuples():
        symbol = row.CARVER
        if row.SECTYPE == 'FUT':
            table = row.CARVER.lower() + '_carrydata'
        else:
            table = row.CARVER + '_fx'
        df = pd.read_sql_table(table_name=table, con=engine)

        datetime = df[-1:].iloc[0]['DATETIME']
        price = df[-1:].iloc[0]['PRICE']
        if row.SECTYPE == 'FUT':
            # Create dictionary for market
            price_mat = df[-1:].iloc[0]['PRICE_CONTRACT']
            carry_mat = df[-1:].iloc[0]['CARRY_CONTRACT']
            carry = df[-1:].iloc[0]['CARRY']
        else:
            # Create dictionary for market
            price_mat = df[-1:].iloc[0]['PRICE_CONTRACT']
            carry_mat = ''
            carry = ''
        last_load = {'symbol': symbol, 'datetime': datetime, 'price': price, 'carry': carry,
                           'price_contract': price_mat, 'carry_contract': carry_mat}
        dict_list.append(last_load)
    last_loads_df = pd.DataFrame(dict_list)
    return last_loads_df
    '''



'''
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


def get_days_to_roll(scheduled_df ):

    dict_list = []
    market_data_df = pd.read_sql_table(table_name='marketdata', con=engine)
    for row in scheduled_df.itertuples():
        market = row.CARVER
        if row.SECTYPE == 'FUT':
            legacy_price_table = market.lower() + '_price'
            legacy_price_df = pd.read_sql_table(table_name=legacy_price_table, \
                                         con=engine, index_col=['DATETIME'],
                                         parse_dates=['DATETIME'])
            last_price_date = legacy_price_df.iloc[-1:].index[0]
        else:
            legacy_price_table = market + 'fx'
            legacy_price_df = pd.read_sql_table(table_name=legacy_price_table, \
                                                con=engine, index_col=['DATETIME'],
                                                parse_dates=['DATETIME'])
            last_price_date = legacy_price_df.iloc[-1:].index[0]
            print("---------- Forex")
        string = '{:<12}{:<12}'.format(market, str(last_price_date)[:11])
        print(string, datetime.strptime((scheduled_df.loc[market]['DATEFROM']),'%Y-%m-%d' ))
        dict_list.append({'symbol': market, 'roll_date': last_price_date, 'days_to_roll' : (last_price_date.date() - date.today()).days })
    days_left_df = pd.DataFrame(dict_list)
    return days_left_df
        '''

'''
portfolio_positions_df = get_broker_positions(client)
pretty_print(portfolio_positions_df, "IB positions...")

#curr_markets_df = curr_markets_df[pd.notnull(curr_markets_df['PRICE'])]
#system_positions = pd.concat([curr_markets_df, positions_df], axis=1)[['IB', 'MATURITY', 'QUANTITY']]
#system_positions = system_positions.set_index(['IB'])

# CONNECT to Interactive Brokers and get current portfolio positions


# Merge system positions and portfolio positions into one df
merged_positions = system_positions.join(portfolio_positions_df)
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

'''


