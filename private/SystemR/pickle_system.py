#

#


def set_logging():
    import logging
    logger = logging.getLogger('Downloads')
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even DEBUG messages
    file_handler = logging.FileHandler('../SystemR/admin/info.log')
    file_handler.setLevel(logging.DEBUG)

    # create a console handler to show errors
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # Create formatter and add it to handlers
    # formatter = logging.Formatter('%(message)s')
    console_formatter = logging.Formatter('{asctime} {name} {levelname:8s} {message}', datefmt='%Y%m%d %I:%M:%S%p',
                                          style='{')
    file_formatter = logging.Formatter('{asctime},{name},{levelname:8s},{message}', datefmt='%Y%m%d %I:%M:%S%p',
                                       style='{')
    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

def get_capital_offset():

    return 65000

def get_IB_cap():

    from wrapper_v5 import IBWrapper, IBclient
    from swigibpy import Contract as IBcontract
    import pandas as pd
    print("in get_IB_cap")
    callback = IBWrapper()
    client = IBclient(callback)
    print("made callback...")
    (account_value, portfolio_data) = client.get_IB_account_data()
    account_value_df = pd.DataFrame(account_value, columns=['KEY', 'VALUE', 'CURRENCY', 'ACCOUNT'])
    account_value_df.set_index('KEY', inplace=True)
    account = float(account_value_df.loc['NetLiquidation']['VALUE'])
    return account
def update_capital(cap_file, dict):
    print("Update here", cap_file)
    print(dict)

def main():

    from sysdata.configdata import Config
    from sysdata.csvdata import csvFuturesData
    from systems.provided.futures_chapter15.estimatedsystem import futures_system
    import pandas as pd
    import numpy as np
    import os
    from matplotlib.pyplot import show, legend, matshow
    import time

    dir_filename = "../SystemR/admin/directories.csv"
    if not os.path.isfile(dir_filename):
        logger.error("The file, {}, does not exist".format(dir_filename))
    else:
        today = time.strftime("%Y%m%d")
        # point to output files
        dir_df = pd.read_csv(dir_filename, index_col=['DIRECTION'], dtype={'PATH': str})
        admin_path = dir_df.loc['ADMIN'][0]
        positions_file = admin_path  + 'positions_test/system.csv'
        history_file = admin_path + 'positions_test/history/' + today + "system.csv"
        capital_file = admin_path + 'capital_test/capital.csv'
        print("In main....")
        #**** Get capital from IB ****
        ib_capital = get_IB_cap()
        offset = get_capital_offset()
        capital = ib_capital + offset
        print("ib capital: ", ib_capital, "offset: ", offset)
        cap_dict = {'DATE': today, 'IBCAP': ib_capital, 'CORRECTION': offset}
        update_capital(capital_file, cap_dict)
        print('Back in main...:', capital)
        my_config_dict = dict(notional_trading_capital= capital)
        print(my_config_dict)

        new_config = Config(["private.SystemR.production01.yaml", my_config_dict])

        # **** Point to data  ****
        data=csvFuturesData("private.SystemR.data")
        # **** Create the system ****
        system = futures_system(data=data, config=new_config, log_level="on")
        system.accounts.portfolio().sharpe()   # Do calculations to save in cache....
        system.pickle_cache("private.SystemR.system.pck")  ## use any file extension you like


if __name__ == "__main__":

    logger = set_logging()
    try:
        main()
    except Exception as e:
        logger.exception(e)
