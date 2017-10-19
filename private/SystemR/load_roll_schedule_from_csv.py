import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:admin@0.0.0.0/pkdemo')
roll_table = "roll_schedule"

roll_df = pd.read_csv("admin/roll_history.csv", \
                      dtype={'CARVER':str, 'PRICE_CONTRACT': str, \
                              'CARRY_CONTRACT':str},\
                      parse_dates=['DATETIME'])

print(roll_df)

roll_df.to_sql(name=roll_table, con=engine, if_exists='replace', index=False)