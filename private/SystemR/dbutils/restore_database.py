import os
import pandas as pd
from sqlalchemy import create_engine
# Read control files, raw_data_files
backup_dir = os.path.expanduser("~/backup/")
engine = create_engine('mysql+pymysql://root:admin@0.0.0.0/pkdemo')

for root, dirs, files in os.walk(backup_dir):
    for file in files:
        table = file[:-4]
        roll_df = pd.read_csv(backup_dir + file)
        print("Writing table: ", table, "to database...")
        #roll_df.to_sql(name=table, con=engine, if_exists='replace', index=False)



