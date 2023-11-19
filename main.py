from Utils.Process.dataframes import userTable
from Utils.Process.process import user_table_cleaning, store_user_table

if __name__ == '__main__':
    store_user_table('Utils/Cleaning/user_table_final.csv',
                     user_table_cleaning('Utils/RawData/user_table.csv', userTable))
