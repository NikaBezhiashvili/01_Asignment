from Utils.Decorators.decorators import *
import pandas as pd
import datetime



@check_data_exists
def user_table_cleaning(filename, dataframe:pd.core.frame.DataFrame):
    """
    :param filename: gets filepath of rawdata, required for @check_data_exists decorator, it checks if file exists or
                        has data in it
    :param dataset: gets dataframe for cleaning

                    !!!IMPORTANT!!! parameters must have same context! for example, if filename is user_table, dataframe
                                    must be user_table too.
                                    this function cleans and re-designs user_table
     :return: returns clean dataframe

     ##TODO
     1. add identificator column as ID, it should be Unique record
     2. convert Epoch datetype to HUMAN Date type ( "human" means date like '11/19/2023' and not 1700407252 )
     3. check if all of the column data has same datatype , age must be int, etc.
     4. store clean data in new dataframe and return
    """

    # Copies dataframe to temporary dataframe
    clean_dataset:pd.core.frame.DataFrame = dataframe.copy()

    # ADDS NEW ROW 'ID' with row index values
    clean_dataset.insert(0, 'ID', clean_dataset.index)
    # ADDS NEW ROW 'INP_SYSDATE' with date values, used to convert Epoch datetime to sysdate
    clean_dataset['INP_SYSDATE'] = None

    # WORKS FOR EACH RECORD
    for i in clean_dataset['ID']:
        # Gets 'Subscription Date' value

        selected_data = clean_dataset.loc[clean_dataset['ID'] == i]['Subscription Date'].values[0]

        # Converts 'Subscription Date' value to date
        date = datetime.datetime.fromtimestamp(selected_data).strftime('%Y-%m-%d %H:%M:%S')

        # Inserts date value into new column
        # works like this
        # Update clean_dataset
        #  set inp_sysdate = i.date
        # where id = i.id;
        clean_dataset.loc[clean_dataset['ID'] == i, 'INP_SYSDATE'] = date

    return clean_dataset


# print(user_table_cleaning('../RawData/user_table.csv', pd.read_csv('../RawData/user_table.csv')))


def store_user_table(destination, dataframe:pd.core.frame.DataFrame):
    """

    :param destination: new file destination, where the data gets stored
    :param dataframe: clean dataframe
    :return: return decorator value
    """
    dataframe.to_csv(destination, index=False)

    # After storing, we check if data got stored
    @check_data_exists
    def check_if_data_stored(destination):
        pass

    # dataframe.to_csv(destination, index=False) does its job, so i'm returning status of decorator
    return check_if_data_stored(destination)

