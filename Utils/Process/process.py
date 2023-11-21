from Utils.Decorators.decorators import *
import pandas as pd
import datetime


@check_data_exists
def user_table_cleaning(filename, dataframe: pd.core.frame.DataFrame):
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
     3. store clean data in new dataframe and return
    """

    # Copies dataframe to temporary dataframe
    clean_dataset: pd.core.frame.DataFrame = dataframe.copy()

    # ADDS NEW ROW 'ID' with row index values
    clean_dataset.insert(0, 'ID', clean_dataset.index + 1)
    # ADDS NEW ROW 'INP_SYSDATE' with date values, used for convert Epoch datetime to sysdate
    clean_dataset['INP_SYSDATE'] = None

    # WORKS LIKE GENERATOR FOR EACH RECORD
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


@check_data_exists
def reaction_table_cleaning(filename, dataframe: pd.core.frame.DataFrame):
    """
    same description as user_table_cleaning
    :param filename: file
    :param dataframe: raw dataframe
    :return: clean dataframe
    ##TODO
    1. add unique id for record
    2. delete junk data, as we see user_table is connected to reactions_table,
    we have 1000 users but table has >1000 user_id records, which is not relevant for our table link and has no uses.
    3. add new column inp_sysdate where Epoch timestamp will be converted to date.
    4. store clean data in new dataframe and return

    """

    clean_dataset: pd.core.frame.DataFrame = dataframe.copy()

    # Removes junk data, where userid > 1000
    filtered_data = clean_dataset[clean_dataset['User'] <= 1000]

    # ADDS NEW ROW 'ID' with row index values
    filtered_data.insert(0, 'ID', filtered_data.reset_index().index + 1)
    # ADDS NEW ROW 'INP_SYSDATE' with date values, used for convert Epoch datetime to sysdate
    filtered_data.insert(4, 'INP_SYSDATE', None)

    # WORKS LIKE GENERATOR FOR EACH RECORD
    for i in filtered_data['ID']:
        # GETS Epoch number
        selected_data = filtered_data.loc[filtered_data['ID'] == i]['Reaction Date'].values[0]

        # Converts Epoch number to date
        date = datetime.datetime.fromtimestamp(selected_data).strftime('%Y-%m-%d %H:%M:%S')

        # assigns value to INP_SYSDATE column
        filtered_data.loc[filtered_data['ID'] == i, 'INP_SYSDATE'] = date

    return filtered_data

@check_data_exists
def posts_table_cleaning(filename, dataframe: pd.core.frame.DataFrame):
    """
    same description as user_table_cleaning
    :param filename: file
    :param dataframe: raw dataframe
    :return: clean dataframe

    #TODO
    1. add unique id for record
    2. add new column inp_sysdate where Epoch timestamp will be converted to date.
    3. store clean data in new dataframe and return
    """

    # Copies dataframe to temporary dataframe
    new_dataset: pd.core.frame.DataFrame = dataframe.copy()

    # ADDS NEW ROW 'ID' with row index values
    new_dataset.insert(0, 'ID', new_dataset.index + 1)
    # ADDS NEW ROW 'INP_SYSDATE' with date values, used for convert Epoch datetime to sysdate
    new_dataset['INP_SYSDATE'] = None

    for i in new_dataset['ID']:
        # GETS Epoch number
        selected_data = new_dataset.loc[new_dataset['ID'] == i, 'Post Date'].values[0]

        # Converts Epoch number to date
        date = datetime.datetime.fromtimestamp(selected_data).strftime('%Y-%m-%d %H:%M:%S')

        # assigns value to INP_SYSDATE column
        new_dataset.loc[new_dataset['ID'] == i, 'INP_SYSDATE'] = date

    return new_dataset


@check_data_exists
def friends_table_cleaning(filename, dataframe: pd.core.frame.DataFrame):
    """
    same description as user_table_cleaning
    :param filename: file
    :param dataframe: raw dataframe
    :return: clean dataframe

    #TODO
    1. add unique id for record
    """

    # Copies dataframe to temporary dataframe
    new_dataset: pd.core.frame.DataFrame = dataframe.copy()

    # ADDS NEW ROW 'ID' with row index values
    new_dataset.insert(0, 'ID', new_dataset.index + 1)


    return new_dataset
def store_data(destination, dataframe: pd.core.frame.DataFrame):
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
