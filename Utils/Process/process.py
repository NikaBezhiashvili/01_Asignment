from Utils.Decorators.decorators import *
import pandas as pd
import datetime
from pprint import pprint
from Utils.Process.dataframes import (userTable,
                                      reactionsTable,
                                      postsTable,
                                      friendsTable)
from multiprocessing import Process, Manager


""" Table Clearing """
@check_data_exists
def user_table_cleaning(filename, dataframe: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    """
    :param filename: gets filepath of raw data, required for @check_data_exists decorator, it checks if file exists or
                        has data in it
    :param dataframe: gets dataframe for cleaning

                    !!!IMPORTANT!!! parameters must have same context! for example, if filename is user_table, dataframe
                                    must be user_table too.
                                    this function cleans and re-designs user_table
     :return: returns clean dataframe

     ##TODO
     1. add identifier column as ID, it should be Unique record
     2. convert Epoch date type to HUMAN Date type ( "human" means date like '11/19/2023' and not 1700407252 )
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
def reaction_table_cleaning(filename, dataframe: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
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
def posts_table_cleaning(filename, dataframe: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
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
def friends_table_cleaning(filename, dataframe: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
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

""" Storing Data """
def store_data(destination, dataframe: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    """

    :param destination: new file destination, where the data gets stored
    :param dataframe: clean dataframe
    :return: return decorator value
    """
    dataframe.to_csv(destination, index=False)

    # After storing, we check if data got stored
    @check_data_exists
    def check_if_data_stored(destination):
        return True

    # dataframe.to_csv(destination, index=False) does its job, so i'm returning status of decorator
    return check_if_data_stored(destination)


""" Answering Questions, Getting Data """

def get_top_name(top: int) -> pd.core.series.Series:
    """

    :param top: gets number as parameter, determines how many TOP items should be returned
    :return:  returns name and quantity of  the most common names in users
    """

    dataframe = pd.read_csv('Utils/Cleaning/user_table_final.csv') # From Main
    # dataframe = pd.read_csv('../Cleaning/user_table_final.csv') # From current file

    top_names = dataframe['Name'].value_counts()[:top]

    return top_names

def get_top_contributors(top: int) -> list:
    # reaction_dataframe = pd.read_csv('../Cleaning/reaction_table_final.csv') From current file
    # posts_dataframe = pd.read_csv('../Cleaning/post_table_final.csv') From current file
    # user_dataframe = pd.read_csv('../Cleaning/user_table_final.csv') From current file

    reaction_dataframe = pd.read_csv('Utils/Cleaning/reaction_table_final.csv') # From main file
    posts_dataframe = pd.read_csv('Utils/Cleaning/post_table_final.csv') # From main file
    user_dataframe = pd.read_csv('Utils/Cleaning/user_table_final.csv') # From main file


    data = {

    }

    reaction_counts = reaction_dataframe['User'].value_counts().to_dict()
    post_counts = posts_dataframe['User'].value_counts().to_dict()
    for i in user_dataframe['ID']:
        data[i] = reaction_counts.get(i,0) + post_counts.get(i,0)


    top_values  = sorted(data.items(), key=lambda x: x[1], reverse=True)[:top]

    return  top_values

""" Multiprocessing """

def process_user_table(result_queue):
    user_table = user_table_cleaning('Utils/RawData/user_table.csv', userTable)
    store_data('Utils/Cleaning/user_table_final.csv', user_table)
    result_queue.put('User table processing complete')


def process_reaction_table(result_queue):
    reaction_table = reaction_table_cleaning('Utils/RawData/reactions_table.csv', reactionsTable)
    store_data('Utils/Cleaning/reaction_table_final.csv', reaction_table)
    result_queue.put('Reaction table processing complete')


def process_post_table(result_queue):
    posts_table = posts_table_cleaning('Utils/RawData/posts_table.csv', postsTable)
    store_data('Utils/Cleaning/post_table_final.csv', posts_table)
    result_queue.put('Post table processing complete')


def process_friends_table(result_queue):
    friends_table = friends_table_cleaning('Utils/RawData/friends_table.csv', friendsTable)
    store_data('Utils/Cleaning/friends_table_final.csv', friends_table)
    result_queue.put('Friends table processing complete')

def multiprocess():
    with Manager() as manager:
        result_queue = manager.Queue()

        user_process = Process(target=process_user_table, args=(result_queue,))
        reaction_process = Process(target=process_reaction_table, args=(result_queue,))
        posts_process = Process(target=process_post_table, args=(result_queue,))
        friends_process = Process(target=process_friends_table, args=(result_queue,))

        user_process.start()
        reaction_process.start()
        posts_process.start()
        friends_process.start()

        user_process.join()
        reaction_process.join()
        posts_process.join()
        friends_process.join()

        while not result_queue.empty():
            print(result_queue.get())
