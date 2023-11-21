from Utils.Process.dataframes import (userTable,
                                      reactionsTable,
                                      postsTable,
                                      friendsTable)
from Utils.Process.process import (user_table_cleaning,
                                   reaction_table_cleaning,
                                   posts_table_cleaning,
                                   friends_table_cleaning,
                                   store_data)
from multiprocessing import Process, Manager
import time



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


if __name__ == '__main__':
    start_time = time.time()
    # Gets queue processes with generator
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

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Program execution time: {elapsed_time} seconds")
