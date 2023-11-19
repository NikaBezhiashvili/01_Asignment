import multiprocessing
from os.path import exists, getsize

def check_data_exists(func):
    """
    :param func: gets filename from function
    :return:  checks if file exists and has data in it
    """
    def wrapper(filename, *args, **kwargs):
        if not exists(filename):
            raise FileNotFoundError(f"file {filename} not found")
        elif getsize(filename) == 0:
            raise Exception(f"File {filename} is empty")
        else:
            return func(filename, *args, *kwargs)

    return wrapper
#
#
# def multiprocess(func):
#     """
#     :param func: gets function as param
#     :return: returns multiprocess
#     """
#     def wrapper(*args, **kwargs):
#         process = multiprocessing.Process(target=func, args=args,kwargs=kwargs)
#         process.start()
#     return wrapper
#
