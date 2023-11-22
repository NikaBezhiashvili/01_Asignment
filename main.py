from Utils.Process.process import (get_top_name,
                                   get_top_contributors,
                                   multiprocess)
import time


def main():
    start_time = time.time()
    # Gets queue processes with generator
    multiprocess()

    # Gets most common names param 10 means top 10, etc.
    common_names = get_top_name(10)

    # ID / Quantity
    top_contributors = get_top_contributors(5)

    print('Top 10 the most common name on the social network \n', common_names)
    print('Top 10 contributors on the social network ID/Quantity \n', top_contributors)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Program execution time: {elapsed_time} seconds")


if __name__ == '__main__':
    main()
