import pandas as pd
#
try:
    friendsTable = pd.read_csv("Utils/RawData/friends_table.csv")
    postsTable = pd.read_csv("Utils/RawData/posts_table.csv")
    reactionsTable = pd.read_csv("Utils/RawData/reactions_table.csv")
    userTable = pd.read_csv("Utils/RawData/user_table.csv")

except:
    raise Exception("Couldn't read files.")

