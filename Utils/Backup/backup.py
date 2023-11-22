import shutil
import os
from datetime import datetime


def backup_data(source_dir, dest_dir):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_folder = os.path.join(dest_dir, f"backup_{timestamp}")

    try:
        shutil.copytree(source_dir, backup_folder)
        print(f"Backup successful. Data backed up to {backup_folder}")
    except Exception as e:
        print(f"Backup failed. Error: {str(e)}")


def disaster_recovery():
    # Backup user_table
    backup_data('Utils/Cleaning/', 'Utils/Backup/')

