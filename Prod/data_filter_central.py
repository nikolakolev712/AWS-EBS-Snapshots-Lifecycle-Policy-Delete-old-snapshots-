import csv
import json
import pandas as pd
from datetime import datetime

# For the get_volumes() function - This is the place where the values are to be changed:
json_file_ = "aws-ebs-volumes-devops-tools-central"

# For the filter_active() function - This is the place where the values are to be changed:
file_name = "aws-ebs-snapshots-devops-tools-central"
owner_id_ = "874747637540"

# For the filter_date() function - This is the place where the values are to be changed:
date_cvs = "aws-ebs-snapshots-devops-tools-central"
start_date_time = "2021-12-01"

# Get Time object:
def time_(stri, start_date):

    stri = stri.split("T")[0]

    date_dt = datetime.strptime(stri, '%Y-%m-%d')

    date_dt_ = datetime.strptime(start_date, '%Y-%m-%d')

    end_date = datetime.now()

    if date_dt_ <= date_dt <= end_date:
        return True


# Get Volume ID's:
def get_volumes(json_file):

    with open(f"{json_file}.json", "r") as jsonFile:
        jsonObject = json.load(jsonFile)
        jsonFile.close()
    test_vols_volumeid = [i["VolumeId"] for i in jsonObject["Volumes"]]

    return test_vols_volumeid


# Get Active volumes:
def filter_active_inactive(owner_id, snapshots_csv, data_d=get_volumes(json_file_)):
    data_v_1 = []
    with open(f"{snapshots_csv}.csv", "r") as f:
        data_v = csv.DictReader(f)
        for i in data_v:
            data_v_1.append(i)

    data_active = [i for i in data_v_1 if i["Snapshots/OwnerId"]
                   == owner_id and i["Snapshots/VolumeId"] in data_d]
    data_inactive = [i for i in data_v_1 if i["Snapshots/OwnerId"]
                     == owner_id and i["Snapshots/VolumeId"] not in data_d]

    data_active_snapshots = pd.DataFrame(data_active)
    data_inactive_snapshots = pd.DataFrame(data_inactive)

    data_active_snapshots.to_csv("active_volumes_central.csv", index=False)
    data_inactive_snapshots.to_csv("inactive_volumes_central.csv", index=False)


# Filter entries by date:
def filter_date(date_cvs, start_date_, owner_id):

    with open(f"{date_cvs}.csv", "r") as f:
        data = csv.DictReader(f)
        data = [i for i in data if time_(
            i["Snapshots/StartTime"], start_date_) and i["Snapshots/OwnerId"] == owner_id]

    data_snapshots = pd.DataFrame(data)
    # print(data_snapshots)
    data_snapshots.to_csv("date_formated_central.csv", index=False)


filter_active_inactive(owner_id=owner_id_, snapshots_csv=file_name)
filter_date(date_cvs=date_cvs, start_date_=start_date_time, owner_id=owner_id_)
