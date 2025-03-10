# %%
import os
import csv


# %%
def get_user_logon_data(user_id, dataset_path):
    logon_data = []
    with open(os.path.join(dataset_path, "logon.csv"), "r") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if row[2] == user_id:
                logon_data.append(row)
    return logon_data


# %%
def get_user_pc(logon_data):
    pc_dict = {}
    for row in logon_data:
        pc_dict[row[3]] = 1 + pc_dict.get(row[3], 0)
    user_pc = max(pc_dict, key=pc_dict.get)
    return user_pc


# %%
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd


def get_num_other_PC_per_week(user, user_pc, logon_data):
    weekly_pc_counts = defaultdict(set)  # Dictionary to store unique PCs per week
    all_weeks = set()  # Set to track all weeks where logons occurred

    for row in logon_data:
        logon_time = datetime.strptime(row[1], "%m/%d/%Y %H:%M:%S")  # Adjusted format
        week = logon_time.strftime("%Y-%W")  # Year-Week format
        all_weeks.add(week)  # Track all weeks

        if row[3] != user_pc:  # Check if PC is different from user's primary PC
            weekly_pc_counts[week].add(
                row[3]
            )  # Add PC to the week's set (unique values only)

    # Ensure all weeks are included, even with 0 count
    min_week = min(all_weeks)
    max_week = max(all_weeks)

    # Generate all weeks between min and max
    start_date = datetime.strptime(min_week + "-1", "%Y-%W-%w")
    end_date = datetime.strptime(max_week + "-1", "%Y-%W-%w")

    current_date = start_date
    complete_weeks = set()

    while current_date <= end_date:
        week_str = current_date.strftime("%Y-%W")
        complete_weeks.add(week_str)
        current_date += timedelta(days=7)

    # Ensure every week has a count (0 if no other PCs were accessed)
    weekly_counts = {
        week: len(weekly_pc_counts[week]) if week in weekly_pc_counts else 0
        for week in complete_weeks
    }

    # Convert to DataFrame
    output_list = [[user, week, count] for week, count in sorted(weekly_counts.items())]
    return pd.DataFrame(output_list, columns=["user", "week", "num_other_pc"])


# %%
logon_data = get_user_logon_data(
    "ACM1770", os.path.join("Insider threat dataset", "r5.2")
)
user_pc = get_user_pc(logon_data)
num_other_pc = get_num_other_PC_per_week("ACM1770", user_pc, logon_data)

# %%
print(num_other_pc)
