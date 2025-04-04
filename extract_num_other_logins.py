# %%
import os
import csv
from collections import defaultdict
from datetime import datetime, timedelta, time
import pandas as pd


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


def get_after_hours_logons(
    logon_data, user, business_start=time(9, 0, 0), business_end=time(17, 0, 0)
):
    """
    Aggregates after-hours logons per week for a specified user.

    :param logon_data: List of logon events in the format [id, date, user, pc, activity]
    :param user: The specific user to filter logon events for.
    :param business_start: Datetime.time representing start of business hours.
    :param business_end: Datetime.time representing end of business hours.
    :return: DataFrame with ['user', 'week', 'after_hours_logons']
    """

    after_hours_counts = defaultdict(int)

    # Track all weeks for the user
    all_weeks = set()

    for row in logon_data:
        logon_id, timestamp, logon_user, pc, activity = row  # Unpack columns

        if (
            activity.lower() == "logon" and logon_user == user
        ):  # Only process logons for the specified user
            try:
                logon_time = datetime.strptime(timestamp, "%m/%d/%Y %H:%M:%S")
                logon_week = logon_time.strftime("%Y-%W")  # Ensure same format

                # Store this week to ensure it's included in results
                all_weeks.add(logon_week)

                # Extract only the time component
                logon_hour = logon_time.time()

                # Check if the logon occurred outside business hours
                if logon_hour < business_start or logon_hour >= business_end:
                    after_hours_counts[logon_week] += 1

            except ValueError:
                continue  # Skip invalid timestamps

    # Ensure all weeks in range are included (like `get_num_other_PC_per_week`)
    if all_weeks:
        min_week = min(all_weeks)
        max_week = max(all_weeks)

        # Generate all weeks in range
        start_date = datetime.strptime(min_week + "-1", "%Y-%W-%w")
        end_date = datetime.strptime(max_week + "-1", "%Y-%W-%w")

        current_date = start_date
        complete_weeks = set()

        while current_date <= end_date:
            week_str = current_date.strftime("%Y-%W")
            complete_weeks.add(week_str)
            current_date += timedelta(days=7)

        # Fill in missing weeks with 0
        after_hours_counts = {
            week: after_hours_counts.get(week, 0) for week in complete_weeks
        }

    # Convert to DataFrame
    result_data = [
        (user, week, after_hours_counts[week])
        for week in sorted(after_hours_counts.keys())
    ]
    after_hours_df = pd.DataFrame(
        result_data, columns=["user", "week", "after_hours_logons"]
    )

    return after_hours_df


# %%
def find_insider_answers_file(user, insider_root):
    """
    Recursively searches for the insider CSV file for the given user in the `insider_root` directory.

    :param user: The user ID (e.g., "CWW1120")
    :param insider_root: The root folder containing multiple r5.2-* subfolders.
    :return: The full path to the user's insider CSV file if found, else None.
    """
    for root, _, files in os.walk(insider_root):
        for file in files:
            if file.startswith(f"r5.2-") and file.endswith(
                f"-{user}.csv"
            ):  # Match user file format
                return os.path.join(root, file)  # Return full file path if found
    return None  # Return None if no file is found


def extract_weeks_from_csv(file_path):
    """
    Reads a CSV file using `csv.reader` and extracts unique weeks from the timestamps (3rd column).

    :param file_path: Path to the insider CSV file.
    :return: A set of detected `Year-Week` values.
    """
    insider_weeks = set()

    try:
        with open(file_path, mode="r", newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            for row in reader:
                if len(row) < 3:  # Ensure the timestamp column exists
                    continue
                try:
                    logon_time = datetime.strptime(
                        row[2], "%m/%d/%Y %H:%M:%S"
                    )  # Parse timestamp
                    week = logon_time.strftime("%Y-%W")  # Convert to Year-Week format
                    insider_weeks.add(week)
                except ValueError:
                    continue  # Skip rows with invalid timestamps
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

    return insider_weeks


def label_insider_weeks(df, user, insider_root):
    """
    Adds an 'insider' column to the DataFrame by checking if the user's week exists in their insider file.

    :param df: DataFrame containing ['user', 'week', 'num_other_pc']
    :param user: The user ID for whom the dataframe is filtered.
    :param insider_root: Path to the folder containing multiple r5.2-* subfolders.
    :return: DataFrame with an 'insider' column.
    """

    # Locate the user's insider file
    insider_file = find_insider_answers_file(user, insider_root)

    # If no insider file exists for the user, mark all weeks as 0 (not insider)
    if not insider_file:
        df["insider"] = 0
        return df

    # Extract weeks from the insider CSV file
    insider_weeks = extract_weeks_from_csv(insider_file)

    # Label insider weeks in the user's dataframe
    df["insider"] = df["week"].apply(lambda w: 1 if w in insider_weeks else 0)

    return df


# %%
user = "JBI1134"

logon_data = get_user_logon_data(user, os.path.join("Insider threat dataset", "r5.2"))
user_pc = get_user_pc(logon_data)
num_other_pc = get_num_other_PC_per_week(user, user_pc, logon_data)
after_hours_logons = get_after_hours_logons(logon_data, user)
labeled_df = label_insider_weeks(
    after_hours_logons, user, os.path.join("Insider threat dataset", "answers")
)
print(labeled_df[labeled_df])
print(after_hours_logons)
