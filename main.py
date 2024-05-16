import requests
import csv
import os
import time
import subprocess

# Function to fetch data from the API
def fetch_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

# Function to save data to a CSV file
def save_data_to_csv(data, directory, filename):
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    filepath = os.path.join(directory, filename)
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Timestamp", "Open", "High", "Low", "Close", "Volume"])
        for timestamp, values in data['Time Series (5min)'].items():
            writer.writerow([timestamp, values['1. open'], values['2. high'], values['3. low'], values['4. close'], values['5. volume']])
    
    return filepath

# Function to stage new data files using DVC
def stage_data_with_dvc(data_dir, filename):
    subprocess.run(["dvc", "add", os.path.join(data_dir, filename)])

# Function to commit changes to DVC
def commit_with_dvc():
    subprocess.run(["dvc", "commit", "-f"])

# Function to push changes to the remote storage using DVC
def push_to_remote_with_dvc():
    subprocess.run(["dvc", "push"])

# Function to commit changes to Git
def commit_with_git(message):
    subprocess.run(["git", "add", "."])
    subprocess.run(["git", "commit", "-m", message])

# Function to push changes to Git
def push_to_remote_with_git(branch):
    subprocess.run(["git", "push", "origin", branch])

# API URL and parameters
api_key = 'F0TLBDK5P51DDOVT'
symbol = 'IBM'
interval = '5min'
api_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={api_key}'

# Directory to save the data
save_directory = 'data'
filename = f'{symbol}_intraday_data.csv'

# Commit message
commit_message = "Update data"

# Git branch
git_branch = "master"

# Continuous data fetching
while True:
    data = fetch_data(api_url)
    print(data)
    if data:
        filepath = save_data_to_csv(data, save_directory, filename)
        stage_data_with_dvc(save_directory, filename)
        commit_with_dvc()
        push_to_remote_with_dvc()
        commit_with_git(commit_message)
        push_to_remote_with_git(git_branch)
        print(f"Data saved and updated in DVC repository.")
    
    # Fetch data every 5 minutes (300 seconds)
    time.sleep(300)