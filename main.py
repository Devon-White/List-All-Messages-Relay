import csv
import requests
import base64
import os
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

ProjectID = os.getenv("PROJECTID")
AuthToken = os.getenv("AUTHTOKEN")
SpaceURL = os.getenv("SPACEURL")

auth_byte = f"{ProjectID}:{AuthToken}".encode("ascii")
base64_byte = base64.b64encode(auth_byte)
base64_auth = base64_byte.decode("ascii")
first_url = f"https://{SpaceURL}/api/voice/logs?created_after=2022-03-01&page_size=1000"

first_run = True


def create_csv(response):
    global first_run
    df = pd.DataFrame(response['data'],
                      columns=(
                          'id', 'from', 'to', 'direction', 'status', 'duration', 'source', 'type', 'url',
                          'charge', 'created_at', 'charge_details'))
    if first_run is True:
        df.to_csv(f"logs.csv", index=False, encoding='utf-8', mode='w+')
        first_run = False
    else:
        df.to_csv(f"logs.csv", index=False, encoding='utf-8', mode='a', header=False)


def organize_csv():
    [f.unlink() for f in Path("csv").glob("*") if f.is_file() and ".csv" in str(f)]
    with open("logs.csv", "r", encoding="UTF-8") as csvfile:
        csvreader = csv.DictReader(csvfile)

        for row in csvreader:
            date = row["created_at"]
            head, sep, tail = date.partition('T')
            new_head = str(head.partition("-")[0])
            new_tail = str(head.partition("-")[-1])
            final_head = new_tail.partition("-")[0]
            csv_name = f"{new_head}-{final_head}"
            create_final_csv(row=row, name=csv_name)

        print("We Done")


def create_final_csv(row, name):
    global first_run
    cols = ['id', 'from', 'to', 'direction', 'status', 'duration', 'source', 'type', 'url', 'charge', 'created_at',
            'charge_details']
    df = pd.DataFrame(data=row, index=cols)
    df.drop_duplicates(subset=None, inplace=True)
    path = f"csv/{name}.csv"

    if os.path.exists(path) is True:
        first_run = False

    else:
        first_run = True

    match first_run:
        case True:
            df.to_csv(path, index=False, encoding='utf-8', header=True)
        case False:
            df.to_csv(path, index=False, encoding='utf-8', mode='a', header=False)



def request(url):
    url = url
    payload = {}
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Basic {base64_auth}'
    }

    response = requests.request("GET", url, headers=headers, data=payload).json()
    create_csv(response)

    while "next" in response['links'].keys():
        print(f"Parsing Page: {response['links']['self']}")
        with open("last_page", "w") as f:
            f.write(response['links']['self'])
        response = requests.request("GET", response['links']['next'], headers=headers, data=payload).json()
        create_csv(response)
    else:
        with open("last_page", "w") as f:
            f.write(f" Completed scan on:\n{response['links']['self']}")


request(first_url)
organize_csv()
