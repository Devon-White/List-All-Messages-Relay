import requests
import base64
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

ProjectID = os.getenv("PROJECTID")
AuthToken = os.getenv("AUTHTOKEN")
SpaceURL = os.getenv("SPACEURL")

auth_byte = f"{ProjectID}:{AuthToken}".encode("ascii")
base64_byte = base64.b64encode(auth_byte)
base64_auth = base64_byte.decode("ascii")
first_url = f"https://{SpaceURL}/api/voice/logs?page_size=1000"

def create_csv(response):
    print(f"Parsing Page: {response['links']['self']}")

    df = pd.DataFrame(response['data'],
                      columns=('id', 'from', 'to', 'direction', 'status', 'duration', 'source', 'type', 'url',
                               'charge', 'created_at', 'charge_details'))
    df.to_csv("logs.csv", index=False, encoding='utf-8', mode='a')


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
        with open("last_page", "w") as f:
            f.write(response['links']['self'])
        response = requests.request("GET", response['links']['next'], headers=headers, data=payload).json()
        create_csv(response)
    else:
        with open("last_page", "w") as f:
            f.write(f" Completed scan on:\n{response['links']['self']}")

request(first_url)
