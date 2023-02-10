import requests
import base64
import os
import pandas as pd
from urllib.parse import urlparse
from urllib.parse import parse_qs
from dotenv import load_dotenv

load_dotenv()

path = r"PATH TO CSV DIR HERE"
ProjectID = os.getenv("PROJECTID")
AuthToken = os.getenv("AUTHTOKEN")
SpaceURL = os.getenv("SPACEURL")

auth_byte = f"{ProjectID}:{AuthToken}".encode("ascii")
base64_byte = base64.b64encode(auth_byte)
base64_auth = base64_byte.decode("ascii")
first_url = f"https://{SpaceURL}/api/voice/logs?page_size=1000"

print(ProjectID + AuthToken + SpaceURL)
print(base64_auth)


def create_csv(response, page_number):
    print(f"Parsing Page: {response['links']['self']}")

    df = pd.DataFrame(response['data'],
                      columns=('id', 'from', 'to', 'direction', 'status', 'duration', 'source', 'type', 'url',
                               'charge', 'created_at', 'charge_details'))
    if os.getcwd() != path:
        print(os.getcwd())
        os.chdir(path)
        df.to_csv(page_number + ".csv", index=False, encoding='utf-8', mode='a')
    else:
        df.to_csv(page_number + ".csv", index=False, encoding='utf-8', mode='a')


def request(url):
    url = url
    parsed_url = urlparse(url)
    query = parse_qs(parsed_url.query)
    print(query['page_size'])
    payload = {}
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Basic {base64_auth}'
    }

    response = requests.request("GET", url, headers=headers, data=payload).json()

    if "page_number" in query.keys():
        txt = "['']"
        create_csv(response, str(query['page_number']).strip(txt))
        page(response)
    else:
        print("0")
        create_csv(response, "0")
        page(response)


def page(response):
    if "next" in response['links'].keys():
        request(response['links']['next'])


request(first_url)
