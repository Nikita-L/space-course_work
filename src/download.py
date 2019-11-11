import io
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET
from zipfile import ZipFile
import boto3

import requests
from requests.auth import HTTPBasicAuth


URL_TEMPLATE = ("https://scihub.copernicus.eu/dhus/search?"
                "q=(%20footprint:%22Intersects(POLYGON((28.151779174804673%2049.721815766557825,31.967124938964833%2049.721815766557825,31.967124938964833%2051.293270512333294,28.151779174804673%2051.293270512333294,28.151779174804673%2049.721815766557825)))%22%20)%20"
                "AND%20(%20beginPosition:[{start}.000Z%20TO%20{end}.999Z]%20"
                "AND%20endPosition:[{start}.000Z%20TO%20{end}.999Z]%20)%20"
                "AND%20(%20%20(platformname:Sentinel-2%20AND%20filename:S2A_*%20AND%20producttype:S2MSI1C%20"
                "AND%20cloudcoverpercentage:[0%20TO%2010]))&offset=0&limit=25&sortedby=ingestiondate&order=desc")
XML_NAMESPACE = "{http://www.w3.org/2005/Atom}"

USERNAME = "ndranhovskyi"
PASSWORD = "147258369a"


def download_data():  # not used cause we need to unzip file in memory
    auth = HTTPBasicAuth(USERNAME, PASSWORD)

    end = datetime.now().replace(microsecond=0, second=0, minute=0, hour=0)
    start = end - timedelta(days=7)

    start_str = start.isoformat()
    end_str = end.isoformat()

    url = URL_TEMPLATE.format(start=start_str, end=end_str)

    resp = requests.get(url, auth=auth)

    xml = ET.fromstring(resp.content)

    files_link = xml.find(f"{XML_NAMESPACE}entry")[1].attrib["href"]

    resp = requests.get(files_link, auth=auth)

    with open('./data.zip', 'wb') as f:
        f.write(resp.content)


if __name__ == "__main__":
    auth = HTTPBasicAuth(USERNAME, PASSWORD)

    end = datetime.now().replace(microsecond=0, second=0, minute=0, hour=0)
    start = end - timedelta(days=7)

    start_str = start.isoformat()
    end_str = end.isoformat()

    url = URL_TEMPLATE.format(start=start_str, end=end_str)

    resp = requests.get(url, auth=auth)

    xml = ET.fromstring(resp.content)

    files_link = xml.find(f"{XML_NAMESPACE}entry")[1].attrib["href"]

    resp = requests.get(files_link, auth=auth)

#     z = ZipFile(io.BytesIO(resp.content))  # maybe we do not need to unzip to load to bucket
#     files = {name: z.read(name) for name in z.namelist()}

    s3 = boto3.client(
        's3',
        aws_access_key_id = 'AKIA6IKBVUDVJLVJZSHK',
        aws_secret_access_key = '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'
    )

    try:
        keyName = 'input/' + start_str + '_' + end_str

        s3.put_object(
            Body=io.BytesIO(resp.content),
            Bucket='space-pipeline',
            Key=keyName)

    except Exception as exp:
        print('exp: ', exp)
