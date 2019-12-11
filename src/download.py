import io
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET
from zipfile import ZipFile
import boto3
import rasterio
from rasterio.session import AWSSession
import numpy as np
np.seterr(divide='ignore', invalid='ignore')

import requests
from requests.auth import HTTPBasicAuth


URL_TEMPLATE = ("https://scihub.copernicus.eu/dhus/search?"
                "q=(%20footprint:%22Intersects(POLYGON((28.151779174804673%2049.721815766557825,31.967124938964833%2049.721815766557825,31.967124938964833%2051.293270512333294,28.151779174804673%2051.293270512333294,28.151779174804673%2049.721815766557825)))%22%20)%20"
                "AND%20(%20beginPosition:[{start}.000Z%20TO%20{end}.999Z]%20"
                "AND%20endPosition:[{start}.000Z%20TO%20{end}.999Z]%20)%20"
                "AND%20(%20%20(platformname:Sentinel-2%20AND%20filename:S2A_*%20AND%20producttype:S2MSI1C%20"
                "AND%20cloudcoverpercentage:[0%20TO%2010]))&offset=0&limit=25&sortedby=ingestiondate&order=desc")
XML_NAMESPACE = "{http://www.w3.org/2005/Atom}"

USERNAME = "username"
PASSWORD = "password"
AWS_ACCESS_KEY_ID = 'access_key'
AWS_SECRET_ACCESS_KEY = 'secret_access_key'
BUCKET_FOLDER = 'space-pipeline'
BUCKET_FOLDER_INPUT = 'input'
BUCKET_FOLDER_OUTPUT = 'output'

def upload(body, keyName):
    try:
        s3.put_object(
            Body=body,
            Bucket=BUCKET_FOLDER,
            Key=keyName)

    except Exception as exp:
        print('exp: ', exp)

def calculate_ndvi(band4, band8):
    RED = band4.read()
    NIR = band8.read()

    a = (NIR.astype(float) - RED.astype(float))
    b = (NIR + RED)

    ndvi = np.divide(a, b, out=np.zeros_like(a), where=b!=0)
    return ndvi

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

    session = boto3.session.Session(
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
        region_name='eu-west-1'
    )

    s3 = session.client('s3')

    keyName = f"{BUCKET_FOLDER_INPUT}/{start_str}_{end_str}.zip"
    upload(io.BytesIO(resp.content), keyName)

    zip = s3.get_object(Bucket=BUCKET_FOLDER, Key=keyName)['Body'].read()

    with ZipFile(io.BytesIO(zip)) as theZip:
        fileNames = theZip.namelist()
        for fileName in fileNames:
            if fileName.endswith('_B04.jp2'):
                upload(theZip.read(fileName), f"{BUCKET_FOLDER_OUTPUT}/{start_str}_{end_str}/band4.jp2")
            if fileName.endswith('_B08.jp2'):
                upload(theZip.read(fileName), f"{BUCKET_FOLDER_OUTPUT}/{start_str}_{end_str}/band8.jp2")

    url1 = f"s3://{BUCKET_FOLDER}/{BUCKET_FOLDER_OUTPUT}/{start_str}_{end_str}/band4.jp2"
    url2 = f"s3://{BUCKET_FOLDER}/{BUCKET_FOLDER_OUTPUT}/{start_str}_{end_str}/band8.jp2"

    with rasterio.env.Env(AWSSession(session)):
        with rasterio.open(url1) as band4, rasterio.open(url2) as band8:
            result = calculate_ndvi(band4, band8)
            print(result.min(), result.max())
            upload(result.tobytes(), f"{BUCKET_FOLDER_OUTPUT}/{start_str}_{end_str}.csv")
