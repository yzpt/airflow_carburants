import requests
from datetime import datetime
import zipfile
import xml.etree.ElementTree as ET
from google.cloud import storage
from google.cloud import bigquery
import logging
import os