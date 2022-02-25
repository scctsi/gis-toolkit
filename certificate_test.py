import ssl
from urllib import request
from urllib.request import urlopen
import certifi
import urllib
import certifi

temp = urlopen('https://google.com/',context=ssl.create_default_context(cafile=certifi.where()))

# print(certifi.where())
print(temp)