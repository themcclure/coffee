"""
Scrape the roast info from the suppliers website, massage and cache it
"""

from bs4 import BeautifulSoup
import requests

page = requests.get('https://www.sweetmarias.com/green-coffee.html')
c = page.content
s = BeautifulSoup(c, 'html.parser')

