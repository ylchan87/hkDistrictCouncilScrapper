# hkDistrictCouncilScrapper

## About the data
The code scraps HK 18 District Council data from i.e.:

https://www.districtcouncils.gov.hk/central/tc_chi/welcome/welcome.html

Will scrap data under pages:
- dcCouncil (區議會大會)
- committee (委員會)
- work group (工作小組)

Data includes:
- Meetings
  - Agenda
  - Minutes
  - Docs presented in meeting
- Circulatory docs in between meetings


Data scraped on Dec2019 can be found at:

https://drive.google.com/open?id=14AYUkAWPKH5RzSl0bYx2R7qi8tsd2ajq

Link should live till Dec2020

## About the code
Uses:
- Trio + Asks for concurrent fetching from internet
- BeautifulSoup for parsing html

To run:
```
python scrapeOneDC.py
```

default output dir is ./data
