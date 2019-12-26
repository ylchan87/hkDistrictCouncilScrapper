from bs4 import BeautifulSoup
from tableExtractor import parseTable
import trio
import asks
import re
import os

dryRun = False # if true, don't actually dl files
storePath = "./data"
saveRecordings = False
dlLimit = trio.CapacityLimiter(8)

asks.init('trio')
session = asks.Session(connections=8)

#========
#utils
#========

datePattern = re.compile("(?P<y>20[0-9]{2}).?(?P<m>[01]?[0-9]).?(?P<d>[0123]?[0-9])")

def formGoodFileName(s):
    s = s.strip()
    s = s.replace("/", "_")
    for c in ["\n","\t"," "]:
        s = s.replace(c,'')
    for c in ["*","+","?",":","\"","'","|"]:
        s = s.replace(c,'_')
    return s

def splitUrl(url):
    if "://" in url:
        protocol, addr = url.split("://")
    else:
        protocol, addr = None, url
    tmp = addr.split("/")
    base = tmp[0]
    head = "/".join(tmp[1:-1])
    tail = tmp[-1]
    if protocol: base = protocol + "://" + base
    return base,head,tail

async def fetchPage(url):
    async with dlLimit:
        print("Start fetchPage: ", url)
        response = await session.get(url)
        print("Done fetchPage: ", url, len(response.content))
    return response

callHist = {}
async def downloadFile(fileName, url):
    assert fileName not in callHist, 'Dup call of dlfile?'
    callHist[fileName] = url

    if os.path.exists(fileName):
        print ("skip already exist ", fileName)
        return True

    if dryRun:
        print("To dl : ", url)
        print("As : ", fileName)
        return True

    dirname = os.path.dirname(fileName)
    basename = os.path.basename(fileName)
    head, ext = os.path.splitext(basename)
    if len(head.encode())>240: #max filename = 255byte in linux ext4
        print("Name too long : ", fileName)
        while len(head.encode())>240:
            head = head[:-2] + "_"
        fileName = dirname + "/" + head + ext
        print("Trunc name    : ", fileName)

    tmpName = fileName + '_tmp'    
    dirName = os.path.dirname(fileName)
    if not os.path.exists(dirName):
        os.makedirs(dirName)

    async with dlLimit:
        print("Start downloadFile: ", url)
        try:
            if ext in [".html", ".htm"]:
                r = await session.get(url, retries=3)
                async with await trio.open_file(tmpName, 'wb') as out_file:
                    await out_file.write(r.content)
            else:
                r = await session.get(url, stream=True, retries=3)
                async with await trio.open_file(tmpName, 'wb') as out_file:
                    async with r.body:
                        async for bytechunk in r.body:
                            await out_file.write(bytechunk)
        except Exception as e:
            print(e)
            print("Error invalid link?: ", url)
            print("fileName : ", fileName)
            return False

    os.rename(tmpName, fileName)
    print("Done downloadFile: ", url)
    print("Saved as : ", fileName)
    return True

#==========
# parsers
#==========
def extractLinks(inSoup, baseUrl):
    base,head,tail = splitUrl(baseUrl)

    linkTags = inSoup.find_all('a')
    links = []
    for linkTag in linkTags:
        link = linkTag.attrs['href']
        if 'rel' in linkTag.attrs and 'external' in linkTag.attrs['rel']:
            link = base + "/" + link
        else:
            link = base + "/" + head + "/" + link
        links.append( (link, linkTag.text) )
    return links

def extractCollapsables(soup):
    """
    extract the collapsable divs in
    https://www.districtcouncils.gov.hk/central/tc_chi/meetings/working_group/workgroup_meetings.php
    """
    pattern = re.compile("javascript:ReverseDisplay\('(?P<target>.+)'\)")
    cs = soup.find_all('a', href=pattern)
    output = []
    for c in cs:
        name = c.text.strip()
        target = re.search(pattern, c.attrs['href'])
        target = target.groupdict()['target']
        targetElem = soup.find(id=target)
        output.append( (name, targetElem) )
    return output

def amendTableDates(rows):
    """
    amend col1 (date) of the table
    """
    date = '20160101'
    interMeetingCounter = 1
    for i in range(len(rows)):
        match = re.search(datePattern, rows[i][1].text)
        if match:
            date = "%d%02d%02d" % tuple(map(int,match.groups())) #give yyyymmdd
            rows[i][1] = date
            interMeetingCounter = 1
        else:
            rows[i][1] = date + "_interMeeting%d" % interMeetingCounter
            interMeetingCounter +=1
    return rows

async def parse7ColTable(breadCrumb, url, rows):
    """
    Parse tables in
    https://www.districtcouncils.gov.hk/central/tc_chi/meetings/dcmeetings/dc_meetings.php
    https://www.districtcouncils.gov.hk/central/tc_chi/meetings/committees/committee_meetings.php

    The 7cols are
    會議 	日期 	時間 	會議議程 	會議記錄 	會議錄音 	討論文件
    """
    rows  = amendTableDates(rows)
    async with trio.open_nursery() as nursery:
        for row in rows:
            key = row[1]
            if "interMeeting" in key:
                for link in extractLinks(row[4], url):
                    nursery.start_soon(parseDocSets, breadCrumb+[key], link[0])
            else:
                for link in extractLinks(row[3], url):
                    base,head,tail = splitUrl(link[0])
                    filename = "/".join([storePath] + breadCrumb + [key,'agenda',tail])
                    nursery.start_soon(downloadFile, filename, link[0])

                for link in extractLinks(row[4], url):
                    base,head,tail = splitUrl(link[0])
                    filename = "/".join([storePath] + breadCrumb + [key,'minutes',tail])
                    nursery.start_soon(downloadFile, filename, link[0])

                if saveRecordings:
                    for link in extractLinks(row[5], url):
                        nursery.start_soon(parseRecordings, breadCrumb+[key], link[0])
                
                for link in extractLinks(row[6], url):
                    nursery.start_soon(parseDocSets, breadCrumb+[key], link[0])

async def parse6ColTable(breadCrumb, url, rows):
    """
    Parse tables in
    https://www.districtcouncils.gov.hk/central/tc_chi/meetings/working_group/workgroup_meetings.php

    The 6cols are
    會議 	日期 	時間 	會議議程 	會議記錄 	討論文件
    """
    rows  = amendTableDates(rows)
    async with trio.open_nursery() as nursery:
        for row in rows:
            key = row[1]
            if "interMeeting" in key:
                for link in extractLinks(row[4], url):
                    nursery.start_soon(parseDocSets, breadCrumb+[key], link[0])
            else:
                for link in extractLinks(row[3], url):
                    base,head,tail = splitUrl(link[0])
                    filename = "/".join([storePath] + breadCrumb + [key,'agenda',tail])
                    nursery.start_soon(downloadFile, filename, link[0])

                for link in extractLinks(row[4], url):
                    base,head,tail = splitUrl(link[0])
                    filename = "/".join([storePath] + breadCrumb + [key,'minutes',tail])
                    nursery.start_soon(downloadFile, filename, link[0])

                for link in extractLinks(row[5], url):
                    nursery.start_soon(parseDocSets, breadCrumb+[key], link[0])

async def parseCouncil(dc):
    url = "https://www.districtcouncils.gov.hk/%s/tc_chi/meetings/dcmeetings/dc_meetings.php"%dc
    breadCrumb = [dc, 'council']
    ret = await fetchPage(url)
    soup = BeautifulSoup(ret.content, 'lxml')
    tables = soup.find_all(id=re.compile('table20[0-9]{2}'))
    tables.sort(key=lambda t:t.attrs['id'])    
    tables = [parseTable(t) for t in tables]

    rows = []
    for t in tables: rows+=t[1:] #first row in table is header    
    await parse7ColTable(breadCrumb, url, rows)

async def parseCommittee(dc):
    url = "https://www.districtcouncils.gov.hk/%s/tc_chi/meetings/committees/committee_meetings.php"%dc
    breadCrumb = [dc, 'committee']
    ret = await fetchPage(url)
    soup = BeautifulSoup(ret.content, 'lxml')

    async with trio.open_nursery() as nursery:
        cs = extractCollapsables(soup)
        for committee,elem in cs:
            tables = elem.find_all('table')
            tables = [parseTable(t) for t in tables]
            tables.reverse() #we assume the tables arranges from new to old in website

            rows = []
            for t in tables: rows+=t[1:] #first row in table is header

            committee = formGoodFileName(committee)
            nursery.start_soon(parse7ColTable, breadCrumb + [committee], url, rows)

async def parseWorkGroup(dc):
    url = "https://www.districtcouncils.gov.hk/%s/tc_chi/meetings/working_group/workgroup_meetings.php"%dc
    breadCrumb = [dc, 'workgroup']
    ret = await fetchPage(url)
    soup = BeautifulSoup(ret.content, 'lxml')

    async with trio.open_nursery() as nursery:
        cs = extractCollapsables(soup)
        for workgroup,elem in cs:
            workgroup = formGoodFileName(workgroup)

            tables = elem.find_all('table')
            tables = [parseTable(t) for t in tables]
            tables.reverse() #we assume the tables arranges from new to old in website

            rows = []
            for t in tables: rows+=t[1:] #first row in table is header
            
            
            ncols = len(rows[0])
            if ncols==6:
                func = parse6ColTable 
            elif ncols==7:
                func = parse7ColTable 
            else:
                print("Error : Unexpected col num in table ", workgroup, url)
                continue
            
            nursery.start_soon(func, breadCrumb + [workgroup], url, rows)
    
async def parseDocSets(breadCrumb, url):
    """
    parse page like:
    https://www.districtcouncils.gov.hk/central/tc_chi/meetings/dcmeetings/dc_meetings_doc.php?year=2019&meeting_id=circulate#16690
    """
    breadCrumb += ['docSets']

    ret = await fetchPage(url)
    soup = BeautifulSoup(ret.content, 'lxml')

    #find anchor in url, if found go to 1st table after anchor
    match = re.search('#(?P<anchor>.*)$', url)
    if match: 
        anchor = match.groupdict()['anchor']
        anchorElem = soup.find(attrs={"name": anchor})
        if not anchorElem:
            print('Error parseDocSets, anchor not found ', url)
            return
        table = anchorElem.find_next('table')
        if not table:
            print('Error parseDocSets, table not found ', url)
            return
    else:
        tables = soup.find_all('table')
        if len(tables)!=1:
            print("Error parseDocSets, table count %s not expected "%len(tables), url)
            return
        table = tables[0]

    rows = parseTable(table)[1:]
    async with trio.open_nursery() as nursery:
        for idx, r in enumerate(rows):
            docId = formGoodFileName(r[0].text)
            if not docId: docId = 'NoID'
            docId = "%02d_"%(idx+1) + docId

            def dlDocs(breadCrumb, links):
                nameUseCount = {}
                for link in links:
                    _,ext = os.path.splitext(link[0])
                    filename = formGoodFileName(link[1]) + ext
                    if filename not in nameUseCount:
                        nameUseCount[filename] = 1
                    else:
                        nameUseCount[filename] += 1
                        filename = formGoodFileName(link[1]) + \
                                   "_" + str(nameUseCount[filename]) + ext
                    filename = "/".join([storePath] + breadCrumb + [filename])
                    nursery.start_soon(downloadFile, filename, link[0])

            mainDocLinks = extractLinks(r[1], url)
            dlDocs(breadCrumb + [docId, 'mainDocs'], mainDocLinks)

            annexDocLinks = extractLinks(r[2], url)
            dlDocs(breadCrumb + [docId, 'annexDocs'], annexDocLinks)

            remarkDocLinks = extractLinks(r[3], url)
            dlDocs(breadCrumb + [docId, 'remarkDocs'], remarkDocLinks)
            
async def parseRecordings(breadCrumb, url):
    """
    parse page like:
    https://www.districtcouncils.gov.hk/central/tc_chi/meetings/dcmeetings/dc_meetings_audio.php?meeting_id=13783
    """
    breadCrumb += ['recordings']
    ret = await fetchPage(url)
    soup = BeautifulSoup(ret.content, 'lxml')
    tables = soup.find_all('table')
    if len(tables)!=2:
        print("Error parseRecordings", url)
        return
    rows = parseTable(tables[1])[1:]
    async with trio.open_nursery() as nursery:
        for idx, r in enumerate(rows):
            links = extractLinks(r[1], url)
            if len(links)>0:
                _,ext = os.path.splitext(links[0][0])
                filename = "%02d_"%(idx+1) + formGoodFileName(links[0][1]) + ext
                filename = "/".join([storePath] + breadCrumb + [filename])
                nursery.start_soon(downloadFile, filename, links[0][0])

if __name__=="__main__":
    # dcs = ['wc']
    dcs = [
        "central",
        "wc",
        "south",
        "east",
        "kt",
        "ssp",
        "ytm",
        "wts",
        "kc",
        "island",
        "tw",
        "yl",
        "north",
        "st",
        "sk",
        "kwt",
        "tp",
        "tm",
    ]
    
    for dc in dcs[5:]:
        trio.run(parseCouncil, dc)
        trio.run(parseCommittee, dc)
        trio.run(parseWorkGroup, dc)
