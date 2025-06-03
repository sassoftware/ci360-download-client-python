#! python3
'''
Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
Created on Nov 3, 2017
last update on Jun 3, 2025
Version 0.93
@authors: Mathias Bouten, Shashikant Deore, Will Flowers
NOTE: This Discover Download script can be used to better understand 
      the download API and as a base for you to interact with 
      the API to download collected customer data. This script is
      not officially supported by SAS.
'''

import requests, base64
import json, jwt, gzip, csv, codecs
import os, sys, argparse, time
from argparse import RawTextHelpFormatter
from datetime import datetime, timedelta
from tqdm import tqdm
from urllib.parse import urlsplit
import pandas
import backoff

#version and update date
version = 'V0.93'
updateDate = '05 June 2025'
downloadClient = 'ci360pythonV0.93'

# default values
limit     = "20" 
csvflag   = 'no'
delimiter = '|'
csvheader = 'yes'
csvquote = 'no'
csvquotechar = '"'
append = 'no'
sohDelimiter = "\x01"
progressbar = 'no'
allhourstatus='true'
#subhourrange="60"
#autoreset = 'yes'
dayOffset = "60"
max_retry_attempts = 4

# folders
base_dir = os.path.dirname(os.path.realpath(__file__)) # dir of Python script
dir_log    = f'{base_dir}/log/'
dir_csv    = f'{base_dir}/dscwh/'
dir_zip    = f'{base_dir}/dscdonl/'
dir_config = f'{base_dir}/dsccnfg/'
dir_extr   = f'{base_dir}/dscextr/'
dir_sql    = f'{base_dir}/sql/'

# global variables
querystring = {}
resetQueryString = {} 
gSql = ''
gSqlInsert = ''
cleanFiles = 'yes'
responseText =''

##### functions #####

# function to do the version specific changes to the already created objects if any
def versionUpdate():

    # Change#1 : update download_history_detail.csv & download_history_detail.csv to add dataRangeProcessingStatus column
    ColumnDelimiter=';'
    for martNm in ('detail','dbtReport'):
        historyFile='download_history_' + martNm 
        historyFilePath = dir_config + historyFile + '.csv'
        if fileExists(historyFilePath):
            # read first line and see if it contains the required number of columns
            with open(historyFilePath) as f:
                historyHeader = f.readlines()[1]
                f.close
            
            countofDelmHeader = historyHeader.count(ColumnDelimiter)
            if not ( countofDelmHeader == 3 ) :

                backupFile=logFileNmtimeStamped(historyFile,fmt='{filename}_%Y%m%dT%H%M%S')
                backupFilePath=dir_config + backupFile + '.csv'

                logger('  backing up ' + historyFile + ' as ' + backupFile , 'n', True)
                # back up the existing file 
                with open(historyFilePath) as hf:
                    with open(backupFilePath, "w") as bkf:
                        for line in hf:
                            bkf.write(line)
                
                logger('  updating ' + historyFile + ' to new version' , 'n', True)
                # from the backup re-write existing history file with required number of columns in header and in data lines
                headerline = 'dataRangeStart;dataRangeEnd;download_dttm;dataRangeProcessingStatus' + "\n"
                rows = 0
                with open(backupFilePath) as bkf:
                    with open(historyFilePath, "w") as hf:
                        for line in bkf:
                            rows = rows+1                        
                            if (rows == 1):       
                                hf.write(headerline)
                            else:
                                # append delimiter to line
                                newline = line.replace('\n','') + ColumnDelimiter + "\n"
                                hf.write(newline)

def getNextDataRangeStart():
    # set  nextDataRangeStart = lastDataRangeEnd + 1 ms 
    historyFile = dir_config + 'download_history_' + martName + '.csv'
    
    try:
        with open(historyFile) as f:
            last = f.readlines()[-1]
    
        lastDataRangeEnd = last.split(';',3)[1]
        adjustedTime = datetime.strptime(lastDataRangeEnd, '%Y-%m-%dT%H:%M:%S.%fZ')
        adjustedTime += pandas.to_timedelta(1, unit='ms')
        adjustedTimeStr = adjustedTime.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        return adjustedTimeStr
    except FileNotFoundError as e:
        print('\n', e)
        raise SystemExit('\nFATAL: When you use the -d parameter, a history file must exist.')

def logFileNmtimeStamped(filename, fmt='{filename}_%Y%m%dT%H%M%S.log'):
    #return datetime.datetime.now().strftime(fmt).format(filename=filename)
    return datetime.now().strftime(fmt).format(filename=filename)

def readConfig(configFile):
    keys = {}
    seperator = '='
    with open(configFile) as f:
        for line in f:
            if line.startswith('#') == False and seperator in line: 
                # Find the name and value by splitting the string
                name, value = line.split(seperator, 1)
                # Assign key value pair to dict
                keys[name.strip()] = value.strip()
    return keys

def printDownloadDetails(json_data):

    #if there is a message attribute in json response log the message
    if json_data.get('message') :
        logger('WARNING:' + str(json_data['message']),'n' )

    if martName == 'identity' or martName == 'snapshot':
        logger('  download of dataMart snapshot','n')
    else:
        TotalDownloadPackages=json_data['count']
        CurrentPageDownloadPackages=len(json_data['items'])
        logger('  download of dataMart ' + martName \
            + ' - total downloads ' + str(TotalDownloadPackages) + ' package(s)' \
            + ' - downloading ' + str(CurrentPageDownloadPackages) + ' package(s)','n')

    # logging
    logger('  request URL: ' + url, 'n', False)
    logger('  config: ' + json.dumps(config), 'n', False)
    logger('  arguments: ' + str(args),'n',False)

def printResetDetails(json_data):
    # print details of the json response like number of total reset packages & number of reset package on current page
    TotalResetPackages=json_data['count']
    CurrentPageResetPackages=len(json_data['items'])
    logger('  reset of dataMart ' + martName \
        + ' - total resets ' + str(TotalResetPackages) + ' package(s)' \
        + ' -  current page resets ' + str(CurrentPageResetPackages) + ' package(s)','n')

    # logging
    logger('  request URL: ' + url, 'n', False)
    logger('  config: ' + json.dumps(config), 'n', False)
    logger('  arguments: ' + str(args),'n',False)

    # extract the query parameters from url & print ?
    

def createDiscoverAPIUrl(config):
    baseUrl = config['baseUrl']
    if martName == 'detail':
        url = baseUrl + 'detail/partitionedData'
    elif martName == 'dbtReport':
        url = baseUrl + 'dbtReport'
    elif martName == 'identity' or martName =='snapshot':
        url = baseUrl + 'detail/nonPartitionedData'
    else:
        print('Error: wrong martName ')
        sys.exit()
    return url

def logger(line, action, console=True):
    #logfile = dir_log + 'discover_download.log'
    logfilePath = dir_log + logfile
    nowDttm = str(datetime.now())
    with open(logfilePath,'a') as log:
        if action == 'n':
            log.write('\n' + nowDttm + ': ' + line)
            if console == True:
                print('\n' + line, sep='', end='', flush=True)
        elif action == 'a':
            log.write(line)
            if console == True:
                print(line, sep='', end='', flush=True)

def log_retry_attempt(details):
    #print ("Backing off {wait:0.1f} seconds afters {tries} tries "
    #       "calling function {target} with args {args} and kwargs "
    #       "{kwargs}".format(**details))
    logger('  Caught retryable error after ' + str(details["tries"]) + ' tries. Waiting  ' + str(round(details["wait"],2)) + ' more seconds then retrying...', 'n', True)
    logger('  responseText: ' + str(responseText) ,'n', True)

def after_all_retries(details):
    _, exception, _ = sys.exc_info()
    logger('  error executing ' + str(details["target"]),'n',True)
    logger('  exception ' + str(exception) ,'n', True)
    sys.exit(1)
    
@backoff.on_exception(
    backoff.expo
    ,requests.exceptions.RequestException
    ,max_tries=max_retry_attempts
    ,factor=5
    ,on_backoff=log_retry_attempt
    ,on_giveup=after_all_retries
    #,jitter=backoff.full_jitter
    #,giveup=lambda e: e.response is not None and e.response.status_code < 500
    #,max_time=30
    )
def getResetUrls(url):
    global responseText
    resetQueryString["agentName"] = config['agentName']
    if martName == 'dbtReport' :
        resetQueryString["martType"] = 'dbt-report'
    else:
        resetQueryString["martType"] = martName
    
    resetQueryString["dayOffset"] = dayOffset

    getURLs_start = time.time() # track get download URL request time
    
    logger('  send reset request to Discover API with querystring: ','n')
    logger('    ' + json.dumps(resetQueryString),'n')
    response = requests.request("GET", url, headers=headers, params=resetQueryString)
    # to test retry meachanism force the response status code 
    # response.status_code=409
    #print_response(response)
    responseText=response.text
    response.raise_for_status()
            
    getURLs_end = time.time() # track get download URL request time
    getURLs_duration = round((getURLs_end - getURLs_start),2)
    logger('  getResetUrls request duration: ' + str(getURLs_duration) + ' seconds','n')

    json_data = json.loads(response.text)
    
    response_file = dir_config + 'ResetResponse.json'
    with open(response_file, "w") as f:
            f.write(json.dumps(json_data, indent=4, sort_keys=True))
    
    if 'error' in json_data:
        print('\n Error: ' + json_data['error'] + " - " + json_data['message'])
        print('\n Check connection details! \n')
        sys.exit()
            
    return json_data

# Function to log the request information to console if required
def print_request(req):
    print('HTTP/1.1 {method} {url}\n{headers}\n\n{body}'.format(
        method=req.method,
        url=req.url,
        headers='\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        body=req.body,
    ))

def print_response(res):
    print('HTTP/1.1 {status_code}\n{headers}\n\n{body}'.format(
        status_code=res.status_code,
        headers='\n'.join('{}: {}'.format(k, v) for k, v in res.headers.items()),
        body=res.content,
    ))
    Response_info= 'HTTP/1.1 {status_code}\n{headers}\n\n{body}'.format(status_code=res.status_code, headers='\n'.join('{}: {}'.format(k, v) for k, v in res.headers.items()),body=res.content)
    logger(Response_info, 'n')  

@backoff.on_exception(backoff.expo,requests.exceptions.RequestException,max_tries=max_retry_attempts,factor=5,on_backoff=log_retry_attempt,on_giveup=after_all_retries)
def getDownloadUrls(url):
    global responseText
    querystring["limit"] = limit
    querystring["agentName"] = config['agentName']
    getURLs_start = time.time() # track get download URL request time
    
    logger('  send download request to Discover API with querystring: ','n')
    logger('    ' + json.dumps(querystring),'n')
    response = requests.request("GET", url, headers=headers, params=querystring)
    # to test retry meachanism force the response status code 
    # response.status_code=409
    responseText=response.text
    
    response.raise_for_status()

            
    getURLs_end = time.time() # track get download URL request time
    getURLs_duration = round((getURLs_end - getURLs_start),2)
    logger('  getDownloadUrls request duration: ' + str(getURLs_duration) + ' seconds','n')

    json_data = json.loads(response.text)
    
    response_file = dir_config + 'response.json'
    with open(response_file, "w") as f:
            f.write(json.dumps(json_data, indent=4, sort_keys=True))
    
    if 'error' in json_data:
        print('\n Error: ' + json_data['error'] + " - " + json_data['message'])
        print('\n Check connection details! \n')
        sys.exit()
            
    return json_data

def createDiscoverResetAPIUrl(config):
    baseUrl = config['baseUrl']
    if martName == 'detail':
        #url = baseUrl + 'partitionedData/resets'
        url = baseUrl + 'partitionedData/resets'
    elif martName == 'dbtReport':
        #url = baseUrl + 'dbtReport/resets'
        url = baseUrl + 'partitionedData/resets'        
    else:
        print('Error: wrong martName ')
        sys.exit()
    return url

def createDiscoverAPIUrlFromHref(config,href):
    # function to generate the reset API 
    baseUrl = config['baseUrl']
    baseUrlHost = "{0.scheme}://{0.netloc}".format(urlsplit(baseUrl))
    url= baseUrlHost +  href
    return url

@backoff.on_exception(backoff.expo,requests.exceptions.RequestException,max_tries=max_retry_attempts,factor=5,on_backoff=log_retry_attempt,on_giveup=after_all_retries)
def getSchema( url, tablename ):
    global responseText
    global gSql 
    global gSqlInsert
    r = requests.get(url)
    # to test retry meachanism force the response status code 
    # r.status_code=409
    responseText=r.text
    r.raise_for_status()

    json_meta = json.loads(r.text)
    columnHeader = ''
    sqlTable  = 'create table ' + tablename + '('
    sqlInsert = 'insert into ' + tablename + ' values ('
    sqlColumn = ''
    sqlInsertColumn = ''

    for item in json_meta:
        meta_table = item['table_name']       
        if tablename.lower() == meta_table.lower():
            column = item['column_name']
            columnType = item['column_type']
            sqlColumn = sqlColumn + '\n  ' + column + ' ' + columnType + ', '
            sqlInsertColumn = sqlInsertColumn + '%s,'
            columnHeader = columnHeader + column + delimiter  
    
    #finish create table statement    
    gSql = gSql + sqlTable + sqlColumn[:-2] + ');\n\n'   
    gSqlInsert = sqlInsert + sqlInsertColumn[:-1] + ')'
        
    #remove last delimiter and return line 
    return columnHeader[:-len(delimiter)]

@backoff.on_exception(backoff.expo,requests.exceptions.RequestException,max_tries=max_retry_attempts,factor=5,on_backoff=log_retry_attempt,on_giveup=after_all_retries)
def downloadWithProgress( url, outputfile, writeType):
    global responseText
    r = requests.get(url, stream=True)
    # to test retry meachanism force the response status code 
    # r.status_code=409
    #responseText=r.text
    r.raise_for_status()
    # Total size in bytes.
    file_size = int(r.headers.get('content-length', 0))
    block_size = 1024
    wrote = 0 
    with open(outputfile, writeType) as f:
        #for data in tqdm(iterable = r.iter_content(block_size), total= file_size/block_size , unit='KB', unit_scale=True, leave=True):
        for i in tqdm(range(file_size), ncols = 100, unit='KB'):
            data = r.raw.read(block_size) # read content block in bytes
            wrote = wrote + len(data)     # update actual number of written bytes
            i = i + block_size            # update iterator to continue loop from right point
            f.write(data)                 # write data to file
            f.flush()                     
    if file_size != 0 and wrote != file_size:
        print("ERROR, something went wrong during download - try again") 

@backoff.on_exception(backoff.expo,requests.exceptions.RequestException,max_tries=max_retry_attempts,factor=5,on_backoff=log_retry_attempt,on_giveup=after_all_retries)
def download( url, outputfile, writeType):
    global responseText
    r = requests.get(url)
    # to test retry meachanism force the response status code 
    #r.status_code=409
    #responseText=r.text
    r.raise_for_status()
    #with open(outputfile, writeType) as f:
    #    f.write(r.content)                 # write data to file

    # Retry for PermissionError in the open statement
    retry_attempts = 5
    for attempt in range(retry_attempts):
        try:
            with open(outputfile, writeType) as f:
                f.write(r.content)  # Write data to file
            break  # If the operation succeeds, exit the loop
        except PermissionError:
            if attempt < retry_attempts - 1:  # Don't sleep after the last attempt
                print(f"PermissionError occurred. Retry {attempt + 1}/{retry_attempts} after 1 second.")
                time.sleep(2)  # Wait for 1 second before retrying
            else:
                raise  # If it fails after the max retries, raise the exception

def unzipFile(in_file,out_file,in_delimiter,out_delimiter,header):
    #read file line by line and write columns as per schema 
    error = 0
    errorMsg = ''

    with gzip.open(in_file, "rb") as in_f, \
        open(out_file, "wb") as out_f:

        # go line by line and make changes in line to match header columns and data columns
        rows = 0
        for line in in_f:
            line2=str(line, 'utf-8')
            rows = rows+1
            # when its first row check if number of header cols are different from number of data columns
            if rows == 1:
                countofDelmHeader = header.count(out_delimiter)
                countofDelmData = str(line2).count(in_delimiter)
        
            # when schema is old but datafile is newer version then remove the extra columns
            if countofDelmHeader < countofDelmData :
                #split the fields into list
                fields = line2.split(in_delimiter)
                #limit the list to required number of columns as per header
                #join fileds and create a line string again 
                line2=in_delimiter.join(fields[0:countofDelmHeader + 1])  + "\n"
            # when schema is newer but datafile is older version then add the extra empty columns
            elif countofDelmHeader > countofDelmData :
                # remove the existing newline char ..add the empty columns and in the end of line add the new line char
                line2 = line2.replace('\n','') + in_delimiter*(countofDelmHeader - countofDelmData) + "\n"

            try:
                #out_f.write(line2.replace(in_delimiter, in_delimiter).encode())
                out_f.write(line2.encode())
            except (UnicodeEncodeError) as e:
                error = error+1
                errorMsg = errorMsg +'\nerror in row: ' + str(rows) + ' - ' + str(e)

        logger("...unzipped file with " + str(rows) + " rows - errors: " + str(error),'a')

def createCSV(in_file, out_file, in_delimiter, out_delimiter, header):
    #read unzipped file line by line and replace delimiter
    error = 0
    errorMsg = ''
    with codecs.open(in_file, 'r','utf-8') as in_f, \
         codecs.open(out_file, 'w','utf-8') as out_f:
             
        # create reader based on existing delimiter
        csvReader = csv.reader(in_f, delimiter=in_delimiter)
        
        # set CSV field quoting based on parameter
        if csvquote == 'yes':
            csvWriter = csv.writer(out_f, delimiter=out_delimiter, lineterminator='\n', quoting=csv.QUOTE_ALL, quotechar=csvquotechar)
        else:
            csvWriter = csv.writer(out_f, delimiter=out_delimiter, lineterminator='\n')

        # print column header line in csv file if flag is yes
        if csvheader == 'yes':
            out_f.write(header+"\n")

        # go line by line and output to CSV with new delimiter
        rows = 0
        for line in csvReader:
            rows = rows+1
            try:
                csvWriter.writerow(line)
            except (UnicodeEncodeError) as e:
                error = error+1
                errorMsg = errorMsg +'\nerror in row: ' + str(rows) + ' - ' + str(e)
        
        
        # for line in in_f:
        #     rows = rows+1
        #     try:
        #         out_f.write(line.replace(in_delimiter, out_delimiter))
        #     except (UnicodeEncodeError) as e:
        #         error = error+1
        #         errorMsg = errorMsg +'\nerror in row: ' + str(rows) + ' - ' + str(e)

    logger("...CSV created with " + str(rows) + " rows - errors: " + str(error),'a')
    
    if error != 0:
        logError(in_file, errorMsg)
        logger(" Error during csv creation process - see separate log file", 'n')

    if cleanFiles == 'yes':
        os.remove(in_file)

def createSingleTableFiles( entity, schemaUrl ):
    name = entity['entityName']
    tablefile = dir_csv+name+'.csv'
    if not os.path.exists(tablefile):
        header = getSchema(schemaUrl, name)
        with open(tablefile,'w') as f:
            f.write(header+"\n")

def appendCSV(in_file, out_file, in_delimiter, out_delimiter):
    #read unzipped file line by line and replace delimiter
    error = 0
    errorMsg = ''
    with codecs.open(in_file, 'r','utf-8') as in_f, \
         codecs.open(out_file, 'a','utf-8') as out_f:
        # go line by line and replace delimiter
        rows = 0
        for line in in_f:
            rows = rows+1
            try:
                out_f.write(line.replace(in_delimiter, out_delimiter))
            except (UnicodeEncodeError) as e:
                error = error+1
                errorMsg = errorMsg +'\nerror in row: ' + str(rows) + ' - ' + str(e)

    logger("...appended " + str(rows) + " rows - errors: " + str(error),'a')
    
    if error != 0:
        logError(in_file, errorMsg)
        logger(" Error during append process - see separate log file", 'n')
    
    if cleanFiles == 'yes':
        os.remove(in_file)

def logError(file, message):
    errorLog = dir_log + 'error_' + file + '.log'
    with open(errorLog, 'a') as f:
        f.write(message)
       
def downloadEntity( entity, schemaUrl, prefix ):
    name = entity['entityName']
    header = getSchema(schemaUrl, name)
    zippedFile = dir_extr+prefix+name+'.gz'
    unzippedFile = dir_zip+prefix+name+'.soh'
    csvFile = dir_csv+prefix+name+'.csv'
    sqlFile = dir_sql+prefix+'create_tables_'+martName+'.sql'
    tablefile = dir_csv+name+'.csv'
    items = len(entity['dataUrlDetails'])
    logger('    ' + name + ' - total items: ' + str(items),'n')
    
    i=0
    for dataUrlDetail in entity['dataUrlDetails']:
        i=i+1

        # when its first file in the hour create a new file else append to existing file
        if (i==1):
            writeType='wb'
        else:
            writeType='ab'

        url = dataUrlDetail['url']
        logger('      item#: ' + str(i) , 'n')
        if progressbar == 'yes':
            #print(" - download with progress bar")
            #downloadWithProgress( url, zippedFile, "ab")
            downloadWithProgress( url, zippedFile, writeType)            
        elif progressbar == 'no':
            #print(" - download without progress bar")
            #download( url, zippedFile, "ab")
            download( url, zippedFile, writeType)
        
    #unzip downloaded file
    ''' sinshd - created a new function to do line by line unzipping
    with gzip.open(zippedFile, "rb") as zipped, \
        open(unzippedFile, "wb") as unzipped:
        #read zipped data
        unzipped_content = zipped.read()
        #save unzipped data into file
        unzipped.write(unzipped_content)
        logger("...unzipped",'a')
      '''
    unzipFile(zippedFile,unzippedFile,sohDelimiter,delimiter,header)
    
    #create CSV file - replace SOH delimiter
    if csvflag == 'yes' and append == 'no':        
        #sinshd - 
        createCSV(unzippedFile, csvFile, sohDelimiter, delimiter, header)
        #createCSV2(unzippedFile, csvFile, sohDelimiter, delimiter, header)
    elif csvflag == 'yes' and append == 'yes':
        createSingleTableFiles(entity, schemaUrl)
        appendCSV(unzippedFile, tablefile, sohDelimiter, delimiter)
        
        
    #remove zipped file
    if cleanFiles == 'yes':
        os.remove(zippedFile)
        
    with open(sqlFile,'w') as f:
        f.write(gSql)
    
    return

def logHistory(dataRangeStart, dataRangeEnd,dataRangeProcessingStatus):

    if resetInProgress == True :
        historyFile = dir_config + 'reset_download_history_' + martName + '.csv' 
    else:
        historyFile = dir_config + 'download_history_' + martName + '.csv'
    
    #historyFile = dir_config + 'download_history_' + martName + '.csv'
    nowDttm = str(datetime.now())

    # if martName = detail or dbtReport
    headerline = 'dataRangeStart;dataRangeEnd;download_dttm;dataRangeProcessingStatus'
    recordline = dataRangeStart + ';' + dataRangeEnd + ';' + nowDttm + ';' + dataRangeProcessingStatus
    
    # open file - if not exist create it with headerline
    if not os.path.exists(historyFile):
        with open(historyFile, 'w') as f:
            f.write(headerline+"\n")

    # append rows to history file
    with open(historyFile, 'a') as f:
        f.write(recordline+"\n")

def logResetRange(dataRangeStart=None, dataRangeEnd=None, resetCompleted_dttm=None):
    historyFile = dir_config + 'reset_range_' + martName + '.csv'
    # if martName = detail or dbtReport
    headerline = 'dataRangeStart;dataRangeEnd;resetCompleted_dttm;download_dttm'

    # open file - if not exist create it with headerline
    if not os.path.exists(historyFile):
        with open(historyFile, 'w') as f:
            f.write(headerline+"\n")

    # append line only when dataRangeStart is set to something 
    # this way we can call logResetRange to just create an header row only
    if not (dataRangeStart == None):
        nowDttm = str(datetime.now())
        recordline = dataRangeStart + ';' + dataRangeEnd + ';' + resetCompleted_dttm + ';' + nowDttm
        # append rows to history file
        with open(historyFile, 'a') as f:
            f.write(recordline+"\n")    

def logHistorySnapshot(entity):
    historyFile = dir_config +'download_history_snapshot.csv'
    nowDttm = str(datetime.now())
    lastModifiedTimestamp = entity['dataUrlDetails'][0]['lastModifiedTimestamp']

    headerline = 'entityName;lastModifiedTimestamp;download_dttm'
    recordline = entity['entityName'] + ';' + lastModifiedTimestamp + ';' + nowDttm
    
    # open file - if not exist create it with headerline
    if not os.path.exists(historyFile):
        with open(historyFile, 'w') as f:
            f.write(headerline+"\n")

    # append rows to history file
    with open(historyFile, 'a') as f:
        f.write(recordline+"\n")

def readDownloadHistory(historyFile):
    # function to read the mart history file as dataframe 
    # this will be later used to do lookups
    # e.g. to check if the history records exits 
    # historyFile = dir_config + 'download_history_' + martName + '.csv'
    df = pandas.read_csv(historyFile
			            ,sep=';'
			            ,header=0
			            ,names=['dataRangeStart','dataRangeEnd','download_dttm','dataRangeProcessingStatus']
			            ,parse_dates =['dataRangeStart','dataRangeEnd','download_dttm'])
    return df

def readResetRange(resetFile):
    # function to read the mart reset file as dataframe 
    # this will be later used for lookups
    # e.g. to check if the reset records exits 
    # resetFile = dir_config + 'reset_range_' + martName + '.csv'
    
    # weflower 2025-02-05: pandas 1.3.0+ deprecated error_bad_lines and replaced with on_bad_lines
    # https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
    
    df = pandas.read_csv(resetFile
			            ,sep=';'
                        ,on_bad_lines='warn'
			            ,header=0
			            ,names=['dataRangeStart','dataRangeEnd','resetCompleted_dttm','download_dttm']
			            ,parse_dates =['dataRangeStart','dataRangeEnd','resetCompleted_dttm','download_dttm'])
    return df

def fileExists( fileName ):
    # function to check if the input fileName exists
    fileExists=False
    if os.path.exists(fileName):
        fileExists=True
    return fileExists

def loopThroughDownloadPackages(url):

    # call CI360 Discover API to get Reset URLs
    json_data = getDownloadUrls(url)

    # print details of the json response like number of total download packages & number of download package on current page
    printDownloadDetails(json_data)

    #loop through download packages 
    packageNumber=0
    for item in json_data['items']:
        schemaUrl = item['schemaUrl']
        prefix = ''
        
        # only for detail and dbtReport data mart display the ranges
        if martName == 'detail' or martName == 'dbtReport':
            rangeStartDt = item['dataRangeStartTimeStamp']
            rangeStart = rangeStartDt.replace(':','-').replace('.000Z','')
            rangeEndDt = item['dataRangeEndTimeStamp']
            rangeEnd = rangeEndDt.replace(':','-').replace('.999Z','')
            
            if not is_json_key_present(item, 'dataRangeProcessingStatus'):
                processingStatus  = ''
            else:  
                processingStatus = item['dataRangeProcessingStatus']

            prefix = rangeStart+ "_"
            packageNumber=packageNumber+1
            str_packageNumber = str(packageNumber)
            # add a zero infront of package number if number is lower 10
            if packageNumber < 10:
                str_packageNumber = '0' + str_packageNumber   
                 
            logger('********** Tables of package ' + str_packageNumber \
                    + ' - start: ' + str(rangeStart) + ' **********', 'n')
            # dataRangeProcessingStatus : DATA_AVAILABLE NO_DATA ERROR RESET_INPROGRESS
            logger('    dataRangeProcessingStatus : ' + processingStatus , 'n')
            
        for entity in json_data['items'][packageNumber-1]['entities']:  
            ###createSingleTableFiles(entity, schemaUrl)          
            downloadEntity(entity, schemaUrl, prefix)      
            if martName == 'identity' or martName == 'snapshot' :
                logHistorySnapshot(entity)
    
        if martName == 'detail' or martName == 'dbtReport':
            logHistory(rangeStartDt, rangeEndDt,processingStatus)

    logger('********** Finished Downloading Current Page **********', 'n')

    for link in json_data['links']:
        if link['rel'] == 'next' :
            nextHref = link['href']
            #form the next url using href
            url=createDiscoverAPIUrlFromHref(config,nextHref)
            loopThroughDownloadPackages(url)

def is_json_key_present(json, key):
    try:
        buf = json[key]
    except KeyError:
        return False

    return True

def loopThroughResetPackages(url):

    # call CI360 Discover API to get Reset URLs
    json_data = getResetUrls(url)
    # print details of the json response like number of total reset packages & number of reset package on current page
    printResetDetails(json_data)
    
    #loop through reset packages 
    packageNumber = 0
    for item in json_data['items']:
        packageNumber=packageNumber + 1
        dataRangeStart = item['dataRangeStartTimeStamp']
        dataRangeEnd = item['dataRangeEndTimeStamp']
        resetCompleted_dttm = item['resetCompletedTimeStamp']    

        str_packageNumber = str(packageNumber)
        # add a zero infront of package number if number is lower 10
        if packageNumber < 10:
            str_packageNumber = '0' + str_packageNumber   
        logger('********** Reset of package ' + str_packageNumber \
            + ' - start: ' + str(dataRangeStart) \
            + ' - end: ' + str(dataRangeEnd) \
            + ' - resetCompleted_dttm: ' + str(resetCompleted_dttm) + ' **********' , 'n')

        # get the download packages from the current downloadUrl and loop through all download packages
        downloadUrlHref=item['downloadUrl']
        downloadUrl=createDiscoverAPIUrlFromHref(config,downloadUrlHref)

        logger('           checking if reset range exists in download history...  ', 'n')
        # check if the input range exists in download history
        # create a dataframe with the filtered data from download history dataframe        
        hist_dataRange_df = download_history_df[(download_history_df['dataRangeStart']==dataRangeStart)]
        if (len(hist_dataRange_df.index) > 0 ):
            logger('exists ', 'a')
            # check if reset range is already downloaded (record exists in reset history)
            logger('           checking if reset range exists in reset history...  ', 'n')
            reset_dataRange_df = reset_range_df[(reset_range_df['dataRangeStart']==dataRangeStart)
                                                 &(reset_range_df['dataRangeEnd']==dataRangeEnd)
                                                 &(reset_range_df['resetCompleted_dttm']==resetCompleted_dttm)]
            if (len(reset_dataRange_df.index) == 0 ):
                logger(' does not exists ..starting download', 'a')
                # download reseted data
                loopThroughDownloadPackages(downloadUrl)
                # add the reset entry into reset history table 
                logResetRange(dataRangeStart, dataRangeEnd, resetCompleted_dttm)      
            else:
                logger(' exists..skipping reset ', 'a')
        else:
            logger(' does not exists..skipping reset ', 'a')
            # check if reset range is already downloaded (record exists in reset history)
        
    logger('********** Finished Reset of packages on current page **********', 'n')
    for link in json_data['links']:
        if link['rel'] == 'next' :
            nextResetRangeHref = link['href']
            #form the next url using href
            url=createDiscoverAPIUrlFromHref(config,nextResetRangeHref)
            loopThroughResetPackages(url)


###############################################

# set dynamic log file name to create a new log file in each run 
#logfile = dir_log + 'discover_download.log'
#logfile = dir_log + logFileNmtimeStamped('discover_' + martName)
#print ('logfile:',logfile)
#logger('CI360 DISCOVER Download API (' + version + ') - last updated '+ updateDate,'n')

#check command line arguments
parser = argparse.ArgumentParser(description=
        'Download data tables from SAS Customer Intelligence 360. \
        \n Required: You must specify the data mart with the -m or --mart parameter. \
        \n Optional: Additional parameters enable you to specify the time range, whether \
        \n to create a CSV file, and so on. \
        \n \
        \n Examples with short parameters: \
        \n           py discover.py -m detail -svn 17 \
        \n           py discover.py -m detail -d yes -cf yes -a yes -pb yes \
        \n           py discover.py -m detail -l 2 \
        \n           py discover.py -m detail -st 2025-04-07T10 -et 2025-04-07T12 \
        \n           py discover.py -m dbtReport -cf yes -cd ";" -ch yes \
        \n \
        \n Examples with long parameters: \
        \n           py discover.py --mart=detail --schemaversion=17 \
        \n           py discover.py --mart=detail --delta=yes --csvflag=yes --append=yes --progressbar=yes \
        \n           py discover.py --mart=detail --limit=2 \
        \n           py discover.py --mart=detail --start=2025-04-07T10 --end=2025-04-07T12 \
        \n           py discover.py --mart=dbtReport --csvflag=yes --csvdelimiter=";" --csvheader=yes \
        ', formatter_class=RawTextHelpFormatter)

# wiflow 2025-06-03 added long form parameters
parser.add_argument('-m', '--mart', action='store', dest='mart', type=str, 
    help='Data mart (the set of data tables) to donwload. Enter one of these values: detail, dbtReport, snapshot',
    required=True)
parser.add_argument('-l', '--limit', action='store', dest='limit', type=int, 
    help='Limit of partitions to download. For example: 30 (the default is 20).',
    required=False)
parser.add_argument('-cf', '--csvflag', action='store', dest='csvflag', type=str, 
    help='Create a CSV file from the downloaded tables. Enter yes or no (the default is no).',
    required=False)
parser.add_argument('-cd', '--csvdelimiter', action='store', dest='delimiter', type=str, 
    help='Delimiter for the CSV file. The default is \"|\" (pipe).',
    required=False)
parser.add_argument('-ch', '--csvheader', action='store', dest='csvheader', type=str, 
    help='Whether the CSV file should have a column header row. Enter yes or no (the default is yes).',
    required=False)
parser.add_argument('-st', '--start', action='store', dest='start', type=str, 
    help='Start time for the data range. For example: 2025-04-07T10', required=False)
parser.add_argument('-et', '--end', action='store', dest='end', type=str, 
    help='End time for the data range. For example: 2025-04-07T12', required=False)
parser.add_argument('-a', '--append', action='store', dest='append', type=str, 
    help='Append the downloaded data to one file. Enter yes or no (the default is no).', required=False)
parser.add_argument('-d', '--delta', action='store', dest='delta', type=str, 
    help='Download only the updated data from tables (the delta). Enter yes or no (the default is no).', required=False)
parser.add_argument('-cl', '--clean', action='store', dest='clean', type=str, 
    help='Clean the .zip file after processing. Enter yes or no (the default is yes).', required=False)
parser.add_argument('-pb', '--progressbar', action='store', dest='progressbar', type=str, 
    help='Show a progress bar for downloads. Enter yes or no (the default is no).', required=False)

#added 2018-11-21 by Mathias Bouten - new API features
#parser.add_argument('-ahs', action='store', dest='allhourstatus', type=str, 
#    help='enter includeAllHoursStatus flag: ie. true - default false', required=False)
parser.add_argument('-shr', '--subhourrange', action='store', dest='subhourrange', type=str, default=60,
    help='Download data ranges in minutes, instead of the default hourly range. For example: 10', required=False)
parser.add_argument('-svn', '--schemaversion', action='store', dest='schemaversion', type=str, default=1,
    help='Schema version, like: 17. The default is 1.', required=False)
parser.add_argument('-ar', '--autoreset', action='store', dest='autoreset', type=str, default='yes',
    help='Download reprocessed (reset) data from the data mart. Enter yes or no (the default is yes).', required=False)
parser.add_argument('-ct', '--category', action='store', dest='category', type=str, default='discover',
    help='Category for the tables to download (for example: all, discover, engagedirect, etc.). The default value is discover.', required=False)
#added 2020-01-27 - sinshd -new API features - test mode download
parser.add_argument('-tm', '--testmode', action='store', dest='testmode', type=str,
    help='Enable download of test mode tables. For example: PLANTESTMODE', required=False)

# wiflow 2025-06-03 - added parameter to quote columns in CSV file per GitHub request:
# https://github.com/sassoftware/ci360-download-client-python/issues/2
parser.add_argument('-cq', '--csvquote', action='store', dest='csvquote', type=str,
    help='For CSV files, encloses field values with quotation marks ("). Enter yes or no (the default is no). \
          \nThis parameter has no effect if -cf or --csvflag is not set.', required=False)
parser.add_argument('-cqc', '--csvquotechar', action='store', dest='csvquotechar', type=str,
    help='For CSV files, override the default character (") that encloses fields. For example: @.\
          \nThis parameter has no effect if -cq or --csvquote is not set.', required=False)

args = parser.parse_args()

if args.mart is not None:
    martName = args.mart
    download_msg = 'all'
    print('  datamart: ' + martName)
if args.limit is not None:
    limit = str(args.limit)
    querystring["limit"] = limit
    print('  limit: ' + limit)
if args.delimiter is not None:
    delimiter = str(args.delimiter)
    print('  delimiter: ' + delimiter)
if args.csvflag is not None:
    csvflag = str(args.csvflag)
    print('  csvflag: ' + csvflag)
if args.csvheader is not None:
    csvheader = str(args.csvheader)
    print('  csvheader: ' + csvheader)
if args.csvquote is not None:
    csvquote = str(args.csvquote)
    print('  csvquote: ' + csvquote)
if args.csvquotechar is not None:
    csvquotechar = str(args.csvquotechar)
    print('  csvquotechar: ' + csvquotechar)
if args.clean is not None:
    cleanFiles = str(args.clean)
    print('  cleanFiles: ' + cleanFiles)
if args.progressbar is not None:
    progressbar = str(args.progressbar)
    print('  progressbar: ' + progressbar)
if args.start is not None:
    dataRangeStartTimeStamp = str(args.start) + ":00:00.000Z"
    querystring["dataRangeStartTimeStamp"] = dataRangeStartTimeStamp   
    print('  start: ' + dataRangeStartTimeStamp)
if args.end is not None:
    dataRangeEndTimeStamp = str(args.end) + ":00:00.000Z"
    querystring["dataRangeEndTimeStamp"] = dataRangeEndTimeStamp
    print('  end: ' + dataRangeEndTimeStamp)
if args.append is not None:
    append = str(args.append)
    print('  append: ' + append)
if args.delta is not None:
    # sinshd - use new function to get the max(lastEnd) + 1 instead of last(start) + 1 hour,
    #  as this can create a gap when download is in minute level mode 
    # dataRangeStartTimeStamp = getLastDataRangeStart()
    dataRangeStartTimeStamp = getNextDataRangeStart()
    querystring["dataRangeStartTimeStamp"] = dataRangeStartTimeStamp 
    print('  start: ' + dataRangeStartTimeStamp)
    # sinshd - getDataRangeEndOfNow is a system clock , this can limit the data even though its available in source
    # instead do not set the end , API by default should return upto the current date time when only start time is set
    #dataRangeEndTimeStamp = getDataRangeEndOfNow()    
    #querystring["dataRangeEndTimeStamp"] = dataRangeEndTimeStamp    
    #print('  end: ' + dataRangeEndTimeStamp)

#added 2018-11-21 by Mathias Bouten - new API features

# sinshd - set the allhourstatus = true by default
#if args.allhourstatus is not None:
#    allhourstatus = str(args.allhourstatus)
querystring["includeAllHourStatus"] = allhourstatus
print('  includeAllHourStatus: ' + allhourstatus)

if args.subhourrange is not None:
    subhourrange = str(args.subhourrange)
    querystring["subHourlyDataRangeInMinutes"] = subhourrange
    print('  subHourlyDataRangeInMinutes: ' + subhourrange)

if args.schemaversion is not None:
    schemaversion = str(args.schemaversion)
    querystring["schemaVersion"] = schemaversion
    print('  schemaVersion: ' + schemaversion)

if args.autoreset is not None:
    autoreset = str(args.autoreset)
    print('  autoreset: ' + autoreset)

if args.category is not None:
    category = str(args.category)
    querystring["category"] = category
    print('  category: ' + category)

if args.testmode is not None:
    testmode = str(args.testmode)
    querystring["code"] = testmode
    print('  testmode: ' + testmode)

querystring["downloadClient"] = downloadClient
print('  downloadClient: ' + downloadClient)
################### START SCRIPT #####################

# call version update function to update existing files

# set dynamic log file name to create a new log file in each run 
fileNm = 'discover_' + martName
logfile = logFileNmtimeStamped(fileNm)
print ('logfile:',logfile)

logger('CI360 DISCOVER Download API (' + version + ') - last updated '+ updateDate,'n')

# track start time
start = time.time()    

# make any changes as we change the versions
versionUpdate()

resetInProgress=False

# read config file
config = readConfig(dir_config + 'config.txt')

# PyJWT returns str type for jwt.encode() function: https://pyjwt.readthedocs.io/en/latest/changelog.html#improve-typings
# For backwards compatibility for older PyJwt releases, decode or return token value based on type.
def decodeToken(token):
    if (type(token)) == bytes:
        return bytes.decode(token)
    else:
        return token

# Generate the JWT
encodedSecret = base64.b64encode(bytes(config['secret'], 'utf-8'))
token = jwt.encode({'clientID': config['tenantId']}, encodedSecret, algorithm='HS256')
#print('\nJWT token: ' + bytes.decode(token))
headers = {'authorization': "Bearer "+ decodeToken(token),'cache-control': "no-cache"}

# modify discover Reset API URL
#url = createDiscoverResetAPIUrl(config)

# do reset if autoreset is set to yes 
if martName == 'detail' or martName == 'dbtReport':
    if autoreset == 'yes' :
        # modify discover Reset API URL
        url = createDiscoverResetAPIUrl(config)

        # set resetInProgress=True to indicate we are now in reset mode.
        # when resetInProgress, record download history to reset_download_history_mart.csv
        resetInProgress=True
        # check if mart download history exists. if not exists then no need to run the resets
        # this will avoid running resets when nothing is downloaded
        logger(' starting resets','n')
        historyFile = dir_config + 'download_history_' + martName + '.csv'
        logger(' checking if ' +  historyFile + ' exists ...','n')
        if fileExists(historyFile) :            
            logger(' found ', 'a')
            #store the download history into a dataframe 
            download_history_df=readDownloadHistory(historyFile)

            resetFile = dir_config + 'reset_range_' + martName + '.csv'
            logger(' checking if ' +  resetFile + ' exists ...','n')
            if fileExists(resetFile) :            
                logger(' found ', 'a')
            else:
                logger(' not found...', 'a')                
                logger(' creating ', 'a')
                logResetRange()
            #store the reset history into a dataframe 
            reset_range_df=readResetRange(resetFile)
            # Start looping through reset packages
            loopThroughResetPackages(url)
        else:
            logger(' not found..skipping reset ', 'a')
        logger(' finished resets','n')
        resetInProgress=False

# modify discover API URL
url = createDiscoverAPIUrl(config)

# Start looping through download packages
logger(' starting downloads','n')
loopThroughDownloadPackages(url)
logger(' finished downloads','n')

# track end time and calculate duration
end = time.time() 
duration = round((end - start),2)
        
logger('Done - execution time: ' + str(duration) + ' seconds','n')

print('\n')
