import requests
import json
import datetime as datetime
import dateutil.parser

import pprint as pprint

searchLimitMax200 = '200'
HorizonInstance = 'horizon.stellar.org'



# Simple decriment + re-increment. To improve, could do merge sort
def getFirstBlockHeightAfterOrEqualToDate(desiredDateISO8601):
    print('\n')
    currentBlockHeight = requests.get('https://' + HorizonInstance).json()['core_latest_ledger']
    testSequenceNum = currentBlockHeight - 20000
    while(True):
        requestAddress = 'https://' + HorizonInstance + '/ledgers/' + str(testSequenceNum)
        data = requests.get(requestAddress).json()
        blockTimeISO8601 = data['closed_at']
        if (dateutil.parser.parse(blockTimeISO8601) > dateutil.parser.parse(desiredDateISO8601)):
            testSequenceNum -= 16000    
        else: break
    while(True):
        requestAddress = 'https://' + HorizonInstance + '/ledgers/' + str(testSequenceNum)
        data = requests.get(requestAddress).json()
        blockTimeISO8601 = data['closed_at']
        if (dateutil.parser.parse(blockTimeISO8601) < dateutil.parser.parse(desiredDateISO8601)):
            testSequenceNum += 400
        else: break
    while(True):
        requestAddress = 'https://' + HorizonInstance + '/ledgers/' + str(testSequenceNum)
        data = requests.get(requestAddress).json()
        blockTimeISO8601 = data['closed_at']
        if (dateutil.parser.parse(blockTimeISO8601) > dateutil.parser.parse(desiredDateISO8601)):
            testSequenceNum -= 20
        else: break
    while(True):
        requestAddress = 'https://' + HorizonInstance + '/ledgers/' + str(testSequenceNum)
        data = requests.get(requestAddress).json()
        blockTimeISO8601 = data['closed_at']
        if (dateutil.parser.parse(blockTimeISO8601) < dateutil.parser.parse(desiredDateISO8601)):
            testSequenceNum += 1
        else: break
    print('Final: ' + str(testSequenceNum) + ' closed at ' + str(blockTimeISO8601) + '\n')
    return testSequenceNum


getFirstBlockHeightAfterOrEqualToDate('2021-8-12T0:00:00Z')

