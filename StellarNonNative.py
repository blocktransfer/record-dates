import requests
import json
import datetime as datetime
import dateutil.parser

import pprint as pprint

searchLimitMax200 = '200'
HorizonInstance = 'horizon.stellar.org'

def StellarNonNative(queryAsset, issuerAddress, desiredDateISO8601):
    getAssetholdersBalancesNow(queryAsset, issuerAddress)
  
    # Step 2 : Get all the transfers after the record date
    recordDateBlockHeight = getFirstBlockHeightAfterOrEqualToDate(desiredDateISO8601)
    #
    
    
    
    #


    # Step 3 : Update the balances from #1 based on #2

  
def getAssetholdersBalancesNow(queryAsset, issuerAddress):
    StellarBlockchainBalances = {}
    requestAddress = 'https://' + HorizonInstance + '/accounts?asset=' + queryAsset + ':' + issuerAddress + '&limit=' + searchLimitMax200
    data = requests.get(requestAddress).json()
    blockchainRecords = data['_embedded']['records']
    while(blockchainRecords != []):
        for accounts in blockchainRecords:
            accountAddress = accounts['id']
            for balances in accounts['balances']:
                if balances['asset_type'] != 'native' and balances['asset_code'] == queryAsset:
                    accountBalance = float(balances['balance'])                                       # TODO: Run some test cases on float exports
                    break
            StellarBlockchainBalances[accountAddress] = accountBalance
        # Go to next cursor
        requestAddress = data['_links']['next']['href'].replace('%3A', ':')
        data = requests.get(requestAddress).json()
        blockchainRecords = data['_embedded']['records']

# Simple decriment + re-increment. To improve, we reccomend merge sort
def getFirstBlockHeightAfterOrEqualToDate(desiredDateISO8601): # '20XX-MM-DDT00:00:00Z' ++record date
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
    return testSequenceNum