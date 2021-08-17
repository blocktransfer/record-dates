import requests
import json
import datetime as datetime
import dateutil.parser

import pprint as pprint

searchLimitMax200 = '200'
HorizonInstance = 'horizon.stellar.org'

def StellarNonNative(queryAsset, issuerAddress, desiredDateISO8601):
    
    # This breaks in environments with extremely high throughput,
    # namely wherein shareholders transfer assets so often that balances
    # change from the runtime of getting the current sequence number and
    # getting the assetHolderBalances. This means assetholders who transfer
    # shares between the randomly-timed execution of this script finding
    # runtimeSequenceNumber and getAssetholdersBalancesNow (estimated at a
    # few seconds) could incidentally receive less shares on record, while
    # the recipient receives more. This is a major issue past 200 holders, 
    # and the only way to rectify it is to manually go back and check 
    # transfers during the scripting period afterwards, ensuring no 
    # misappropriations of dividends or voting rights post-call.
    
    # To fix: impliment via direct ledger database records rather than API

    runtimeSequenceNumber = requests.get('https://' + HorizonInstance).json()['core_latest_ledger']
    assetholdersBalancesNow = getAssetholdersBalancesNow(queryAsset, issuerAddress)
    
    # get block height now 
    
    
    # Step 2 : Get all the transfers after the record date
    
    
    
    
    # get record date block height
    recordDateBlockHeight = getFirstBlockHeightAfterOrEqualToDate(desiredDateISO8601)
    
    
    
    
    
    
    #
    
    # KNOW: /payments includes /operations transferring value 
    # likewise, 
    
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
    return StellarBlockchainBalances

# Simple decriment + re-increment. To improve, we reccomend merge sort. Med. priority if API calls maintained
def getFirstBlockHeightAfterOrEqualToDate(desiredDateISO8601): # '20XX-MM-DDT00:00:00Z' ++record date
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