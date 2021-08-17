import requests
import json
import dateutil.parser

searchLimitMax200 = '200'
HorizonInstance = 'horizon.stellar.org'

def StellarNonNative(queryAsset, issuerAddress, desiredDateISO8601): # '20XX-MM-DDT00:00:00Z'
    # This breaks in environments with extremely high throughput,
    # namely wherein shareholders transfer assets so often that balances
    # change from the runtime of getting the current sequence number and
    # getting the assetHolderBalances. This means assetholders who transfer
    # shares between the randomly-timed execution of this script finding
    # runtimeSequenceNumber and getAssetholderBalancesNow (estimated at a
    # few seconds) could incidentally receive less shares on record, while
    # the recipient receives more. This is a major issue past 200 holders, 
    # and the only way to rectify it is to manually go back and check 
    # transfers during the scripting period afterwards, ensuring no 
    # misappropriations of dividends or voting rights post-call.
    # To fix: impliment via direct ledger database records rather than API
    # or uncover a different direct-ledger query to bypass txn simulation.
    
    runtimeSequenceNumber = requests.get('https://' + HorizonInstance).json()['core_latest_ledger']
    assetholderBalancesNow = getAssetholderBalancesNow(queryAsset, issuerAddress)
    recordDateBlockHeight = getFirstBlockHeightAfterOrEqualToDate(desiredDateISO8601)
    recordDateAssetholderBalances = updateAssetholderBalancesBasedOnTransfersAfterRecordDate(queryAsset, issuerAddress, assetholderBalancesNow, runtimeSequenceNumber, recordDateBlockHeight)
    return recordDateAssetholderBalances


def getAssetholderBalancesNow(queryAsset, issuerAddress):
    StellarBlockchainBalances = {}
    requestAddress = 'https://' + HorizonInstance + '/accounts?asset=' + queryAsset + ':' + issuerAddress + '&limit=' + searchLimitMax200
    data = requests.get(requestAddress).json()
    blockchainRecords = data['_embedded']['records']
    while(blockchainRecords != []):
        for accounts in blockchainRecords:
            accountAddress = accounts['id']
            for balances in accounts['balances']:
                if balances['asset_type'] != 'native' and balances['asset_code'] == queryAsset:
                    accountBalance = float(balances['balance'])                                       # TODO: Run some test cases on cumulative float exports
                    break
            StellarBlockchainBalances[accountAddress] = accountBalance
        # Go to next cursor
        requestAddress = data['_links']['next']['href'].replace('%3A', ':')
        data = requests.get(requestAddress).json()
        blockchainRecords = data['_embedded']['records']
    return StellarBlockchainBalances

# Simple decriment + re-increment. To improve, we reccomend merge sort. Med. priority if API calls maintained
def getFirstBlockHeightAfterOrEqualToDate(desiredDateISO8601): # ++record date (first allowable block @ 00:00:00Z of next day === EoD UST)
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
    print(blockTimeISO8601)
    return testSequenceNum
    
def updateAssetholderBalancesBasedOnTransfersAfterRecordDate(queryAsset, issuerAddress, assetholderBalancesNow, runtimeSequenceNumber, recordDateBlockHeight):
    while(recordDateBlockHeight <= runtimeSequenceNumber):
        # get recordDateBlockHeight ledger
        requestAddress = 'https://' + HorizonInstance + '/ledgers/' + str(recordDateBlockHeight) + '/payments'
        data = requests.get(requestAddress).json()
        blockPayments = data['_embedded']['records']
        while(blockPayments != []):
            for transfer in blockPayments:                 #redundant transfer['type'] == 'payment'              # TODO: Important | Claimed balances & trades are not accounted for
                if transfer['transaction_successful'] and transfer['type'] == 'payment' and transfer['asset_type'] != 'native' and transfer['asset_issuer'] == issuerAddress and transfer['asset_code'] == queryAsset:
                    assetholderBalancesNow[transfer['from']] += float(transfer['amount'])
                    assetholderBalancesNow[transfer['to']] -= float(transfer['amount'])
                    pprint('Updated transfer balances given' + str(transfer))
            requestAddress = data['_links']['next']['href'].replace('\u0026', '&')
            data = requests.get(requestAddress).json()
            blockPayments = data['_embedded']['records']
            recordDateBlockHeight += 1
    return assetholderBalancesNow