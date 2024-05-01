import pandas as pd
import random

column_list = ['PolicyNumber', 'CoverageAmount', 'StartDate', 'EndDate', 'AdminFee', 'Commission',
               'BusinessDescription', 'BusinessEvent', 'ClientType', 'ClientRef',
               'EffectiveDate', 'InsurerPolicyNumber', 'IPTAmount', 'Premium', 'PolicyFee', 'PolicyType', 'Insurer',
               'Product', 'RenewalDate', 'RootPolicyRef']

# Dictionary for converting different column names to the standardized list. If the system cannot find one of the column
# names for the standard list, it will try to look for an alternate one via the dictionary keys. The standardized column name will
# be available as the value of that particular key.

column_dict = {
    'PolicyRef': 'PolicyNumber',
    'InsuredAmount': 'CoverageAmount',
    'InitiationDate': 'StartDate',
    'ExpirationDate': 'EndDate',
    'AdminCharges': 'AdminFee',
    'BrokerFee': 'Commission',
    'CompanyDescription': 'BusinessDescription',
    'ContractEvent': 'BusinessEvent',
    'ConsumerCategory': 'ClientType',
    'ConsumerID': 'ClientRef',
    'ActivationDate': 'EffectiveDate',
    'InsuranceCompanyRef': 'InsurerPolicyNumber',
    'TaxAmount': 'IPTAmount',
    'CoverageCost': 'Premium',
    'ContractFee': 'PolicyFee',
    'ContractCategory': 'PolicyType',
    'Underwriter': 'Insurer',
    'InsurancePlan': 'Product',
    'NextRenewalDate': 'RenewalDate',
    'PrimaryPolicyRef': 'RootPolicyRef'
}

def processData(filePaths):
    # takes in a list of filePaths and reads them in order to obtain data. Currently uses the csv to create a broker
    # name and the names of the files that are returned.
    dataList = []
    for path in filePaths:
        try:
            broker = pd.read_csv(path)
        except:
            raise Exception("Invalid name of csv or filepath. Please try again.")
        normalizedDataSet = normalizeData(broker, path.split('.')[0], str(random.randint(1000, 9999)))
        normalizedDataSet.to_csv(path.split('.')[0] + 'Normalized.csv')
        dataList.append(normalizedDataSet)
    return(pd.concat(dataList, ignore_index=True))

def normalizeData(dataSet, brokerName, brokerId):
    # The function accepts a pandas dataset to normalize, as well as the broker name and Id to be added to the data. This
    # allows for policies belonging to a particular broker to be returned via an Id or Name.
    dataDict = {}
    for colName, col in dataSet.items():
        if colName in column_list:
            currentCol = colName
        else:
            currentCol = column_dict[colName]
        if 'Date' in currentCol:
            dateTemp = pd.to_datetime(col, format='mixed', errors='coerce')
            dateTemp = dateTemp.dt.strftime('%Y/%m/%d')
            col = pd.to_datetime(dateTemp, errors='coerce')
        dataDict[currentCol] = col

    dataDict['brokerName'] = brokerName
    dataDict['brokerId'] = brokerId
    normalizedDataFrame = pd.DataFrame(dataDict)
    return normalizedDataFrame

def dataReporting(dataSet):

    # Create a dictionary containing all requested data.
    reportDict = {'customerCount': dataSet['ClientRef'].nunique(), 'policyCount': dataSet['PolicyNumber'].nunique(),
                  'totalInsuranceAmount': dataSet['CoverageAmount'].sum()}

    mask = (pd.to_datetime(dataSet['StartDate']) > pd.to_datetime('today')) & \
           (pd.to_datetime(dataSet['RenewalDate']) <= pd.to_datetime('today'))

    currentPolicies = dataSet.loc[mask]

    reportDict['averagePolicyDuration'] = (pd.to_datetime(currentPolicies['EndDate']) -
                                           pd.to_datetime(currentPolicies['StartDate'])).agg('mean')
    return reportDict

def searchByBroker(brokerInfo, dataSet):
    brokerPolicies = dataSet.query('brokerName == @brokerInfo or brokerId == @brokerInfo')
    brokerPolicies.to_csv('brokerPolicies.csv')
    return brokerPolicies

if __name__ == '__main__':
    optionSelect = input("To record new broker information, press 1. To output record data, press 2. To view policies of a specific broker, press 3.")
    if optionSelect == '1':
        combinedDataSet = processData(['broker1.csv', 'broker2.csv'])
        combinedDataSet.to_csv('combinedDataSet.csv')
    elif optionSelect == '2':
        print(dataReporting(pd.read_csv('combinedDataSet.csv')))
    elif optionSelect == '3':
        brokerInfo = input("Input the ID or Name of Broker.")
        print(searchByBroker(brokerInfo, pd.read_csv('combinedDataSet.csv')))
    else:
        raise Exception("Invalid Input")

def testDataProcessing():
    combinedDataSet = processData(['broker1.csv', 'broker2.csv'])
    assert(len(combinedDataSet) == 60)