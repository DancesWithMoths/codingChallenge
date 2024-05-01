import brokerChallenge
import pytest as pt
import pandas as pd

def testDataNormalization():
    normalizedData = brokerChallenge.normalizeData(pd.read_csv('broker1.csv'), 'broker1', '1234')
    assert (len(normalizedData.columns) == 22)
    assert ()

def testDataProcessing():
    combinedDataSet = brokerChallenge.processData(['broker1.csv', 'broker2.csv'])
    assert (len(combinedDataSet) == 61)
    with pt.raises(Exception, match='Invalid name of csv or filepath. Please try again.'):
        raise Exception('Invalid name of csv or filepath. Please try again.')

def testDataReporting():
    dataReport = brokerChallenge.dataReporting(pd.read_csv('testDataReportingSet.csv'))
    assert(dataReport['customerCount'] == 52)
    assert(dataReport['policyCount'] == 60)
    assert(dataReport['totalInsuranceAmount'] == 46637000)

def testBrokerSearching():
    brokerData = brokerChallenge.searchByBroker('broker1', pd.read_csv('combinedDataSet.csv'))
    assert (len(brokerData) == 40)
    assert (brokerData['brokerName'] == 'broker1').all()

if __name__ == '__main__':
    testDataNormalization()
    testDataProcessing()
    testDataReporting()