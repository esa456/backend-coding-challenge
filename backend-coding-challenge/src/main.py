import pandas as pd
import os
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from json import dumps
############################################################################################    
########################################## DATA INGESTION AND FORMATTING ##################################################
############################################################################################
#converts .csv files into dataFrame
def ingest(fileName):
        
    df = pd.read_csv(fileName)
 
    return df
############################################################################################
def checkType(df):
    
    #contains the incoming data fields and expected formats
    typeDict = {'period_id' : 'int', 'period_name' : 'str', 'week_commencing_date' : 'date', 'brand_id' : 'int', \
                'brand' : 'str', 'brand' : 'str', 'gross_sales' : 'float', 'units_sold' : 'int'}
    
    dfNew = df.copy()
    
    
    for t in typeDict:
        for ind in df.index:
            #Ensure week_commencing_date is of type date
            try: 
                if typeDict[t] == 'date':
                    date = datetime.strptime(dfNew.loc[ind, t], '%d/%M/%Y').date()
                    dfNew.loc[ind, t] = date 
                
                #ensure correct values are of type integer
                elif typeDict[t] == 'int':                 
                    newInt = int(dfNew.at[ind, t])
                    dfNew.loc[ind, t] = newInt
                
                #ensure correct values are of type string
                elif typeDict[t] == 'str':
                    newStr = str(dfNew.at[ind, t])
                    dfNew.loc[ind, t] = newStr
                
                #ensure correct values are of type float
                elif typeDict[t] == 'float':
                    newFloat = float(dfNew.at[ind, t])
                    dfNew.loc[ind, t] = newFloat
            
            except KeyError:
                continue
            
   
    return dfNew
############################################################################################    
def baseDict(productPath, columnName):
    
    productSales = ingest(productPath)
    productSalesComplete = checkType(productSales)
    
    
    #Gives us a unique list of 'columnName'
    barcodeList = []
    for i in productSalesComplete.index:
        barcode_no = productSalesComplete.loc[i, columnName]
        barcodeList.append(barcode_no)
    
    uniqueList = list(set(barcodeList))
    
    productDict = dict()
    
    #creates a dictionary where data is split by barcode_no or brand_id
    for unique in uniqueList:
        index = (productSalesComplete[productSalesComplete[columnName]==unique].index.values)
        # print(index)
             
        productDF = productSalesComplete.iloc[index, :].copy()
        #turn into dictionary for easier computation
        aDict = productDF.to_dict('records')
        productDict.update({unique: aDict})
        

    productSalesDict = PWCD(productDict)

    
    return productSalesDict
############################################################################################    
#adds previous_week_commencing_date to current period data
def PWCD(productSalesDict):
    for iKey in productSalesDict.keys():
        data = productSalesDict[iKey]
      

        for dataDict in data:
            period_id = dataDict['period_id']
            week_commencing = dataDict['week_commencing_date']
            
            #current
            if period_id == 2:
                previous_week_commencing = week_commencing - relativedelta(years=1)
                
                #calculated field allows us to check for edge cases later on
                dataDict.update({'previous_week_commencing_date' : previous_week_commencing, 'calculated' : 'no'})
            
            else:
                dataDict.update({'previous_week_commencing_date' : 'null', 'calculated' : 'no'})
    
    return productSalesDict

############################################################################################    
########################################## CALCULATIONS ##################################################
############################################################################################
#performs initial calculations without edgecases
def calculations(productSalesDict):
    
    for iKey in productSalesDict.keys():
        data = productSalesDict[iKey]
      
        for dataDict in data:
            period_id = dataDict['period_id']
            week_commencing = dataDict['week_commencing_date']
            previous_week_commencing = dataDict['previous_week_commencing_date']
            current_gross_sales = dataDict['gross_sales']
            current_units_sold = dataDict['units_sold']
            calculated = dataDict['calculated']
            
            #current
            if period_id == 2:     
                
                comparisonLoop(data, dataDict, previous_week_commencing, current_gross_sales, current_units_sold)
                
                
    return productSalesDict
############################################################################################    
#checks to see if a previous weeks data exists and performs computation
def comparisonLoop(data, dataDict, previous_week_commencing, current_gross_sales, current_units_sold):
 
    for anotherDataDict in data:
        period_id = anotherDataDict['period_id']
        week_commencing = anotherDataDict['week_commencing_date']
        prev_gross_sales = anotherDataDict['gross_sales']
        prev_units_sold = anotherDataDict['units_sold']
        
        #previous
        if period_id == 1:
            
            #the week commencing for previous data matches the calculated prev week in current data
            if week_commencing == previous_week_commencing:

            
                GSG = formula(current_gross_sales, prev_gross_sales)
                USG = formula(current_units_sold, prev_units_sold)
                
                dataDict.update({'perc_gross_sales_growth' : GSG, 'perc_unit_sales_growth' : USG, 'calculated' : 'yes'}) 
                
                #we do this to show previous data matches current data
                anotherDataDict.update({'calculated' : 'yes'})
                
    return dataDict
############################################################################################ 
#checks for edge cases and fills in the rest of the data
def calculations2(productCalcDict):
    
    for iKey in productCalcDict.keys():
        data = productCalcDict[iKey]
      
        for dataDict in data:
            period_id = dataDict['period_id']
            current_week = dataDict['week_commencing_date']
            previous_week_commencing = dataDict['previous_week_commencing_date']
            calculated = dataDict['calculated']
            
            #current data has not matched with any previous data
            if period_id == 2 and calculated == 'no':
                
                edgeCases(data, dataDict, previous_week_commencing, current_week) 
            
            #previous data has not matched with any current data
            if period_id == 1 and calculated == 'no':
                
                edgeCases2(data, dataDict, previous_week_commencing, current_week)
                
    return productCalcDict
############################################################################################ 
 #if no previous period weekly sales 
def edgeCases(data, dataDict, previous_week_commencing, current_week):
    
    for anotherDataDict in data:
        period_id = anotherDataDict['period_id']
        week_commencing = anotherDataDict['week_commencing_date']
        
        if period_id == 1:
            if week_commencing != previous_week_commencing:
            
                GSG = 'null' #no previous week
                USG = 'null' #no previous week
                
                dataDict.update({'perc_gross_sales_growth' : GSG, 'perc_unit_sales_growth' : USG, 'calculated' : 'yes'}) 
            
    return dataDict 
############################################################################################    
#if no current period sales but previous period sales
def edgeCases2(data, dataDict, previous_week_commencing, current_week):
    
    for anotherDataDict in data:
        period_id = anotherDataDict['period_id']
        previous_week_commencing = anotherDataDict['previous_week_commencing_date']
        
        if period_id == 2:
            if current_week != previous_week_commencing:
                
                
                GSG = -100 #no current week
                USG = -100 #no current week
                
                dataDict.update({'perc_gross_sales_growth' : GSG, 'perc_unit_sales_growth' : USG, 'calculated' : 'yes'}) 
            
    return dataDict  
############################################################################################    
#calculates % Growth
def formula(current_gross_sales, prev_gross_sales):
    
    var1 = (current_gross_sales - prev_gross_sales)
    var2 = (var1/prev_gross_sales)*100
    
    result = round(var2, 2)
    
    #final edge case where no sales for either period
    if result == 0.0:
        result = 'null' #no growth
        
    return result
############################################################################################    
########################################## OUTPUT ##################################################
############################################################################################
# JSON serializer for objects not serializable by default json code
def json_serial(obj):

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))
    
############################################################################################
#finds non edgecase dictionaries
def findUnwantedDicts(productCalcDict):
    
    for iKey in productCalcDict.keys():
        data = productCalcDict[iKey]
        # print(data)
        
        for aDict in data:
            # print(len(aDict))
            
            if len(aDict) == 9:
                aDict.update({'delete' : 'yes'})

    return productCalcDict
############################################################################################
#removes the unwanted dictionaries
def removeUnwantedDicts(productCalcDict):
    
    findUnwantedDicts(productCalcDict)
    
    for j in range(len(productCalcDict) + 1):
        for iKey in productCalcDict.keys():
            data = productCalcDict[iKey]
    
            for i in range(len(data)):
                try:
                    if data[i]['delete'] == 'yes':
                        del data[i]
                        break
                except KeyError:
                    continue
                
    return productCalcDict   
############################################################################################
#Change the key nammes in dict to reflect final output
def changeHeaders(productCalcDict, string):
    
    for iKey in productCalcDict.keys():
        data = productCalcDict[iKey]
        
        for aDict in data:
            aDict['current_week_commencing_date'] = aDict.pop('week_commencing_date')
            
            if string == 'brand':
                aDict['brand_name'] = aDict.pop('brand')
        
    
    return productCalcDict
############################################################################################
#remove unwanted information
def removeInfo(productCalcDict):
    
    delList = ['calculated', 'gross_sales', 'units_sold', 'period_id', 'period_name']
    
    for iKey in productCalcDict.keys():
        data = productCalcDict[iKey]
        
        for aDict in data:
            
            for elem in delList:
                
                aDict.pop(elem)
    
    return productCalcDict
############################################################################################
#Gives us final output Dicts
def outputDicts(productCalcDict, string):
    
    removeUnwantedDicts(productCalcDict)
    changeHeaders(productCalcDict, string)
    productOutputDict = removeInfo(productCalcDict)
     

    
    return productOutputDict
############################################################################################
#creates json file and output folder
def output(brandCalcDict, productCalcDict):
    
    brandOutputDict = outputDicts(brandCalcDict, 'brand')
    productOutputDict = outputDicts(productCalcDict, 'notbrand')
    
    
    outputList = [productOutputDict, brandOutputDict]

    
    try:
        os.mkdir('output')
    except:
        print('Folder already exists')
        
    json = dumps(outputList, default=json_serial, sort_keys=True, indent=4)
    
    os.chdir('output')
    
    f = open("results.json", "w")
    f.write(json)
    f.close()

    return 
############################################################################################    
########################################## MAIN FUNCTION ##################################################
############################################################################################
def main():
    
    #ingest sample data
    brandPath = os.getcwd() + '/data/sales_brand.csv'
    productPath = os.getcwd() + '/data/sales_product.csv'

    brandSalesDict = baseDict(brandPath, 'brand')
    productSalesDict = baseDict(productPath, 'product_name')
    
    #perform calculations
    brandCalcDict = calculations(brandSalesDict)
    productCalcDict = calculations(productSalesDict)
    
    calculations2(brandCalcDict)
    calculations2(productCalcDict)
    
    #output results
    output(brandCalcDict, productCalcDict)
    
    return
############################################################################################    
########################################## END TO END ##################################################
############################################################################################

def run():
    
    main()

    return True



if __name__ == "__main__":
    run()