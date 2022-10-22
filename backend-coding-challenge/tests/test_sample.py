import sys, os, pytest, pandas, datetime, numpy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.main import run, baseDict, calculations2, formula

############################################################################################    
########################################## DATA INGESTION AND FORMATTING ##################################################
############################################################################################
#ensure product data is in the correct datatypes
def test_checkTypeProduct():
    productPath = os.getcwd() + '/data/sales_product.csv'
    productSalesDict = baseDict(productPath, 'product_name')
    
    for iKey in productSalesDict.keys():
        data = productSalesDict[iKey]
        
        for aDict in data:

            #type int
            period_id = aDict['period_id']
            assert type(period_id) == int
            
            #type str
            period_name = aDict['period_name']
            assert type(period_name) == str
            
            #type date
            week_commencing_date = aDict['week_commencing_date']
            assert type(week_commencing_date) == datetime.date
            
            #type str
            barcode_no = aDict['barcode_no']
            assert type(barcode_no) == int
            
            #type str
            product_name = aDict['product_name']
            assert type(product_name) == str
            
            #type int
            gross_sales = aDict['gross_sales']
            assert type(gross_sales) == float
            
            #type int
            units_sold = aDict['units_sold']
            assert type(units_sold) == int
            

#ensure brand data is in the correct datatypes
def test_checkTypeBrand():
    brandPath = os.getcwd() + '/data/sales_brand.csv'
    brandSalesDict = baseDict(brandPath, 'brand')
    
    for iKey in brandSalesDict.keys():
        data = brandSalesDict[iKey]
        
        for aDict in data:
            
            #type int
            period_id = aDict['period_id']
            assert type(period_id) == int
            
            #type str
            period_name = aDict['period_name']
            assert type(period_name) == str
            
            #type date
            week_commencing_date = aDict['week_commencing_date']
            assert type(week_commencing_date) == datetime.date
            
            #type str
            brand_id = aDict['brand_id']
            assert type(brand_id) == int
            
            #type str
            brand = aDict['brand']
            assert type(brand) == str
            
            #type int
            gross_sales = aDict['gross_sales']
            assert type(gross_sales) == float
            
            #type int
            units_sold = aDict['units_sold']
            assert type(units_sold) == int

############################################################################################    
########################################## CALCULATIONS ##################################################
############################################################################################
#no current period sales but previous period sales --> -100
def test_edgeCase1_product():
    productPath = os.getcwd() + '/data/sales_product.csv'
    productSalesDict = baseDict(productPath, 'product_name')
    newProductSalesDict = calculations2(productSalesDict)
    
    result = newProductSalesDict['Product C'][1]

            
    assert ( (result['perc_gross_sales_growth']) == -100  and (result['perc_unit_sales_growth']) == -100 )
    
    
#no current period sales but previous period sales --> -100
def test_edgeCase1_brand():
    brandPath = os.getcwd() + '/data/sales_brand.csv'
    brandSalesDict = baseDict(brandPath, 'brand')
    newBrandSalesDict = calculations2(brandSalesDict)
    
    result = newBrandSalesDict['Brand B'][0]
            
    assert ( (result['perc_gross_sales_growth']) == -100  and (result['perc_unit_sales_growth']) == -100 )
    

#no previous period weekly sales
def test_edgeCase2_product():
    productPath = os.getcwd() + '/data/sales_product.csv'
    productSalesDict = baseDict(productPath, 'product_name')
    newProductSalesDict = calculations2(productSalesDict)
    
    
    result = newProductSalesDict['Product B'][5]

    #needs to have both these values null and no previous week commencing data in the list        
    assert ( (result['perc_gross_sales_growth']) == 'null'  and (result['perc_unit_sales_growth']) == 'null' )
    
    
    data = newProductSalesDict['Product B']
  
    for dataDict in data:
        period_id = dataDict['period_id']
        week_commencing = dataDict['week_commencing_date']

        if period_id == 1:
            assert week_commencing != result['previous_week_commencing_date']
    
    
#no previous period weekly sales
def test_edgeCase2_brand():
    brandPath = os.getcwd() + '/data/sales_brand.csv'
    brandSalesDict = baseDict(brandPath, 'brand')
    newBrandSalesDict = calculations2(brandSalesDict)
    
    
    result = newBrandSalesDict['Brand C'][1]

    #needs to have both these values null and no previous week commencing data in the list        
    assert ( (result['perc_gross_sales_growth']) == 'null'  and (result['perc_unit_sales_growth']) == 'null' )
    
    
    data = newBrandSalesDict['Brand C']
  
    for dataDict in data:
        period_id = dataDict['period_id']
        week_commencing = dataDict['week_commencing_date']

        if period_id == 1:
            assert week_commencing != result['previous_week_commencing_date']
    
#no sales for either period        
def test_edgeCase3():
    assert formula(5.5, 5.5) == 'null'
    
    
def test_formula():
    assert formula(45.67, 32.4333) == 40.81
    
    
    
    
    