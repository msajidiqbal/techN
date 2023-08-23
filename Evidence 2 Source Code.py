"""
Author: M Iqbal
Purpose: Program to perform Complaints Data Cleansing
"""
# load libraries

import pandas as pd
import re
import os
import sys
from datetime import datetime, date
import shutil

# supress the warning messages
pd.options.mode.chained_assignment = None

# functions
def get_data(number):
    """Function to collect data from item master database using item number as a input parameter

    Args:
        number (integer): item number

    Returns:
        tuple: vendor name, product category, product sub category, product name
    """    
    # Function to collect data from item master database using item number as a input parameter
    lines = vendor_data.loc[vendor_data["ITEM NUMBER"] == number]
    if len(lines) >=1:
        vendorname = lines["VENDOR"].iloc[0]
        subcat = lines["SUBCAT"].iloc[0]
        subsubcat = lines["SUBSUBCAT"].iloc[0]
        prodname = lines["ITEM NAME"].iloc[0]
        return (vendorname,subcat,subsubcat,prodname) 
    else:
        return None

def assign_complaintcategory(k):
    """function to update complaint categories starting from New Quarter - 2023

    Args:
        k (string): complaint category assigned by customer service team

    Returns:
        string: updated complaint category
    """    
    try:
        v = str(k).strip()
        if v == "Broken/Damaged":
            return "Damaged Packaging"
        elif v == "Damaged/Crushed":
            return "Damaged Product"
        elif v == "Damaged/Dented":
            return "Damaged Packaging"
        elif v == "Damaged/Cracked":
            return "Damaged Product"
        elif v == "Damaged":
            return "Damaged Product"
        elif v == "Missing/Incorrect Information":
            return "Labelling Issue"
        elif v == "Package/Container Issues":
            return "Damaged Packaging"
        elif v == "Electrical Issue":
            return "Missing/Faulty Components"
        elif v == "Missing Component":
            return "Missing/Faulty Components"
        elif v == "Lower potency than listed":
            return "Potency"
        elif v == "Density":
            return "Product Quality - Feedback"
        elif v == "Dry/Poor Quality":
            return "Product Quality - Feedback"
        elif v == "Inconsistent Burn":
            return "Product Quality - Feedback"
        elif v == "Liquid Appearance":
            return "Product Quality - Feedback"
        elif v == "Sensory":
            return "Product Quality - Feedback"
        elif v == "Strength":
            return "Product Quality - Feedback"
        elif v == "Underfilled":
            return "Weight Variance"
        else:
            return k
    except:
        pass

def get_size(lot_number):
    """Function to return product size using lot number

    Args:
        lot_number (string): lot number of product

    Returns:
        string: product weight
    """    
    if len(new_lot_data.loc[new_lot_data["Batch number"] == str(lot_number).strip()]) >=1:
        lines = new_lot_data.loc[new_lot_data["Batch number"] == str(lot_number).strip()]
        item_number = lines["Item number"].iloc[0]
        size = lines["Size"].iloc[0]
        prodname = lines["Product name"].iloc[0]
        return (size)
    elif len(new_lot_data.loc[new_lot_data["Batch number"] == str(lot_number).strip().upper()]) >=1:
        lines = new_lot_data.loc[new_lot_data["Batch number"] == str(lot_number).strip().upper()]
        item_number = lines["Item number"].iloc[0]
        size = lines["Size"].iloc[0]
        prodname = lines["Product name"].iloc[0]
        return (size)
    elif len(new_lot_data.loc[new_lot_data["Batch number"] == str(lot_number).strip().lower()]) >=1:
        lines = new_lot_data.loc[new_lot_data["Batch number"] == str(lot_number).strip().lower()]
        item_number = lines["Item number"].iloc[0]
        size = lines["Size"].iloc[0]
        prodname = lines["Product name"].iloc[0]
        return (size)
    elif len(new_lot_data.loc[new_lot_data["Batch number"] == (lot_number)]) >=1:
        lines = new_lot_data.loc[new_lot_data["Batch number"] == (lot_number)]
        item_number = lines["Item number"].iloc[0]
        size = lines["Size"].iloc[0]
        prodname = lines["Product name"].iloc[0]
        return (size)
    else:
        return None

def filter_item_number(item):
    """Helper Function to extract Item numbers if item number character len is not 6.
    Alternatively use re library to extract 6 continous integer number

    Args:
        item (string): extract integer number

    Returns:
        string: item number
    """    
    if len(item) == 6:
        return item
    else:
        elements = re.split('- |_|__',item)
        return elements[0]
    
def insert_parameters(cmp_data,col_name,index,clensing_value,err_list):
    """Helper function to assign values, and collect data for statistics

    Args:
        cmp_data (_type_): _description_
        col_name (_type_): _description_
        index (_type_): _description_
        clensing_value (_type_): _description_
        err_list (_type_): _description_
    """    
    if clensing_value != "":
        cmp_data[col_name][index] = clensing_value
    else:
        err_list.append(cmp_data["Case Number"][index])



def assign_category(k):
    """Function to update product categories

    Args:
        k (string): product category

    Returns:
        string: product category
    """    
    try:
        v = str(k).strip()
        if v == "Beverages":
            return "Edibles"
        elif v == "Capsules":
            return "Extracts"
        elif v == "Concentrates":
            return "Extracts"
        elif v == "Oils":
            return "Extracts"
        elif v == "Pre Rolls":
            return "Flower"
        elif v == "Pre-Rolled":
            return "Flower"
        elif v == "Seeds":
            return "Flower"
        elif v == "Clones":
            return "Flower"
        elif v == "Dried Flower":
            return "Flower"
        elif v == "Topicals NPC":
            return "Topicals"
        else:
            return k
    except:
        pass

      
def count_differences(original_data, processed_data,column_name):
    """Helper function to count difference - for statistical purposes

    Args:
        original_data (_type_): _description_
        processed_data (_type_): _description_
        column_name (_type_): _description_

    Returns:
        Integer: number of differences
    """    
    counter = 0
    for i,j in enumerate(original_data['Case Number']):
        if original_data[column_name][i] != processed_data[column_name][i]:
            counter += 1
    return counter

def item_fixes_count(item):
    """Helper Function to count number of fixes for item number

    Args:
        item (integer): item number

    Returns:
        integer: number of correct item numbers
    """    
    if len(item) == 6:
        return 0
    else:
        return 1
# --------------------------- MAIN PROGRAM ----------------------------

# Identify working directory and load files - complaints file, item master file, lot data file

print("working directory: ", os.getcwd())
get_path = os.getcwd()

# read files
vendor_file = get_path + '\\item-master\\data.csv'

print("                                loading files...\n")
# 1 - Vendor Item Master Data file
vendor_data = pd.read_csv(vendor_file, low_memory=False)
print("vendor data is loaded")

#2 - Lot Data - D365
lot_file = get_path + '\\item-master\\On-hand.csv'
lot_data = pd.read_csv(lot_file,low_memory=False )
new_lot_data = lot_data[['Item number','Product name','Size','Batch number']]
print("Lot data is loaded")

#3 - Complaints data and create new columns 
cmp_data = pd.read_csv("complaints.csv", low_memory=False, encoding= 'unicode_escape')
print("Complaints data is loaded")
original_data = cmp_data
cmp_data["Product Subcategory"] = ""
cmp_data["Case Status"] = ""
cmp_data["Priority"] = ""


print("                                Clensing Item Numbers...\n")


cmp_item_numer_issue = []

    
# iterate over all item numbers in cmp_data
item_counter = 0
for index,value in enumerate(cmp_data["Item Number/SKU"]):
    if type(value) == str:
        item_number = filter_item_number(value)
        print("%s<>%s" %(value,item_number))
        item_counter += item_fixes_count(value)
        try:
            cmp_data["Item Number/SKU"][index] = int(item_number)
        except:
            cmp_data["Item Number/SKU"][index] = item_number
    else:
        continue
try:
    cmp_data.to_csv("complaints.csv",index=False)
except:
    print("Please close the complaint file")

cmp_data = pd.read_csv("complaints.csv")

for index,value in enumerate(cmp_data["Item Number/SKU"]):
    if len(str(value)) != 6:
        cmp_item_numer_issue.append((cmp_data["Case Number"][index], cmp_data["Item Number/SKU"][index],cmp_data["Category"][index],))
        


print("       Updating Licensed Producer, Category, Product SubCatergory and Product Name...\n")
# get data from vendor item master and update product name, product sub category, subsub category

# lot_err_list contains all cases with missing/unmatch lot numbers
lot_err_list = []

# itm_err_list contains all cases with no parameters found per item number in vendor item master
itm_err_list = []

for indx,value in enumerate(cmp_data["Item Number/SKU"]):
    clensing_parameters = get_data(value)
    if clensing_parameters !=None:
        cmp_data["Licensed Producer"][indx] = clensing_parameters[0]
        cmp_data["Category"][indx] = clensing_parameters[1]
        cmp_data["Category"][indx] = assign_category(cmp_data["Category"][indx])
        cmp_data["Sub Category"][indx] = assign_complaintcategory(cmp_data["Sub Category"][indx])
        key = cmp_data["Category"][indx]
        cmp_data["Product Subcategory"][indx] = clensing_parameters[2]
        cmp_data["Product Name"][indx] = clensing_parameters[3]
    else:
        itm_err_list.append((cmp_data["Case Number"][indx],cmp_data["Item Number/SKU"][indx],cmp_data["Category"][indx],))

try:
    cmp_data.to_csv("complaints.csv",index=False)
except:
    print("Please close the complaint file")

cmp_data = pd.read_csv("complaints.csv")

print("                            Updating Product Sizes...\n")

for indx,value in enumerate(cmp_data["Item Number/SKU"]):
    lot_number = cmp_data["Lot Number"][indx]
    
    sizes = get_size(lot_number)
    try:
        if sizes != None:
                print(cmp_data["Case Number"][indx])
                cmp_data["Package(Variant) Size"][indx] = sizes.lower()         
        else:
            lot_err_list.append((cmp_data["Case Number"][indx],(cmp_data["Lot Number"][indx]),cmp_data["Category"][indx],))
    except:
        pass
# Date entries update to mm-dd-yyyy format
try: 
    cmp_data["Created On"] = pd.to_datetime(cmp_data["Created On"]).dt.date
    cmp_data["Created On"] = pd.to_datetime(cmp_data["Created On"]).dt.strftime('%m-%d-%Y')

except:
    pass

try: 
    cmp_data["Packaging Date"] = pd.to_datetime(cmp_data["Packaging Date"]).dt.date
    cmp_data["Packaging Date"] = pd.to_datetime(cmp_data["Packaging Date"]).dt.strftime('%m-%d-%Y')

except:
    pass

try: 
    cmp_data["Purchase Date"] = pd.to_datetime(cmp_data["Purchase Date"]).dt.date
    cmp_data["Purchase Date"] = pd.to_datetime(cmp_data["Purchase Date"]).dt.strftime('%m-%d-%Y')

except:
    pass


reported_date_variance = []
for i,j in enumerate(cmp_data["Case Number"]):
    try:
        if len(cmp_data["Created On"][i]) != 10 and not None:
            veriable1 = (cmp_data["Case Number"][i],cmp_data["Created On"][i])
            reported_date_variance.append(veriable1)
    except:
        veriable1 = (cmp_data["Case Number"][i],cmp_data["Created On"][i])
        reported_date_variance.append(veriable1)
        
packaging_date_variance = []
for i,j in enumerate(cmp_data["Case Number"]):
    try:
        if len(cmp_data["Packaging Date"][i]) != 10 and not None:
            variable = (cmp_data["Case Number"][i],cmp_data["Packaging Date"][i])
            packaging_date_variance.append(variable)
    except:
        variable = (cmp_data["Case Number"][i],cmp_data["Packaging Date"][i])
        packaging_date_variance.append(variable)
        
purchase_date_variance = []
for i,j in enumerate(cmp_data["Case Number"]):
    try:
        if len(cmp_data["Purchase Date"][i]) != 10 and not None:
            variable2 = (cmp_data["Case Number"][i],cmp_data["Purchase Date"][i])
            purchase_date_variance.append(variable2)
    except:
        variable2 = (cmp_data["Case Number"][i],cmp_data["Purchase Date"][i])
        purchase_date_variance.append(variable2)


try:
    cmp_data.to_csv("complaints.csv",index=False)
except:
    print("Please close the complaint file")

cmp_data = pd.read_csv("complaints.csv")

now = datetime.now()
dt_string1 = now.strftime("%b-%d-%Y-%H-%M-%S")

today = date.today()
d4 = today.strftime("%b-%d-%Y")



final_file_name = dt_string1+"-cleansed-complaints.csv"
try:
    cmp_data.to_csv(final_file_name,index=False)
except:
    print("Please close the complaint file")

shutil.move(final_file_name,'cleansed-file/'+final_file_name)

weekly_report_data = cmp_data[["Line of Business","Created On","Case Number","Case Status","Priority","Category","Product Subcategory","Sub Category","Licensed Producer","Product Name","Item Number/SKU","Lot Number","Package(Variant) Size","Packaging Date"]]

weekly_file_name = dt_string1+"-cleansed-short-complaints.csv"
try:
    weekly_report_data.to_csv(weekly_file_name,index=False)
except:
    print("Please close the complaint file")

shutil.move(weekly_file_name,'cleansed-file/'+weekly_file_name)

processed_data = cmp_data

# write log files

filename = dt_string1+'-log.txt'
stdoutOrigin=sys.stdout 
sys.stdout = open(filename, "w")

# mm/dd/YY H:M:S
print("Timestamp")
now = datetime.now()
dt_string = now.strftime("%m/%d/%Y %H:%M:%S")
print("date and time =", dt_string)
print("Total Complaints = ", len(cmp_data["Case Number"]))
print("\n")

print("Complaints with Item Number/SKU issue \n")
for i in cmp_item_numer_issue:
    print(i)
print("\n")

print("All cases require cleansing due to unmatch item number - Total = ",len(set(itm_err_list)) )
for i in set(itm_err_list):
    print(i)
print("\n")    

print("All cases require cleansing due to unmatch lot number - Total = ", len(set(lot_err_list)))
for i in set(lot_err_list):
    print(i)
print("\n")
print("---------------Detailed Statistics-------------")
print("total item number fixes: ", item_counter)
total_clicks = 0
for col_name in original_data.columns:
    if original_data[col_name].equals(processed_data[col_name]) == False:
        count_value = count_differences(original_data, processed_data,col_name)
        print("Data Type: %s -- # of changes made: %s" %(col_name,count_value))
        total_clicks += count_value
print("Number of changes made: ", total_clicks)
print()
print("------Complaints not complying with desired date format mm-dd-yyyy ------")
print("date variance in date reported \n")
print("Complaint | Date Issue ")
for i in reported_date_variance:
    print(i)

print("date variance in packaging date \n")
print("Complaint | Date Issue ")
for i in packaging_date_variance:
    print(i)

print("date variance in purchase date: \n")
print("Complaint | Date Issue ")
for i in purchase_date_variance:
    print(i)

sys.stdout.close()
sys.stdout=stdoutOrigin
shutil.move(filename,'logs/'+filename)
print("                            All Done       ")
sys.exit()
