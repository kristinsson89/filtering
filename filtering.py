#!/usr/bin/python
# IMPORT PACKAGES
from google.cloud import bigquery
import pandas as pd
from google.cloud import storage
import numpy as np
from cStringIO import StringIO
import sys
#import googlemaps # Google maps LONG and LAT #google_api = 'AIzaSyAFcmro7rWaTvoheACQLa73Kq34I7yU0vU' 

if (len(sys.argv) == 1):
  sys.exit("Usage: python filtering.py [number of expected rows]") 

if (sys.argv[1] == "help" or sys.argv[1] == "h"):
  sys.exit("Usage: python filtering.py [number of expected rows]")
  
file_name = sys.argv[1]# 'Assure Funding Merged County.csv'
results = int(sys.argv[2])

print "------------------------------------------"
print "------------------------------------------"
print " "
print "Filtering %s " % file_name
print " "

print "Expecting %s rows" % results
print " "
print "------------------------------------------"
print "------------------------------------------"

# Set up the project
client = storage.Client(project='starry-braid-156516')
bucket = client.get_bucket('starry-braid-156516')



# Set up the storage bucket
blob = storage.Blob(file_name, bucket)
content = blob.download_as_string() # Download as string
data = pd.read_csv(StringIO(content), sep = ",") # Read into pandas

company = tuple(set(data['Company']))
state = tuple(set(data['State']))
city = tuple(set(data['City']))

# Remove from company strings

company_fix = (str(company).replace(', LLC','').replace(',LLC','').replace(', LLC','').replace(' ,LLC','').replace('LLC','')
               .replace(', L.L.C.','').replace(' L.L.C.','').replace('L.L.C.','').replace(' L.L.C.','').replace(' L. L. C.','')
               .replace(' L.L.','').replace(' L.C.','').replace(' L.L.C','').replace(' Llc','').replace(', Lcc','')
               .replace(', Inc.','').replace(',Inc.','').replace('(inc)','').replace('( inc)','').replace(' Inc','').replace('inc','')
               .replace('(usa)','').replace('(ca)','')
               .replace(' ,.ltd','').replace(' llp','').replace(' lp','').replace(' L.P.','')
               .replace(' corp','').replace(' co','').replace(' co ','').replace(' CORP.','').replace(' CO.','')
               .replace(' CO..','').replace(' CO','').replace(' ctr','').replace(' Court','').replace('Co','')
               .replace(' Svc','').replace('rp','')
               .replace(' P.C.','')
               .replace('ltd','').replace(' LTD.','').replace(', LTD','').replace(', Ltd','').replace(' Ltd', '')
               .replace(' md','').replace(' es','').replace(' Dept','')
               .replace(' . ','').replace(' .','').replace('1)','').replace('.','').replace('#','')
               .replace('','').replace('|','').replace(' . ','').replace('&amp;','&'))

st = data.groupby(['State'])['State'].count()
twenty = float(len(data))*0.2
pop_state = st[st>twenty].index.values.tolist()

query = "SELECT * FROM acuteiq.tbl_company WHERE UPPER(Company_Name) IN %s AND UPPER(state) IN %s AND UPPER(City) IN %s" % (str(company).upper(), str(state).upper(), str(city).upper())

query2 = "SELECT * FROM acuteiq.tbl_company WHERE UPPER(company_name_cleaned_2) IN %s AND UPPER(state) IN %s AND UPPER(City) IN %s" % (str(company_fix).upper(), str(state).upper(), str(city).upper())

data = pd.read_gbq(query, "starry-braid-156516")

data2 = pd.read_gbq(query2, "starry-braid-156516")

data = data.append(data2).drop_duplicates()

states = tuple(set(data['state'])) # Make a states list, distribution in the futur also
county = tuple(set(data['county'])) # Make a county list
company = tuple(set(data['id']))

states_list = []
for item in states:
  if str(item) != 'None':
    states_list.append(str(item))

county_list = []
for item in county:
  if str(item) != 'None':
    county_list.append(str(item))

print 
# Original size of company database so we know how much % we have left. If there is too little left then we might tell
# the code to losen the boundaries 
originalSize = 21326910 #Want to update always but for now I will let this do.

firstQuery = "SELECT count(*) as count FROM acuteiq.tbl_company WHERE contact_name NOT LIKE 'NULL' AND email NOT LIKE 'NULL'  AND id NOT IN %s AND state IN " % str(company)

#Make a query to filter out where companies are not in the same state and county as in data (Might be to much)
def states_county(step):
  if(step == 0):
    query = firstQuery + str(tuple(states_list)) + 'AND county IN ' + str(tuple(county_list))
    print "-----------------------------------"
    print "Filtered by STATE AND COUNTY."
    print "-----------------------------------"
  else:
    query = firstQuery + str(tuple(states_list))
    print "-----------------------------------"
    print "Filtered by STATE."
    print "-----------------------------------"
  
  df = pd.read_gbq(query, "starry-braid-156516")
  return df, query

# Get query
df, query_SC = states_county(0)

# Find size after state and county filtering

print "-----------------------------------"
print "SIZE = %s" % df['count'][0]
print "-----------------------------------"

# Don't know if 20% is something to aim for, but will start with that. Then take out county, just filter by states
if (float(df['count'][0])/float(originalSize) < 0.3):
  df, query_SC = states_county(1)
  print "-----------------------------------"
  print "STATE AND COUNTY to narrow, filtering just by STATE. SIZE = %s" % df['count'][0]
  print "-----------------------------------"

# Function getting the query with sic codes
def sic_code(sub):
  sic_code = tuple(set(data['industry_sic_code'].apply(str).str[0:sub]))
  sic = []
  for x in sic_code:
    if x.isdigit():
      sic.append(x)  
  query = query_SC + 'AND SUBSTR(industry_sic_code,1,%s) IN '%sub + str(tuple(sic))
  df = pd.read_gbq(query, "starry-braid-156516")
  return df, query


df, query_SIC = sic_code(2)
print "-----------------------------------"
print "Filtered by SIC code XX. SIZE = %s" % df['count'][0]
print "-----------------------------------"

# Get sic codes from data and just select first two numbers in the sic code
if (float(df['count'][0])/float(originalSize) < 0.005):
  df, query_SIC = sic_code(1)
  print "-----------------------------------"
  print "SIC code to narrow"
  print "-----------------------------------"
  print "Filtered by SIC code X. SIZE = %s" % df['count'][0]
  print "-----------------------------------"
elif (float(df['count'][0])/float(originalSize) > 0.15):
  df, query_SIC = sic_code(3)
  print "-----------------------------------"
  print "SIC code too broad. SIZE = %s" % df['count'][0]
  print "-----------------------------------"
  print "Filtered by SIC code XXX. SIZE = %s" % df['count'][0]
  print "-----------------------------------"
  
  # Employees number calculations for while loop
mean_emp = data['number_of_employees'].mean() #Mode gives zero, Mean makes more sense at least here
std_emp = data['number_of_employees'].std()

# Yearly sales calculations for while loop
mean_sales_non0 = data['yearly_sales'][data['yearly_sales']!=0]
mean_sales =  mean_sales_non0.mean()
std_sales =  mean_sales_non0.std()

# State county calculations for while loop
SC = data.groupby(['state','county']).count()['id']

# VARIABLES FOR WHILE LOOP
x = 1
y = (SC > 1).sum()
if (y > 10):
  y = 10
count = 0

#Find upper and lower limits

alpha = (0.2*results)

if (alpha < 50):
  upper = results + 50
else:
  upper = results + alpha


while (df['count'][0] > (upper) or df['count'][0] < results ): 

  empMIN = mean_emp - x*std_emp
  empMAX = mean_emp + x*std_emp

  query_EMP = query_SIC + ' AND number_of_employees BETWEEN %s AND %s' % (empMIN, empMAX)
  #df = pd.read_gbq(query_EMP, "starry-braid-156516")

#   print "-----------------------------------"
#   print "Filtered by number of employees. SIZE = %s" % df['count'][0]
#   print "-----------------------------------"

  salesMIN = mean_sales - x*std_sales
  salesMAX = mean_sales + x*std_sales

  query_SALES = query_EMP + ' AND yearly_sales BETWEEN %s AND %s AND yearly_sales != %s' % (salesMIN, salesMAX,0)
  #df = pd.read_gbq(query_SALES, "starry-braid-156516")

#   print "-----------------------------------"
#   print "Filtered by yearly sales. SIZE = %s" % df['count'][0]
#   print "-----------------------------------"

  SC = data.groupby(['state','county']).count()['id']
  SC = SC.sort_values(ascending = False)
  SC = str(tuple(SC.index[0:y]))
      
  SC = SC.replace("u'", "").replace("(", "('").replace("('('", "(('").replace("')')", "'))").replace("),')","))").replace("',",",")
          
  if (len(pop_state) < 1):
    query_SC = query_SALES + ' AND CONCAT(state,\', \',county) IN %s' % SC
  else:
    query_SC = query_SALES + ' AND (CONCAT(state,\', \',county) IN %s OR state IN %s)' % (SC, tuple(pop_state))
    #print query_SC                      
  df = pd.read_gbq(query_SC, "starry-braid-156516")
                            
  print "-----------------------------------"
  print "SIZE = %s" % df['count'][0]
  print "x = %s" % x
  print "y = %s" % y
  print "-----------------------------------"

  if (df['count'][0] > (upper)):
    x = x*0.7
    if (count % 2 == 0):
      y = y - 1
      if (y < 1):
        y = 1
  elif (df['count'][0] < (results)):
    x = x*1.1
    #y = y + 1
  if (count >= 15):
    break
  else:
    count = count + 1                                        

getAll = "SELECT Company_Name, Company_Address, city, county, state, zip, phone, industry_sic_code, sic_descripton, contact_name, contact_job_title, email, number_of_employees, yearly_sales" + query_SC[24:]
df = pd.read_gbq(getAll, "starry-braid-156516")

final = df.sample(results)

print "-----------------------------------"
print "Final results have %s rows" % len(final)
print "-----------------------------------"


### EXPORT TO BUCKET

# Write the DataFrame to GCS (Google Cloud Storage)

file_dir = "data_files" +"/"+ "RESULTS_" + file_name.replace(" ", "_")

final.to_csv(file_dir, index = False)

import os
os.system("gsutil cp %s gs://starry-braid-156516" % file_dir)

# Create storage bucket if it does not exist
#if not bucket.exists():
#    bucket.create()
#storage write --variable df --object $bucket_object

print "-----------------------------------"
print "RESULTS IN BUCKET %s" % file_dir
print "-----------------------------------"