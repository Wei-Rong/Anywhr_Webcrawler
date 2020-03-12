#!/usr/bin/env python
# coding: utf-8


#import libraries to be used for webscraping and data processing
import urllib.request
import urllib.parse
import urllib.error
from bs4 import BeautifulSoup
import requests
import json
import re
import pandas as pd
import numpy as np


#Chosen source to scrape data from is Wikipedia.
#Their website has extensive list of airports, but separated into different pages.
#Use an alphabet list to enumerate through the webpages to scrape, since all the pages have similar link format.
alphabet_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
                 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
index = 0
airports = []
for item in alphabet_list:
    url = "https://en.wikipedia.org/wiki/List_of_airports_by_IATA_code:_" + alphabet_list[index]
    page = urllib.request.urlopen(url)
    #Webscraping done using BeautifulSoup library, objective is to get the names of all airports first
    soup = BeautifulSoup(page, "lxml")
    #Have to include this if-else check because only the 'W' page has an additional hide button that toggles the table.
    #It has a slightly different class tag as well, thus have to tweak the webscraping search settings.
    if(alphabet_list[index] != 'W'):
        mytables=soup.find("table", class_='wikitable sortable')
    else:
        mytables=soup.find("table", class_='wikitable sortable mw-collapsible')
    #Look between the <tr> and <td> tags of the table to find the names
    #Have to slice the cells list in order to just get the name and not unneeded info, such as location served.
    for row in mytables.findAll('tr'):
        cells = [item.text for item in row.findAll('td')]
        airportnames = cells[2:3]
        airports.append(airportnames)
    index = index + 1


#Remove blank entries obtained from the webscraping
airports = list(filter(None, airports))


#Cleaning the data, such as removing the reference tags on some elements as well as bracketed acronyms
airports = [item[0].replace('[1]','') for item in airports]
airports = [item.replace('[2]','') for item in airports]
airports = [re.sub(r'\(.*\)', '', item) for item in airports]


#Check that the list is correct
print(airports)


#Now, convert the names into links themselves, so that further scraping of each airport page can be done.
#The objective is to get the lat-long coordinates now for each airport if possible.
#This method is possible because the naming for the Wikipedia links are quite standard.
airport_links = [item.replace(' ','_') for item in airports]
airport_links = [urllib.parse.quote(item) for item in airport_links]
airport_links = ['https://en.wikipedia.org/wiki/' + item for item in airport_links]


#Check that the list is correct
print(airport_links)


#Perform webscraping of the individual airport pages.
latlong = []
#missing_indexes list is to keep track of which airports we could not successfully scrape the lat-long coordinates for.
#We will perform address search for these airports later on to try to obtain the lat-long, using Google's Geocoding API.
missing_indexes = []
for index in range(0,len(airport_links)):
    url = airport_links[index]
    try:
        page = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        if e.getcode() == 404 or e.getcode() == 400:
            #Exception raised means not successful in obtaining the lat-long, hence add to missing_indexes list.
            missing_indexes.append(index)
            continue
        raise
    soup = BeautifulSoup(page, "lxml")
    if soup.find("span", class_='geo') is not None:
        latlong.append(soup.find("span", class_='geo').text)
    else:
        missing_indexes.append(index)


#Check that the lat-long coordinates were obtained in the correct format.
print(latlong)


#Total for both lists adds up to 9028, which is correct as it matches the total number of airports we have.
print(len(latlong))
print(len(missing_indexes))


#Check that the indexes are in the correct format, we will use them for iteration later.
print(missing_indexes)


#Split the lat-long into list of lists for concatenation later
latlong_new = [item.split(';') for item in latlong]
print(latlong_new)


#Split the airports into list of lists for concatenation
airports_new = [[item] for item in airports]
concat = []
#Create list of lists to concatenate with those airports we could not find lat-long coordinates for earlier
none_list = ['none; none'] * 9028
none_list = [item.split(';') for item in none_list]
counter = 0
#Combine the lists to get one list with either airport and lat-long coordinates per entry, or airport and 'none, none'.
for index in range(0,len(airports_new)):
    if(index not in missing_indexes):
        concat.append(airports_new[index] + latlong_new[counter])
        counter = counter + 1
    else:
        concat.append(airports_new[index] + none_list[index])


#Check the correctness of the combined list
print(concat)


#Create a pandas dataframe from the list for easier data manipulation to follow
col_names = ["airport name", "lat", "long"]
airport_df = pd.DataFrame(concat, columns = col_names)
print(airport_df)


#Using Google Geocoding API, use the airport names to find lat-long coordinates for those entries that are missing them.
GEONAMES_API = "https://maps.googleapis.com/"
GEONAMES_KEY = "AIzaSyAUMCHSVc8IPYt-JK6Gd1omIAJ46xS7KHM"
#Call the API using the airport name as the search term
feature_url = "{}maps/api/geocode/json?address={{}}&key={}".format(GEONAMES_API, GEONAMES_KEY)
#Only do so for those entries without lat-long coordinates
for element in missing_indexes:
    url = feature_url.format(airport_df.loc[element]['airport name'])
    response = requests.get(url)
    raw_result = json.loads(response.text)
    #Change the value in the dataframe with the result found
    #Have to use try-except, because possibly not all searches will return lat-long results
    try:
        lat_coord = raw_result['results'][0]['geometry']['location']['lat']
        long_coord = raw_result['results'][0]['geometry']['location']['lng']
        airport_df.at[element, 'lat'] = lat_coord
        airport_df.at[element, 'long'] = long_coord
    except:
        continue


#There are still 341 entries that the lat-long coordinates could not be found for, unfortunately.
airport_df[airport_df.values == 'none']


#Now that the lat-long coordinates for almost all airports have been obtained, we can convert them to postal code.
#This is done through reverse geocoding, and 2 reverse geocoding APIs will be used.
#The first is from BigDataCloud
postalcode = []
GEONAMES_API = "https://api.bigdatacloud.net/"
#Call their API and provide latitude and longitude to get postal code
feature_url = "{}data/reverse-geocode-client?latitude={{}}&longitude={{}}&localityLanguage=en".format(GEONAMES_API)
for i, row in airport_df.iterrows():
    #For those entries without lat-long coordinates, set postal code as none, no point to search
    if(airport_df.loc[i, 'lat']=='none'):
        postalcode.append('none')
    else:
        url = feature_url.format(airport_df.loc[i,'lat'],airport_df.loc[i,'long'])
        response = requests.get(url)
        raw_result = json.loads(response.text)
        #try-except because possibly not all queries will have postal code returned
        try:
            postalcode.append(raw_result['postcode'])
        except:
            #if exception raised, means postal code not found, so set as none
            postalcode.append('none')
            continue


#Length is 9028, means all entries accounted for correctly
#Can see alot of 'none' entries, hence have to combine results with other reverse geocoding services
print(len(postalcode))
postalcode.count('none')


#Just to check results
print(postalcode)


#Use 2nd reverse geocoding API service to boost our conversion rate from lat-long to postal code.
#This API service is by LocationIQ
postalcode2 = []
GEONAMES_API = "https://us1.locationiq.com/"
GEONAMES_KEY = "fe85a8d87b9418"
#Call their API while suppling lat-long, same method as above
feature_url = "{}v1/reverse.php?key={}&lat={{}}&lon={{}}&format=json".format(GEONAMES_API, GEONAMES_KEY)
for i, row in airport_df.iterrows():
    if(airport_df.loc[i, 'lat']=='none'):
        postalcode2.append('none')
    else:
        url = feature_url.format(airport_df.loc[i,'lat'],airport_df.loc[i,'long'])
        response = requests.get(url)
        raw_result = json.loads(response.text)
        try:
            postalcode2.append(raw_result['address']['postcode'])
        except:
            postalcode2.append('none')
            continue
print(postalcode2)


#Correct length, can see alot of 'none' entries as well.
print(len(postalcode2))
postalcode2.count('none')


#Combine postal code results obtained from both API services to get a more comprehensive list.
for index in range(0,len(postalcode)):
    if(postalcode[index]=='none'):
        postalcode[index] = postalcode2[index]


#Check results
print(postalcode)


#Correct length, more entries of postal codes now (2328 entries).
print(len(postalcode))
postalcode.count('none')


#Append postal code results to our dataframe, already sorted in correct order
airport_df['postalcode']  = postalcode
print(airport_df)


#Export dataframe as csv
airport_df.to_csv('C:/Users/USER/Desktop/Airport_Postal_Codes.csv', index = False)

