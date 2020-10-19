# coding=UTF-8
#!/usr/bin/python
import datetime
import pandas as pd
import argparse
from stdbscan import *
from gis_process import *
import sys, json
import time
import warnings
import logging
"""
    The minimal command to run this algorithm is:
    $ python main.py -f sample.csv

    Or could be executed with advanced configurations:
    $ python main.py -f sample.csv -p 5 -s 500 -t 60

    In the current momment, the dataset must have the
    'latitude', 'longitude' and 'date_time' columns, but
    if you want, can be easily changed.

"""

def parse_dates(x):
    return datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')

def main():

    warnings.simplefilter(action='ignore', category=FutureWarning)
    parser = argparse.ArgumentParser(description='ST-DBSCAN in Python')
    parser.add_argument('-j','--jsonfilename', help='Json Input File Name', required=True)
    parser.add_argument('-rp','--rootDir', help='RootDir', required=True)
    parser.add_argument('-p','--minPts',   help='Minimum number of points', required=False, type=int, default=15)
    parser.add_argument('-s','--spatial',  help='Spatial Threshold (in meters)', required=False, type=float, default=500)
    parser.add_argument('-t','--temporal', help='Temporal Threshold (in seconds)', required=False, type=float, default=60)
    args = parser.parse_args()
    args = parser.parse_args()

    jsonfilename = args.jsonfilename
    rootDir = args.rootDir
    minPts = args.minPts
    spatial_threshold   = args.spatial
    temporal_threshold  = args.temporal
    args = parser.parse_args()
    # print(jsonfilename)
    # tempfiledir = '../temp'
    # jsonfilename = tempfiledir + '/temp_step_1533799712235.json'
    import json
    with open(jsonfilename) as data_file:
        josn_data = json.load(data_file)
    # print(josn_data)
    summary = []
    LocationInfo = pd.DataFrame(columns = ['start', 'end', 'longitude', 'latitude','cluster', 'PatientID'])
    Locations = pd.DataFrame(columns = ['date_time',  'latitude', 'longitude', 'cluster', 'PatientID'])
    for patient in josn_data:
        currentJson = josn_data[patient]
        # currentJson = josn_data['58fa83f997078']
        df = pd.DataFrame(currentJson)
        try:
            # df['date_time'] =  df['date_time'].apply(lambda x:
            #                                          datetime.datetime.strptime(x,'%d/%m/%y %H:%M:%S'))
            df['date_time'] =  df['date_time'].apply(lambda x:
                                                     datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
        except Exception as e:
            print(str(e))
	        # continue
            # print(patient)
            # print(df['date_time'])


        datetimeIndex = df['date_time'].apply(lambda x:datetime.datetime.strftime(x,'%d/%m/%y')).drop_duplicates()


        for date in datetimeIndex:
	        mask = (df['date_time'] >= (datetime.datetime.strptime(date,'%d/%m/%y'))) & (df['date_time'] < (datetime.datetime.strptime(date,'%d/%m/%y') + datetime.timedelta(days = 1)))
	        datedf = df[mask]
	        # print(date)
	        # print(datedf)
	        try:
		        jsonResult = GISProcess(datedf)
		        # print(jsonResult)
	        except Exception as e:
		        jsonResult = {'PatientID': patient, 'date':date};
		        summary.append(jsonResult)
		        # josn_data[patient] = {}
		        # print(patient + " skipped")
		        # print('Failed : '+ str(e))
		        # continue

	        LocationResult = st_dbscan(datedf, spatial_threshold, temporal_threshold, minPts)
	        LocationResult['PatientID'] = patient
	        Locations = Locations.append(LocationResult, ignore_index=True)

	        TTD = calculate_total_travel_distances(LocationResult)
	        tldResult = calculate_total_location_distances(LocationResult)
	        locationInfo, TLD =  tldResult
	        locationInfo['PatientID'] = patient
	        # print(locationInfo)
	        LocationInfo = LocationInfo.append(locationInfo, ignore_index=True)

	        jsonResult['TTD'] = TTD
	        jsonResult['TLD'] = TLD
	        jsonResult['PatientID'] = patient
	        jsonResult['date'] = datetime.datetime.strftime(datetime.datetime.strptime(date, '%d/%m/%y'),'%Y-%m-%d %H:%M:%S');
	        summary.append(jsonResult)
	        # s = pd.Series({'CenterX':jsonResult['CenterX'],  'CenterY':jsonResult['CenterY'], 'SDEx': jsonResult['SDEx'],
	        #                'SDEy':jsonResult['SDEy'], 'AngleRotation':jsonResult['AngleRotation2'], 'SDELength':jsonResult['SDELength'],
	        #                'SDEArea':jsonResult['SDEArea'],  'MCPArea':jsonResult['area'], 'date': date})


	        # break
        # break

    resultToBeStored = {'summary':summary, 'locations': Locations.to_json(orient='records', date_format='iso', date_unit='s'), 'locationInfo':LocationInfo.to_json(orient='records', date_format='iso', date_unit='s')}
    # print(resultToBeStored)
        # josn_data[patient] = temp


#     df =  pd.read_json(jsonline, orient='records');
# #    df['date_time'] = parse_dates(df['date_time'])
#
#     result = mcp_area(df)
#     result = st_dbscan(df, spatial_threshold, temporal_threshold, minPts)
#
#
#     result = calculate_total_travel_distances(result)



    resultJSONstr = json.dumps(resultToBeStored, sort_keys=True, indent=4, separators=(',', ': '))
    timestr = time.strftime("%Y%m%d-%H%M%S")
    outputFilename = rootDir+"/temp/result_gis_{}_{}_{}_{}.json".format(spatial_threshold,
                                                  temporal_threshold,
                                                  minPts,
                                                  timestr)
    with open(outputFilename, "w") as text_file:
        print(resultJSONstr, file=text_file)
        return print(outputFilename)

    # output_name = "result_{}_{}_{}_{}.csv".format(spatial_threshold,
    #                                               temporal_threshold,
    #                                               minPts,
    #                                               timestr)
    # result.to_csv(output_name,index=False,sep=';')
    # print(resultJSONstr)
    # print(outputFilename)


if __name__ == "__main__":
    main()