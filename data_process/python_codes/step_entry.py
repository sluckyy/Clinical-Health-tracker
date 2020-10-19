import warnings
import pandas as pd
import argparse
from step_process import *
import datetime
import time
import json

def main():
	warnings.simplefilter(action='ignore', category=FutureWarning)
	parser = argparse.ArgumentParser(description='ST-DBSCAN in Python')
	parser.add_argument('-j', '--jsonfilename', help='Json Input File Name', required=True)
	parser.add_argument('-rp', '--rootDir', help='Server Root Dir', required=True)

	args = parser.parse_args()
	args = parser.parse_args()

	jsonfilename = args.jsonfilename
	rootDir = args.rootDir

	tempfiledir = '../temp'
	# jsonfilename = tempfiledir + '/temp_step1531807739611.json'
	# jsonfilename = '/Users/zhuoyingli/Documents/WebstormProjects/healthtracker/temp/temp_step_1534317527409.json'
	with open(jsonfilename) as data_file:
		json_data = json.load(data_file)

	summaryReport = pd.DataFrame(columns = ['PatientID', 'date', 'stepcount', 'distancewalked', 'activetime', 'averagespeed'])
	inactivetimeRecords = pd.DataFrame(columns = ['PatientID', 'time', 'start', 'end'])


	for patient in json_data:
		currentJson = json_data[patient]
		# currentJson = json_data['58fa83f997078']

		df = pd.DataFrame(currentJson)

		try:
			# df['start'] = df['start'].apply(lambda x:
			#                                 datetime.strptime(x, '%d/%m/%y %H:%M'))
			# df['end'] = df['end'].apply(lambda x:
			#                             datetime.strptime(x, '%d/%m/%y %H:%M'))
			df['start'] =  df['start'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
			df['end'] =  df['end'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))

		except Exception as e:
			# print(patient)
			temp = 1
			# print(df['start'], df['end'])
			# json_data[patient] = {}
			# continue

		summary, inactivetime = stepProcess(df, patient)
		summaryReport = summaryReport.append(summary, ignore_index=True)
		inactivetimeRecords = inactivetimeRecords.append(inactivetime, ignore_index=True)

	resultJSON = {'summary': summaryReport.to_json(orient='records', date_format='iso', date_unit='s'), 'inactivetime': inactivetimeRecords.to_json(orient='records', date_format='iso', date_unit='s')}
	resultJSONstr = json.dumps(resultJSON, sort_keys=True, indent=4, separators=(',', ': '))
	timestr = time.strftime("%Y%m%d-%H%M%S")
	outputFilename = rootDir+"/temp/result_step_{}.json".format(timestr)

	with open(outputFilename, "w") as text_file:
		print(resultJSONstr, file=text_file)
		return print(outputFilename)

	# print(resultJSONstr)


if __name__ == "__main__":
	main()
