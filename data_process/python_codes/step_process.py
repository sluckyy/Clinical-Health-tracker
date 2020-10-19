import datetime
import pandas as pd



def stepProcess(df, PatientID):
    df = df.sort_values(['start'], ascending=[True])
    summaryreport = pd.DataFrame(columns = ['PatientID', 'date', 'stepcount', 'distancewalked', 'activetime', 'averagespeed'])
    lastindex = -1
    inactivetime = pd.DataFrame(columns = ['PatientID', 'time', 'start', 'end'])
    # variables for calculating daily report data
    currentDate = -1
    currentstep = 0
    currentdistance = 0
    currenttime = 0
    for index, row in df.iterrows():
        # initialise variables for the loop
        if (currentDate == -1):
            currentstep = 0
            currentdistance = 0
            currenttime = 0
            currentDate = row['start'].date()

        if (currentDate != row['start'].date()):
            if currenttime != 0.0:
                currentspeed = currentdistance / currenttime
            else:
                currentspeed = 0.0
            s = pd.Series({'PatientID': PatientID, 'date': currentDate, 'stepcount':currentstep, 'distancewalked':currentdistance, 'activetime':currenttime, 'averagespeed':currentspeed})
            summaryreport = summaryreport.append(s, ignore_index=True)
            currentstep = 0
            currentdistance = 0
            currenttime = 0
            currentDate = row['start'].date()

        currentstep += int(row['steps'])
        currentdistance += float(row['distance'])
        currenttime += (row['end'] - row['start']).seconds / 60.0


        # calculate inactive between the current and last records
        if(lastindex != -1):
            start = df.loc[lastindex]['end']
            end = row['start']
            internal = (end - start).seconds / 60
            if (internal >= 60.0):
                s = pd.Series({'PatientID': PatientID, 'time': internal, 'start':start, 'end':end})
                inactivetime = inactivetime.append(s, ignore_index=True)
        lastindex = index


    # store the data of last date for the above loop
    if currenttime != 0.0:
            currentspeed = currentdistance / currenttime
    else:
        currentspeed = 0.0
    s = pd.Series({'PatientID': PatientID, 'date': currentDate, 'stepcount':currentstep, 'distancewalked':currentdistance, 'activetime':currenttime, 'averagespeed':currentspeed})
    summaryreport = summaryreport.append(s, ignore_index=True)

    # # handle the last record as above
    # start = df.loc[lastindex]['end']
    # end =  datetime.datetime((start+ datetime.timedelta(days=1)).year, (start+ datetime.timedelta(days=1)).month, (start+ datetime.timedelta(days=1)).day, 0,0,0,0)
    #
    # if( start < end) :
    #     internal = (end - start).seconds / 60
    #     if (internal >= 60.0):
    #         s = pd.Series({'time': internal, 'start':start, 'end':end})
    #         inactivetime = inactivetime.append(s, ignore_index=True)

    # summaryreport['PatientID'] = PatientID
    # inactivetime['PatientID'] = PatientID
    # result = {'summary':summaryreport.to_json(orient='records', date_format='iso', date_unit='s' ), 'inactivetime':inactivetime.to_json(orient='records', date_format='iso', date_unit='s')}
    result = (summaryreport, inactivetime)

    return result



# def stepProcess(df):
#     startDate = df['start'].min()
#     endDate = df['end'].max()
#     dayDiff = (endDate - startDate).days
#     restinginternals = pd.DataFrame(columns = ['time', 'start', 'end'])
#     # print(df)
#     for day in range(0, dayDiff):
#         thisDate = (startDate + datetime.timedelta(days=day))
#         currentDay = df[df['start'].dt.date == thisDate.date()]
#         dailystepcount = sum((currentDay['steps']).astype(int))
#         dailydistancewalked = sum((currentDay['distance']).astype(float))
#         dailytimeinternals = currentDay['end'] - currentDay['start']
#         dailytimeactive = sum(dailytimeinternals.dt.seconds) / 60.0
#         if dailytimeactive != 0.0:
#             avgspeed = dailydistancewalked / dailytimeactive
#         else:
#             avgspeed = 0.0
#         # print( dailystepcount, dailydistancewalked, dailytimeactive , avgspeed)
#         zeroofday = datetime.datetime(thisDate.year, thisDate.month, thisDate.day, 0,0,0,0)
#         zeroofnextday = datetime.datetime((thisDate+ datetime.timedelta(days=1)).year, (thisDate+ datetime.timedelta(days=1)).month, (thisDate+ datetime.timedelta(days=1)).day, 0,0,0,0)
#         lastindex = -1
#         currentDayrestinginternals = pd.DataFrame(columns = ['time', 'start', 'end'])
#         for index, row in currentDay.iterrows():
#             if(lastindex == -1):
#                 start = zeroofday
#                 end = row['start']
#             else:
#                 start = currentDay.loc[lastindex]['end']
#                 end = row['start']
#             internal = (end - start).seconds / 60
#
#
#             lastindex = index
#             # print (start , end , internal)
#             if (internal != 0.0):
#                 s = pd.Series({'time': internal, 'start':start, 'end':end})
#                 currentDayrestinginternals = currentDayrestinginternals.append(s, ignore_index=True)
#             # print(index ,row)
#             # break
#
#         start = currentDay.loc[lastindex]['end']
#         end = zeroofnextday
#         if( start < end) :
#             internal = (end - start).seconds / 60
#
#             if (internal != 0.0):
#                 s = pd.Series({'time': internal, 'start':start, 'end':end})
#                 currentDayrestinginternals = currentDayrestinginternals.append(s, ignore_index=True)
#
#
#         dailytimeinactive = sum(currentDayrestinginternals['time'])
#         print(dailytimeactive, dailytimeinactive, dailytimeactive+dailytimeinactive)
#         restinginternals = restinginternals.append(currentDayrestinginternals, ignore_index=True)
#     print(restinginternals)