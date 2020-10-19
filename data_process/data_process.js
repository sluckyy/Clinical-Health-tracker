const rmquery = require('../server/db/rmdbquery')
const query = require('../server/db/dbquery')
const GreatCircle = require('great-circle')
const fs = require('fs')
const path = require('path')
const appDir = path.dirname(require.main.filename)
const rootDir = appDir.replace('/server/data', '')
const winston = require('winston')
const {format, createLogger} = require('winston')
const { combine, timestamp, prettyPrint } = format
const {minimunPts, spatialThreshold, temporalThreshold, python_env} = require('./config')
const logger = createLogger({
  format: combine(
    timestamp(),
    prettyPrint()
  ),
  transports: [
    new winston.transports.File({ filename: '../logs/dataProcess.log' })
  ]
})

// const dailyUpdateByID = async (PatientID) => {
//   var firstDay = '2017-12-31'
//   // firstDay = '2018-4-1'
//   var lastDay = '2018-08-02'
//   var currentDay = firstDay
//   while (new Date(currentDay) < new Date(lastDay)) {
//     var datesForThisUpdate = await getUpdateDatesTest(PatientID, currentDay)
//     if (datesForThisUpdate === undefined) return
//     console.log(datesForThisUpdate)
//     var recordFromGIS = await GISDataUpdate(PatientID, datesForThisUpdate)
//     var recordFromSTEP = await STEPDataUpdate(PatientID, datesForThisUpdate)
//     var result = await TAHUpdate2(PatientID, datesForThisUpdate)
//     await updatePatientData(PatientID, datesForThisUpdate)
//     var nextDay = toLocaleTime(currentDay, true, 1)
//     currentDay = nextDay
//   }
// }
//
// const dailyUpdateAll = async () => {
//   var IOSPatients = await rmquery.retriveAllPatientIDsIOS()
//   var AndroidPatients = await rmquery.retriveAllPatientIDsAndroid()
//   var allPatients = IOSPatients.concat(AndroidPatients).unique()
//
//   var firstDay = '2017-12-31'
//   // firstDay = '2018-4-1'
//   var lastDay = '2018-08-02'
//   var currentDay = firstDay
//   while (new Date(currentDay) < new Date(lastDay)) {
//     for (var index = 0; index < allPatients.length; ++index) {
//       var PatientID = allPatients[index]['unique_id']
//       var datesForThisUpdate = await getUpdateDatesTest(PatientID, currentDay)
//
//       if (datesForThisUpdate === undefined) return
//       console.log(datesForThisUpdate)
//       var recordFromGIS = await GISDataUpdate(PatientID, datesForThisUpdate)
//       var recordFromSTEP = await STEPDataUpdate(PatientID, datesForThisUpdate)
//       var result = await TAHUpdate(PatientID, datesForThisUpdate)
//       await updatePatientData(PatientID, datesForThisUpdate)
//     }
//     var nextDay = toLocaleTime(currentDay, true, 1)
//     currentDay = nextDay
//   }
// }
//

// main entry of update all patients
const updateAllUsers = async () => {
  var IOSPatients = await rmquery.retriveAllPatientIDsIOS()
  var AndroidPatients = await rmquery.retriveAllPatientIDsAndroid()
  var allPatients = IOSPatients.concat(AndroidPatients).unique()
  // console.log(allPatients)
  // for(var index in allPatients){
  // return
  for (var index = 0; index < allPatients.length; ++index) {
    var PatientID = allPatients[index]['unique_id']
    // console.log(PatientID)
    await UserBasedUpdate(PatientID)
  }
  logger.log('info', 'Update Finished!')
}
//
// const getUpdateDatesTest = async (PatientID, date) => {
//   var OnlineLastRecordIOS = await rmquery.readLocationDatalastRecordByID([PatientID])
//   var OnlineLastRecordA = await rmquery.readAndroidLocationDataLastRecordByID([PatientID])
//
//   var newLastRecordIOS, newLastRecordA
//   if (OnlineLastRecordIOS[0] !== undefined) newLastRecordIOS = OnlineLastRecordIOS[0]['last']
//   if (OnlineLastRecordA[0] !== undefined) newLastRecordA = OnlineLastRecordA[0]['last']
//
//   var newlastRecord = date
//
//   var newCompletedDate = toLocaleTime(newlastRecord, true)
//
//   var LocalRecord = await query.readPatientRecordByID([PatientID])
//   if (LocalRecord.length !== 0) {
//     if (new Date(newlastRecord) === new Date(LocalRecord[0]['lastRecord'])) return undefined
//   }
//   if (LocalRecord.length === 0) {
//     var lastCompletedDate = '2002-01-01'
//     var newPatientRecord = []
//     newPatientRecord.push([PatientID, lastCompletedDate, lastCompletedDate, 'Rain'])
//     await query.createPatientRecord([newPatientRecord])
//   } else {
//     var lastCompletedDate = toLocaleTime(LocalRecord[0]['lastCompletedDate'])
//   }
//
//   var datesForThisUpdate = {
//     'PatientID': PatientID,
//     'lastCompletedDate': lastCompletedDate,
//     'thisCompletedDate': newCompletedDate,
//     'thisLastRecord': newlastRecord
//   }
//   return datesForThisUpdate
// }

const getUpdateDates = async (PatientID) => {
  var OnlineLastRecordIOS = await rmquery.readLocationDatalastRecordByID([PatientID])
  var OnlineLastRecordA = await rmquery.readAndroidLocationDataLastRecordByID([PatientID])

  var newLastRecordIOS, newLastRecordA
  if (OnlineLastRecordIOS[0] !== undefined) newLastRecordIOS = OnlineLastRecordIOS[0]['last']
  if (OnlineLastRecordA[0] !== undefined) newLastRecordA = OnlineLastRecordA[0]['last']

  var newlastRecord
  if (new Date(newLastRecordA) > new Date(newLastRecordIOS)) {
    newlastRecord = newLastRecordA
  } else {
    newlastRecord = newLastRecordIOS
  }
  var newCompletedDate = toLocaleTime(newlastRecord, true)

  var LocalRecord = await query.readPatientRecordByID([PatientID])
  if (LocalRecord.length !== 0) {
    if (new Date(newlastRecord).getTime() === new Date(LocalRecord[0]['lastRecord']).getTime()) return undefined
  }
  if (LocalRecord.length === 0) {
    var lastCompletedDate = '2005-01-01'
    var newPatientRecord = []
    newPatientRecord.push([PatientID, lastCompletedDate, lastCompletedDate])
    await query.createPatientRecord([newPatientRecord])
  } else {
    var lastCompletedDate = toLocaleTime(LocalRecord[0]['lastCompletedDate'])
  }

  var datesForThisUpdate = {
    'PatientID': PatientID,
    'lastCompletedDate': lastCompletedDate,
    'thisCompletedDate': newCompletedDate,
    'thisLastRecord': newlastRecord
  }
  return datesForThisUpdate
}


// entry of update a patient's gis and step data
const UserBasedUpdate = async (PatientID) => {
  var datesForThisUpdate = await getUpdateDates(PatientID)
  if (datesForThisUpdate === undefined) {
    logger.log('info', PatientID + ': no update')
    return
  }
  // console.log(datesForThisUpdate)
  var recordFromGIS = await GISDataUpdate(PatientID, datesForThisUpdate)
  var recordFromSTEP = await STEPDataUpdate(PatientID, datesForThisUpdate)

  // var result =  await TAHUpdate(PatientID, datesForThisUpdate)
  var result = await TAHUpdate2(PatientID, datesForThisUpdate)

  await updatePatientData(PatientID, datesForThisUpdate)
}


const updatePatientData = async (PatientID, datesForThisUpdate) => {
  await query.updatePatientRecordByID([datesForThisUpdate['thisLastRecord'], datesForThisUpdate['thisCompletedDate'], PatientID])
}

// process gis data after python
const processGISResultData = async (resultfilename) => {
  var processedData = JSON.parse(fs.readFileSync(resultfilename.replace('\n', ''), 'utf8'))
  var jsonArraySummary = []
  var locationInfoRecords = []
  var locationsRecords = []
  // if(patcientId === "" || patcientId === undefined) continue
  // console.log('this: ' + patcientId)
  var temp1 = processedData['summary']
  if (temp1 === undefined) temp1 = {
    'PatientID': patcientId
  }
  var temp2 = processedData['locationInfo']
  if (temp2 === undefined) temp2 = {}
  var temp3 = processedData['locations']
  if (temp3 === undefined) temp3 = {}

  // if(temp1 !== {})jsonArraySummary.push(temp1)
  jsonArraySummary = temp1
  if ((typeof temp2) === "string" && (temp2 !== "")) locationInfoRecords = locationInfoRecords.concat(JSON.parse(temp2))
  if ((typeof temp3) === "string" && (temp3 !== "")) locationsRecords = locationsRecords.concat(JSON.parse(temp3))
  // return
  if (locationInfoRecords.length !== 0) {
    var locationInfoRecordsProcessed = []
    // for (var index in locationInfoRecords) {
    for (var index = 0; index < locationInfoRecords.length; ++index) {
      var element = locationInfoRecords[index]
      var PatientID = null
      if (element['PatientID'] !== undefined) {
        PatientID = element['PatientID']
      } else {
        continue
      }
      var start = null
      if (element['start'] !== undefined) {
        start = element['start']
      }
      var end = null
      if (element['end'] !== undefined) {
        end = element['end']
      }
      var cluster = null
      if (element['cluster'] !== undefined) {
        cluster = element['cluster']
      }
      var longitude = null
      if (element['longitude'] !== undefined) {
        longitude = element['longitude']
      }
      var latitude = null
      if (element['latitude'] !== undefined) {
        latitude = element['latitude']
      }

      locationInfoRecordsProcessed.push([PatientID, start, end, cluster, longitude, latitude])
    }
    while (locationInfoRecordsProcessed.length > 3000) {
      var subarray = locationInfoRecordsProcessed.slice(0, 3000)
      locationInfoRecordsProcessed = locationInfoRecordsProcessed.slice(3000)
      await query.createLocationInfo([subarray])
    }
    await query.createLocationInfo([locationInfoRecordsProcessed])
  }

  if (jsonArraySummary.length !== 0) {
    var jsonArraySummaryProcessed = []
    // for (var index in jsonArraySummary) {
    for (var index = 0; index < jsonArraySummary.length; ++index) {
      var element = jsonArraySummary[index]
      var PatientID = null
      if (element['PatientID'] !== undefined) {
        PatientID = element['PatientID']
      } else {
        continue
      }
      var date = null
      if (element['date'] !== undefined) {
        date = element['date']
      }
      var CenterX = null
      if (element['CenterX'] !== undefined) {
        CenterX = element['CenterX']
      }
      var CenterY = null
      if (element['CenterY'] !== undefined) {
        CenterY = element['CenterY']
      }
      var SDEx = null
      if (element['SDEx'] !== undefined) {
        SDEx = element['SDEx']
      }
      var SDEy = null
      if (element['SDEy'] !== undefined) {
        SDEy = element['SDEy']
      }
      var SDELength = null
      if (element['SDELength'] !== undefined) {
        SDELength = element['SDELength']
      }
      var SDEArea = null
      if (element['SDEArea'] !== undefined) {
        SDEArea = element['SDEArea']
      }
      var MCPArea = null
      if (element['MCPArea'] !== undefined) {
        MCPArea = element['MCPArea']
      }
      var TTD = null
      if (element['TTD'] !== undefined) {
        TTD = element['TTD']
      }
      var TLD = null
      if (element['TLD'] !== undefined) {
        TLD = element['TLD']
      }
      var AngleRotation = null
      if (element['AngleRotation'] !== undefined) {
        AngleRotation = element['AngleRotation']
      }
      var TAH = null
      if (element['TAH'] !== undefined) {
        TAH = element['TAH']
      }
      jsonArraySummaryProcessed.push([PatientID, date, CenterX, CenterY, SDEx, SDEy, SDELength, SDEArea, MCPArea, TTD, TLD, AngleRotation, TAH])
    }
    while (jsonArraySummaryProcessed.length > 3000) {
      var subarray = jsonArraySummaryProcessed.slice(0, 3000)
      jsonArraySummaryProcessed = jsonArraySummaryProcessed.slice(3000)
      await query.createGISsummary([subarray])
    }
    await query.createGISsummary([jsonArraySummaryProcessed])
  }

  if (locationsRecords.length !== 0) {
    var locationsRecordsProcessed = []
    // for (var index in locationsRecords) {
    for (var index = 0; index < locationsRecords.length; ++index) {
      var element = locationsRecords[index]
      var PatientID = null
      if (element['PatientID'] !== undefined) {
        PatientID = element['PatientID']
      } else {
        continue
      }
      var date_time = null
      if (element['date_time'] !== undefined) {
        date_time = element['date_time']
      }
      var cluster = null
      if (element['cluster'] !== undefined) {
        cluster = element['cluster']
      }
      var longitude = null
      if (element['longitude'] !== undefined) {
        longitude = element['longitude']
      }
      var latitude = null
      if (element['latitude'] !== undefined) {
        latitude = element['latitude']
      }

      locationsRecordsProcessed.push([PatientID, date_time, cluster, longitude, latitude])
    }
    while (locationsRecordsProcessed.length > 3000) {
      var subarray = locationsRecordsProcessed.slice(0, 3000)
      locationsRecordsProcessed = locationsRecordsProcessed.slice(3000)
      await query.createLocations([subarray])
    }
    await query.createLocations([locationsRecordsProcessed])
  }
}


// process step data after python
const processSTEPResultData = async (resultfilename) => {
  var processedData = JSON.parse(fs.readFileSync(resultfilename.replace('\n', ''), 'utf8'))

  var jsonArraySummary = []
  var inactivetimeRecords = []
  var temp1 = processedData['summary']
  if (temp1 === undefined) temp1 = {}
  var temp2 = processedData['inactivetime']
  if (temp2 === undefined) temp2 = {}

  if ((typeof temp1) === "string" && (temp1 !== "")) jsonArraySummary = JSON.parse(temp1)
  if ((typeof temp2) === "string" && (temp2 !== "")) inactivetimeRecords = JSON.parse(temp2)

  if (inactivetimeRecords.length !== 0) {
    var inactivetimeRecordsProcessed = []
    // for (var index in inactivetimeRecords) {
    for (var index = 0; index < inactivetimeRecords.length; ++index) {
      var element = inactivetimeRecords[index]
      var PatientID = null
      if (element['PatientID'] !== undefined) {
        PatientID = element['PatientID']
      } else {
        continue
      }
      var start = null
      if (element['start'] !== undefined) {
        start = element['start']
      }
      var end = null
      if (element['end'] !== undefined) {
        end = element['end']
      }
      var time = null
      if (element['time'] !== undefined) {
        time = element['time']
      }
      inactivetimeRecordsProcessed.push([PatientID, start, end, time])
    }
    while (inactivetimeRecordsProcessed.length > 3000) {
      var subarray = inactivetimeRecordsProcessed.slice(0, 3000)
      inactivetimeRecordsProcessed = inactivetimeRecordsProcessed.slice(3000)
      await query.createInactiveTime([subarray])
    }
    await query.createInactiveTime([inactivetimeRecordsProcessed])
  }

  if (jsonArraySummary.length !== 0) {
    var jsonArraySummaryProcessed = []
    // for (var index in jsonArraySummary) {
    for (var index = 0; index < jsonArraySummary.length; ++index) {
      var element = jsonArraySummary[index]
      var PatientID = null
      if (element['PatientID'] !== undefined) {
        PatientID = element['PatientID']
      } else {
        continue
      }
      var date = null
      if (element['date'] !== undefined) {
        date = element['date']
      }
      var stepcount = null
      if (element['stepcount'] !== undefined) {
        stepcount = element['stepcount']
      }
      var distancewalked = null
      if (element['distancewalked'] !== undefined) {
        distancewalked = element['distancewalked']
      }
      var activetime = null
      if (element['activetime'] !== undefined) {
        activetime = element['activetime']
      }
      var averagespeed = null
      if (element['averagespeed'] !== undefined) {
        averagespeed = element['averagespeed']
      }

      var TimeDuration = await activityTimeDuration(PatientID, date)

      jsonArraySummaryProcessed.push([PatientID, date, stepcount, distancewalked, activetime, averagespeed, TimeDuration])
    }
    while (jsonArraySummaryProcessed.length > 3000) {
      var subarray = jsonArraySummaryProcessed.slice(0, 3000)
      jsonArraySummaryProcessed = jsonArraySummaryProcessed.slice(3000)
      await query.createSTEPSummary([subarray])
    }
    await query.createSTEPSummary([jsonArraySummaryProcessed])
  }
}


// calcuate the duration between the 1st and the last activity records
const activityTimeDuration = async (PatientID, date) => {
  let stepRecords = await rmquery.readHealthDataByIDByDate([PatientID, toLocaleTime(date, true, 0), toLocaleTime(date, true, 1)])
  if (stepRecords.length === 0) return 0
  stepRecords.sort(function (a, b) {
    var timeA = new Date(a['start_time'])
    var timeB = new Date(b['start_time'])
    if (timeA < timeB) return -1
    return 1
  })
  return ((new Date(stepRecords[stepRecords.length-1]['start_time']) - new Date(stepRecords[0]['start_time'])) / 1000 / 3600)
}

function outputTempFileAsJSON (datatype, jsonData) {
  var tempfilename = rootDir + '/temp/temp_' + datatype + '_' + new Date().getTime() + '.json'
  fs.writeFileSync(tempfilename, JSON.stringify(jsonData), 'utf8')
  return tempfilename
}

// entry of process gps data
const GISDataUpdate = async (PatientID, datesForUpdate) => {
  var lastCompletedDate = datesForUpdate['lastCompletedDate']
  var thisCompletedDate = datesForUpdate['thisCompletedDate']

  var IOSLocationRecordsToBeUpdated = []
  var AndroidLocationRecordsToBeUpdated = []

  if (thisCompletedDate !== null) IOSLocationRecordsToBeUpdated = await rmquery.readLocationDataByIDByDate([PatientID, lastCompletedDate, thisCompletedDate])
  if (thisCompletedDate !== null) AndroidLocationRecordsToBeUpdated = await rmquery.readAndroidLocationDataByIDByDate([PatientID, lastCompletedDate, thisCompletedDate])

  var LocationRecordsToBeUpdated = IOSLocationRecordsToBeUpdated.concat(AndroidLocationRecordsToBeUpdated)
  if (LocationRecordsToBeUpdated.length === 0) return
  var jsonArray = []
  LocationRecordsToBeUpdated.forEach(function (element) {
    jsonArray.push({
      date_time: element['time'],
      latitude: element['latitude'],
      longitude: element['longitude']
    })
  })

  var JSONData = {}
  JSONData[PatientID] = jsonArray
  var tempdatafilename = await outputTempFileAsJSON('gis', JSONData)

  logger.log('info', tempdatafilename)
  var params = [rootDir + '/python_codes/stdbscan_entry.py', '-j', tempdatafilename, '-rp', rootDir, '-p', minimunPts, '-s', spatialThreshold, '-t', temporalThreshold]

  var ResultFileName = await getDataFromPython(params)
  ResultFileName = ResultFileName.replace('\n', '')
  logger.log('info', ResultFileName)
  await processGISResultData(ResultFileName)
  // Update lastRecord and lastCompleteDate
  // remove temp files
  fs.access(ResultFileName, error => {
    if (!error) {
      fs.unlinkSync(ResultFileName)
    } else {
      logger.log('error', error)
    }
  })
  fs.access(tempdatafilename, error => {
    if (!error) {
      fs.unlinkSync(tempdatafilename)
    } else {
      logger.log('error', error)
    }
  })

}

// entry of process step data
const STEPDataUpdate = async (PatientID, datesForUpdate) => {
  var lastCompletedDate = datesForUpdate['lastCompletedDate']
  var thisCompletedDate = datesForUpdate['thisCompletedDate']

  var IOSHealthRecordsToBeUpdated = []
  if (thisCompletedDate !== null) IOSHealthRecordsToBeUpdated = await rmquery.readHealthDataByIDByDate([PatientID, lastCompletedDate, thisCompletedDate])

  var AndroidHealthRecordsToBeUpdated = []
  if (thisCompletedDate !== null) AndroidHealthRecordsToBeUpdated = await rmquery.readAndroidHealthDataByIDByDate([PatientID, lastCompletedDate, thisCompletedDate])

  var HealthRecordsToBeUpdated = IOSHealthRecordsToBeUpdated.concat(AndroidHealthRecordsToBeUpdated)

  if (HealthRecordsToBeUpdated.length === 0) return

  var jsonArray = []
  HealthRecordsToBeUpdated.forEach(function (element) {
    jsonArray.push({
      start: element['start_time'],
      end: element['end_time'],
      steps: element['steps'],
      distance: element['distance']
    })
  })

  var JSONData = {}
  JSONData[PatientID] = jsonArray
  var tempdatafilename = await outputTempFileAsJSON('step', JSONData)
  // var tempdatafilename = '/Users/zhuoyingli/Documents/WebstormProjects/healthtracker/temp/temp_step_1533882554894.json'
  logger.log('info', tempdatafilename)

  var params = [rootDir + '/python_codes/step_entry.py', '-j', tempdatafilename, '-rp', rootDir]
  var ResultFileName = await getDataFromPython(params)
  ResultFileName = ResultFileName.replace("\n", '')
  logger.log('info', ResultFileName)
  await processSTEPResultData(ResultFileName)
  // Update lastRecord and lastCompleteDate
  // remove temp files
  fs.access(ResultFileName, error => {
    if (!error) {
      fs.unlinkSync(ResultFileName)
    } else {
      logger.log('error', error)
    }
  })
  fs.access(tempdatafilename, error => {
    if (!error) {
      fs.unlinkSync(tempdatafilename)
    } else {
      logger.log('error', error)
    }
  })
}

const getDataFromPython = (params) => {
  var spawn = require('child_process')
  try {
    var ls = spawn.spawnSync(python_env, params, {
      shell: true
    })

    return ls.stdout.toString()
  } catch (err) {
    logger.log('error', params)
    logger.log('error', err)
    logger.log('error', ls.stderr.toString())
    exit(0)
  }
}
// Entry of Calculations for TAH based on the given patientid and the dates
const TAHUpdate = async (PatientID, datesForUpdate) => {
  var lastCompleteDate = datesForUpdate['lastCompletedDate']
  var thisCompleteDate = datesForUpdate['thisCompletedDate']
  var records = await query.readGISSummaryByIDByDates([PatientID, lastCompleteDate, thisCompleteDate])

  await TAHCalculation(PatientID, records)
}

// add the calculation of missing TAH
const TAHUpdate2 = async (PatientID, datesForUpdate) => {
  var lastCompleteDate = datesForUpdate['lastCompletedDate']
  var thisCompleteDate = datesForUpdate['thisCompletedDate']
  var records = await query.readGISSummaryByIDByDates([PatientID, lastCompleteDate, thisCompleteDate])

  await TAHCalculation(PatientID, records)

  // reprocess TAH for the dates without TAH results from 10 ago
  var params = [PatientID, toLocaleTime(thisCompleteDate, true, -10)]
  var unprocessRecords = await query.readUnprocessTAHRecorddsByIDByDate(params)

  await TAHCalculation(PatientID, unprocessRecords)
}


// Calculate TAH of the given GISSUmmary results
const TAHCalculation = async (PatientID, GISSummaryRecords) => {

  for (var index = 0; index < GISSummaryRecords.length; ++index) {
    var timeFilter = 1200
    var recordLimit = 7
    var element = GISSummaryRecords[index]
    var currentdate = element['date']
    // check if missing data, result in update the total number of gps data of the date
    await validGISRecordsByIDByDate(PatientID, currentdate)

    if (element['TAH'] === null || element['TAH'] === -1 || element['TAH'] === 0) {
      var sevenDaysBefore = toLocaleTime(currentdate, true, -7)
      var recordsForHomeClustering = await retrieveInActiveTimeRecords(PatientID, sevenDaysBefore, currentdate, timeFilter, recordLimit)
      if (recordsForHomeClustering.length !== 0) {
        var resultLocation = distance_clustering(recordsForHomeClustering)

        var locationparams = [PatientID, currentdate, toLocaleTime(currentdate, true, 1)]
        var locationsToBeProcessed = await query.readLocationsByIDByDates(locationparams)
        var PTAH = PercentageofTimeAtLocation(locationsToBeProcessed, resultLocation)
        // Store PTAH result
        await query.updateGISSummaryByIDByDate([PTAH, PatientID, currentdate])
        // console.log('ID: ' + PatientID + '  Date: ' + currentdate + '  %: ' + PTAH)
      } else {
        var sevenDaysafter = toLocaleTime(currentdate, true, 7)
        var recordsForHomeClustering = await retrieveInActiveTimeRecords(PatientID, currentdate, sevenDaysafter, timeFilter, recordLimit)
        if (recordsForHomeClustering.length !== 0) {
          var resultLocation = distance_clustering(recordsForHomeClustering)

          var locationparams = [PatientID, currentdate, toLocaleTime(currentdate, true, 1)]
          var locationsToBeProcessed = await query.readLocationsByIDByDates(locationparams)

          var PTAH = PercentageofTimeAtLocation(locationsToBeProcessed, resultLocation)
          // Store PTAH result
          await query.updateGISSummaryByIDByDate([PTAH, PatientID, currentdate])

          // console.log('ID: ' + PatientID + '  Date: ' + currentdate + '  %: ' + PTAH)
        } else {
          await query.updateGISSummaryByIDByDate([-1, PatientID, currentdate])

          // console.log('ID: ' + PatientID + '  Date: ' + currentdate + '  cannot calculate PTAH')
        }
      }
    }
  }
}

async function retrieveInActiveTimeRecords(PatientID, start, end, timeFilter, recordsLimit) {
  var params = [PatientID, start, end, timeFilter, recordsLimit]
  var InActiveTimeRecords = await query.readTopXInactiveTimeByIDByDate(params)
  var matchedLocationInfoRecords = []

  for (var index = 0; index < InActiveTimeRecords.length; ++index) {
    var params = [InActiveTimeRecords[index]['PatientID'], InActiveTimeRecords[index]['start'], InActiveTimeRecords[index]['end']]
    var LocationInfoRecord = await query.readLocationInfoByIDByDate(params)

    if (Array.isArray(LocationInfoRecord) && LocationInfoRecord.length >= 1) matchedLocationInfoRecords = matchedLocationInfoRecords.concat(LocationInfoRecord)
  }
  return matchedLocationInfoRecords
}

// clustering locations based on spatialThreshold
function distance_clustering (inactiveLocations) {
  var clusterCounter = []
  for (var index = 0; index < inactiveLocations.length; ++index) {
    if (index === 0) {
      inactiveLocations[index]['cluster'] = 0
      clusterCounter.push(1)
      continue
    }
    for (var index2 = 0; index2 < index; ++index2) {
      var distance = GreatCircle.distance(inactiveLocations[index]['latitude'],
        inactiveLocations[index]['longitude'], inactiveLocations[index2]['latitude'],
        inactiveLocations[index2]['longitude'], 'M')
      if (distance <= (spatialThreshold * 2))  {
        inactiveLocations[index]['cluster'] = inactiveLocations[index2]['cluster']
        clusterCounter[inactiveLocations[index2]['cluster']]++
        break
      } else {
        inactiveLocations[index]['cluster'] = clusterCounter.length
        clusterCounter.push(1)
      }
    }
  }

  var maxClusters = 0, maxIndex = 0
  for (var index in clusterCounter) {
    // for(var index = 0 index < clusterCounter.length ++index){
    if (clusterCounter[index] > maxClusters) {
      maxClusters = clusterCounter[index]
      maxIndex = index
    }
  }

  var resultLocation
  // for (var index in inactiveLocations){
  for (var index = 0; index < inactiveLocations.length; ++index) {
    if (inactiveLocations[index]['cluster'].toString() === maxIndex) {
      resultLocation = inactiveLocations[index]
      break
    }
  }
  return resultLocation
}

// calculate the total number of gps data of the date for both origin data and data with cluster info
const validGISRecordsByIDByDate = async (PatientID, Date) => {
  var nextDate = toLocaleTime(Date, true, 1)
  var RMIOSLocationRecords, RMAndroidLocationRecords
  RMIOSLocationRecords = await rmquery.readLocationDataByIDByDate([PatientID, Date, nextDate])
  RMAndroidLocationRecords = await rmquery.readAndroidLocationDataByIDByDate([PatientID, Date, nextDate])

  // original gps data
  var RMLocationRecordsToBeUpdated = RMIOSLocationRecords.concat(RMAndroidLocationRecords)
  // gps data with cluster info after processed
  var LocalIOSLocationRecords = await query.readLocationsByIDByDates([PatientID, Date, nextDate])

  await query.updateGISSummaryValueRecordsByIDByDate([RMLocationRecordsToBeUpdated.length, LocalIOSLocationRecords.length, PatientID, Date])
}

// calculation of TAH
// if the gps point is located in the given cluster gps point, add up the time
function PercentageofTimeAtLocation (locations, Location) {
  var lastIndex = -1
  var totalTimeAtLocation = 0
  for (var index = 0; index < locations.length; ++index) {

    // check if with a given range from the cluster gps point
    var distance = GreatCircle.distance(locations[index]['latitude'],
      locations[index]['longitude'], Location['latitude'],
      Location['longitude'], 'M')
    if (distance <= (spatialThreshold * 2)) {
      locations[index].athome = true
    }
    // add update the time spent at home
    if (lastIndex !== -1) {
      if (locations[lastIndex]['athome'] === true) {
        var date = new Date(locations[index]['date_time'])
        var lastdate = new Date(locations[lastIndex]['date_time'])
        totalTimeAtLocation += parseInt((date - lastdate) / 1000 / 60)
      }
    }

    lastIndex = index
  }
  // console.log('in: ' + totalTimeAtLocation)
  return totalTimeAtLocation
}

// date string handler
function toLocaleTime (date, midnight, dayDiff) {
  var localDate = new Date(date)
  if (dayDiff !== undefined) localDate.setTime(localDate.getTime() + 964e5 * dayDiff)
  if (midnight) {
    localDate.setHours(0, 0, 0)
  }
  var dateString = localDate.getFullYear() + "-" + (localDate.getMonth() + 1) + "-" + localDate.getDate() +
    " " + localDate.getHours() + ":" + localDate.getMinutes() + ":" + localDate.getSeconds()
  return dateString
}

Array.prototype.unique = function() {
  var a = this.concat()
  for (var i = 0; i < a.length; ++i) {
    for (var j = i + 1; j < a.length; ++j) {
      if (a[i] === a[j])
        a.splice(j--, 1)
    }
  }
  return a
}

module.exports = {
  UserBasedUpdate: UserBasedUpdate,
  // dailyUpdateByID: dailyUpdateByID,
  updateAllUsers: updateAllUsers,
  // dailyUpdateAll: dailyUpdateAll,
  TAHUpdate2: TAHUpdate2,
  TAHUpdate: TAHUpdate,
  logger: logger
}
