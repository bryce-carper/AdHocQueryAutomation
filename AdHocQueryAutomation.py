# Script used to begin pulling in all of partner's fine-grain data.
# Reduces frequency of queries during business hours,
# and postpones queries during cube update hours.
#
# Fairly hack-y, but not in a way that affects efficiency of the Python.
# Efficiency of the MDX may not be improvable, due to restrictions on queries run inside Cube.





# Modules
import adodbapi as ad
import pandas as pd
import sys
import shelve
import time
import datetime
# import psycopg2



#######################
#   Options
#######################

printQueryStatus = True



# All time increments in seconds.

# How many attempts to make total. How many to make before waiting a few minutes to try again.
maxAttempts = 5
longAttemptThreshold = 3



# All time increments in seconds.

# How long to wait between failed attempts. How much longer to wait if you've failed a few times already.
waitBetweenAttempts = 95
additionalWaitForLongAttempt = 220

# How long to wait at start of script, to avoid back-to-back queries from other scripts.
waitAtStart = 10

# How long to wait after a successful query, before running another one.
waitBetweenQueries = 95
additionalWaitDuringDay = 270
additionalWaitDuringBusinessDay = 840

# End 1/2 hour before partner's cube servers shutter for updates, pick up after servers have been officially 'open' for half an hour.
morningStartTime = datetime.time(6, 30, 0)
eveningEndTime = datetime.time(21, 30, 0)


#######################
#   Write query here
#######################


# Strings for measures, as they appear in partner cube.
measures1 = '{[Measures].[measure1],[Measures].[measure2],[Measures].[measure3]}'
# Last-year measures, used on earliest provided year, to retrieve data from a year further back.
measures2 = '{[Measures].[measure1LY],[Measures].[measure2LY],[Measures].[measure3LY]}'

# Strings for members, used in creating MDX query.
products = '[Dim].[products].[products].AllMembers'
stores = '[Dim].[stores].[stores].AllMembers'
salesChannels = '[Dim].[vendor].[vendor].AllMembers'

# Query template.
queryTemplate = 'SELECT {{measures}} ON COLUMNS , NON EMPTY CrossJoin(NonEmpty(CrossJoin(NonEmpty({setA}, {{measures}}), NonEmpty({setB}, {{measures}})), {{measures}}), NonEmpty({setC}, {{measures}})) ON ROWS  FROM [dbName] WHERE ([Dim].[Att].[Att].[{{queryDate}}])'

# Format query template with member strings. Measure strings and query date may vary by context.
queryTemplate = queryTemplate.format(setA = salesChannels, setB = stores, setC = products)



#########################################
#   Connection and file output options
#########################################


# ConnString attributes. Comment out to disable. Do not leave as blank strings.
connectionAttributes = [
                         r'Provider=MSOLAP.7'
                        ,r'Data Source=<serverAddress>'
                        ,r'Location=<serverAddress>'
                        ,r'Initial Catalog=<dbname>'
                        ,r'User ID=<userid>'
                        ,r'Password=<password>'
                        ,r'Persist Security Info=True'
                        ,r'MDX Compatibility=1'
                        ,r'Safety Options=2'
                        ,r'MDX Missing Member Mode=Error'
                        ,r'Update Isolation Level=2'
                        # ,r'Packet Size=16384'
                        ]



# Authentication for local database. Not in use.
# pgHost = 'localhost'
# pgUser = 'bcarper'
# pgPwd = 'Swordfish'
# pgdb = 'localdata'
# pgSchema = 'intake'
# pgTable = 'partnerUpdateIntake'

# Output and file suffix for saved files. Currently saving to .csv instead of uploading to PostGres.
saveFilePath = r'C:<pathToSaveDirectory>'
saveFileSuffix = '_SalesData.csv'

# Shelve path and storage variables.
shelvePath = r'C:<pathToShelveDirectory>'
with shelve.open(shelvePath) as db:
    missingDates = db['missingDates']
    lastPull = db['lastPull']



################
#   Functions
################

# Get current clock time.
def currentTime(): return datetime.datetime.time(datetime.datetime.now())


def runQuery(dataDate, LY=False):
    # Check if pulling for a date before 's earliest available date. Query by LY metrics for that date + 364 days if so.
    if LY == True:
        queryMeasures = measuresLY
        queryDate = dataDate + pd.Timedelta(364, unit = 'd')
    else:
        queryMeasures = measuresTY
        queryDate = dataDate

    # Attempt some number of times. Exit out and store in missing dates if failed.
    for i in range(maxAttempts):
        readSuccess = 0

        try:
            # Format query as needed.
            query = queryTemplate.format(measures = queryMeasures, queryDate = queryDate)
            # Attempt connection w/string.
            connection = ad.connect(";".join(connectionAttributes))
            # Set db cursor (required for this type of read.)
            cursor = connection.cursor()
            # Execute query.
            cursor.execute(query)
            # Fetch columns.
            columns = cursor.columnNames
            # Fetch results.
            results = cursor.fetchall()
            # Mark as successful read. Could just be done by check on results variable.
            readSuccess = 1
        except:
            readSuccess = 0
            print("Read failure:", str(queryDate) + ":", datetime.datetime.today())
        finally:
            # Leave politely.
            cursor.close()
            connection.close()

        if readSuccess == 1:
            # Formatting dataframe. Done in one line to avoid memory usage from storing multiple dataframes.
            # Can be picked apart if not run while I'm trying to use my RAM for, you know, work.
            # Better use of variables would also avoid this.
            # Most complex parts of statements are:
            # 1. Dropping columns that have 'member_unique_name' in them. This is everything in and including .drop() method.
            # 2. In defining the dataframe from records, columns = [{v: k for k, v in columns.items()}[i] for i in range(len(columns))] simply sets column names.
            df = (pd.DataFrame.from_records(list(results), columns = [{v: k for k, v in columns.items()}[i] for i in range(len(columns))])).drop(columns=[col for col in [{v: k for k, v in columns.items()}[i] for i in range(len(columns))] if 'member_unique_name' in col])
            # Write to .csv
            df.to_csv(saveFilePath + '\\' + str(dataDate) + saveFileSuffix, index = False)
            # Break from loop.
            break

        # If unsuccessful, wait before another attempt.
        time.sleep(waitBetweenAttempts)

        # If you've failed a number of times already, wait a little bit longer.
        if i > longAttemptThreshold - 1:
            time.sleep(additionalWaitForLongAttempt)

    # If total failure for a given date, file it away in the pickle and return that it failed.
    if readSuccess == 0:
        missingDates.append(dataDate)
        return False

    # You've done well. Report true!
    return True



####################################################################################
#   Connect and manage connection; Attempt execution and fetch; Close connection.
####################################################################################

# Set range to pull dates based on last pull. Must coerce to a datetime that can naturally be seem as a string of format 'YYY-MM-DD'.
dateRange = pd.date_range(start = lastPull + pd.Timedelta(1, unit = 'd'), end = pd.datetime.today() - pd.Timedelta(1, unit = 'd')).date.tolist()

# Initial wait time. Not super necessary.
time.sleep(waitAtStart)

# Run queries once for each specified date.
for dataDate in dateRange:
    while not (morningStartTime < currentTime() and currentTime() < eveningEndTime):
        time.sleep(900)

    # Print query status if desired.
    if printQueryStatus:
        print("Starting query.")

    # If query date is before earliest date, run as a LY query.
    if pd.to_datetime(dataDate) < pd.to_datetime('2015-02-02'):
        test = runQuery(dataDate, LY = True)
    else:
        test = runQuery(dataDate)

    # Update shelve with new lastPull and missingDates records.
    with shelve.open(shelvePath) as db:
        db['missingDates'] = missingDates
        db['lastPull'] = pd.to_datetime(dataDate)

    # Print query status if desired.
    if printQueryStatus:
        print("Ending query.")

    # If error, print to console.
    if test == False:
        print('Ended due to errors.')
        break

    # workday/daytime extra sleep.
    if (datetime.datetime.today().hour < 17):
        time.sleep(additionalWaitDuringDay)
        if datetime.datetime.today().weekday() in range(5):
            time.sleep(additionalWaitDuringBusinessDay)

    # General query wait sleep.
    time.sleep(waitBetweenQueries)



####################################################################################
#   Filter, clean, sort, and format data
####################################################################################



####################################################################################
#   Upload data to PostgreSQL
####################################################################################

# connection = psycopg2.connect(host = pgHost, database = pgdb, user = pgUser, password = pgPwd)
#
# attempts = 0
#
# while True:
#     try:
#         df.to_sql(pgTable, connection, schema = pgSchema, if_exists = 'append')
#         break
#     attempts += 1
#     if attempts = 10:
#         df.to_csv(saveFilePath + '\\' + saveFileName + '.csv', index = False)
#         break
#
# connection.close()


# Notify that script has executed successfully.
print("Finished")
