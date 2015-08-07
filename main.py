# The MIT License (MIT)
# 
# Copyright © 2015 Glenn Fitzpatrick
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import csv
import cx_Oracle
import logging
import os


debug = True


# connect to database, return database connection object
def dbConnect(connect_string):

    print("Opening connection to database...", end=" ")

    try:
        connection = cx_Oracle.connect(connect_string)

        print("connected!")
        print()

        logging.info("Connected to database")

        return connection

    except cx_Oracle.Error as e:
        logging.error("Unable to connect to database!")
        logging.error("Database connection closed.")
        
        print("unable to connect to database!")
        print("Database connection closed.")
        print()

        return None



# query database, return query results
def dbQuery(connection, query):

    result = {}

    if debug:
        print()
        print("Opening cursor")
    logging.info("Opening cursor")
    cur = connection.cursor()

    if debug:
        print("Running query...")
        print()
        print(query)

    logging.info("Running query...")
    logging.info(query)
    
    cur.execute(query)

    description = []

    for d in cur.description:
        description.append(d[0])

    result['columns'] = description
    
    reader = cur.fetchall()

    cur.close()

    data = []
    
    for row in reader:
        data.append(row)

    if debug:
        print()
        print("Cursor closed. Retrieved", str(len(data)), "rows.")
        print()

    logging.info("Cursor closed. Retrieved {} rows.".format(str(len(data))))

    result['data'] = data

    return result



# query database, commit data to database
def dbCommit(connection, query, data = []):

    if debug:
        print()
        print("Opening cursor")
    logging.info("Opening cursor")
    
    cur = connection.cursor()

    if debug:
        print("Updating database...")
    logging.info("Updating database...")
    
    if data:
        
        for row in tqdm(data, leave=True):
            cur.execute(query.format(*row))
            logging.info(query.format(*row))

    else:
        if debug:
            print()
            print(query)
        logging.info(query)
        cur.execute(query)
        

    connection.commit()

    if debug:
        print()
    
    if data:
        
        if len(data) == 1:
            if debug:
                print("Committed 1 row.")
            logging.info("Committed 1 row")
        else:
            if debug:
                print("Committed", str(len(data)), "rows.")
            logging.info("Committed {} rows".format(str(len(data))))
            
    else:
        if debug:
            print("Commit.")
        logging.info("Commit.")

    cur.close()

    if debug:
        print("Cursor closed.")
        print()
    logging.info("Cursor closed")
    
    

# close database connection
def dbClose(connection):

    connection.close()
    print("Database connection closed.")
    logging.info("Database connection closed.")
    print()




if __name__ == "__main__":

    USER = ''
    DOMAIN = ''
    PORT = ''
    DATABASE = ''

    SEARCH_QUERIES = [
# (query1, query1 description),
# (query2, query2 description),
# etc...
]

    

    # change working directory to the user's desktop
    # this is where we'll save the company data from the MCH to be standardized / geocoded
    username = os.environ.get("USERNAME")
    os.chdir(os.path.join('C:\\', 'users', username, 'Desktop'))

    # create the destination folder on the desktop if it doesn't already exist
    if not os.path.exists('Queries'):
        os.makedirs('Queries')
    os.chdir('Queries')

    pw = input('Enter password: ')

    CONNECT_STRING = '{}/{}@{}:{}/{}'.format((USER, pw, DOMAIN, PORT, DATABASE),)

    os.system('cls')

    logging.info("Opening database connection: {}".format(CONNECT_STRING))

    connection = dbConnect(CONNECT_STRING)

    for query in enumerate(SEARCH_QUERIES):

        filename = query[1][1].strip()

        while "  " in filename:
            filename = filename.replace("  ", " ")

        outputFile = open(str('{}. {}.csv'.format(query[0] + 1, filename)), 'w', newline='', encoding='utf_8_sig')
        writer = csv.writer(outputFile)

        cleanedquery = query[1][0].strip()
        
        while "  " in cleanedquery:
            cleanedquery = cleanedquery.replace("  ", " ")

        print("Query {} of {}:".format(query[0] + 1, len(SEARCH_QUERIES)))

        result = dbQuery(connection, cleanedquery)
        
        writer.writerow([cleanedquery,])

        writer.writerow([])

        writer.writerow(result.get('columns'))

        for row in result.get('data'):

            writer.writerows([row])

        outputFile.close()


    dbClose(connection)
