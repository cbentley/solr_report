#!/usr/bin/env python3.1

'''
solr_report.py
by Chris Bentley

Required command line parameters:
    win|mac input-absolute-file-path [output-absolute-file-path]

Search Solr for records for collections that are digitized and have a finding aid.

There are some differences in the way Windows and Mac OS handle data, hence the need for the win|mac parameter.

URLs for accessing CJH's Digitool Apache Solr Service:
    Admin web page:
        [url1]
    The search used to output the JSON for this script:
        [url2] 

Used the following file paths when running this script in Sept. 2012:
    windows:
    input: "[path1]"
    output: "[path2]"
    mac os:
    input: "[path3]"
    output: "[path4]"
'''

import sys
import os
import json
import re
import csv

def process_args():
    '''Process the command line arguments and save to args dictionary. Return args.'''
    args = {}
    
    if len(sys.argv) > 2:
        # Get OS version
        if sys.argv[1] == "win":
            args['os_ver'] = sys.argv[1]
        elif sys.argv[1] == "mac":
            args['os_ver'] = sys.argv[1]
        else:
            sys.exit('First parameter must be win or mac.')

        # Get input file path
        if os.path.exists(sys.argv[2]):
            args['infilepath'] = sys.argv[2]
        else:
            sys.exit('Second parameter must be a valid file path. File %s was not found.' % sys.argv[2])
        
        # Get optional output file path
        if len(sys.argv) == 4:
            args['outfilepath'] = sys.argv[3]
    else:
        sys.exit('Usage: {} win|mac input-absolute-file-path [output-absolute-file-path]'.format(sys.argv[0]))
    
    return args

class DigibaeckReport:
    def __init__(self, args):
        self.os_ver = args['os_ver']
        self.infilepath = args['infilepath']
        self.maincallnumber = ""
        self.extent = ""
        self.records = []
    
    def retrievaData(self):
        '''Retrieve JSON data.'''
        # Not currently using; retrieving data separately
        pass
    
    def deserialize(self):
        '''Deserialize JSON data.'''
        file = open(self.infilepath, 'rU')
        json_text = file.read()
        file.close()
        self.deserialized_text = json.loads(json_text)
    
    def parseData(self):
        '''Parse the deserialized data and save to self.records list.'''
        for doc in self.deserialized_text['response']['docs']:

            # Pull the main call number out of doc['callnumber'][0], because there is sometimes more than one call number in that value
            self.maincallnumber = doc['callnumber'][0]
            # If the call number starts with an AR number, then only get that AR number, and disregard anything after it            
            if self.maincallnumber.startswith("A") and "." in self.maincallnumber:
                self.maincallnumber = re.search(r'([\w\s]*)\.', self.maincallnumber)
                if self.maincallnumber:
                    self.maincallnumber = self.maincallnumber.group(1)
            # If the call number starts with an MF number, but there is also an AR number after it, then remove the initial MF number and get everything from the AR number onward
            elif self.maincallnumber.startswith("M") and "AR" in self.maincallnumber:
                self.maincallnumber = re.search(r'.*(AR.*)', self.maincallnumber)
                if self.maincallnumber:
                    self.maincallnumber = self.maincallnumber.group(1)
    
            # The total number of extent fields varies for each doc, so loop through the fields to collect the values
            for extent_item in doc['extent']:
                self.extent += extent_item + " "
                
            # Save to self.records
            if self.os_ver == "win":
                self.records.append([self.maincallnumber, doc['callnumber'][0], doc['title'][0], self.extent.strip()])
            elif self.os_ver == "mac":
                self.records.append([self.maincallnumber.encode('utf8'), doc['callnumber'][0].encode('utf8'), doc['title'][0].encode('utf8'), self.extent.strip().encode('utf8')])

            # Reset variables
            self.maincallnumber = ""
            self.extent = ""
    
    def outputToConsole(self):
        '''Output records (sorted by call number) to console.'''
        print("main call number, all call numbers, title, extent")
        for record in sorted(self.records, key=lambda x: int(x[0][3:])):
            if self.os_ver == "win":
                print(record[0] + ", " + record[1] + ", " + record[2])
            elif self.os_ver == "mac":
                print(record[0].encode('utf8') + ", " + record[1].encode('utf8') + ", " + record[2].encode('utf8'))
        
    def outputToCSV(self, outfilepath):
        '''Output records (sorted by call number) to CSV file.'''
        if self.os_ver == "win":
            with open(outfilepath, 'w', encoding='utf8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["main call number", "all call numbers", "title", "extent"])
                for record in sorted(self.records, key=lambda x: int(x[0][3:])):
                    writer.writerow(record)
        elif self.os_ver == "mac":
            with open(outfilepath, 'w') as f:
                writer = csv.writer(f)
                writer.writerow(["main call number", "all call numbers", "title", "extent"])
                for record in sorted(self.records, key=lambda x: int(x[0][3:])):
                    writer.writerow(record)

def main():
    # Process command line arguments
    args = process_args()

    # Create object
    digibaeck_report = DigibaeckReport(args)
    
    # Run methods
    digibaeck_report.deserialize()
    digibaeck_report.parseData()
    digibaeck_report.outputToConsole()
    if 'outfilepath' in args:
        digibaeck_report.outputToCSV(args['outfilepath'])

if __name__ == "__main__": main()
