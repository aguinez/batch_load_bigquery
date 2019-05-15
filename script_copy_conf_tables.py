from os import getenv
import pymssql
from google.cloud import bigquery
import csv
import pandas as pd
import datetime
import os


bq = bigquery.Client()
#tablas = "qaTestDetails".split(",")
tablas = "qaTestDetails,Schedule,DTDetails,qaTestNames,MaterialDescriptions,Machine,"
 + "ShiftDefinition,DTCodeTexts,CharacteristicNames,MaterialCharacteristic,TargetMaterial,"
 + "PriceMaterial,ManagementOEE,ProductionLink,qaFieldDefinitions,qaTemplateHeaderDetails,qaTemplate,AmplaChangeLog,OrderItems".split(",")
#tablas = "TargetMaterial,PriceMaterial,ManagementOEE,ProductionLink,qaFieldDefinitions,qaTemplateHeaderDetails,qaTemplate,AmplaChangeLog,OrderItems".split(",")

IP=os.environ['DBIP']
USR=os.environ['DBUSR']
PASS=os.environ['DBPSWD']
SCHEMA=os.environ['SCHEMA']
PROJ=os.environ['PROJ']


def getSchemasMssql(tablas):
    schemaOrder = dict()
    for tabla in tablas:
        conn = pymssql.connect(IP, USR, PASS, SCHEMA)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'".format(tabla))
        schema = cursor.fetchall()
        fields = []
        dt = ''
        for rowname in schema:
            if rowname[7] == 'nchar' or rowname[7] == 'char' or rowname[7] == 'varchar' or rowname[7] == 'nvarchar' or rowname[7] == 'time':
                dt = 'string'
            elif rowname[7] == 'bigint' or rowname[7] == 'int':
                dt = 'integer'
            elif rowname[7] == 'smalldatetime':
                dt = 'datetime'
            elif rowname[7] == 'decimal' or rowname[7] == 'real':
                dt = 'float'
            elif rowname[7] == 'bit':
                dt = 'boolean'
            else:
                dt = rowname[7]
            fields.append({'name':rowname[3], 'type':dt})
        schemaOrder[tabla] = fields
        conn.close()
    return schemaOrder

schemaOrder = getSchemasMssql(tablas)
for tabla in tablas:
    success = False
    while success != True:
        try:
            conn = pymssql.connect(IP, USR, PASS, SCHEMA)
            cursor = conn.cursor()
            print(tabla)
            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%s'))
            cursor.execute('select * from {}'.format(tabla))
            result = cursor.fetchall()
            df = pd.DataFrame(result)
            df.to_gbq('ams_tables.' + tabla, project_id=PROJ, if_exists='replace', location='US', table_schema=schemaOrder[tabla])
            print("tabla subida\n")
            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%s') + '\n')
            success = True
        except:
            continue