#!/usr/bin/python

from google.cloud import bigquery
import pymssql
import os
from os import getenv

#IP=os.environ['DBIP_SP']
#USR=os.environ['DBUSR_SP']
#PASS=os.environ['DBPSWD_SP']
#SCHEMA_SP=os.environ['SCHEMA_SP']
#PROJ=os.environ['PROJ']

columNames = "DateTime,TagName,Value,vValue,Quality,QualityDetail,OPCQuality,wwTagKey,wwRowCount,wwResolution,wwEdgeDetection,wwRetrievalMode,wwTimeDeadband,wwValueDeadband,wwTimeZone,wwVersion,wwCycleCount,wwTimeStampRule,wwInterpolationType,wwQualityRule,wwStateCalc,StateTime,PercentGood,wwParameters,StartDateTime,SourceTag,SourceServer,wwFilter,wwValueSelector,wwMaxStates,wwOption".split(",")
publication = []

IP= '10.192.218.53'
USR= 'usr_bigdatapan'
PASS= 'Prd0301BiG2019'
SCHEMA_SP= 'runtime'
PROJ= 'maderas-prod'

bq = bigquery.Client(project=PROJ)
dataset_id = 'sp_tables'
table_id = "history"
table_ref = bq.dataset(dataset_id).table(table_id)
tabla = bq.get_table(table_ref)

def get_vars():
    query= "SELECT TAG_SP FROM model.model_vars"
    query_job = bq.query(query) 
    result=list(query_job.result())
    varstring = ''
    for tag in result:
        varstring = varstring + "'" + tag['TAG_SP'] + "',"
    varstring = varstring[:-1]
    return varstring

def fill_json(columNames, rows):
    i = 0
    dict_vacio = {}
    for row in rows:
        for c in columNames:
            dict_vacio[c] = row[i]
            i = i +1
        i = 0
        publication.append(dict_vacio)
    return publication


vars = get_vars()
ext = str(len(vars.split(",")))
conn = pymssql.connect(IP, USR, PASS, SCHEMA_SP)
cursor = conn.cursor()
print("cursor creado")
query = "SELECT TOP " + ext + " CONCAT(SUBSTRING(CAST(DateTime AS VARCHAR), 0, 18),'00'),TagName,Value,vValue,Quality,QualityDetail,OPCQuality,wwTagKey,wwRowCount,wwResolution,wwEdgeDetection,wwRetrievalMode,wwTimeDeadband,wwValueDeadband,wwTimeZone,wwVersion,wwCycleCount,wwTimeStampRule,wwInterpolationType,wwQualityRule,wwStateCalc,StateTime,PercentGood,wwParameters,StartDateTime,SourceTag,SourceServer,wwFilter,wwValueSelector,wwMaxStates,wwOption, substring(cast(DateTime as varchar), 1, 10) as datepart FROM History WHERE TagName IN(" + vars + ") and wwRetrievalMode = 'Interpolated' and wwResolution='60000' ORDER BY DateTime DESC"

try:
    cursor.execute(query)
    rows = cursor.fetchall()
    print("vars guardadas")
    fill_json(columNames, rows)
    conn.close()
except Exception as e:
    file = open('history.log', 'a+')
    file.write(str(e) + "\n")
    file.close()
if len(rows)>300:
    errors = bq.insert_rows(tabla, rows)
    print("variables guardadas")