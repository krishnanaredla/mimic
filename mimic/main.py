import faker.fixedSchema as fs
import yamlparse.parser as yp
from loguru import logger
import pandas as pd
from typing import List

path = r"C:\Work\jupyter\config.yaml"

config =  yp.getConfig(path,logger)

for table in config["tables"]:
    fixedColumns      = []
    SequentialColumns = []
    ExpressionColumns = []
    for column in table['columns']:
        if column['column_type'] == 'Fixed':
            fixedColumns.append(column)
        if column['column_type'] == 'Sequential':
            SequentialColumns.append(column)
        if column['column_type'] == 'Expresssion' :
            ExpressionColumns.append(column)

#print(fixedColumns)

getFixedSchema = fs.Switcher()
for column in fixedColumns:
    df = pd.DataFrame([getFixedSchema.faker])

def processFixedSchema(data:List[dict]):
    rows = 10
    getFixedSchema = fs.Switcher()
    df = pd.DataFrame([getFixedSchema.faker('getNameData') for pos in range(rows)])
    for column in data:
        if 'custom_values' in column:
            df[column['name']] = pd.DataFrame([getFixedSchema.faker(column['value'],**column['custom_values'])
             for pos in range(rows)])
        else:
            df[column['name']] = pd.DataFrame([getFixedSchema.faker(column['value']) 
            for pos in range(rows)])
    return df

print(processFixedSchema(fixedColumns))
