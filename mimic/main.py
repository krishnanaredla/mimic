from mimic.faker import fixedSchema as fs
from mimic.yamlparse import parser as yp
from loguru import logger
import pandas as pd
from typing import List
import random
import numpy as np
import concurrent.futures
import argparse
import os


def getConfigfromFile(path: str) -> List[dict]:
    """Get the schema config from a yaml file
    and retrun dictionary"""
    return yp.getConfig(path, logger)


def getConfigfromString(variable: str) -> List[dict]:
    """Input will be a string in json/yaml format 
    and retrun dictionary.Will be used in dataiku"""
    return yp.getDict(variable, logger)


def processStaticSchema(data: List[dict], df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Generating Static Data")
    for column in data:
        try:
            df[column["name"]] = column["value"]
        except Exception as e:
            logger.error(
                "Static Data Generation Failed for column {0}".format(column["name"])
            )
            logger.error(e)
    return df


def processSelectionSchema(data: List[dict], df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Generating Selection Data")
    for column in data:
        try:
            df[column["name"]] = np.random.choice(
                np.asarray(column["value"]), size=len(df)
            )
        except Exception as e:
            logger.error(
                "Selection Data Generation Failed for column {0}".format(column["name"])
            )
            logger.error(e)
    return df


def processFixedSchema(data: List[dict], df: pd.DataFrame, rows: int) -> pd.DataFrame:
    logger.info("Generating Fake Data from mimesis")
    getFixedSchema = fs.Switcher()
    try:
        names = pd.DataFrame(
            [getFixedSchema.faker("getNameData") for pos in range(rows)]
        )
    except Exception as e:
        logger.error(e)
    for column in data:
        if column["value"].lower() in [
            "full_name",
            "first_name",
            "last_name",
            "gender",
        ]:
            try:
                df[column["name"]] = names[column["value"].lower()]
                continue
            except Exception as e:
                logger.error(e)
        if "custom_values" in column:
            try:
                df[column["name"]] = pd.DataFrame(
                    [
                        getFixedSchema.faker(column["value"], **column["custom_values"])
                        for pos in range(rows)
                    ]
                )
            except Exception as e:
                logger.error(
                    "Failed during generating data for custom values for column {0}".format(
                        column["name"]
                    )
                )
                logger.error(e)
        else:
            try:
                df[column["name"]] = pd.DataFrame(
                    [getFixedSchema.faker(column["value"]) for pos in range(rows)]
                )
            except Exception as e:
                logger.error("Failed for column {0}".format(column(column["name"])))
    return df


def processTable(table: List[dict]):
    """Process each table in schema
    Currently three modes are supported 
    -> Expression 
    -> Sequential
    -> Static
    more information can be found in the examples folder
    """
    # Process each table
    ExpressionColumns = []
    SelectionColumns = []
    StaticColumns = []
    name = table["name"]
    rows = table["rows"]
    column_order = []
    logger.info("Processing table {0}".format(name))
    for column in table["columns"]:
        if column["column_type"].lower() == "expression":
            ExpressionColumns.append(column)
        if column["column_type"].lower() == "selection":
            SelectionColumns.append(column)
        if column["column_type"].lower() == "static":
            StaticColumns.append(column)
        column_order.append(column["name"])
    df = pd.DataFrame({"id": range(rows)})
    # process each type
    df = processStaticSchema(StaticColumns, df)
    df = processSelectionSchema(SelectionColumns, df)
    df = processFixedSchema(ExpressionColumns, df, rows)
    df = df[column_order]#.drop("id",axis=0)
    return {"name": name, "data": df}


def processSchema(config: dict) -> List[dict]:
    """
    Main Function which takes the schema of all tables and process 
    """
    logger.info("Processing Schema")
    tables = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_process = {
            executor.submit(processTable, table): table for table in config["tables"]
        }
        for future in concurrent.futures.as_completed(future_to_process):
            data = future.result()
            tables.append(data)
    return tables


output = r"tests/data"


def generateFakeData(path: str, output: str) -> None:
    logger.info("Process Started")
    config = getConfigfromFile(path)
    data = processSchema(config)
    if not os.path.exists(output):
        os.mkdir(output)
    for file in data:
        fileName =  os.path.join(output, file["name"] + ".parquet") 
        file["data"].to_parquet(fileName)
    logger.info("Fake Data Generated Successfully")

parser = argparse.ArgumentParser(
    description="CLI for generating Fake data"
)
parser.add_argument(
    "--config","-c", help="config file location"
)

parser.add_argument(
    "--output","-o", help="output location"
)

args = parser.parse_args()

if __name__ == "__main__":
    generateFakeData(args.config, args.output)
