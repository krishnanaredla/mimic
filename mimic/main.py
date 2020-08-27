import sys
import os

PACKAGE_PARENT = ".."
SCRIPT_DIR = os.path.dirname(
    os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
)
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

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


def processExpressionSchema(
    data: List[dict], df: pd.DataFrame, rows: int
) -> pd.DataFrame:
    logger.info("Generating Fake data from mimesis")
    getFixedSchema = fs.Switcher()
    logger.info("Getting Names Data")
    data_dict = {}
    for column in data:
        if "custom_values" in column:
            logger.info("Getting Custom values for {0}".format(column["name"]))
            print(column["custom_values"])
            data_dict.update(
                {
                    column["name"]: getFixedSchema.faker(
                        column["value"], rows, **column["custom_values"]
                    )
                }
            )
            logger.info("completed custom values for {0}".format(column["name"]))
            print(data_dict)
        else:
            logger.info("getting generic values for {0}".format(column["name"]))
            data_dict.update(
                {column["name"]: getFixedSchema.faker(column["value"], rows)}
            )
            logger.info("Completed generic values for {0}".format(column["name"]))
            print(data_dict)
    df = pd.DataFrame(data_dict)
    return df


def processFixedSchema(data: List[dict], df: pd.DataFrame, rows: int) -> pd.DataFrame:
    logger.info("Generating Fake Data from mimesis")
    getFixedSchema = fs.Switcher()
    logger.info("Getting Names Data")
    try:
        names = pd.DataFrame(
            # [getFixedSchema.faker("getNameData") for pos in range(rows)]
            getFixedSchema.faker("getNameData", rows)
        )

        logger.info("Completed Names")
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
            logger.info("Getting Custom values for {0}".format(column["name"]))
            try:
                df[column["name"]] = pd.DataFrame(
                    # [
                    #    getFixedSchema.faker(column["value"], **column["custom_values"])
                    #    for pos in range(rows)
                    # ]
                    getFixedSchema.faker(
                        column["value"], rows, **column["custom_values"]
                    )
                )
                logger.info("completed custom values for {0}".format(column["name"]))
            except Exception as e:
                logger.error(
                    "Failed during generating data for custom values for column {0}".format(
                        column["name"]
                    )
                )
                logger.error(e)
        else:
            try:
                logger.info("getting generic values for {0}".format(column["name"]))
                df[column["name"]] = pd.DataFrame(
                    # [getFixedSchema.faker(column["value"]) for pos in range(rows)]
                    getFixedSchema.faker(column["value"], rows)
                )
                logger.info("Completed generic values for {0}".format(column["name"]))
            except Exception as e:
                logger.error("Failed for column {0}".format(column["name"]))
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
    # df = processExpressionSchema(ExpressionColumns, df, rows)
    df = df[column_order]
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


def writeOutput(df: pd.DataFrame, name: str, output: str) -> None:
    if not os.path.exists(output):
        os.mkdir(output)
    logger.info("Writing file {0}".format(name))
    fileName = os.path.join(output, name + ".parquet")
    df.to_parquet(fileName)
    # print(df)
    return None


def generateFakeData(path: str, output: str) -> None:
    logger.info("Process Started")
    config = getConfigfromFile(path)
    data = processSchema(config)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_process = {
            executor.submit(writeOutput, file["data"], file["name"], output): file
            for file in data
        }
        for future in concurrent.futures.as_completed(future_to_process):
            data = future.result()
    logger.info("Fake Data Generated Successfully")
    return None


parser = argparse.ArgumentParser(description="CLI for generating Fake data")
parser.add_argument("--config", "-c", help="config file location")

parser.add_argument("--output", "-o", help="output location")

args = parser.parse_args()

if __name__ == "__main__":
    generateFakeData(args.config, args.output)
