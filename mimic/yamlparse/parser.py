import yaml


def getConfig(fileLocation:str,logger) -> dict :
    with open(fileLocation,"r") as stream:
        try:
            logger.info("Loading the Config File to get Schema for fake data")
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logger.error("Loading yaml failed")
            logger.error(exc)
        return config