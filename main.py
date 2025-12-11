from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.config_entity import TrainingPipelineConfig

from networksecurity.logging.logger import logging
import sys


if __name__ == "__main__":
    try:
        logging.info("Entered the try block!")
        training_pipeline_config = TrainingPipelineConfig()

        data_ingestion_config = DataIngestionConfig(training_pipeline_config)

        data_ingestion = DataIngestion(data_ingestion_config)

        logging.info("Data ingestion initiated")

        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()

        print(data_ingestion_artifact)

    except Exception as e:
        raise NetworkSecurityException(e, sys)