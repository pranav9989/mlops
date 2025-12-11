# data_ingestion.py
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact

import os
import sys
import pymongo
import numpy as np
import pandas as pd
from typing import List
from sklearn.model_selection import train_test_split

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        """
        Expect an *instance* of DataIngestionConfig here.
        """
        try:
            # assign the instance passed in (not the class)
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def export_collection_as_dataframe(self) -> pd.DataFrame:
        """
        Read data from MongoDB Atlas and return a pandas DataFrame.
        """
        try:
            if not MONGO_DB_URL:
                raise NetworkSecurityException(
                    Exception("MONGO_DB_URL not set in environment"), sys
                )

            database_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name

            # create client and fetch collection
            mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            collection = mongo_client[database_name][collection_name]

            df = pd.DataFrame(list(collection.find()))

            # drop _id properly if present
            if "_id" in df.columns:
                df = df.drop(columns=["_id"], axis=1)

            # replace string "na" with numpy nan
            df.replace({"na": np.nan}, inplace=True)

            # close client
            mongo_client.close()

            if df.empty:
                logging.warning("The dataframe is empty after reading the collection.")

            return df

        except NetworkSecurityException:
            # re-raise our own exceptions unchanged
            raise
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def export_data_into_feature_store(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        try:
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            # create parent directories
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            logging.info(f"Exported feature store to: {feature_store_file_path}")
            return dataframe

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def split_data_as_train_test(self, dataframe: pd.DataFrame):
        try:
            ratio = self.data_ingestion_config.train_test_split_ratio
            train_set, test_set = train_test_split(dataframe, test_size=ratio)
            logging.info("Performed train/test split on the dataframe")

            logging.info("Exited split_data_as_train_test method of DataIngestion class")

            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)

            logging.info(f"Exporting train and test files to: {dir_path}")

            train_set.to_csv(
                self.data_ingestion_config.training_file_path, index=False, header=True
            )

            test_set.to_csv(
                self.data_ingestion_config.testing_file_path, index=False, header=True
            )
            logging.info("Exported train and test files successfully.")

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            dataframe = self.export_collection_as_dataframe()
            dataframe = self.export_data_into_feature_store(dataframe)
            self.split_data_as_train_test(dataframe)
            data_ingestion_artifact = DataIngestionArtifact(
                train_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path,
            )

            return data_ingestion_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)
