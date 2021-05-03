import logging

from boiler.heating_obj.io.sync_heating_obj_file_loader import SyncHeatingObjFileLoader
from boiler.heating_obj.io.sync_heating_obj_pickle_reader import SyncHeatingObjPickleReader

import config

if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")

    reader = SyncHeatingObjPickleReader()
    loader = SyncHeatingObjFileLoader(
        filepath=config.BOILER_PREPROCESSED_HEATING_CIRCUIT_DATASET_PATH,
        reader=reader
    )
    boiler_df = loader.load_heating_obj()
    for column, dtype in zip(boiler_df.columns, boiler_df.dtypes):
        print(column, dtype)
    print()
