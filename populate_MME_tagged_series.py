#!/home/uwcc-admin/curw_rfield_extractor/venv/bin/python3
import traceback
import json
import pandas as pd
from datetime import datetime, timedelta
import sys
import numpy as np
import csv
import time

from db_adapter.logger import logger
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_FCST_USERNAME, CURW_FCST_PORT, CURW_FCST_PASSWORD, CURW_FCST_HOST, \
    CURW_FCST_DATABASE
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.curw_fcst.unit import UnitType, get_unit_id
from db_adapter.curw_fcst.variable import get_variable_id
from db_adapter.curw_fcst.station import get_wrf_stations
from db_adapter.curw_fcst.source import get_source_id
from db_adapter.curw_fcst.timeseries import Timeseries

wrf_v3_stations = {}


def list_of_lists_to_df_first_column_as_index(data):
    """

    :param data: data in list of lists format
    :return: equivalent pandas dataframe
    """
    original_data = np.array(data)
    index = original_data[:, 0]
    data = original_data[:, 1:]

    # datetime_index = index.astype('datetime64', copy=False)

    return pd.DataFrame.from_records(data=data, index=index)


def read_attribute_from_config_file(attribute, config, compulsory):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :param compulsory: Boolean value: whether the attribute is must present or not in the config file
    :return:
    """
    if attribute in config and (config[attribute]!=""):
        return config[attribute]
    elif compulsory:
        logger.error("{} not specified in config file.".format(attribute))
        exit(1)
    else:
        logger.error("{} not specified in config file.".format(attribute))
        return None


def select_d03_sub_region(all_grids, lon_min, lon_max, lat_min, lat_max):
    selected_grids = all_grids[(all_grids.longitude >= lon_min) & (all_grids.longitude <= lon_max) &
                                   (all_grids.latitude >= lat_min) & (all_grids.latitude <= lat_max)]

    return selected_grids


def calculate_MME_series(TS, start, end, variables, station_id, variable_id, unit_id):

    index = pd.date_range(start=start, end=end, freq='15min')
    df = pd.DataFrame(index=index)

    for variable in variables:

        model = variable[0]
        version = variable[1]
        sim_tag = variable[2]
        coefficient = variable[3]

        try:
            source_id = get_source_id(pool=pool, model=model, version=version)
        except Exception:
            msg = "Exception occurred while loading source id from database."
            logger.error(msg)
            exit(1)

        print("#########################", model, version)
        fcst_ts = TS.get_latest_timeseries(sim_tag=sim_tag, source_id=source_id, station_id=station_id,
                                 variable_id=variable_id, unit_id=unit_id)

        timeseries = list_of_lists_to_df_first_column_as_index(fcst_ts)

        print(timeseries)

        df = df.join(timeseries, lsuffix='_left', rsuffix='_right')
        df.fillna(0)

        print(df)


def update_MME_tagged_series(pool, start, end, variables, sub_region, tms_meta, fgt):

    for index, row in sub_region.iterrows():
        lat = float('%.6f' % row['latitude'])
        lon = float('%.6f' % row['longitude'])

        tms_meta['latitude'] = str(lat)
        tms_meta['longitude'] = str(lon)

        station_prefix = 'wrf_{}_{}'.format(lat, lon)

        station_id = wrf_v3_stations.get(station_prefix)

        TS = Timeseries(pool=pool)
        #
        # tms_id = TS.get_timeseries_id_if_exists(tms_meta)
        #
        # if tms_id is None:
        #     tms_id = TS.generate_timeseries_id(tms_meta)
        #
        #     run_meta = {
        #         'tms_id': tms_id,
        #         'sim_tag': tms_meta['sim_tag'],
        #         'start_date': fgt,
        #         'end_date': fgt,
        #         'station_id': station_id,
        #         'source_id': tms_meta['source_id'],
        #         'unit_id': tms_meta['unit_id'],
        #         'variable_id': tms_meta['variable_id']
        #     }
        #     try:
        #         TS.insert_run(run_meta)
        #     except Exception:
        #         logger.error("Exception occurred while inserting run entry {}".format(run_meta))
        #         traceback.print_exc()

        timeseries = calculate_MME_series(TS=TS, start=start, end=end, variables=variables, station_id=station_id,
                                          variable_id=tms_meta['variable_id'], unit_id=tms_meta['unit_id'])

        break

        # try:
        #     TS.insert_formatted_data(timeseries, True)  # upsert True
        #     TS.update_latest_fgt(id_=tms_id, fgt=fgt)
        # except Exception:
        #     time.sleep(5)
        #     try:
        #         TS.insert_formatted_data(timeseries, True)  # upsert True
        #         TS.update_latest_fgt(id_=tms_id, fgt=fgt)
        #     except Exception:
        #         msg = "Inserting the timseseries for tms_id {} and fgt {} failed.".format(timeseries[0][0],
        #                                                                          timeseries[0][2])
        #         logger.error(msg)
        #         traceback.print_exc()


if __name__=="__main__":

    try:
        # load d03 grids
        d03_grids = pd.read_csv('d03_grids_sorted.csv', delimiter=",")

        # load connection parameters
        mme_config = json.loads(open('MME_config.json').read())

        wrf_regions = read_attribute_from_config_file('wrf_regions', mme_config, True)

        # load meta data
        meta_config = json.loads(open('config.json').read())

        # source details
        model = read_attribute_from_config_file('model', meta_config, True)
        version = read_attribute_from_config_file('version', meta_config, True)

        # sim_tag
        sim_tag = read_attribute_from_config_file('sim_tag', meta_config, True)

        # unit details
        unit = read_attribute_from_config_file('unit', meta_config, True)
        unit_type = UnitType.getType(read_attribute_from_config_file('unit_type', meta_config, True))

        # variable details
        variable = read_attribute_from_config_file('variable', meta_config, True)

        pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD,
                        db=CURW_FCST_DATABASE)

        fgt = (datetime.now() + timedelta(hours=5, minutes=30)).strftime(COMMON_DATE_TIME_FORMAT)

        try:
            wrf_v3_stations = get_wrf_stations(pool)

            variable_id = get_variable_id(pool=pool, variable=variable)
            unit_id = get_unit_id(pool=pool, unit=unit, unit_type=unit_type)
            source_id = get_source_id(pool=pool, model=model, version=version)
        except Exception:
            msg = "Exception occurred while loading common metadata from database."
            logger.error(msg)
            sys.exit(1)

        tms_meta = {
            'model': model,
            'version': version,
            'sim_tag': sim_tag,
            'variable': variable,
            'unit': unit,
            'unit_type': unit_type.value,
            'variable_id': variable_id,
            'unit_id': unit_id,
            'source_id': source_id
        }

        for region in wrf_regions:
            lon_min = region.get('lon_min')
            lon_max = region.get('lon_max')
            lat_min = region.get('lat_min')
            lat_max = region.get('lat_max')
            start = region.get('start')
            end = region.get('end')
            variables = region.get('variables')

            sub_region = select_d03_sub_region(all_grids=d03_grids, lon_min=lon_min, lon_max=lon_max,
                                               lat_min=lat_min, lat_max=lat_max)

            update_MME_tagged_series(pool=pool, start=start, end=end, variables=variables, sub_region=sub_region,
                                     tms_meta=tms_meta, fgt=fgt)

    except Exception as e:
        print('An exception occurred.')
        traceback.print_exc()
    finally:
        print("Process finished")
        destroy_Pool(pool=pool)


