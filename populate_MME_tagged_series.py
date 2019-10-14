#!/home/uwcc-admin/curw_rfield_extractor/venv/bin/python3
import traceback
import json
import pandas as pd
from datetime import datetime, timedelta
import sys

from db_adapter.logger import logger
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_FCST_USERNAME, CURW_FCST_PORT, CURW_FCST_PASSWORD, CURW_FCST_HOST, \
    CURW_FCST_DATABASE
from db_adapter.curw_fcst.unit import UnitType, get_unit_id
from db_adapter.curw_fcst.variable import get_variable_id
from db_adapter.curw_fcst.station import get_station_id, get_wrf_stations
from db_adapter.curw_fcst.source import get_source_id

wrf_v3_stations = {}


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


def update_MME_tagged_series(start, end, variables, sub_region, tms_meta):

    print(sub_region)
    # station_prefix = 'wrf_{}_{}'.format(lat, lon)
    #
    # station_id = wrf_v3_stations.get(station_prefix)

    # tms_id = ts.get_timeseries_id_if_exists(tms_meta)
    #
    # if tms_id is None:
    #     tms_id = ts.generate_timeseries_id(tms_meta)
    #
    #     run_meta = {
    #         'tms_id': tms_id,
    #         'sim_tag': tms_meta['sim_tag'],
    #         'start_date': start_date,
    #         'end_date': end_date,
    #         'station_id': station_id,
    #         'source_id': tms_meta['source_id'],
    #         'unit_id': tms_meta['unit_id'],
    #         'variable_id': tms_meta['variable_id']
    #     }
    #     try:
    #         ts.insert_run(run_meta)
    #     except Exception:
    #         logger.error("Exception occurred while inserting run entry {}".format(run_meta))
    #         traceback.print_exc()

    return


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

            update_MME_tagged_series(start=start, end=end, variables=variables, sub_region=sub_region, tms_meta=tms_meta)

    except Exception as e:
        print('An exception occurred.')
        traceback.print_exc()
    finally:
        print("Process finished")


