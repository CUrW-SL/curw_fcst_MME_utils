#!/home/uwcc-admin/curw_fcst_MME_utils/venv/bin/python3
import traceback
from datetime import datetime, timedelta

from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.curw_fcst.common import get_distinct_fgts_for_given_id, get_curw_fcst_hash_ids
from db_adapter.curw_fcst.timeseries import Timeseries

# from db_adapter.constants import CURW_FCST_USERNAME, CURW_FCST_PORT, CURW_FCST_PASSWORD, CURW_FCST_HOST, \
#     CURW_FCST_DATABASE

FLO2D_250_ID = 9
FLO2D_150_ID = 10


def select_fgts_older_than_month(fgts):

    select_fgts = []

    deadline = datetime.now() - timedelta(days=30)

    for fgt in fgts:
        if fgt < deadline:
            select_fgts.append(fgt)

    return select_fgts


def flush_timeseries(pool, hash_ids):

    TS = Timeseries(pool=pool)

    ###################################################################################
    # delete a specific timeseries defined by a given hash id and fgt from data table #
    ###################################################################################
    count = 0
    for id in hash_ids:
        fgts = get_distinct_fgts_for_given_id(pool=pool, id_=id)

        outdated_fgts = select_fgts_older_than_month(fgts)
        count += 1
        for fgt in outdated_fgts:
            fgts.remove(fgt)
            TS.delete_timeseries(id_=id, fgt=fgt)
            print(count, id, fgt)

        TS.update_start_date(id_=id, start_date=min(fgts), force=True)

    print("{} of hash ids are deleted.".format(count))


if __name__=="__main__":

    try:

        set_db_config_file_path('/home/uwcc-admin/curw_fcst_MME_utils/db_adapter_config.json')

        pool = get_Pool(host=connection.CURW_FCST_HOST, port=connection.CURW_FCST_PORT, user=connection.CURW_FCST_USERNAME,
                        password=connection.CURW_FCST_PASSWORD, db=connection.CURW_FCST_DATABASE)

        # pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT,
        #                 user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD, db=CURW_FCST_DATABASE)

        flo2d_250_hash_ids = get_curw_fcst_hash_ids(pool=pool, sim_tag="hourly_run", source_id=FLO2D_250_ID,
                                          variable_id=None, unit_id=None, station_id=None,
                                          start=None, end=None)

        flush_timeseries(pool=pool, hash_ids=flo2d_250_hash_ids)

        flo2d_150_hash_ids = get_curw_fcst_hash_ids(pool=pool, sim_tag="hourly_run", source_id=FLO2D_150_ID,
                                                    variable_id=None, unit_id=None, station_id=None,
                                                    start=None, end=None)

        flush_timeseries(pool=pool, hash_ids=flo2d_150_hash_ids)

    except Exception as e:
        print('An exception occurred.')
        traceback.print_exc()
    finally:
        print("Process finished")
        destroy_Pool(pool=pool)
