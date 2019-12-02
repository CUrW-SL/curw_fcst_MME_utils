#!/home/uwcc-admin/curw_fcst_MME_utils/venv/bin/python3
import traceback
from datetime import datetime, timedelta

from db_adapter.constants import CURW_FCST_DATABASE, CURW_FCST_PORT, CURW_FCST_PASSWORD, CURW_FCST_USERNAME, \
    CURW_FCST_HOST
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.curw_fcst.common import get_distinct_fgts_for_given_id, get_curw_fcst_hash_ids
from db_adapter.curw_fcst.timeseries import Timeseries


def select_fgts_older_than_month(fgts):

    select_fgts = []

    deadline = datetime.now() - timedelta(days=30)

    for fgt in fgts:
        if fgt < deadline:
            select_fgts.append(fgt)

    return select_fgts


if __name__=="__main__":

    try:

        pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD,
                        db=CURW_FCST_DATABASE)

        hash_ids = get_curw_fcst_hash_ids(pool=pool, sim_tag="hourly_run", source_id=9,
                                          variable_id=2, unit_id=None, station_id=None,
                                          start=None, end=None)

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
                TS.delete_timeseries(id_=id, fgt=fgt)
                print(count, id, fgt)

        print("{} of hash ids are deleted.".format(count))

    except Exception as e:
        print('An exception occurred.')
        traceback.print_exc()
    finally:
        print("Process finished")
        destroy_Pool(pool=pool)