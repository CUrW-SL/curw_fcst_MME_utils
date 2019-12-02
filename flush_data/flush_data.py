#!/home/uwcc-admin/curw_fcst_MME_utils/venv/bin/python3
import traceback

from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_FCST_USERNAME, CURW_FCST_PORT, CURW_FCST_PASSWORD, CURW_FCST_HOST, \
    CURW_FCST_DATABASE
from db_adapter.curw_fcst.timeseries import Timeseries


def get_curw_fcst_hash_ids(pool, sim_tag=None, source_id=None, variable_id=None, unit_id=None, station_id=None,
                           start=None, end=None):

    pre_sql_statement = "SELECT `id` FROM `run` WHERE "

    condition_list = []
    variable_list = []

    score = 0

    if sim_tag is not None:
        condition_list.append("`sim_tag`=%s")
        variable_list.append(sim_tag)
        score +=1
    if source_id is not None:
        condition_list.append("`source`=%s")
        variable_list.append(source_id)
        score +=1
    if variable_id is not None:
        condition_list.append("`variable`=%s")
        variable_list.append(variable_id)
        score +=1
    if unit_id is not None:
        condition_list.append("`unit`=%s")
        variable_list.append(unit_id)
        score +=1
    if station_id is not None:
        condition_list.append("`station`=%s")
        variable_list.append(station_id)
        score +=1
    if start is not None:
        condition_list.append("`start_date`=%s")
        variable_list.append(start)
        score +=1
    if end is not None:
        condition_list.append("`end_date`=%s")
        variable_list.append(end)
        score +=1

    if score == 0:
        return None

    conditions = " AND ".join(condition_list)

    sql_statement = pre_sql_statement + conditions + ";"

    print(sql_statement)

    ids = []
    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            row_count = cursor.execute(sql_statement, tuple(variable_list))
            if row_count > 0:
                results = cursor.fetchall()
                for result in results:
                    ids.append(result.get('id'))
        return ids
    except Exception:
        traceback.print_exc()
    finally:
        if connection is not None:
            connection.close()


if __name__=="__main__":

    try:

        pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD,
                        db=CURW_FCST_DATABASE)

        hash_ids = get_curw_fcst_hash_ids(pool=pool, sim_tag="daily_run", source_id=None,
                                          variable_id=None, unit_id=None, station_id=None,
                                          start=None, end=None)

        TS = Timeseries(pool=pool)

        ################################################################################
        # delete as a whole (entry in run table and all related entries in data table) #
        ################################################################################
        # count = 0
        # for id in hash_ids:
        #     TS.delete_all_by_hash_id(id_=id)
        #     count+=1
        #     print(count, id)
        # print("{} of hash ids are deleted".format(len(hash_ids)))

        ###################################################################################
        # delete a specific timeseries defined by a given hash id and fgt from data table #
        ###################################################################################
        # fgt = "2019-10-17 19:52:00"
        # count = 0
        # for id in hash_ids:
        #     TS.delete_timeseries(id_=id, fgt=fgt)
        #     count += 1
        #     print(count, id)
        #
        # print("{} of hash ids are deleted".format(len(hash_ids)))

    except Exception as e:
        print('An exception occurred.')
        traceback.print_exc()
    finally:
        print("Process finished")
        destroy_Pool(pool=pool)