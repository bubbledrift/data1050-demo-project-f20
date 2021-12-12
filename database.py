import logging
import pymongo
import pandas as pds
import expiringdict

import utils

client = pymongo.MongoClient()
logger = logging.Logger(__name__)
utils.setup_logger(logger, 'db.log')
RESULT_CACHE_EXPIRATION = 10             # seconds


def upsert_bpa(df):
    """
    Update MongoDB database `energy` and collection `energy` with the given `DataFrame`.
    """
    db = client.get_database("corona")
    collections = ["ca","me","ms","nh","nd","gu","ne","ct","wi","nc","al","mo","il","nv","mi","in","wa","vt","md","de","mt","vi",
                    "id","la","dc","nj","ri","mn","az","fsm","sc","ky","or","va","wy","ut","co","ar","pw","mp","ma","pr","ga",
                    "nyc","as","ok","ak","ks","tx","fl","wv","hi","tn","pa","ia","oh","rmi","sd","ny","nm"]

    state_df = [ca_df,me_df,ms_df,nh_df,nd_df,gu_df,ne_df,ct_df,wi_df,nc_df,al_df,mo_df,il_df,nv_df,mi_df,in_df,wa_df,vt_df,
                md_df,de_df,mt_df,vi_df,id_df,la_df,dc_df,nj_df,ri_df,mn_df,az_df,fsm_df,sc_df,ky_df,or_df,va_df,wy_df,ut_df,
                co_df,ar_df,pw_df,mp_df,ma_df,pr_df,ga_df,nyc_df,as_df,ok_df,ak_df,ks_df,tx_df,fl_df,wv_df,hi_df,tn_df,pa_df,
                ia_df,oh_df,rmi_df,sd_df,ny_df,nm_df]

    for x, y in zip(collections, state_df):
        collection = db.get_collection(x)
        update_count = 0
        for record in y.to_dict('records'):
            result = collection.replace_one(
                filter={'Datetime': record['Datetime']},    # locate the document if exists
                replacement=record,                         # latest document
                upsert=True)                                # update if exists, insert if not
            if result.matched_count > 0:
                update_count += 1
    print(f"rows={df.shape[0]}, update={update_count}, "
          f"insert={df.shape[0]-update_count}")


def fetch_all_bpa(state):
    db = client.get_database("corona")
    collection = db.get_collection(state)
    ret = list(collection.find())
    logger.info(str(len(ret)) + ' documents read from the db')
    return ret


_fetch_all_bpa_as_df_cache = expiringdict.ExpiringDict(max_len=1,
                                                       max_age_seconds=RESULT_CACHE_EXPIRATION)


def fetch_all_bpa_as_df(state, allow_cached=False):
    """Converts list of dicts returned by `fetch_all_bpa` to DataFrame with ID removed
    Actual job is done in `_worker`. When `allow_cached`, attempt to retrieve timed cached from
    `_fetch_all_bpa_as_df_cache`; ignore cache and call `_work` if cache expires or `allow_cached`
    is False.
    """
    def _work():
        data = fetch_all_bpa(state)
        if len(data) == 0:
            return None
        df = pds.DataFrame.from_records(data)
        df.drop('_id', axis=1, inplace=True)
        return df

    if allow_cached:
        try:
            return _fetch_all_bpa_as_df_cache['cache']
        except KeyError:
            pass
    ret = _work()
    _fetch_all_bpa_as_df_cache['cache'] = ret
    return ret


if __name__ == '__main__':
    print(fetch_all_bpa_as_df(state))
