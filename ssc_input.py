#!/usr/bin/env python3
"""
Author : Travis Simmons
Date   : July 31, 2024
Purpose: Rock the Casbah
"""
# Sample deployment

# python3 prepare.py -i /home/u24/travissimmons/cjx/season10/50_hand_label_test_2020_03_02 -o /home/u24/travissimmons/cjx/season10/gifs
# makeflow process.makeflow -j 1
import logging
#import traceback
import argparse
import os
#import shutil
#import sys
import glob
#import subprocess
from multiprocessing import Pool#, Manager, cpu_count
import json
from pystac_client import Client
#from requests.adapters import HTTPAdapter
from urllib3 import Retry
from pystac_client.stac_api_io import StacApiIO

#import geopandas as gpd
#import os
import numpy as np
import pandas as pd
from shapely.geometry import LineString, mapping
from itertools import repeat
import netCDF4 as ncf
#from itertools import chain
from datetime import datetime
from datetime import timedelta
#import tqdm
from collections import defaultdict
#from concurrent.futures import ThreadPoolExecutor, as_completed
#from itertools import repeat
#from typing import List, Tuple
import functools
print = functools.partial(print, flush=True)

from random import randint
from time import sleep
import time


logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%dT%H:%M:%S',
                    level=logging.INFO)


def generate_time_search(timekey):
        # timekey = "2024-01-01T00:00:00Z,2024-04-01T23:59:59Z"
        time1 = timekey.split(',')[0].split('T')[0]
        all_date = [int(i) for i in time1.split('-')]
        time2 = timekey.split(',')[1].split('T')[0]
        final_hours = [i for i in timekey.split(',')[1].split('T')[1].split(':')]
        all_date2 = [int(i) for i in time2.split('-')]


        start_date = datetime(all_date[0], all_date[1], all_date[2])
        end_date = datetime(all_date2[0], all_date2[1], all_date2[2])

        add_days = timedelta(days=30)
        add_ending_hours = timedelta(hours = int(final_hours[0]), minutes=int(final_hours[1]), seconds=int(final_hours[2][:-1]))


        start_dates = []
        ending_dates = []

        while start_date <= end_date:
            start_dates.append(start_date)
            start_date += add_days
            ending_dates.append(start_date)

        ending_dates[-1] = end_date + add_ending_hours

        parsed_dates = []

        for i in range(len(start_dates)):
            
            
            parsed_dates.append(','.join([start_dates[i].strftime('%Y-%m-%dT%H:%M:%SZ'), ending_dates[i].strftime('%Y-%m-%dT%H:%M:%SZ')]))
        return parsed_dates
# find hls tiles given a point

def find_hls_tiles(date_range = False, sword_path = False, cont = False,reach_id=False, collections = ['HLSL30.v2.0', 'HLSS30.v2.0']):

    STAC_URL = 'https://cmr.earthdata.nasa.gov/stac'


    ## NEW APPROACH
    tries = 5 # was 20, but we now also retry within the stac catalog code now.
    tries_cnt = 0
    links = []
    success = False
    for i in range(tries):
        logging.info('processing %s', reach_id)
        try:
            # sleep(randint(1,60))
            logging.info('getting reach node coords')
            line_geo = get_reach_node_cords(sword_path,reach_id, cont)
            logging.info('got reach node coords, %s', line_geo)
            tries_cnt += 1

            retry = Retry(
              total=2, backoff_factor=1, status_forcelist=[429, 502, 503, 504], allowed_methods=None
            )
            logging.info('retry number: %s', retry)
            stac_api_io = StacApiIO(max_retries=retry)
            logging.info('Opening stac catalog')
            catalog = Client.open(f'{STAC_URL}/LPCLOUD/', stac_io=stac_api_io)
            logging.info('Searching Catalog')
            search = catalog.search(
            collections=collections,
            intersects=line_geo,
            datetime = date_range.replace(',', '/'),
            query={"eo:cloud_cover": {"lt": 30}}
            # limit=1000  # optional, just sets page size
            )
            logging.info('Search complete')
            links = []
            all_items = search.items()


            for i in all_items:
            # print(i, list(i.keys()))
                i = i.to_dict()
                preferred_band = "B02"   # blue
                for key, asset in i["assets"].items():
                    if key == preferred_band:
                        links.append(asset["href"])

                # for key in i['assets']:
                #     # print(key)
                #     if key.startswith('B'):
                #         # link = i.assets[key].href.replace('https://data.lpdaac.earthdatacloud.nasa.gov/', 's3://')
                #         link = i['assets'][key]['href']
                #         links.append(link)
                

                
            logging.info('%s succeeded after %s tries', reach_id, tries_cnt)
            logging.info('example link from geo...%s',list(set([os.path.basename(a_link).split('.')[2] for a_link in links])))
            success = True
            break
        except Exception as e:
            er = e
            if 'rate' in str(e):
                #pass
                # previously 1,120 2 minutes!!!
                sleep(randint(5, 10))
            #else:
                #pass
                #sleep(randint(1,20))
            logging.info('%s failed error: %s, tries: %s', reach_id, e, tries_cnt)
    
    # if not success:
    #     try:
    #         links.append(f"failed,error: {e}")
    #     except:
    #         links.append('failed with no error')
    return list(set(links))



def find_download_links_for_reach_tiles(sword_path, reach_id, cont, temporal_range):
    
    
    

    # df = pd.DataFrame(columns=['x', 'y'])
    # geometry_chunks = [Point(xy) for xy in zip(lat_list, lon_list)]

    # # Create a LineString
    # line = LineString(zip(lat_list, lon_list))

    # Convert to GeoJSON-like dict for STAC search
    # line_geojson = mapping(line)
    

    all_links = find_hls_tiles(sword_path=sword_path, cont = cont, date_range=temporal_range, reach_id=reach_id)

    all_links = list(set(all_links))
    out_data = {reach_id:all_links}
    
    logging.info('finished searching for %s; found %s tiles', reach_id, len(all_links))
    return out_data
    
    
    
    
    

    # line_geojson = get_reach_node_cords(sword_path,reach_id, cont)
    # # df = pd.DataFrame(columns=['x', 'y'])
    # # geometry_chunks = [Point(xy) for xy in zip(lat_list, lon_list)]

    # # # Create a LineString
    # # line = LineString(zip(lat_list, lon_list))

    # # Convert to GeoJSON-like dict for STAC search
    # # line_geojson = mapping(line)
    
    # all_links = []
    # if type(line_geojson) != str:
    #     links = find_hls_tiles(line_geo=line_geojson, date_range=temporal_range, reach_id=reach_id)
    # else:
    #     # there was an error pulling the reach_node coords, save it as an error
    #     links = [line_geojson]
    # all_links.extend(list(set(links)))

    # all_links = list(set(all_links))
    # out_data = {reach_id:all_links}
    
    # logging.info('finished searching for ', reach_id, 'found', len(all_links), 'tiles...')
    # return out_data

def get_five_points(lat_list, lon_list):
    n = len(lat_list)
    if n < 3:
        return "error, reach must have at least 3 points"

    first_idx = 0
    last_idx = n - 1
    mid_idx = n // 2
    mid_start_idx = (first_idx + mid_idx) // 2
    mid_end_idx = (mid_idx + last_idx) // 2

    points = [
        (lat_list[first_idx], lon_list[first_idx]),      # First
        (lat_list[mid_start_idx], lon_list[mid_start_idx]),  # Between start and middle
        (lat_list[mid_idx], lon_list[mid_idx]),          # Middle
        (lat_list[mid_end_idx], lon_list[mid_end_idx]),  # Between middle and end
        (lat_list[last_idx], lon_list[last_idx])         # Last
    ]
    
    # # Create a LineString
    line = LineString(points)

    # Convert to GeoJSON-like dict for STAC search
    line_geojson = mapping(line)

    return line_geojson



def get_reach_node_cords(sword_path, reach_id, cont):

    lat_list, lon_list = [], []
    tries = 20
    for i in range(tries):
        try:
            rootgrp = ncf.Dataset(sword_path, "r", format="NETCDF4")
            break
        except:
            sleep(randint(1,20))
            pass
    
    node_ids_indexes = np.where(rootgrp.groups['nodes'].variables['reach_id'][:].data.astype('U') == str(reach_id))

    logging.info('node_ids_indexes, %s', node_ids_indexes)
    if len(node_ids_indexes[0])!=0:
        for y in node_ids_indexes[0]:

            lat = float(rootgrp.groups['nodes'].variables['x'][y].data.astype('U'))
            lon = float(rootgrp.groups['nodes'].variables['y'][y].data.astype('U'))
            # all_nodes.append([lat,lon])
            lat_list.append(lat)
            lon_list.append(lon)


    rootgrp.close()
    return get_five_points(lat_list, lon_list)






def ssc_process_continent(reach_ids, cont, sword_path, temporal_range):


## old
    input_vars = zip(repeat(sword_path), reach_ids, repeat(cont), repeat(temporal_range))
    logging.info(reach_ids[:2])
    logging.info('starting workers...')
    pool = Pool(processes=min(len(reach_ids), os.cpu_count())) # maximum number is the CPU count
    #pool = Pool(processes=len(reach_ids))              # start 4 worker processes
    result = pool.starmap(find_download_links_for_reach_tiles, input_vars )


    inverted = defaultdict(list)
    for i in result:
        for reach_id, links in i.items():
            for link in links:
                inverted[link].append(reach_id)

    # Convert defaultdict to regular dict 
    result = dict(inverted)

    # pool.close()
    return result

def get_reach_ids(cont_number:list, indir:str, run_globe:bool, sword_path:str):
    # reach_ids = [logging.info(os.path.basename(i).split('_')[0]) for i in glob.glob(os.path.join(indir, 'swot','*'))\
        #  if os.path.basename(i)[0] in cont_number]
    if not run_globe:
        all_reach_ids =glob.glob(os.path.join(indir, 'swot','*SWOT.nc'))
        reach_ids = [os.path.basename(i).split('_')[0] for i in all_reach_ids if int(os.path.basename(i)[0]) in cont_number]
    else:
        sword_data = ncf.Dataset(sword_path)
        reach_ids = [str(i) for i in sword_data['reaches']['reach_id'][:] if str(i)[-1] == '1']
        sword_data.close()
        
    return reach_ids

def get_cont_info(index:int, indir:str):
    with open(os.path.join(indir,'continent.json')) as f:
        cont_data = json.load(f)
    cont = list(cont_data[index].keys())[0]
    cont_number = cont_data[index][cont]
    return cont, cont_number

def write_json(json_object, filename):
    """Write JSON object as a JSON file to the specified filename."""

    with open(filename, 'w') as jf:
        json.dump(json_object, jf, indent=2)



def parse_hls_from_link(link: str) -> dict:
    """
    Parse an HLS band filename like:
      HLS.L30.T11SLC.2022166T182646.v2.0.B02.tif
    """
    base = os.path.basename(link)
    parts = base.split(".")

    # Expected (band files): HLS.<sensor>.<tile>.<YYYYDDDTHHMMSS>.v2.0.<band>.tif
    # Example parts: ['HLS','L30','T11SLC','2022166T182646','v2','0','B02','tif']
    if len(parts) < 8 or parts[0] != "HLS":
        # fallback: return minimal info
        return {"filename": base}

    dt_str = parts[3]                 # '2022166T182646'

    # Parse YYYY + Julian day + time
    dt = pd.to_datetime(dt_str, format="%Y%jT%H%M%S", utc=True)

    return {
        "filename": base,
        "dt_utc": dt,                 # full timestamp
        "date": dt.date(),            # just the date
    }




# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description='Rock the Casbah',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i',
                        '--index',
                        help='index of continent to process',
                        metavar='int',
                        type=int,
                        default=-235
                        )

    parser.add_argument('-w',
                        '--swotfile',
                        help='where to find the swot file',
                        metavar='str',
                        type=str,
                        default="D:\SWOT_Q\oc_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc"
                        )


    parser.add_argument('-t',
                        '--temporal_range',
                        help='Temporal range to search for tiles',
                        metavar='str',
                        type=str,
                        default="2023-03-31T00:00:00Z,2026-12-12T23:59:59Z")

    parser.add_argument('-n',
                        '--indir',
                        help='input directory',
                        metavar='str',
                        type=str,
                        default= '/data/input')

    parser.add_argument('-o',
                        '--outdir',
                        help='output directory',
                        metavar='str',
                        type=str,
                        default= '/data/input')
    
    parser.add_argument('-g',
                    '--run_globe',
                    help='run the globe, do not use swot data inputs',
                    action='store_true',
                    default=False)
    
    parser.add_argument('-s',
                    '--starting_chunk',
                    help='What chunk to pick up at',
                    type=int,
                    default=0)


    return parser.parse_args()
logging.info('running...')


def main():
# testing=True
    
# if testing:
    #"""Make a jazz noise here"""
    args = get_args() 
    indir = args.indir
    outdir = args.outdir
    index = args.index
    temporal_range = args.temporal_range
    run_globe = args.run_globe
    starting_chunk = args.starting_chunk
    swotfile=args.swotfile
    for arg, val in args.__dict__.items():
        logging.info("%s: %s", arg, val)
    index=4 # hard coded, for test
    outdir="D:/Luisa/data/ssc_input_test_2026_02_04"# hard coded, for test
    if index == -235 or None:
        # index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
        index_range = range(0,8)
    
    else:
        index_range = range(index, index+1)
        
    #logging.info('here is index %s', index)
    logging.info('we are going to open the swot nc file, %s', swotfile)
    # Open file in read-only mode
    nc = ncf.Dataset(swotfile, mode="r")

    # List variables
    print(nc.variables.keys())
    print("Dimensions:", nc.dimensions.keys())
    print("Variables at root:", nc.variables.keys())
    print("Groups:", nc.groups.keys())


    # Access a variable
    temp = nc.groups["consensus"]  # read all data into memory
    print(temp.variables.keys())
    print("Dimensions:", temp.dimensions.keys())
    print("Variables at root:", temp.variables.keys())
    print("Groups:", temp.groups.keys())
    temp2=temp.variables["consensus_q"][:]
    temp3=temp.variables["time_int"][:]
    temp4 = nc.groups["reaches"]
    print("Dimensions:", temp4.dimensions.keys())
    print("Variables at root:", temp4.variables.keys())
    print("Groups:", temp4.groups.keys())
    temp5=temp4.variables["reach_id"][:]

    # Close file
    nc.close()

    df = pd.DataFrame({
        "reach_id": temp5.filled(np.nan),
        "Q": temp2,
        "date": temp3,
    })
    df_save=df#.dropna(subset='Q')
    df_exploded_save=df_save.explode(["date", "Q"])

    df_exploded_save=df_exploded_save.dropna(subset='Q')
    df_exploded_save = df_exploded_save[df_exploded_save["date"] >= 0].copy()
    
    test=df_exploded_save[df_exploded_save['reach_id']==51111100013]
    logging.info('r5: %s',test)
    
    reach_ids_swot_q=df_exploded_save['reach_id'].unique()
    reach_ids=reach_ids_swot_q[0:5] #eliminate this 0 to 5 when its time to run
    
    for index in index_range:

        #cont, cont_number = get_cont_info(index = index, indir = indir)
        #cont='oc' # hard coded, for test
        #cont_number=5 # hard coded, for test
        #logging.info('processing %s', cont)
        # there is no point in processing just one continent. Process all.
        
        #please replace this part using the right continent indexes
        reach_index=index+1
        if reach_index==1:
            cont='af'
            cont_number=reach_index
        if reach_index==2:
            cont='eu' #eu is 2
            cont_number=reach_index
        if reach_index==3:
            cont='eu' # which is 3?
            cont_number=reach_index
        if reach_index==4:
            cont='as'
            cont_number=reach_index
        if reach_index==5:
            cont='oc'
            cont_number=reach_index
        if reach_index==6:
            cont='sa'
            cont_number=reach_index
        if reach_index==7:
            cont='na'
            cont_number=reach_index
        
        sword_path =  "D:/SWORD/SWORD_v17b_netcdf/netcdf/"+cont+"_sword_v17b.nc"
        #sword_path="D:/SWORD/SWORD_v16_netcdf/SWORD_v16_netcdf/netcdf/"+cont+"_sword_v16.nc"#"D:/SWORD/SWORD_v16_shp/oc_shp_merged/union/sword_v16_oc.shp"
        #os.path.join(indir, 'sword', f'{cont}_sword_v16_patch.nc')
        #if not os.path.exists(sword_path):
        #    sword_path = os.path.join(indir, 'sword', f'{cont}_sword_v16.nc')
        #reach_ids = get_reach_ids(cont_number = cont_number, indir=indir, run_globe=run_globe, sword_path=sword_path)
        
        
        rid_chunks =  [ reach_ids[i:i+50] for i in range(0,len(reach_ids),50) ]
        # rid_chunks = rid_chunks[305:]
        
        chunk_num = starting_chunk
        rid_chunks = rid_chunks[chunk_num:]

        if len(rid_chunks) > 0:
            if len(rid_chunks[-1]) == 1:
                rid_chunks[-2].extend(rid_chunks[-1])
                rid_chunks.pop(-1)
            for rid_chunk in rid_chunks:
                start_time = time.time()
                logging.info('processing chunk %s of %s', chunk_num, len(rid_chunks))
                bands = ssc_process_continent(rid_chunk, cont, sword_path, temporal_range)
                #logging.info('bands variable: %s',bands)
                logging.info('rid_chunk %s', rid_chunk)
                # we should filter by date, now.
                rows = []
                for link, reach_list in bands.items():
                    meta = parse_hls_from_link(link)
                    meta["link"] = link
                    meta["reach_ids"] = reach_list
                    meta["n_reaches"] = len(reach_list) if isinstance(reach_list, list) else None
                    rows.append(meta)
                    #logging.info('meta: %s',meta)

                df_bands = pd.DataFrame(rows)
                #logging.info('df_bands["reach_ids"]: %s',df_bands["reach_ids"])
                df_exploded_bands = df_bands.explode("reach_ids").rename(columns={"reach_ids": "reach_id"})

                df_exploded_save["reach_id"] = pd.to_numeric(df_exploded_save["reach_id"], errors="coerce").astype("Int64")
                df_exploded_bands["reach_id"] = pd.to_numeric(df_exploded_bands["reach_id"], errors="coerce").astype("Int64")

                # df_exploded_save["reach_id"] = df_exploded_save["reach_id"].astype(str)
                # df_exploded_bands["reach_id"] = df_exploded_bands["reach_id"].astype(str)
                #breakpoint()
                #logging.info('example date: %s',df_exploded_save["date"][1])
                df_exploded_save = df_exploded_save[df_exploded_save["date"] >= 0].copy()
                #logging.info('example date v2: %s',df_exploded_save["date"][1])
                df_exploded_save["date"] = (
                     pd.to_datetime("2000-01-01")
                     + pd.to_timedelta(df_exploded_save["date"].astype(int), unit="S")
                 )
                #df_exploded_save["date"] = df_exploded_save["date"].dt.normalize()
                df_exploded_save["date"] = pd.to_datetime(df_exploded_save["date"]).dt.normalize()
                df_exploded_bands["date"] = pd.to_datetime(df_exploded_bands["date"]).dt.normalize()
                
                test=df_exploded_save[df_exploded_save['reach_id']==51111100013]
                logging.info('r4: %s',test)
                
                bands_agg = (
                    df_exploded_bands
                    .groupby(["reach_id", "date"], as_index=False)
                    .agg(
                        links=("link", lambda x: list(pd.unique(x))),
                        #bands=("band", lambda x: list(pd.unique(x))),
                        n_links=("link", "nunique"),
                    )
                )
                df_exploded_bands=bands_agg
                # bad = (df_save_sorted
                #        .groupby("reach_id")["date"]
                #        .apply(lambda s: not s.is_monotonic_increasing))
                
                # print("Any reach_id groups not sorted?:", bad.any())
                # if bad.any():
                #    print("Example bad reach_ids:", bad[bad].index[:10].tolist())
                #breakpoint()
                # print(df_save_sorted[["reach_id","date"]].head(15))

                # print(df_save_sorted[["reach_id","date"]].tail(15))

                # print(df_save_sorted.dtypes)
                def clean_reach_id(x):
                    try:
                        if pd.isna(x):
                            return None
                        return str(int(float(x)))  # 57208000161.0
                    except Exception:
                        return str(x).strip()
                
                # Clean reach IDs
                #df_exploded_save["reach_id"] = df_exploded_save["reach_id"].apply(clean_reach_id)
                #df_exploded_bands["reach_id"] = df_exploded_bands["reach_id"].apply(clean_reach_id)
                
                # Make sure dates are datetimes
                df_exploded_save["date"] = pd.to_datetime(df_exploded_save["date"], errors="coerce").dt.normalize()
                df_exploded_bands["date"] = pd.to_datetime(df_exploded_bands["date"], errors="coerce").dt.normalize()

                #  drop NaT + missing keys on BOTH sides
                df_exploded_save = df_exploded_save.dropna(subset=["reach_id", "date"]).copy()
                df_exploded_bands = df_exploded_bands.dropna(subset=["reach_id", "date"]).copy()
                test=df_exploded_save[df_exploded_save['reach_id']==51111100013]
                logging.info('r2: %s',test)
                # Sort in required order 
                df_save_sorted = df_exploded_save.sort_values(["reach_id", "date"], kind="mergesort").reset_index(drop=True)
                df_bands_sorted = df_exploded_bands.sort_values(["reach_id", "date"], kind="mergesort").reset_index(drop=True)
                test=df_save_sorted[df_save_sorted['reach_id']==51111100013]
                logging.info('r3: %s',test)
                # check monotonic within each reach_id on sides
                bad_left = df_save_sorted.groupby("reach_id")["date"].apply(lambda s: not s.is_monotonic_increasing)
                bad_right = df_bands_sorted.groupby("reach_id")["date"].apply(lambda s: not s.is_monotonic_increasing)
                
                # print("bad_left groups:", int(bad_left.sum()))
                # print("bad_right groups:", int(bad_right.sum()))
                # if bad_left.any():
                #     print("example bad_left reach_ids:", bad_left[bad_left].index[:5].tolist())
                # if bad_right.any():
                #     print("example bad_right reach_ids:", bad_right[bad_right].index[:5].tolist())
                # print(df_save_sorted["date"].isna().sum(), df_bands_sorted["date"].isna().sum())
                # print(df_save_sorted[["reach_id","date"]].head(5))
                # print(df_save_sorted[["reach_id","date"]].tail(5))

                # Merge 1 day
                # df_merged_pm1 = pd.merge_asof(
                #     df_save_sorted,
                #     df_bands_sorted,
                #     on="date",
                #     by="reach_id",
                #     direction="nearest",
                #     tolerance=pd.Timedelta(days=1),
                # )

                # df_save_sorted and df_bands_sorted must already be cleaned, datetime, and sorted by reach_id/date
                # (the ones you printed from)
                
                # Index right side by reach_id for fast lookup
                bands_by_reach = {rid: g.sort_values("date") for rid, g in df_bands_sorted.groupby("reach_id", sort=False)}
                #logging.info('bands_by_reach: %s',bands_by_reach)
                
                test=df_save_sorted[df_save_sorted['reach_id']==51111100013]
                logging.info('r: %s',test)
                
                merged_parts = []
                
                for rid, left_g in df_save_sorted.groupby("reach_id", sort=False):
                    right_g = bands_by_reach.get(rid)
                    #logging.info('rid: %s',rid)
                    if rid == 51111100013:
                        logging.info('rid: %s',rid)
                        logging.info('right_g: %s',right_g)
                        logging.info('left_g: %s',left_g)
                        #logging.info('rid: %s',rid)
                    #if right_g is None:
                    #    # no bands for this reach -> keep left rows with NaNs on right columns
                    #    merged_parts.append(left_g.assign(links=pd.NA))
                    #    continue
                    if right_g is not None:
                        logging.info('rid: %s',rid)
                        #logging.info('right_g: %s',right_g)
                        left_g = left_g.sort_values("date")
                        right_g = right_g.sort_values("date")
                        #logging.info('left_g: %s',left_g)
                        m = pd.merge_asof(
                            left_g,
                            right_g,
                            on="date",
                            direction="nearest",
                            tolerance=pd.Timedelta(days=1),
                            suffixes=("", "_bands"),
                        )
                        #logging.info('m: %s',m)
                        sumnlinks=np.sum(m['n_links'])
                        logging.info('sumnlinks: %s',sumnlinks)
                        merged_parts.append(m)
                
                df_merged_pm1 = pd.concat(merged_parts, ignore_index=True)

                sumnlinks=np.sum(df_merged_pm1['n_links'])
                logging.info('sumnlinks: %s',sumnlinks)

               # breakpoint()
                # how far apart the match was
                df_merged_pm1["date_diff_days"] = (
                    (df_merged_pm1["date"] - df_merged_pm1["date_bands"]).dt.days #positive - Q is later, negative - SSC is later
                    if "date_bands" in df_merged_pm1.columns else None
                )
                #logging.info('df_merged_pm1: %s',df_merged_pm1)
                #from collections import defaultdict

                # df_merged_pm1 must contain: reach_id, links (where links is a list of urls or NaN)
                out = defaultdict(set)
                
                for rid, links in zip(df_merged_pm1["reach_id"], df_merged_pm1["links"]):
                    if isinstance(links, list) and len(links) > 0:
                        for link in links:
                            out[link].add(str(rid))
                
                # Convert sets to sorted lists (optional)
                bands_like_dict = {link: sorted(list(rids)) for link, rids in out.items()}


                chunk_num += 1
                end_time = time.time()
                logging.info(f"Execution time: %s seconds to process chunk", end_time - start_time)
                write_json(bands_like_dict, os.path.join(outdir, f'{cont}_hls_datefilt2_list_chunk_{chunk_num}_time_{int(end_time - start_time)}.json'))
        else:
            logging.info("No reaches located for continent: %s", cont.upper())
        logging.info("All chunks processed — script completed.")
              
# --------------------------------------------------
if __name__ == '__main__':
   main()
