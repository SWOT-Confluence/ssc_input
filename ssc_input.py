#!/usr/bin/env python3
"""
Author : Travis Simmons
Date   : July 31, 2024
Purpose: Rock the Casbah
"""
# Sample deployment

# python3 prepare.py -i /home/u24/travissimmons/cjx/season10/50_hand_label_test_2020_03_02 -o /home/u24/travissimmons/cjx/season10/gifs
# makeflow process.makeflow -j 1
import traceback
import argparse
import os
import shutil
import sys
import glob
import subprocess
from multiprocessing import Pool, Manager, cpu_count
import json
from pystac_client import Client  
import geopandas as gpd
import os
import numpy as np
import pandas as pd
from shapely.geometry import Point, LineString, shape, mapping
from itertools import repeat
import netCDF4 as ncf
from itertools import chain
from datetime import datetime
from datetime import timedelta
import tqdm
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import repeat
from typing import List, Tuple
import functools
print = functools.partial(print, flush=True)

from random import randint
from time import sleep
import time


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
    tries = 20
    tries_cnt = 0
    links = []
    success = False
    for i in range(tries):
        print('proceessing', reach_id)
        try:
            # sleep(randint(1,60))
            line_geo = get_reach_node_cords(sword_path,reach_id, cont)
            tries_cnt += 1
            catalog = Client.open(f'{STAC_URL}/LPCLOUD/')
            search = catalog.search(
            collections=collections,
            intersects=line_geo,
            datetime = date_range.replace(',', '/')
            # limit=1000  # optional, just sets page size
            )
            links = []
            all_items = search.items()


            for i in all_items:
            # print(i, list(i.keys()))
                i = i.to_dict()
                for key in i['assets']:
                    # print(key)
                    if key.startswith('B'):
                        # link = i.assets[key].href.replace('https://data.lpdaac.earthdatacloud.nasa.gov/', 's3://')
                        link = i['assets'][key]['href']

                        links.append(link)
                

                
            print(reach_id, 'succeeded after', tries_cnt, 'tries')
            print('example link from geo...',list(set([os.path.basename(a_link).split('.')[2] for a_link in links])))
            success = True
            break
        except Exception as e:
            er = e
            sleep(randint(1,20))
            if 'rate' in str(e):
                sleep(randint(1, 120))
            print(reach_id, 'failed', 'error:', e, tries_cnt)
    
    if not success:
        try:
            links.append(f"failed,error: {e}")
        except:
            links.append('failed with no error')
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
    
    print('finished searching for ', reach_id, 'found', len(all_links), 'tiles...')
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
    
    # print('finished searching for ', reach_id, 'found', len(all_links), 'tiles...')
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
    print(reach_ids[:2])
    print('starting workers...')
    pool = Pool(processes=len(reach_ids))              # start 4 worker processes
    result = pool.starmap(find_download_links_for_reach_tiles, input_vars )


    inverted = defaultdict(list)
    for i in result:
        for reach_id, links in i.items():
            for link in links:
                inverted[link].append(reach_id)

    # Convert defaultdict to regular dict (optional)
    result = dict(inverted)

    # pool.close()
    return result

def get_reach_ids(cont_number:list, indir:str, run_globe:bool, sword_path:str):
    # reach_ids = [print(os.path.basename(i).split('_')[0]) for i in glob.glob(os.path.join(indir, 'swot','*'))\
        #  if os.path.basename(i)[0] in cont_number]
    if not run_globe:
        all_reach_ids =glob.glob(os.path.join(indir, 'swot','*'))
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

    parser.add_argument('-t',
                        '--temporal_range',
                        help='Temporal range to search for tiles',
                        metavar='str',
                        type=str,
                        default="2023-03-31T00:00:00Z,2026-12-12T23:59:59Z")

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
print('running...')
def main():
    """Make a jazz noise here"""
    args = get_args() 
    indir = '/data/input'
    outdir = args.outdir
    index = args.index
    temporal_range = args.temporal_range
    run_globe = args.run_globe
    starting_chunk = args.starting_chunk
    
    if index == -235 or None:
        # index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
        index_range = range(0,8)
    
    else:
        index_range = range(index, index+1)
        
    print('here is index', index)
    for index in index_range:

        cont, cont_number = get_cont_info(index = index, indir = indir)
        print('processing', cont)
        sword_path = os.path.join(indir, 'sword', f'{cont}_sword_v16_patch.nc')
        reach_ids = get_reach_ids(cont_number = cont_number, indir=indir, run_globe=run_globe, sword_path=sword_path)
        
        rid_chunks =  [ reach_ids[i:i+50] for i in range(0,len(reach_ids),50) ]
        # rid_chunks = rid_chunks[305:]
        
        chunk_num = starting_chunk
        rid_chunks = rid_chunks[chunk_num:]
        
        if len(rid_chunks[-1]) == 1:
            rid_chunks[-2].extend(rid_chunks[-1])
            rid_chunks.pop(-1)
        for rid_chunk in rid_chunks:
            start_time = time.time()
            print('processing chunk', chunk_num, 'of', len(rid_chunks))
            bands = ssc_process_continent(rid_chunk, cont, sword_path, temporal_range)


            chunk_num += 1
            end_time = time.time()
            print(f"Execution time: {end_time - start_time:.4f} seconds to process chunk")
            write_json(bands, os.path.join(outdir, f'{cont}_hls_list_chunk_{chunk_num}_time_{int(end_time - start_time)}.json'))
        print("All chunks processed — script completed.")
              
# --------------------------------------------------
if __name__ == '__main__':
    main()