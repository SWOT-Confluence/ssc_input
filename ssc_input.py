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
from multiprocessing import Pool, Manager
import json
from pystac_client import Client  
import geopandas as gpd
import os
import numpy as np
import pandas as pd
from shapely.geometry import Point, LineString, shape
from itertools import repeat
import netCDF4 as ncf
from itertools import chain
from datetime import datetime
from datetime import timedelta
import tqdm

from random import randint
from time import sleep


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

def find_hls_tiles(line_geo=False, band=False, limit=False, collections = ['HLSL30.v2.0', 'HLSS30.v2.0'], date_range = False):

    STAC_URL = 'https://cmr.earthdata.nasa.gov/stac'


    catalog = Client.open(f'{STAC_URL}/LPCLOUD/')


    if date_range == False:
# ['2020-01-01:00:00:00Z', '..']
        # search = catalog.search(
        #     collections=collections, intersects = line_geo, datetime=date_range.replace(',', '/'))
        raise ValueError('Please supply a date for ssc...')
    else:
        all_temporal_ranges = generate_time_search(date_range)
        links = []
        # all_temporal_ranges = ['1']
        for i in all_temporal_ranges:
            try:
                # search = catalog.search(
                #     collections=collections, intersects = line_geo, datetime=i.replace(',', '/'))

                try:
                    search = catalog.search(
                        collections=collections, intersects = line_geo)
                except e:
                    print('here is the fail', e)

                item_collection = search.items_as_dicts()


                if limit:
                    item_collection = item_collection[:limit]
                # print(list(item_collection[0].keys()))

                if band:
                    if type(band) == list:
                        for i in item_collection:
                            for b in band:
                                link = i['assets'][band]['href']
                                links.append(link)
                    
                    else:
                        for i in item_collection:
                            link = i['assets'][band]['href']
                            links.append(link)
                
                else:
                    for i in item_collection:
                        # print(i, list(i.keys()))
                        for key in i['assets']:
                            # print(key)
                            if key.startswith('B'):
                                # link = i.assets[key].href.replace('https://data.lpdaac.earthdatacloud.nasa.gov/', 's3://')
                                link = i['assets'][key]['href']

                                links.append(link)
            except:
                continue 

        return list(set(links))

def find_download_links_for_reach_tiles(sword_path, reach_id, cont, temporal_range):

    lat_list, lon_list = get_reach_node_cords(sword_path,reach_id, cont)
    # print(lat_list)
    df = pd.DataFrame(columns=['x', 'y'])
    # df['x'] = lat_list
    # df['y'] = lon_list
    geometry_chunks = [Point(xy) for xy in zip(lat_list, lon_list)]
    # geometry_chunks =  [ geometry[i:i+10] for i in range(0,len(geometry),10) ]
    # if len(geometry_chunks[-1]) == 1:
    #     geometry_chunks[-2].extend(geometry_chunks[-1])
    #     geometry_chunks.pop(-1)
    
    all_links = []
    fail_count = 0
    success_count = 0
    errors = []
    cnt = 0
    for geometry in geometry_chunks:
        # print(reach_id,':','Processing node ', cnt, 'of', len(geometry_chunks))
        cnt += 1
        # print(geometry)
        line_geo = geometry
        # geo_df = gpd.GeoDataFrame( geometry=geometry)
        # geo_df['ID'] = reach_id

        # if len(geo_df) != 0:
            # geo_df2 = geo_df.groupby(['ID'])['geometry'].apply(lambda x: LineString(x.tolist()))
            # geo_df2 = gpd.GeoDataFrame(geo_df2, geometry='geometry')
            # line_geo = list(geo_df2.geometry.unique())[0]
            # print(line_geo)
        attempt_number = 5
        # sleep(randint(10,100))
        for attempt in range(attempt_number):
            try:
                # if len(geo_df2)!=0:
                links = find_hls_tiles(line_geo=line_geo, date_range=temporal_range)
                all_links.extend(links)
                success_count += 1
                # print(reach_id, f'chunk {cnt} succeeded')
                # print(reach_id, 'found', len(all_links), 'so far...')
                break
            except Exception as e:
                # all_links.extend(['foo'])
                fail_count += 1
                errors.append(e)
                
                sleep(randint(10,100))
                continue
            else:
                break
    # print('got it', all_links)
    all_links = list(set(all_links))
    out_data = {reach_id:all_links}
    return all_links,out_data, success_count, fail_count, errors





def get_reach_node_cords(sword_path, reach_id, cont):

    lat_list, lon_list = [], []

    # sword_fp = os.path.join(sword_path, f'{cont.lower()}_sword_v15.nc')

     
    
    rootgrp = ncf.Dataset(sword_path, "r", format="NETCDF4")
    
    node_ids_indexes = np.where(rootgrp.groups['nodes'].variables['reach_id'][:].data.astype('U') == str(reach_id))

    if len(node_ids_indexes[0])!=0:
        for y in node_ids_indexes[0]:

            lat = float(rootgrp.groups['nodes'].variables['x'][y].data.astype('U'))
            lon = float(rootgrp.groups['nodes'].variables['y'][y].data.astype('U'))
            # all_nodes.append([lat,lon])
            lat_list.append(lat)
            lon_list.append(lon)


        rootgrp.close()
    return [lat_list[0], lat_list[-1]], [lon_list[0], lon_list[-1]]

def find_download_links_with_progress(*args):
    result = find_download_links_for_reach_tiles(*args[:-1])  # Your original function logic here
    args[-1].increment()  # Update the shared counter
    return result

class Counter:
    def __init__(self):
        self.count = 0

    def increment(self):
        self.count += 1





def ssc_process_continent(reach_ids, cont, sword_path, temporal_range, progress_bar_bool):

    input_vars = zip(repeat(sword_path), reach_ids, repeat(cont), repeat(temporal_range))

    if progress_bar_bool:
        with Manager() as manager:
            counter = manager.Namespace()  # Use a namespace to store the shared counter
            counter.count = 0

            def update_progress_bar(counter):
                # This function will be used to update the progress bar
                with tqdm.tqdm(total=len(reach_ids)) as pbar:
                    while counter.count < len(reach_ids):
                        pbar.n = counter.count  # Update progress bar
                        pbar.refresh()  # Force an update of the display
                        sleep(0.1)  # Small delay to reduce CPU usage

            with Pool(8) as pool:
                # Start the progress bar updater in a separate thread
                from threading import Thread
                progress_thread = Thread(target=update_progress_bar, args=(counter,))
                progress_thread.start()

                # Process the tasks
                input_with_counter = [(*input_var, counter) for input_var in input_vars]
                result = pool.starmap(find_download_links_with_progress, input_with_counter)

                progress_thread.join()  # Ensure progress thread finishes

    # input_vars = zip(repeat(sword_path), reach_ids, repeat(cont), repeat(temporal_range))

    # if progress_bar_bool:

    #     # pool = Pool(processes=4)
    #     # mapped_values = list(tqdm.tqdm(pool.imap_unordered(find_download_links_for_reach_tiles, input_vars), total=len(reach_ids)))

    #     with Pool(8) as pool:
    #         result = pool.starmap(find_download_links_for_reach_tiles, tqdm.tqdm(input_vars, total=len(reach_ids)))
    
    else:
        pool = Pool(processes=8)              # start 4 worker processes
        result = pool.starmap(find_download_links_for_reach_tiles, input_vars )
    links = []
    out_data = []
    fail_count = 0
    success_count = 0 
    errors = []
        # return list(set(all_links)), success_count, fail_count, errors
    for i in result:
        links.extend(i[0])
        out_data.append([i[1]])
        success_count += i[2]
        fail_count += i[3]
        errors.extend(i[4])


    pool.close()
    print(f'{fail_count} chunks failed.')
    print('Here are some sample errors', errors)
    print('here are some example links',links[:10])
    # flatten_list = list(chain.from_iterable(links))
    # print('flattened list 1', flatten_list[:10])
    # flatten_list = list(set(flatten_list))
    # print('set flattened list', flatten_list)
    # no_bands = list(set([i[:-10] for i in flatten_list]))
    # print('nobands',no_bands[:10])
    print(f'Found {len(links)} scenes for {cont}...')
    return out_data

def get_reach_ids(cont_number:list, indir:str):
    # reach_ids = [print(os.path.basename(i).split('_')[0]) for i in glob.glob(os.path.join(indir, 'swot','*'))\
        #  if os.path.basename(i)[0] in cont_number]
    all_reach_ids =glob.glob(os.path.join(indir, 'swot','*'))
    reach_ids = [os.path.basename(i).split('_')[0] for i in all_reach_ids if int(os.path.basename(i)[0]) in cont_number]
 
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
                        )

    parser.add_argument('-t',
                        '--temporal_range',
                        help='Temporal range to search for tiles',
                        metavar='str',
                        type=str,
                        default="2020-01-01T00:00:00Z,2025-04-25T23:59:59Z")
    parser.add_argument('-o',
                        '--outdir',
                        help='output directory',
                        metavar='str',
                        type=str,
                        default= '/mnt/input')
    
    parser.add_argument('-p',
                    '--progress_bar',
                    help='Add progress bar',
                    action='store_true',
                    default=False)


    return parser.parse_args()

def main():
    """Make a jazz noise here"""
    args = get_args() 
    indir = '/mnt/input'
    outdir = args.outdir
    index = args.index
    temporal_range = args.temporal_range
    progress_bar_bool = args.progress_bar
    cont, cont_number = get_cont_info(index = index, indir = indir)
    sword_path = os.path.join(indir, 'sword', f'{cont}_sword_v16_patch.nc')
    reach_ids = get_reach_ids(cont_number = cont_number, indir=indir)
    # reach_ids = [74299800431, 74268900211, 74286300021, 78220000121, 74299800441, 74268900221, 74286300031, 78220000131, 74299800451, 74286300041, 74299800461, 78220000141, 74268900241, 74286300051, 73214000021, 74299800471, 78220000151, 74268900251, 74286300061, 73214000031, 74299800481, 78220000161, 74286300071, 74268900271, 74286300081, 73214000051, 74268900281, 73214000061]  
    
    rid_chunks =  [ reach_ids[i:i+10] for i in range(0,len(reach_ids),50) ]
    chunk_num = 0
    if len(rid_chunks[-1]) == 1:
        rid_chunks[-2].extend(rid_chunks[-1])
        rid_chunks.pop(-1)
    for rid_chunk in rid_chunks:
        print('processing chunk', chunk_num, 'of', len(rid_chunks))
        bands = ssc_process_continent(rid_chunk, cont, sword_path, temporal_range, progress_bar_bool)
        write_json(bands, os.path.join(outdir, f'{cont}_hls_list_{chunk_num}.json'))
        chunk_num += 1
# --------------------------------------------------
if __name__ == '__main__':
    main()