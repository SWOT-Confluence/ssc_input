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
from multiprocessing import Pool
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
        for i in all_temporal_ranges:
            search = catalog.search(
                collections=collections, intersects = line_geo, datetime=i.replace(',', '/'))




            item_collection = search.get_all_items()

            if limit:
                item_collection = item_collection[:limit]

            if band:
                if type(band) == list:
                    for i in item_collection:
                        for b in band:
                            link = i.assets[b].href
                            links.append(link)
                
                else:
                    for i in item_collection:
                        link = i.assets[band].href
                        links.append(link)
            
            else:
                for i in item_collection:
                    for key in i.assets:
                        if key.startswith('B'):
                            # link = i.assets[key].href.replace('https://data.lpdaac.earthdatacloud.nasa.gov/', 's3://')
                            link = i.assets[key].href

                            links.append(link)

        return links

def find_download_links_for_reach_tiles(sword_path, reach_id, cont, temporal_range):
    try:
        lat_list, lon_list = get_reach_node_cords(sword_path,reach_id, cont)

        df = pd.DataFrame(columns=['x', 'y'])
        df['x'] = lat_list[:5]
        df['y'] = lon_list[:5]
        df['ID'] = reach_id
        geometry = [Point(xy) for xy in zip(df.x, df.y)]

        geo_df = gpd.GeoDataFrame(df, geometry=geometry)

        geo_df2 = geo_df.groupby(['ID'])['geometry'].apply(lambda x: LineString(x.tolist()))
        geo_df2 = gpd.GeoDataFrame(geo_df2, geometry='geometry')
        if len(geo_df2)!=0:
            print('Found something...')
            line_geo = list(geo_df2.geometry.unique())[0]
            links = find_hls_tiles(line_geo=line_geo, date_range=temporal_range)
        else:
           return ['foo']
    except Exception as e:
        links = ['foo']
        print(e)
        print(traceback.format_exc()) 
    return list(set(links))





def get_reach_node_cords(sword_path, reach_id, cont):

    lat_list, lon_list = [], []

    # sword_fp = os.path.join(sword_path, f'{cont.lower()}_sword_v15.nc')
    # print(f'Searching across {len(files)} continents for nodes...')

     
    
    rootgrp = ncf.Dataset(sword_path, "r", format="NETCDF4")
    
    print(rootgrp.groups['nodes'].variables['reach_id'][:].data.astype('U')) 
    node_ids_indexes = np.where(rootgrp.groups['nodes'].variables['reach_id'][:].data.astype('U') == str(reach_id))

    if len(node_ids_indexes[0])!=0:
        for y in node_ids_indexes[0]:

            lat = float(rootgrp.groups['nodes'].variables['x'][y].data.astype('U'))
            lon = float(rootgrp.groups['nodes'].variables['y'][y].data.astype('U'))
            # all_nodes.append([lat,lon])
            lat_list.append(lat)
            lon_list.append(lon)


        rootgrp.close()

    # print(f'Found {len(all_nodes)} nodes...')
    return lat_list, lon_list

def ssc_process_continent(reach_ids, cont, sword_path, temporal_range):


    pool = Pool(processes=7)              # start 4 worker processes
    result = pool.starmap(find_download_links_for_reach_tiles, zip(repeat(sword_path), reach_ids, repeat(cont), repeat(temporal_range)))

    pool.close()

    flatten_list = list(chain.from_iterable(result))
    flatten_list = list(set(flatten_list))
    no_bands = list(set([i[:-10] for i in flatten_list]))
    print(f'Found {len(no_bands)} scenes for {cont}...')
    return no_bands

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
                        help='Input directory containing pointclouds',
                        metavar='int',
                        type=int,
                        )

    parser.add_argument('-t',
                        '--temporal_range',
                        help='Temporal range to search for tiles',
                        metavar='str',
                        type=str,
                        default="2020-01-01T00:00:00Z,2025-04-25T23:59:59Z")


    return parser.parse_args()

def main():
    """Make a jazz noise here"""
    args = get_args() 
    indir = '/mnt/input'
    index = args.index
    temporal_range = args.temporal_range
    cont, cont_number = get_cont_info(index = index, indir = indir)
    sword_path = os.path.join(indir, 'sword', f'{cont}_sword_v16_patch.nc')
    reach_ids = get_reach_ids(cont_number = cont_number, indir=indir)
    print(reach_ids)
    # reach_ids = [74299800431, 74268900211, 74286300021, 78220000121, 74299800441, 74268900221, 74286300031, 78220000131, 74299800451, 74286300041, 74299800461, 78220000141, 74268900241, 74286300051, 73214000021, 74299800471, 78220000151, 74268900251, 74286300061, 73214000031, 74299800481, 78220000161, 74286300071, 74268900271, 74286300081, 73214000051, 74268900281, 73214000061]  
    bands = ssc_process_continent(reach_ids[:2], cont, sword_path, temporal_range)
    write_json(bands, os.path.join(indir, f'{cont}_hls_list.json'))
# --------------------------------------------------
if __name__ == '__main__':
    main()