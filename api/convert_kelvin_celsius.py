
#!/usr/bin/env python
# coding: utf-8

import sys
from time import time
from time import sleep
import xarray as xr
import boto3
import os
import rioxarray
import rasterio
import argparse

def _split_full_path(bucket_full_path):
    if 's3://' in bucket_full_path:
        bucket_full_path=bucket_full_path.replace('s3://','')
    print(bucket_full_path)
    bucket = bucket_full_path.split('/')[0]
    bucket_filepath = '/'.join(bucket_full_path.split('/')[1:])
    return (bucket, bucket_filepath)


def s3_push_delete_local(local_file, bucket_full_path):
        s3 = boto3.client('s3')
        with open(local_file, "rb") as f:
            (bucket, bucket_filepath) = _split_full_path(bucket_full_path)
            s3.upload_fileobj(f, bucket, bucket_filepath)
        os.remove(local_file)




def write_GeoTif_like(templet_tif_file, output_ndarry, output_tif_file):
    import rasterio
    orig = rasterio.open(templet_tif_file)
    print('write file ', output_tif_file)
    with rasterio.open(output_tif_file, 'w', driver='GTiff', height=output_ndarry.shape[0],
                       width=output_ndarry.shape[1], count=1, dtype=output_ndarry.dtype,
                       crs=orig.crs, transform=orig.transform, nodata=-9999) as dst:
        dst.write(output_ndarry, 1)





def _get_year_month(product, tif):
    fn = tif.split('/')[-1]
    fn = fn.replace(product,'')
    fn = fn.replace('.tif','')
    print(fn)
    fn=fn[-3:]
    return fn

def _xr_open_rasterio_retry(s3_file_name):
    cnt=10
    while(cnt>0):
        try:
            da = xr.open_rasterio(s3_file_name)
            return da
        except rasterio.errors.RasterioIOError:
                        print("Unexpected error:", sys.exc_info()[0])
                        print('oops',cnt)
                        print('oops',s3_file_name)
                        cnt = cnt - 1
                        sleep(4)


def xr_build_cube_concat_ds(tif_list, product):

    start = time()
    my_da_list =[]
    year_month_list = []
    for tif in tif_list:
        tiffile = tif
        #print(tiffile)
        da = _xr_open_rasterio_retry(tiffile)
        my_da_list.append(da)
        tnow = time()
        elapsed = tnow - start
        #print(tif, elapsed)
        print('.',flush=True)
        year_month_list.append(_get_year_month(product, tif))

    da = xr.concat(my_da_list, dim='band')
    da = da.rename({'band':'day'})
    da = da.assign_coords(day=year_month_list)
    DS = da.to_dataset(name=product)
    return(DS)



def create_s3_list_of_days(main_prefix, year, temperatureType):
    output_name = f'{temperatureType}_'
    the_list = []
    for i in range(1,366):
        day = f'{i:03d}'
        file_object = main_prefix + temperatureType + '/' + str(year) +  '/' + output_name + str(year) + day + '.tif'
        the_list.append(file_object)
    return the_list



def main_runner(year, temperatureType):

    main_bucket_prefix='s3://dev-et-data/in/DelawareRiverBasin/Temp/'
    #year='1950'
    #temperatureType = 'Tasavg'

    tif_list = create_s3_list_of_days(main_bucket_prefix, year, temperatureType)

    ds = xr_build_cube_concat_ds(tif_list, temperatureType)

    for i in range(0,ds.dims['day']):
        print(ds[temperatureType][i]['day'])

    ds = ds - 273.15 # convert data array xarray.DataSet from Kelvin to Celsius

    output_main_prefix='s3://dev-et-data/in/DelawareRiverBasin/TempCelsius/'

    path ='./tmp'
    os.makedirs(path, exist_ok=True)

    write_out_celsius_tifs(output_main_prefix, ds, year, output_name=temperatureType)



def write_out_celsius_tifs(main_prefix, ds, year, output_name):
    num_days=ds.dims['day']
    for i in range(0,num_days ):
        dayi = i+1
        day="{:03d}".format(dayi)
        s3_file_object = main_prefix + output_name + '/' + str(year) +  '/' + output_name + '_' + str(year) + day + '.tif'
        print(s3_file_object)
        file_object = './tmp/' + output_name + '_' + str(year) + day + '.tif'

        print(file_object)

        np_array = ds[output_name].isel(day=i).values
#         print(type(np_array))
        my_template = 's3://dev-et-data/in/DelawareRiverBasin/Temp/Tasavg/1950/Tasavg_1950017.tif'
        write_GeoTif_like(my_template, np_array, file_object)
        s3_push_delete_local(file_object, s3_file_object)


def get_parser():
    parser = argparse.ArgumentParser(description='Run the kelvin code')
    parser.add_argument('-y', '--year', help='specify year or Annual or all example: -y 1999 ', default='Annual', type=str)
    parser.add_argument('-t', '--type', help='temp type ex: Tasavg , Tasmax, Tasmin'  , default='Tasmax', type=str)
    return parser

def command_line_runner():
    parser = get_parser()
    args = vars(parser.parse_args())

    if args['year']:
        year = args['year']
        print("year", args['year'])
    if args['type']:
        tempType =  args['type']
        print("type", args['type'])

    main_runner(year, tempType)


if __name__ == '__main__':
    command_line_runner()

