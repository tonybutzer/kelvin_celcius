
#!/usr/bin/env python
# coding: utf-8

from time import time
import xarray as xr
import boto3
import os
import rioxarray
import rasterio

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





def xr_build_cube_concat_ds(tif_list, product):

    start = time()
    my_da_list =[]
    year_month_list = []
    for tif in tif_list:
        tiffile = tif
        #print(tiffile)
        da = xr.open_rasterio(tiffile)
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




get_ipython().system(' aws s3 ls dev-et-data/in/DelawareRiverBasin/Temp/Tasavg/1950/')





def create_s3_list_of_days(main_prefix, year, temperatureType):
    output_name = f'{temperatureType}_'
    the_list = []
    for i in range(1,366):
        day = f'{i:03d}'
        file_object = main_prefix + temperatureType + '/' + str(year) +  '/' + output_name + str(year) + day + '.tif'
        the_list.append(file_object)
    return the_list



def main_runner():

    main_bucket_prefix='s3://dev-et-data/in/DelawareRiverBasin/Temp/'
    year='1950'
    temperatureType = 'Tasavg'

    tif_list = create_s3_list_of_days(main_bucket_prefix, year, temperatureType)

    tif_list_pruned = tif_list[0:364:30]
    ds = xr_build_cube_concat_ds(tif_list_pruned, temperatureType)

    for i in range(0,ds.dims['day']):
        print(ds['Tasavg'][i]['day'])

    ds = ds - 273.15 # convert data array xarray.DataSet from Kelvin to Celsius

    output_main_prefix='s3://dev-et-data/in/DelawareRiverBasin/TempCelsius/'
    year='1950'

    get_ipython().system('mkdir -p ./tmp')

    write_out_celsius_tifs(output_main_prefix, ds, year, 'Tasavg')



def write_out_celsius_tifs(main_prefix, ds, year, output_name):
    num_days=ds.dims['day']
    for i in range(0,(num_days - 1)):
        dayi = i+1
        day="{:03d}".format(dayi)
        s3_file_object = main_prefix + temperatureType + '/' + str(year) +  '/' + output_name + '_' + str(year) + day + '.tif'
        file_object = './tmp/' + output_name + '_' + str(year) + day + '.tif'

        print(file_object)

        np_array = ds[output_name].isel(day=dayi).values
#         print(type(np_array))
        my_template = 's3://dev-et-data/in/DelawareRiverBasin/Temp/Tasavg/1950/Tasavg_1950017.tif'
        write_GeoTif_like(my_template, np_array, file_object)
        s3_push_delete_local(file_object, s3_file_object)

