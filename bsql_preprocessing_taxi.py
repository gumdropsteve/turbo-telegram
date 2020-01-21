# modified from https://github.com/holoviz/datashader/blob/master/examples/taxi_preprocessing_example.py
"""Download data needed for the examples"""

from __future__ import print_function

if __name__ == "__main__":

    from os import path, makedirs, remove
    from download_sample_data import bar as progressbar
    
#     import pandas as pd
    from blazingsql import BlazingContext
    # start up BlazingSQL
    bc = BlazingContext()
    import numpy as np
    import sys
    
    # make sure we have requests
    try:
        import requests
    # in case we don't
    except ImportError:
        # let the user know what to do 
        print('Download script required requests package: conda install requests')
        sys.exit(1)
    
    def _download_dataset(url):
        # grab data
        r = requests.get(url, stream=True)
        # tag path of new file
        output_path = path.split(url)[1]
        # write to the new file
        with open(output_path, 'wb') as f:
            # determine size of data
            total_length = int(r.headers.get('content-length'))
            # go through piece by piece
            for chunk in progressbar(r.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1):
                # as long as there's still some left
                if chunk:
                    # write it
                    f.write(chunk)
                    # flush internal buffer
                    f.flush()
    
    # tag examples directory 
    examples_dir = path.dirname(path.realpath(__file__))
    # tag data directory 
    data_dir = path.join(examples_dir, 'data')
    # if data directory isn't there
    if not path.exists(data_dir):
        # go ahead and make one
        makedirs(data_dir)
    
#     # Taxi data
#     def latlng_to_meters(df, lat_name, lng_name):
#         # tag lat & lng columns
#         lat = df[lat_name]
#         lng = df[lng_name]
        
#         # tag default shift
#         origin_shift = 2 * np.pi * 6378137 / 2.0
        
#         # determine x value
#         mx = lng * origin_shift / 180.0
        
#         # determine y value
#         _a = np.tan((90 + lat) * np.pi / 360.0)
#         my = np.log(_a) / (np.pi / 180.0)
#         my = my * origin_shift / 180.0
        
#         # replace old values with new values 
#         df.loc[:, lng_name] = mx
#         df.loc[:, lat_name] = my

    # taxi data
    def latlng_to_meters(table, lat_pickup, lng_pickup, lat_dropoff, lng_dropoff):
        # tag default shift
        origin_shift = 2 * np.pi * 6378137 / 2.0
        # tag columns we're not doing anything with 
        base_columns = '''
                       VendorID, 
                       tpep_pickup_datetime,
                       tpep_dropoff_datetime,
                       passenger_count,
                       trip_distance,
                       RateCodeID,
                       store_and_fwd_flag,
                       payment_type,
                       fare_amount,
                       extra,
                       mta_tax,
                       tip_amount,
                       tolls_amount,
                       improvement_surcharge,
                       total_amount
                       '''
        # convert lat/lng coords within focused area to meters and return w/ base columns as pickup/dropoff x/y 
        latlng_to_meters = f'''
                           select
                           
                               {lng_pickup} * {origin_shift} / 180.0 AS pickup_x,
                               LOG( TAN( (90 + {lat_pickup}) * {np.pi} / 360.0 ) ) / ({np.pi} / 180.0) AS pickup_y,
                           
                               {lng_dropoff} * {origin_shift} / 180.0 AS dropoff_x,
                               LOG( TAN( (90 + {lat_dropoff}) * {np.pi} / 360.0 ) ) / ({np.pi} / 180.0) AS dropoff_y,
                               
                               {base_columns}
                           from 
                               {table}
                               where
                                       {lng_pickup} < -73.75
                                   and {lng_pickup} > -74.15
                                   
                                   and {lng_dropoff} < -73.75
                                   and {lng_dropoff} > -74.15
                                   
                                   and {lat_pickup} > 40.68
                                   and {lat_pickup} < 40.84
                                   
                                   and {lat_dropoff} > 40.68
                                   and {lat_dropoff} < 40.84
                                   '''  # bottom half focuses coords, top half converts to meters & renames
        # run query & output results
        return bc.sql(latlng_to_meters)

    # tag path to new csv
    taxi_path = path.join(data_dir, 'nyc_taxi.csv')
    # if path isn't already there
    if not path.exists(taxi_path):
        # what's going on?
        print("Downloading Taxi Data...")
        # tag data url
        url = ('https://storage.googleapis.com/tlc-trip-data/2015/'
               'yellow_tripdata_2015-01.csv')
        
        # download data from that url
        _download_dataset(url)
        df = pd.read_csv('yellow_tripdata_2015-01.csv')
    
        # convert lat/lng coordinates to meters after filtering data to specific coordinates
        print('Filtering & Reprojecting Taxi Data')
        gdf = latlng_to_meters(df, 'pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude')
        
        # save results to CSV
        gdf.to_csv(taxi_path, index=False)
        # delete origional CSV
        remove('yellow_tripdata_2015-01.csv')
        
    # let user know we're done
    print("\nAll data downloaded.")
