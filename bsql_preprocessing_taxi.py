# modified from https://github.com/holoviz/datashader/blob/master/examples/taxi_preprocessing_example.py
"""Download data needed for the examples"""

from __future__ import print_function

if __name__ == "__main__":
    
    import sys
    import numpy as np
    from os import path, makedirs, remove
    
    # make sure we have BlazingSQL
    try:
        from blazingsql import BlazingContext
    # in case we don't
    except ImportError:
        # let the user know what to do 
        print('Download script required BlazingSQL: https://docs.blazingdb.com/docs/install-via-conda')
        sys.exit(1)
    
    # make sure we have requests
    try:
        import requests
    # in case we don't
    except ImportError:
        # let the user know what to do 
        print('Download script required requests package: conda install requests')
        sys.exit(1)
    
    def _download_dataset(url):
        """
        download dataset to data directory 
        """
        # grab data
        r = requests.get(url, stream=True)
        # tag path of new file
        output_path = 'data/' path.split(url)[1]
        # open up the new file to write
        with open(output_path, 'wb') as f:
            # determine size of data
            total_length = int(r.headers.get('content-length'))
            print(f'total length = {total_length}')
            # count number of lines we've processed
            lines = 0
            # go through piece by piece
            for chunk in r.iter_content(chunk_size=1024):
                lines += 1024
                if lines > total_length / 100:
                    lines = 0 
                    per_complete +=1
                    print(f'{per_complete}% complete'
                # as long as there's still some left
                if chunk:
                    # write it
                    f.write(chunk)
                    # flush internal buffer
                    f.flush()
        # let user know we're done
        print('download complete!')
                          
    # tag examples directory 
    telegram_dir = path.dirname(path.realpath(__file__))
    # tag data directory 
    data_dir = path.join(telegram_dir, 'data')
    # if data directory isn't there
    if not path.exists(data_dir):
        # go ahead and make one
        makedirs(data_dir)
                          
    # taxi data
    def latlng_to_meters(table, 
                         lat_pickup='pickup_latitude', lng_pickup='pickup_longitude', 
                         lat_dropoff='dropoff_latitude', lng_dropoff='dropoff_longitude'):
        # tag default shift
        origin_shift = 2 * np.pi * 6378137 / 2.0
        # tag columns we're not doing anything with 
        base_columns = '''VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, 
                          trip_distance, RateCodeID, store_and_fwd_flag, payment_type, fare_amount, 
                          extra, mta_tax,  tip_amount, tolls_amount, improvement_surcharge, total_amount'''
        # convert lat/lng coords within focused area to meters and return w/ base columns as pickup/dropoff x/y 
        latlng_to_meters = f'''
                           select
                               TAN( (90 + {lat_pickup}) * {np.pi} / 360.0 ) AS pickup_y,
                               {lng_pickup} * {origin_shift} / 180.0 AS pickup_x,
                               TAN( (90 + {lat_dropoff}) * {np.pi} / 360.0 ) AS dropoff_y,
                               {lng_dropoff} * {origin_shift} / 180.0 AS dropoff_x,
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
        gdf = bc.sql(latlng_to_meters)
        # convert y columns w/ log
        for col in ['pickup_y', 'dropoff_y']:
            gdf[col] = np.log(gdf[col]) / (np.pi / 180.0)
        # return dataframe
        return gdf

    # tag path to new csv
    taxi_path = path.join(data_dir, 'nyc_taxi.csv')
    # if file isn't already there
    if not path.exists(taxi_path):
        # what's going on?
        print("Downloading Taxi Data...")
        # tag data url
        url = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-01.csv'
        # download data from that url
        _download_dataset(url)
                          
        print('Starting up BlazingSQL')
        # tag current directory
        cwd = os.getcwd()
        taxi_path = cwd + '/data/yellow_tripdata_2015-01.csv'
        # start up BlazingSQL
        bc = BlazingContext()  
        # create table from CSV
        bc.create_table('taxi_15', taxi_path, header=0)
    
        # convert lat/lng coordinates to meters after filtering data to specific coordinates
        print('Filtering & Reprojecting Taxi Data')
        gdf = latlng_to_meters(df, 'pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude')
        
        # save results to CSV
        gdf.to_csv(taxi_path, index=False)
        # delete origional CSV
        remove('yellow_tripdata_2015-01.csv')
        
    # let user know we're done
    print("\nAll data downloaded.")
