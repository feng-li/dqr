#! /usr/bin/python3

import os
import zipfile
from csv import reader

data_file = os.path.expanduser("~/running/data/used_cars_data/used_cars_data.zip")
header = True

columns_raw = ['vin', 'back_legroom', 'bed', 'bed_height', 'bed_length', 'body_type',
               'cabin', 'city', 'city_fuel_economy', 'combine_fuel_economy', 'daysonmarket',
               'dealer_zip', 'description', 'engine_cylinders', 'engine_displacement', 'engine_type',
               'exterior_color', 'fleet', 'frame_damaged', 'franchise_dealer', 'franchise_make',
               'front_legroom', 'fuel_tank_volume', 'fuel_type', 'has_accidents', 'height',
               'highway_fuel_economy', 'horsepower', 'interior_color', 'isCab', 'is_certified', 'is_cpo',
               'is_new', 'is_oemcpo', 'latitude', 'length', 'listed_date', 'listing_color', 'listing_id',
               'longitude', 'main_picture_url', 'major_options', 'make_name', 'maximum_seating',
               'mileage', 'model_name', 'owner_count', 'power', 'price', 'salvage', 'savings_amount',
               'seller_rating', 'sp_id', 'sp_name', 'theft_title', 'torque', 'transmission',
               'transmission_display', 'trimId', 'trim_name', 'vehicle_damage_category', 'wheel_system',
               'wheel_system_display', 'wheelbase', 'width', 'year']

columns_keep = ['vin', 'back_legroom', 'body_type', 'city', 'city_fuel_economy',
                'daysonmarket', 'dealer_zip', 'engine_cylinders', 'engine_displacement',
                'engine_type', 'exterior_color', 'franchise_dealer', 'front_legroom',
                'fuel_tank_volume', 'fuel_type', 'has_accidents', 'height', 'highway_fuel_economy',
                'horsepower', 'interior_color', 'isCab', 'latitude', 'listed_date',
                'listing_color', 'listing_id', 'longitude', 'major_options', 'make_name',
                'maximum_seating', 'mileage', 'model_name', 'owner_count', 'power',
                'price', 'savings_amount', 'seller_rating', 'sp_id', 'sp_name', 'torque',
                'transmission', 'transmission_display', 'trimId', 'trim_name',
                'wheel_system', 'wheelbase', 'width', 'year']


columns_keep_index = [columns_raw.index(columns_keep[i]) for i in range(len(columns_keep))]
# columns_keep_index = [0, 1, 5, 7, 8, 10, 11, 13, 14, 15, 16, 19, 21, 22, 23, 25, 26, 27,
#                       28, 34, 36, 37, 38, 39, 41, 42, 43, 44, 45, 46, 47, 48, 50, 51, 52,
#                       53, 55, 56, 57, 58, 59, 61, 63, 64, 65]

line_count = 0
bad_lines = 0
with zipfile.ZipFile(data_file, 'r') as z:
    data_name = z.namelist()[0]
    with z.open(data_name, 'r') as f:
        while True:
            buffer = f.readline()

            # decode byte type to string, remove the trailing \n, split to list
            line_raw = buffer.decode("utf-8")[:-1].split(',')
            data = line_raw
            line_count += 1
            if line_count == 1:
                if header is True:
                    header_name = line_raw
                    continue
                num_columns = len(line_raw)

            # processing data

            # 1 back_legroom
            x = line_raw[1].split()[0]
            x = x.replace('--', '')
            # x = float(x)
            line_raw[1] = x

            # 5 body_type
            line_raw[5] = line_raw[5].replace(' ', '')
            if len(line_raw[5]) == 0:
                line_raw[5] = 'Unknown'

            # 8 city_fuel_economy
            # line_raw[8] = float(line_raw[8])

            # 10 daysonmarket
            # line_raw[10] = int(line_raw[10])

            # 14 engine_displacement
            # line_raw[14] = float(line_raw[14])

            # 21 front_legroom
            x = line_raw[21].split()[0]
            x = x.replace('--', '')
            # x = float(x)
            line_raw[21] = x

            # 22 fuel_tank_volume
            x = line_raw[21].split()[0]
            x = x.replace('--', '')
            # x = float(x)
            line_raw[21] = x

            # 24 has_accidents 3 cat cols +3
            line_raw[24] = line_raw[24].replace(' ', '')
            if len(line_raw[24]) == 0:
                line_raw[24] = 'Unknown'

            # 25 height
            if len(line_raw[25]) > 0:
                x = line_raw[25].split()[0]
            x = x.replace('--', '')
            # x = float(x)
            line_raw[25] = x

            # 26 highway_fuel_economy
            # line_raw[26] = float(line_raw[26])

            # 27 horsepower

            # 29 isCab
            line_raw[29] = line_raw[24].replace(' ', '')
            if len(line_raw[29]) == 0:
                line_raw[29] = 'Unknown'

            # 35 length
            if len(line_raw[35]) > 0:
                x = line_raw[35].split()[0]
            x = x.replace('--', '')
            # x = float(x)
            line_raw[35] = x

            # 43 maximum_seating
            if len(line_raw[43]) > 0:
                x = line_raw[43].split()[0]
            x = x.replace('--', '')
            # x = float(x)
            line_raw[43] = x

            # 44 mileage
            # line_raw[44] = float(line_raw[44])

            # 46 owner_count
            # line_raw[46] = int(line_raw[46])

            # 48 price
            # line_raw[48] = float(line_raw[48])

            # 51 seller_rating
            # line_raw[51] = float(line_raw[51])

            # 56 transmission
            line_raw[56] = line_raw[56].replace(' ', '')
            if len(line_raw[56]) == 0:
                line_raw[56] = 'Unknown'

            # 61 wheel_system
            line_raw[61] = line_raw[61].replace(' ', '')
            if len(line_raw[61]) == 0:
                line_raw[61] = 'Unknown'
            line_raw[61] = line_raw[61].replace('4WD', 'AWD')

            # 63 wheelbase
            if len(line_raw[63]) > 0:
                x = line_raw[63].split()[0]
            x = x.replace('--', '')
            # x = float(x)
            line_raw[63] = x

            # 64 width
            if len(line_raw[64]) > 0:
                x = line_raw[64].split()[0]
            x = x.replace('--', '')
            # x = float(x)
            line_raw[64] = x

            # 65 Year
            # line_raw[65] = float(line_raw[65])

            if line_count == 10 or len(buffer) == 0:
                break
            print(line_raw)
# print(line_count)
