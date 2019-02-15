import arcpy
import csv
import requests
import traceback
import json
import time
import datetime
import os
from arcgis.gis import GIS
from multiprocessing import Pool, cpu_count

# Get Script Directory
this_dir = r'C:\Users\PycharmProjects\temp'

# Get Start Time - UTC Seconds
start_time = time.time()

# Extracts elevation information where a point intersects a location from an image service
def get_elevation(batchList):

    # Get Batch Number
    batchNum = batchList[-1][0]
    print("Processing batch " + str(batchNum) + "........")

    # Login to ArcGIS Online Account
    gis = GIS("https://www.arcgis.com/", 'Username', 'Password', verify_cert=False)

    # Setup the Imagery Layer
    img_svc_url = 'https://elevation.arcgis.com/arcgis/rest/services/WorldElevation/Terrain/ImageServer/identify'

    # For each OBJECTID, pass in the SHAPE as a Parameter in the Identify Request and Retrieve
    # the Pixel Value and Attribution Information
    elevation_info = []
    missed_batch = []
    for oid, shape in batchList:

        try:
            payload = {
                'geometryType': 'esriGeometryPoint',
                'geometry': shape,
                'returnCatalogItems': True,
                'token': gis._con.token,
                'f': 'json'
            }

            result = requests.post(img_svc_url, data=payload)

            p_value = result.json()['value']
            source_feature = result.json()['catalogItems']['features'][0]
            product_attr = source_feature['attributes']['ProductName']
            dataID_attr = source_feature['attributes']['Dataset_ID']
            source_attr = source_feature['attributes']['Source']
            elevation_info.append([oid, p_value, product_attr, dataID_attr, source_attr])
        except Exception as e:
            print("ERROR: " + traceback.format_exc())
            missed_batch.append(oid)

    # Write the Data out to a CSV
    if not elevation_info:
        print("Batch elevation_%i" % batchNum + " is empty.")
    else:
        csv_table = os.path.join(this_dir, "successful_batch_%i.csv" % batchNum)
        with open(csv_table, 'w',
                  newline='') as csv_file:
            the_writer = csv.writer(csv_file, delimiter=',')
            the_writer.writerow(['Feature_OID', 'Pixel_Value', 'Product_Name', 'Dataset_ID', 'Source'])
            for row in elevation_info:
                the_writer.writerow(row)

        print("Batch %i" % batchNum + " complete!")

    return missed_batch


if __name__ == "__main__":

    # Set Logger Time
    logger_date = datetime.datetime.fromtimestamp(start_time).strftime('%Y_%m_%d')
    logger_time = datetime.datetime.fromtimestamp(start_time).strftime('%H_%M_%S')
    print('Script Started: {} - {}\n'.format(logger_date, logger_time))
    print('Executing process.........')

    # Set the Local Environment
    scratchDB = r'C:\Users\Documents\ArcGIS\Projects\temp\Py_Test.gdb'
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = scratchDB

    # Get OID List
    oidList = [oid for oid in arcpy.da.SearchCursor('Data', ['OID@', 'SHAPE@JSON'])]

    # Get List of Batches
    batchSize = 5
    oidBatch = [oidList[x:x + batchSize] for x in range(0, len(oidList), batchSize)]

    # Start up multiple processes to run the batches
    pool = Pool(processes=cpu_count() - 2)
    results = pool.map_async(get_elevation, oidBatch)
    pool.close()
    pool.join()

    # Get a List of Values Dropped during the Analysis
    missed_list = results.get()
    flat_list = [item for sublist in missed_list for item in sublist]
    json_list = os.path.join(this_dir, 'unsuccessful_list.json')
    with open(json_list, 'w') as json_file:
        json.dump(flat_list, json_file)

    # Calculate the Percentage of Dropped Values
    droppedPerc = (len(flat_list) / len(oidList)) * 100
    print(str(droppedPerc) + "%.2f dropped from original list.")

    # Create a List of the Batched Tables
    arcpy.env.workspace = r"C:\Users\Documents\ArcGIS\Projects\temp"
    csv_tables = arcpy.ListFiles("*.csv")

    # Merge the Tables into One
    print('Merging tables....')
    arcpy.Merge_management(csv_tables, r'C:\Users\Documents\ArcGIS\Projects\temp\Py_Test.gdb\Data_US_Merge')

    # Create New Fields in Feature Class
    arcpy.env.workspace = scratchDB
    arcpy.AddFields_management('Data', [['Z_Value', 'DOUBLE', 'Z Value'],
                                                       ['Product_Name', 'TEXT', 'Product Name'],
                                                       ['Dataset_ID', 'TEXT', 'Dataset ID'],
                                                       ['Source', 'TEXT', 'Source']])

    # Join Results Table to Feature Class
    arcpy.MakeFeatureLayer_management('Data', 'Data_lyr')
    arcpy.MakeTableView_management('Data_US_Merge', 'Data_US_Merge_table')
    arcpy.AddJoin_management('Data_lyr', 'OBJECTID', 'Data_US_Merge_table', 'Feature_OID', 'KEEP_ALL')

    # Copy Values from Table to Feature Class
    elevExp = "!Data_US_Merge_table.Pixel_Value!"
    productExp = "!Data_US_Merge_table.Product_Name!"
    dataExp = "!Data_US_Merge_table.Dataset_ID!"
    sourceEXP = "!Data_US_Merge_table.Source!"
    arcpy.CalculateFields_management('Data_lyr', 'PYTHON3', [['Z_Value', elevExp],
                                                                            ['Product_Name', productExp],
                                                                            ['Dataset_ID', dataExp],
                                                                            ['Source', sourceEXP]])

    # Remove Join
    arcpy.RemoveJoin_management('Data_lyr')

    print("\nProgram Run Time: %.2f Seconds" % (time.time() - start_time))
