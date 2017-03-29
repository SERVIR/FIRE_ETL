# Developer: Daniel Duarte
# Company:   Spatial Development International
# E-mail:    dduarte@spatialdev.com
# Last Modified By: Githika Tondapu

# for more information please visit the following link below
# Website: http://firefly.geog.umd.edu/firms/faq.htm#attributes
import os
import sys
from datetime import datetime, timedelta
import urllib
import json
import arcpy
import pickle
# Add the ETLBaseModule directory location to the Python system path in order to import the shared base modules
sys.path.append('D:\\SERVIR\\Scripts\\FIRE\\SD_Code\\ETLBaseModules\\')  

# ETL framework
from etl_controller import ETLController
from fire_etl_delegate import FireETLDelegate
from arcpy_fire_etl_core import FireLoader, FireTransformer, FireExtractor, FireMetaDataTransformer, FireExtractValidator

# ETL utils
from arcpy_utils import FeatureClass, FileGeoDatabase, AGServiceManager
from etl_utils import ETLDebugLogger, ETLExceptionManager

# For Geodatabases
pathToGeoDatabase = r"D:\SERVIR\Data\Global\Fire.gdb"
partialPathToGeoDB = "D:\\SERVIR\\Data\\Global\\"
geoDBName = "Fire.gdb"
g_UpdateLog = ""
# KS Mod 07-2014 (Adding support for restarting services via web tokens)    part 1        START
def Do_Update_Services(service_Options_List, updateLogFunction):

    addToLog = updateLogFunction
    # For each service, Stop them All
    addToLog("Do_Update_Services: About to stop all related services")
    for current_Service in service_Options_List:
        current_Description = current_Service['Description']
        current_AdminDirURL = current_Service['admin_dir_URL']
        current_Username = current_Service['username']
        current_Password = current_Service['password']
        current_FolderName = current_Service['folder_name']
        current_ServiceName = current_Service['service_name']
        current_ServiceType = current_Service['service_type']

        try:
            # Get a token from the Administrator Directory
            tokenParams = urllib.urlencode({"f":"json","username":current_Username,"password":current_Password,"client":"requestip"})
            tokenResponse = urllib.urlopen(current_AdminDirURL+"/generateToken?",tokenParams).read()
            tokenResponseJSON = json.loads(tokenResponse)
            token = tokenResponseJSON["token"]

            # Attempt to stop the current service
            stopParams = urllib.urlencode({"token":token,"f":"json"})
            stopResponse = urllib.urlopen(current_AdminDirURL+"/services/"+current_FolderName+"/"+current_ServiceName+"."+current_ServiceType+"/stop?",stopParams).read()
            stopResponseJSON = json.loads(stopResponse)
            stopStatus = stopResponseJSON["status"]

            if stopStatus <> "success":
                addToLog("Do_Update_Services: Unable to stop service "+str(current_FolderName)+"/"+str(current_ServiceName)+"/"+str(current_ServiceType)+" STATUS = "+stopStatus)
                pass
            else:
                addToLog("Do_Update_Services: Service: " + str(current_ServiceName) + " has been stopped.")
                pass

        except:
            e = sys.exc_info()[0]
            addToLog("Do_Update_Services: ERROR, Stop Service failed for " + str(current_ServiceName) + ", System Error Message: "+ str(e))
            pass

    # For each service, Start them all
    addToLog("Do_Update_Services: About to restart all related services")
    for current_Service in service_Options_List:
        current_Description = current_Service['Description']
        current_AdminDirURL = current_Service['admin_dir_URL']
        current_Username = current_Service['username']
        current_Password = current_Service['password']
        current_FolderName = current_Service['folder_name']
        current_ServiceName = current_Service['service_name']
        current_ServiceType = current_Service['service_type']

        try:
            # Get a token from the Administrator Directory
            tokenParams = urllib.urlencode({"f":"json","username":current_Username,"password":current_Password,"client":"requestip"})
            tokenResponse = urllib.urlopen(current_AdminDirURL+"/generateToken?",tokenParams).read()
            tokenResponseJSON = json.loads(tokenResponse)
            token = tokenResponseJSON["token"]

            # Attempt to start the current service
            startParams = urllib.urlencode({"token":token,"f":"json"})
            startResponse = urllib.urlopen(current_AdminDirURL+"/services/"+current_FolderName+"/"+current_ServiceName+"."+current_ServiceType+"/start?",startParams).read()
            startResponseJSON = json.loads(startResponse)
            startStatus = startResponseJSON["status"]

            if startStatus == "success":
                addToLog("Do_Update_Services: Started service "+str(current_FolderName)+"/"+str(current_ServiceName)+"/"+str(current_ServiceType))
                pass
            else:
                addToLog("Do_Update_Services: Unable to start service "+str(current_FolderName)+"/"+str(current_ServiceName)+"/"+str(current_ServiceType)+" STATUS = "+startStatus)
                pass
        except:
            e = sys.exc_info()[0]
            addToLog("Do_Update_Services: ERROR, Start Service failed for " + str(current_ServiceName) + ", System Error Message: "+ str(e))
            pass

# KS Mod 07-2014 (Adding support for restarting services via web tokens)    part 1        END

# --------------- ETL ---------------------------------------------------------------------------------------------------
# configure feature class object -------------------------------------
def createFeatureClass(output_basepath, feature_class_name):

    feature_class = FeatureClass(output_basepath, feature_class_name, {

        "geometry_type":"POINT",
        "archive_days": 7,#90,
        "datetime_field":"datetime",
        'datetime_sql_cast':"date",
        "datetime_field_format":"%m/%d/%Y %I:%M:%S %p"
    })
    return feature_class

# Initialize objects, execute ETL and perform post ETL operations
def executeETL(feature_class):

    etl_exception_manager = ETLExceptionManager(sys.path[0], "Fire_exception_reports", {

        "create_immediate_exception_reports":True,
        "delete_immediate_exception_reports_on_finish":True
    })

    debug_log_output_directory = "D:\\Logs\\ETL_Logs\\FIRE"
    etl_debug_logger = ETLDebugLogger(debug_log_output_directory, "Fire", {

        "debug_log_archive_days":7
    })
    update_debug_log = etl_debug_logger.updateDebugLog # retrieve a reference to the debug logger function

    # KS Note 2014-07-23 Need global access to the Logger
    global g_UpdateLog
    g_UpdateLog = update_debug_log

    start_datetime = datetime.datetime.utcnow()
    end_datetime = start_datetime - timedelta(days=5) # this is how many days worth of CSVs it will retrieve from the ten-day rolling FTP
    extract_validator = FireExtractValidator({

        "feature_class":feature_class,
        "satellite_ftp_directory_name_field":"SHORTNAME", 
        "ftp_file_name_field":"ftp_file_name", # field used to determine duplicates and current feature class membership
        "start_datetime":start_datetime,
        "end_datetime":end_datetime,
        "debug_logger":update_debug_log
    })
    pkl_file = open('config.pkl', 'r')
    myConfig = pickle.load(pkl_file) #store the data from config.pkl file
    pkl_file.close()
    extractor = FireExtractor({

        "target_file_extn":"txt", # extension of fire CSV
        "ftp_options": {
            "ftp_host":myConfig['ftp_host'],
            "ftp_user":myConfig['ftp_user'],
            "ftp_pswrd":myConfig['ftp_pswrd'],
        },
        "debug_logger":update_debug_log
    })

    meta_data_transformer = FireMetaDataTransformer({"debug_logger":update_debug_log})

    # projection used to create an XY event layer in arcpy.MakeXYEventLayer_management
    spatial_projection = "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision"

    transformer = FireTransformer({
        'fire_fields':['LATITUDE','LONGITUDE','BRIGHTNESS','SCAN','TRACK','DATE','TIME','SATELLITE','CONFIDENCE','VERSION','BRIGHTT31','FRP'],
        'admin_join_table_fullpath': os.path.join(sys.path[0], 'GAUL_LEVELS.gdb', 'g2006_2'),
        'admin_fields':['ADM0_NAME', 'ADM1_NAME', 'ADM2_NAME'],
        "CreateFeatureclass_management_config":{
            'geometry_type':"POINT"
        },
        "MakeXYEventLayer_management_config":{
            'in_x_field':'LONGITUDE',
            'in_y_field':'LATITUDE',
            'spatial_reference':spatial_projection
        },
        "debug_logger":update_debug_log
    }, decoratee=meta_data_transformer) # NOTE: the transformer is decorating the meta_data_transformer to chain along transform() calls

    loader = FireLoader({

        "datetime_sql_cast":"date", # this is important when you change the underlying SQL database. PostGIS vs SQL for example.
        "feature_class":feature_class,
        "Append_management_config":{
            'schema_type':"NO_TEST"
        },
        'satellite_mapping':{ # this is used to determine which satellite the granule came from in order to update the appropriate rows
            "MYD14T":"A", # aqua
            "MOD14T":"T" # terra
        },
        "debug_logger":update_debug_log
    })

    etl_controller = ETLController("Z:\\ETLscratch\\FIRE", "Fire_ETL", {

        "remove_etl_workspace_on_finish":True
    })

    fire_etl_delegate = FireETLDelegate({

        "ftp_dirs":['/allData/1/MOD14T/Recent/', '/allData/1/MYD14T/Recent/'],# iterate through both aqua and terra FTP directories
        "ftp_file_meta_extn":"met", # extension of fire CSVs meta-data
        "all_or_none_for_success":False,
        "debug_logger":update_debug_log,
        "exception_handler":etl_exception_manager.handleException
    })

    # set ETLDelegate object properties-------------------------------------
    fire_etl_delegate.setExtractValidator(extract_validator)
    fire_etl_delegate.setExtractor(extractor)
    fire_etl_delegate.setTransformer(transformer)
    fire_etl_delegate.setLoader(loader)
    fire_etl_delegate.setETLController(etl_controller)

    # execute the ETL operation -------------------------------------
    successful_new_run = fire_etl_delegate.startETLProcess()

    # perform post-ETL operations -------------------------------------
    feature_class.deleteOutdatedRows()
    etl_debug_logger.deleteOutdatedDebugLogs()
    etl_exception_manager.finalizeExceptionXMLLog()

    return successful_new_run


# --------------- ETL MAIN ---------------------------------------------------------------------------------------------------
def main(*args, **kwargs):

    # create the FileGeoDatabase if it does not already exist
    fire_fgdb = FileGeoDatabase(partialPathToGeoDB, geoDBName, {

        "compact_interval_days":7
    })

    feature_class_name = "global_fire"

    # create the main fire feature class if it does not already exist
    feature_class = createFeatureClass(fire_fgdb.fullpath, feature_class_name)

    # execute the main ETL operation
    is_successful_new_run = executeETL(feature_class)

    if is_successful_new_run:
        pkl_file = open('config.pkl', 'rb')
        myConfig = pickle.load(pkl_file) #store the data from config.pkl file
        pkl_file.close()
        # refresh all services to update the data
        fire_services = ["ReferenceNode/MODIS_Fire_1DAY", "ReferenceNode/MODIS_Fire"]
        # KS Mod 07-2014 (Adding support for restarting services via web tokens)    part 2        START
        # Restart the services
        FIRE_Service_Options = [{
            "Description":"FIRE Dataset Service",
            "admin_dir_URL":myConfig['admin_dir_URL'],
            "username":myConfig['username'],
            "password":myConfig['password'],
            "folder_name":myConfig['folder_name'],
            "service_name":myConfig['service_name'],
            "service_type":myConfig['service_type']
        }]
        global g_UpdateLog
        Do_Update_Services(FIRE_Service_Options, g_UpdateLog)
        # KS Mod 07-2014 (Adding support for restarting services via web tokens)    part 2        END
    arcpy.Compact_management(pathToGeoDatabase)

# method called upon module execution to start the ETL process
main()
