# -*- coding: utf-8 -*-
'''ArcGIS Pro Python toolbox with tools to demostrate how to use standard python
libraries and attributes to make tools better.
'''

import csv
import json
import os

from concurrent import futures
from datetime import date
from datetime import datetime as dt

from functools import lru_cache
from pprint import pprint
from time import sleep

import arcpy
import requests


# functions for caching demo
fc_tracts = 'Census Tracks (NRI)'

def get_unique_values(fc, field, where):
    results = None
    with arcpy.da.SearchCursor(fc, field, where) as cur:
        results = sorted(set([row[0] for row in cur]))
    return list(results)


@lru_cache(maxsize=1)
def get_states():
    arcpy.AddMessage('gettting states')
    return get_unique_values(fc_tracts, 'STATE', None)


@lru_cache(maxsize=100)
def get_counties(state):
    arcpy.AddMessage(f'gettting counties for {state}')
    sleep(3)
    where_clause = f"STATE='{state}'"
    return get_unique_values(fc_tracts, 'COUNTY', where_clause)


@lru_cache(maxsize=200)
def get_tracts(state, county):
    arcpy.AddMessage(f'gettting counties for {state}, {county}')
    sleep(3)
    where_clause = f"STATE='{state}' and COUNTY='{county}'"
    return get_unique_values(fc_tracts, 'TRACTFIPS', where_clause)


class Toolbox:
    def __init__(self):
        """python toolbox samples for demo"""
        self.label = "PYT Demo"
        self.alias = "pytdemo"

        # suggest not running PullDataSeq unless you limit!!
        self.tools = [PullDataSeq, PullDataConcurr, CacheDemo]


class Utils:
    '''Utility class with static methods'''
    
    @staticmethod
    def urlNWPS()->str:
        return r'https://api.water.noaa.gov/nwps/v1/gauges'

    @staticmethod
    def get_nwps_data(id:str=None, data:bool=None):
        """Fetch data from the NWPS Gauge API.  Can get gauge list,
        single gauge details, or single gauge data values.
        @id: gauge id to request gauge details.
        @data: flag for requesting obs/fcast values.
        """

        url_nwps = Utils.urlNWPS()
        try:
            if id:
                if data:
                    # observed and forecast measurements
                    resp = requests.get(f'{url_nwps}/{id}/stageflow')
                    data = resp.json()
                else:
                    # station info
                    resp = requests.get(f'{url_nwps}/{id}')
                    data = resp.json()
                data['lid'] = id
            else:
                # gauge index 
                resp = requests.get(url_nwps)
                data = resp.json()
                data = data['gauges']
        except Exception as e:
            arcpy.AddWarning(f'error in get_nwps_data:\n'
                             f'id: {id}\n' 
                             f'data: {str(data)}\n'  
                             f'url: {url_nwps}')
            arcpy.AddError(f'error getting data for {id}: {e}')
        return data


    @staticmethod
    def writeCsv(out_file:str, header:list, data:list):
        """
        Writes out the passed list to a CSV file with the given header.
        @out_file: full path/filename to (over)write.
        @header: list of properties from the data objects to include in CSV.
        @data: list of objects to write to CSV.
        """
        with open(out_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, header, '', 'ignore')
            writer.writeheader()
            for d in data:
                try:
                    writer.writerow(d)
                except Exception as e:
                    arcpy.AddError(f'error writing csv row: {e}')


class PullDataSeq:
    def __init__(self):
        """Grabs NWPS Gauge data with a serial for-each loop"""
        self.label = "Pull NWPS Data Sequential"
        self.description = "Download NWPS data via sequential requests"

    def getParameterInfo(self):
        """Define the tool parameters."""

        params = []

        param0 = arcpy.Parameter(
            displayName='Clip to Extent',
            name='Xtent',
            datatype='GPExtent',
            parameterType='Required',
            direction='Input'
        )

        params = [param0]
        return params

    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        urlNwps = Utils.urlNWPS

        start = dt.now()
        #resp = requests.get(urlNwps)
        #resp.json()
        gauges = Utils.get_nwps_data()

        end = dt.now()
        ellapsed = end - start
        arcpy.AddMessage(f'pulling gauges index took {ellapsed}')

        gauge_ids = [g['lid'] for g in gauges['gauges']]
        gauge_count = len(gauge_ids)
        arcpy.AddMessage(f'found {gauge_count} gauges')

        nwps_data = []
        start = dt.now()
        req_count = 0
        for id in gauge_ids:
            try:
                gauge_data = Utils.get_nwps_data(id)
                nwps_data.extend(gauge_data)
            except Exception as e:
                pass
            if req_count % 1000 == 0:
                arcpy.AddMessage(f'{req_count} of {gauge_count} requests complete')
            req_count +=1
        end = dt.now()
        ellapsed = end - start
        arcpy.AddMessage(f'pulling all {gauge_count} gauges took {ellapsed}')

        #TODO: actually do something!!!!

        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return


class PullDataConcurr:

    urlNwps = Utils.urlNWPS

    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Pull Data Concurrent"
        self.description = "Pulls NWPS data concurrently"

    def getParameterInfo(self):
        """Define the tool parameters."""
        params = None
        return params

    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """
        Downloads minimal data for all gauges in the nwps gauges
        service.  Saves to a CSV, imports (with overwrite) into the
        project fgdb.  Finally uses an async pattern to grab the url for each
        gauge's hydrograph and updates that in the graph filed of the feature class
        """

        aprx = arcpy.mp.ArcGISProject('CURRENT')

        arcpy.env.overwriteOutput = True
        arcpy.env.addOutputsToMap = True
        arcpy.env.workspace = aprx.defaultGeodatabase
        prj_folder = os.path.dirname(arcpy.env.workspace)

        # geodb, table, featureclass names
        gauges_csv = os.path.join(prj_folder, 'nwps.csv')
        measures_csv = os.path.join(prj_folder, 'measures.csv')
        fc_temp = 'nwps_temp'
        fc_nwps = 'nwps_gauges'
        tbl_temp = 'measures_temp'
        tbl_measures = 'nwps_data'

        arcpy.AddMessage('..Fetching Gauge locations')
        gauges = Utils.get_nwps_data()

        arcpy.AddMessage('..Processing Gauges')
        # Strip gauges down to just id, lat, long, flood category, hydrograph
        graph_url = 'https://water.noaa.gov/resources/hydrographs/{}_hg.png'
        gauge_flds = ['lid', 'latitude', 'longitude', 'flood_cat', 'graph']
        gauges = [{**stat,
                     'flood_cat': stat.get('status').get('observed').get('floodCategory'),
                     'graph': graph_url.format(stat['lid']).lower()
                     }
                     for stat in gauges]
        gauge_locs = [{fld: stat[fld] for fld in gauge_flds} for stat in gauges]

        arcpy.AddMessage('..Saving to CSV')
        Utils.writeCsv(gauges_csv, gauge_flds, gauge_locs)

        arcpy.AddMessage('..Converting to NWPS FeatureClass')
        try:
            arcpy.management.TruncateTable(tbl_measures)
            arcpy.management.TruncateTable(fc_nwps)
        except Exception as e:
            arcpy.AddError(f'error truncating: {e}')

        try:
            arcpy.management.XYTableToPoint(
                in_table=gauges_csv,
                out_feature_class=fc_temp,
                x_field="longitude",
                y_field="latitude",
                z_field=None,
                coordinate_system=arcpy.SpatialReference(4326)
            )
        except Exception as e:
            arcpy.AddError(f'Error converting to featureclass: {e}')

        try:
            arcpy.management.Append(fc_temp, fc_nwps, 'NO_TEST' )
            arcpy.management.Delete(fc_temp)
        except Exception as e:
            arcpy.AddError(f'Error appending featureclass: {e}')

        #return #-----------------------------------------------------------------------------------------------<<<<<<<<<

        # Start async pull for obs/forecast data
        arcpy.AddMessage('..Requesting Gauge measurements')
        measures = self.get_measures_concurrent(gauge_locs) # , limit=1000 #TODO REMOVE LIMIT!!!!!!!!!!!!!!!!!!!!!!
        try:
            header = list(measures[0].keys())
            Utils.writeCsv(measures_csv, header, measures)
        except Exception as e:
            arcpy.AddError(f'Error writing gauge data CSV: {e}')

        # copy into measurement table
        try:
            arcpy.conversion.TableToTable(measures_csv, arcpy.env.workspace, tbl_temp)
            arcpy.management.Append(tbl_temp, tbl_measures, 'NO_TEST')
            arcpy.management.Delete(tbl_temp)
        except Exception as e:
            print(f'error with t2t: {e}')

        return


    def get_measures_concurrent(self, gauges:list=None, limit:int=None):
        '''async parallel data fetch from the NWPS Gauge API'''
        nwps_data = []

        try:
            if not gauges:
                gauges = Utils.get_nwps_data()
            if limit:
                gauges = gauges[:limit]
            g_ids = [g['lid'] for g in gauges]           
            num_gauges = len(g_ids)

            arcpy.AddMessage(f'\nStarting data pull for {num_gauges} gauges')
            get_obs_until = dt
            reqs_done = 0
            start = dt.now()
            obs_limit = start.replace(day=start.day-7,
                                      second=0,
                                      hour=0,
                                      minute=0,
                                      microsecond = 0
                                      ).date()
            with futures.ThreadPoolExecutor (max_workers=20) as executor:
                reqs = {executor.submit(Utils.get_nwps_data, id, True): id for id in g_ids}
                for req in futures.as_completed(reqs):
                    if reqs_done % 1000 == 0 and reqs_done > 0:
                        arcpy.AddMessage(f'{reqs_done} of {num_gauges} requests complete')
                    try:
                        gauge_data = req.result()
                        for data_type in ['observed', 'forecast']:
                            for row in gauge_data[data_type].get('data'):
                                row_timestamp = dt.fromisoformat(row['validTime'])
                                if row_timestamp.date() >= obs_limit:
                                    nwps_data.append({'lid': gauge_data.get('lid'),
                                                'cat': data_type,
                                                'value': row['primary'],
                                                'meas_time': row_timestamp})
                        reqs_done += 1
                    except Exception as e:
                        arcpy.AddMessage(f'error: {e}\n --> {gauge_data}')
            ellapsed = dt.now() - start
            arcpy.AddMessage(f'Pulled data for {reqs_done} gauges; took {ellapsed.total_seconds()} seconds')
            if (reqs_done < num_gauges):
                arcpy.AddWarning(f"{num_gauges - reqs_done} station's data failed to download")

        except Exception as e:
            arcpy.AddMessage(f'error: {e}')

        return nwps_data


    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return


class CacheDemo:
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Select Tracts"
        self.description = "Demo of python caching to select Census tracts"


    def getParameterInfo(self):
        """Define the tool parameters."""
        params = None

        param0 = arcpy.Parameter(
            displayName='Layer',
            name='Layer',
            datatype='GPFeatureLayer',
            parameterType='Required',
            direction='Input'
        )

        param1 = arcpy.Parameter(
            displayName='Select State',
            name='State',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )
        param1.filter.list = get_states()

        param2 = arcpy.Parameter(
            displayName='Select County',
            name='Workspace',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )
        param2.filter.list = []

        param3 = arcpy.Parameter(
            displayName='Select Tract(s)',
            name='Tract',
            datatype='GPString',
            parameterType='Required',
            direction='Input',
            multiValue=True
        )
        param3.filter.list = []

        params = [param0, param1, param2, param3]
        return params

    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        # Parameter 0 is the states. No other parameter depends on this.

        if parameters[1].value is not None and not parameters[1].hasBeenValidated:
            # Clear cascading values so user-interface is cleared while code is working
            parameters[2].value = None
            parameters[2].filter.list = []
            parameters[3].value = None
            parameters[3].filter.list = []

            state = parameters[1].valueAsText
            parameters[2].filter.list = get_counties(state)

        if parameters[2].value is not None and not parameters[2].hasBeenValidated:
            # Clear cascading values so user-interface is cleared while code is working
            parameters[3].value = None
            parameters[3].filter.list = []

            state = parameters[1].valueAsText
            county = parameters[2].valueAsText
            parameters[3].filter.list = get_tracts(state, county)

        return


    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return


    def execute(self, parameters, messages):
        """Select tracts, create comparison chart"""
        
        lyr_name = parameters[0].valueAsText
        tracts = "'" + "','".join(parameters[3].valueAsText.split(';')) + "'"
        where = f"TRACTFIPS in ({tracts})"
        
        lyr = arcpy.management.SelectLayerByAttribute(lyr_name ,'NEW_SELECTION', where)
        aprx = arcpy.mp.ArcGISProject('CURRENT')
        act_view = aprx.activeView
        xtnt = act_view.getLayerExtent(lyr,True)
        act_view.camera.setExtent(xtnt)
        return


    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""

        return
