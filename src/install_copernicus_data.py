import numpy as np
import os
import pandas as pd
import geopandas as gpd
import requests

def get_wkt(gdf: gpd.geodataframe.GeoDataFrame) -> str:
    ''' Input a GeoDataFrame and outputs string in wkt format compatible with Copernicus
    Inputs:
        gdf: GeoDataFrame, the geodataframe containing a geometry field
    Outputs:
        str: A string in wkt format usable by Copernicus
    '''
    gdf_first_geometry = gdf.groupby(level=0).first()
    first_geometry_wkt = gdf_first_geometry.geometry.iloc[0].wkt
    return first_geometry_wkt

def import_mask_layer(path: str) -> gpd.geodataframe.GeoDataFrame:
    ''' Opens the geodataframe used as a mask
    Inputs:
        path: str -> path to geojson file
    Outputs:
        geodataframe
    '''
    gdf = gpd.read_file(path)
    gdf.crs = 'EPSG:4326'
    ddf = gdf.set_crs('EPSG:4326', allow_override = True)
    gdf_exploded = gdf.explode(index_parts=True)
    return gdf_exploded

def make_request(year:int, cloudcover:float, geometry:str, sensor:str="SENTINEL-2"):
    ''' Queries Copernicus and returns a pandas dataframe of the results
    Inputs:
        year: int -> the year we want to search over
        cloudcover: float -> maximum cloud cover between 0 and 100
        geometry: str -> geometry in wkt format (see get_wkt function)
        sensor: str (default = "SENTINEL-2") -> filter based on the type of sensor
    Outputs:
        pandas dataframe
    '''
    assert cloudcover <= 100 and cloudcover >= 0, "Cloud cover must be between 0 and 100"
    # this is the initial search url, it will change over time as we look for "next" data
    start_search_url = r"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=OData.CSC.Intersects(area=geography'SRID=4326;{}') and Collection/Name eq '{}' and ContentDate/Start gt {}-01-01T00:00:00.000Z and ContentDate/Start lt {}-12-01T00:00:00.000Z and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value lt {})".format(geometry, sensor, year, year, cloudcover)
    url = start_search_url
    dfs = [] # list containing pandas dataframes that will be concatenated @ the end
    while True: # while there is more data continue
        response = requests.get(url).json()

        try:
            next_value = response['@odata.nextLink']
            url = r"{}".format(next_value)
            dfs.append(pd.DataFrame.from_dict(response['value']))
        except:
            dfs.append(pd.DataFrame.from_dict(response['value']))
            break

    df = pd.concat(dfs)
    df.reset_index(inplace=True)   
    return df
    

def main():
    json_path = input("Input location to json mask: ")
    default_json_path = os.path.join("..", "sections_mfe", "buffers", "A43_33_44_short.geojson")
    if json_path == '':
        json_path = default_json_path

    gdf = import_mask_layer(json_path)
    responses = make_request(2015, 20, get_wkt(gdf))
    print(responses)

if __name__ == "__main__":
    main()


"""
https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=OData.CSC.Intersects(area=geography'SRID=4326;POLYGON ((5.256018566739281 45.60328671462783, 5.25608161980
865 45.60358096228486, 5.257323280251573 45.60332632488937, 5.25854230625934 45.60308057959336, 5.258928708402387 45.60303207723231, 5.259389480832378 45.60293992274634, 5.25
9675644762583 45.60288818689452, 5.261717506499203 45.60244673283748, 5.264005482538052 45.60198721538676, 5.264265584863447 45.60192945689108, 5.264216529473862 45.601621849
2632, 5.264101154727738 45.6016386796606, 5.261813690039623 45.6020980993951, 5.259525701558204 45.60255849115758, 5.259221339314066 45.6026440583439, 5.258670029143444 45.60
275238028357, 5.258419433611341 45.60280573288075, 5.257202024348945 45.60304662794061, 5.256018566739281 45.60328671462783))') and Collection/Name eq 'SENTINEL-2' and Conten
tDate/Start gt 2015-01-01T00:00:00.000Z and ContentDate/Start lt 2017-12-01T00:00:00.000Z and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OD
ata.CSC.DoubleAttribute/Value lt 20)
"""
