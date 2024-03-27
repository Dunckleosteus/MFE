import numpy as np
import shutil # for deleting directories
import os
import pandas as pd
import geopandas as gpd
import requests

def generate_output(func):
    """ Decorator that returns a ✅ if a function returns boolean true or ❌ in the other case
    Inputs: 
        func: Function -> function that needs to be wrapped
    Outputs: 
        Function
    """
    def wrapper(*args, **kwargs):
        if func(*args, **kwargs):
            print("✔")
        else: 
            print("❌") 
    return wrapper


def get_wkt(gdf: gpd.geodataframe.GeoDataFrame) -> str:
    """Input a GeoDataFrame and outputs string in wkt format compatible with Copernicus
    Inputs:
        gdf: GeoDataFrame, the geodataframe containing a geometry field
    Outputs:
        str: A string in wkt format usable by Copernicus
    """
    gdf_first_geometry = gdf.groupby(level=0).first()
    first_geometry_wkt = gdf_first_geometry.geometry.iloc[0].wkt
    return first_geometry_wkt


def import_mask_layer(path: str) -> gpd.geodataframe.GeoDataFrame:
    """Opens the geodataframe used as a mask
    Inputs:
        path: str -> path to geojson file
    Outputs:
        geodataframe
    """
    gdf = gpd.read_file(path)
    gdf.crs = "EPSG:4326"
    ddf = gdf.set_crs("EPSG:4326", allow_override=True)
    gdf_exploded = gdf.explode(index_parts=True)
    return gdf_exploded


def make_request(
    year: int, cloudcover: float, geometry: str, sensor: str = "SENTINEL-2"
):
    """Queries Copernicus and returns a pandas dataframe of the results
    Inputs:
        year: int -> the year we want to search over
        cloudcover: float -> maximum cloud cover between 0 and 100
        geometry: str -> geometry in wkt format (see get_wkt function)
        sensor: str (default = "SENTINEL-2") -> filter based on the type of sensor
    Outputs:
        pandas dataframe
    """
    assert (
        cloudcover <= 100 and cloudcover >= 0
    ), "Cloud cover must be between 0 and 100"
    # this is the initial search url, it will change over time as we look for "next" data
    start_search_url = r"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=OData.CSC.Intersects(area=geography'SRID=4326;{}') and Collection/Name eq '{}' and ContentDate/Start gt {}-01-01T00:00:00.000Z and ContentDate/Start lt {}-12-01T00:00:00.000Z and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value lt {})".format(
        geometry, sensor, year, year, cloudcover
    )
    url = start_search_url
    dfs = []  # list containing pandas dataframes that will be concatenated @ the end
    while True:  # while there is more data continue
        response = requests.get(url).json()

        try:
            next_value = response["@odata.nextLink"]
            url = r"{}".format(next_value)
            dfs.append(pd.DataFrame.from_dict(response["value"]))
        except:
            dfs.append(pd.DataFrame.from_dict(response["value"]))
            break

    df = pd.concat(dfs)
    df.reset_index(inplace=True)
    return df


def select_ids(df: pd.DataFrame, number: int) -> pd.core.frame.DataFrame:
    # TODO: Finish this function
    """Given a dataframe containing install candidates, return a list of indeces to be installed select @ random from the dataframe
    Inputs:
        df: Panda Dataframe -> the dataframe we are going to randomly select data from
        number: int -> The maximum number of measurements per year
    Outputs:
        Pandas dataframe ["Id", "Name", "Footprint"] and of length number
    """
    # sort the dataframe by data
    df["OriginDate"] = pd.to_datetime(df["OriginDate"])
    df.sort_values(by=["OriginDate"], inplace=True)
    return df.loc[:, ["Id", "Name", "Footprint"]][:number]


def get_access_token(email: str, password: str) -> str:
    """Returns a access token that will be used as a key to install data
    Inputs:
        email: str -> email used to connect to sentinel hub
        password: str -> password used to connect to sentinel hub
    Outputs:
        access_token: str -> access token that will be used as key to download data
    """
    # Define the endpoint and payload
    url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    payload = {
        "username": email.strip(),
        "password": password.strip(),
        "grant_type": "password",
        "client_id": "cdse-public",
    }

    # Make the HTTP POST request
    response = requests.post(
        url, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"}
    )

    data = response.json()
    access_token = data.get("access_token")
    return access_token


def get_credentials_from_file(path: str) -> (str, str):
    """Extracts email and password from a text file containing 2 lines, on the first line email, on the second line password. This allows the user to not have to type it in every time
    Inputs:
        path: str -> path to text file containing email and password on seperate lines
    Outputs:
        (email, password): (str, str) -> tuple containing email and password
    """
    with open(path, "r") as f:
        email: str = f.readline()
        password: str = f.readline()
        return (email, password)

@generate_output
def create_folder_if_not_exist(path: str)->bool:
    """ Creates a folder in a given location if it does not already exist
    Inputs: 
        path: str -> path where folder should be created
    Outputs:
        bool -> True if folder was created, False if there was already a folder and nothing was created
    """
    created:bool=False
    if not os.path.exists(path):
        created=True
        os.makedirs(path)
    return created

@generate_output
def delete_folder_if_exists(path: str)->bool:
    """ Will look for a folder @ a given path and delete it & all it's contents if it's there
    Inputs: 
        path: str -> path where folder should be deleted if it exists
    Outputs:
        bool -> True if folder was found & destroyed, False if no folder was found & nothing was deleted
    """
    deleted:bool=False
    if os.path.exists(path):
        shutil.rmtree(path)
        deleted = True
    return deleted
        

def main():
    json_path = input("Input location to json mask: ")
    default_json_path = os.path.join(
        "..", "sections_mfe", "buffers", "A43_33_44_short.geojson"
    )
    if json_path == "":  # if no user input then use use default path to mask layer
        json_path = default_json_path

    credential_file_path = input("Input location of credential file: ")
    default_credential_file_path = os.path.join("..", "mydata", "pass.txt")
    if credential_file_path == "":
        credential_file_path = default_credential_file_path

    email, password = get_credentials_from_file(credential_file_path)
    print("-> Getting Access token ", end="")
    access_token = get_access_token(email, password)
    print("✔")

    print("-> Opening mask layer ", end="")
    gdf = import_mask_layer(json_path)
    print("✔")

    years_to_get = [2015, 2016, 2017]  # get query year by year
    data_per_year: list = [make_request(x, 20, get_wkt(gdf)) for x in years_to_get]
    # TODO: filter dataframes by geometry to ensure a complete overlap
    data_per_year_selected: list = [select_ids(x, 3) for x in data_per_year]

    # downloading data
    cache = os.path.join("cache") # downloaded data will temporarily be stored here
    print(f"-> Delete cache folder: ", end="")
    delete_folder_if_exists(cache)
    
    print(f"-> Create new cache folder ", end="")
    create_folder_if_not_exist(cache) # after making sure it was deleted the cache can be created again

    output_folder = os.path.join("output")
    print(f"-> Create output folder: ", end="")
    create_folder_if_not_exist(output_folder)

    for dataframe in data_per_year_selected:
        for row in dataframe:
            # TODO: install data
            pass


if __name__ == "__main__":
    main()
