import tqdm # used for progress bars
import glob
import zipfile # unzip installed data
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

@generate_output
def delete_file_if_exists(path: str)->bool:
    """ Will look for a file @ a given path and delete it & all it's contents if it's there
    Inputs: 
        path: str -> path where file should be deleted if it exists
    Outputs:
        bool -> True if file was found & destroyed, False if no file was found & nothing was deleted
    """
    deleted: bool = False
    if os.path.exists(path):
        os.remove(path)
        deleted = True
    return deleted

def download_by_id(id:str, path:str, access_token:str):
    """ Accepts and id, saves to given location
    Inputs: 
        id: str -> the index of data to be downloaded
        path: str -> the file path it needs to be installed to
        access_token: str -> see get_access_token, required for install
    Outputs: 
    """
    url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({id})/$value"
    headers = {"Authorization": f"Bearer {access_token}"}

    session = requests.Session()
    session.headers.update(headers)
    response = session.get(url, headers=headers, stream=True)

    with open(path, "wb") as file:
        for chunk in tqdm.tqdm(response.iter_content(chunk_size=8192)):
            if chunk:
                file.write(chunk)

@generate_output     
def unzip_folder(zip_folder_path: str, zip_to: str): 
    """ Unzips
    Inputs:
        zip_folder_path: str -> path to zip file
        zip_to: str -> path to where the zip file should be extracted to
    Outputs:
    """
    try: 
        with zipfile.ZipFile(zip_folder_path, 'r') as zip_ref:
            zip_ref.extractall(zip_to)
        return True
    except: 
        return False

def create_path_dict(path:str) -> dict: 
    """ Returns a dict of paths assosiated to band type
    Inputs: 
        path: str -> path to the folder containing band data
    """
    l = glob.glob(path) # glob.glob -> glob

    l.sort()
    
    keys = ["B01_path", "B02_path", "B03_path", "B04_path", "B05_path", "B06_path", "B07_path", "B08_path", "B09_path", "B10_path", "B11_path", "B12_path", "B8A_path", "TCI_path"]
    path_dict = dict(zip(keys[:-1], l[:-1]))
    print(type(path_dict))
    return path_dict


persist = True # stops the install and the purge of the cache to avoid installing data 
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

    years_to_get = [2015]  # get query year by year
    data_per_year: list = [make_request(x, 20, get_wkt(gdf)) for x in years_to_get]
    # TODO: filter dataframes by geometry to ensure a complete overlap
    data_per_year_selected: list = [select_ids(x, 1) for x in data_per_year]

    # downloading data
    cache = os.path.join("cache") # downloaded data will temporarily be stored here
    if persist == False:
        print(f"-> Delete cache folder: ", end="")
        delete_folder_if_exists(cache)
    
    print(f"-> Create new cache folder ", end="")
    create_folder_if_not_exist(cache) # after making sure it was deleted the cache can be created again

    output_folder = os.path.join("output")
    print(f"-> Create output folder: ", end="")
    create_folder_if_not_exist(output_folder)


    for num_year, (year, dataframe) in enumerate(zip(years_to_get, data_per_year_selected)):
        print(f"-> Installing year {num_year}/{len(years_to_get)}")
        indeces = dataframe["Id"]
        names = dataframe["Name"]
        create_folder_if_not_exist(os.path.join(cache, str(year)))
        for num, (name, id) in enumerate(zip(names, indeces)):
            print(f"---> Installing file {num} out of {len(names)} in {year}")
            if persist == False:
                download_by_id(id, os.path.join(cache, str(year), f"{name}.zip") , access_token)
            else: 
                print("-> Skipping install")
            # now that the zip file is installed, unzip it
            print("Unzipping folder: ", end="")
            unzip_folder(
                os.path.join(cache, str(year), f"{name}.zip"), 
                os.path.join(cache, str(year), "sisi")
            )
            print("-> Deleting zip file: ", end ="")
            delete_file_if_exists(os.path.join(cache, str(year), f"{name}.zip"))
            l = create_path_dict(os.path.join(cache, str(year), "sisi", "*/GRANULE/*/IMG_DATA*/*"))
        
            # sisi/*/GRANULE/*/IMG_DATA/*
            # now that the folders are unzipped, we need to trim them


if __name__ == "__main__":
    main()
