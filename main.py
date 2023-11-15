from mylib.extract import extract
from mylib.transform_load import load
from mylib.query_vis import query_transform, vis
import os 


if __name__ == "__main__":
    current_directory = os.getcwd()
    print(current_directory)
    extract()
    load()
    query_transform()
    vis()