import json
import pandas as pd
import numpy as np
from typing import List, Dict, Union
import ast
from json import JSONDecodeError
from pyspark.sql.types import StructType, StructField


def read_ndjson(path: str, return_as_df=False) -> Union[List[Dict], pd.DataFrame]:
    list_to_return =[]
    with open(path, "r") as file:
        for line in file:
                try: 
                     list_to_return.append(json.loads(line))
                except JSONDecodeError as e:
                     list_to_return.append(ast.literal_eval(line))
    if return_as_df:
         return pd.DataFrame(list_to_return)
    return list_to_return


def fillna_method(col):
    """
    Custom method to fill NaN values based on column data type.
    - Returns '' for strings
    - Returns 0 for numbers
    - Returns [] for lists
    """
    if col.dtype == np.object:  # For object columns
        # Check if the column contains lists
        if col.dropna().apply(lambda x: isinstance(x, list)).any():
            return col.apply(lambda x: [] if pd.isna(x) else x)
        else:
            return col.fillna('')
    elif np.issubdtype(col.dtype, np.number):  # For numeric columns
        return col.fillna(0)
    else:
        return col.fillna(None) 

def read_ndjson_spark(path: str, schema_path: str, spark):
     with open(schema_path) as f:
          d = json.load(f)
     schemaNew = StructType.fromJson(d)
     df = spark.read.schema(schemaNew).json(path)
     # df = spark.read.json(path)
     return df


def preprocess_data(data: pd.DataFrame) -> pd.DataFrame:
     # data['new_full_entities'] = data.apply(lambda row: {**row['entities'], **row['extended_entities']}, axis=1)
     new_full_entities = []
     for entity_1, entity_2 in zip(data['entities'].to_list(),data['extended_entities'].to_list()):
          temp_dict = {}
          if isinstance(entity_1, dict):
               temp_dict.update(entity_1)
          if isinstance(entity_2, dict):
               temp_dict.update(entity_2)
          new_full_entities.append(temp_dict)

     data['new_full_entities'] = new_full_entities        
     data = data.drop(columns=['entities', 'extended_entities'])
     
     return data

