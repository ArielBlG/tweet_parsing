import json
import pandas as pd
from typing import List, Dict, Union
import ast
from json import JSONDecodeError


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

