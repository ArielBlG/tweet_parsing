import re
from typing import Tuple
from tqdm import tqdm
import multiprocessing as mp
import argparse
import time
import sys

import numpy as np
from presidio_analyzer import PatternRecognizer, AnalyzerEngine, RecognizerRegistry
from presidio_analyzer.pattern import Pattern
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities.engine.result.engine_result import EngineResult

from pyspark.sql import SparkSession

from data_handler import *
from CONSTANTS import * 

def create_mention_as_entity(registry: RecognizerRegistry) -> PatternRecognizer:
    pattern = Pattern(name="mentions", regex=r"@(\w+)", score=1) 
    mention_recognizer = PatternRecognizer(supported_entity="MENTION",
                                      patterns=[pattern],
                                      global_regex_flags=re.IGNORECASE)
    
    registry.load_predefined_recognizers()
    registry.add_recognizer(mention_recognizer)

registry = RecognizerRegistry()

create_mention_as_entity(registry=registry)
analyzer = AnalyzerEngine(registry=registry)

spark = SparkSession.builder.appName("AnonymizeTweets").getOrCreate()

with open("Data/user_hash_dict.json", 'r') as f:
    HASH_DICT = json.load(f)

def get_hash(user_name: str):
    return HASH_DICT.get(user_name, hash(user_name))


def dump_hashed_dict(hash_dict):
    hash_dict.update(HASH_DICT)
    with open("Data/user_hash_dict.json", 'w') as f:
        json.dump(hash_dict, f)




def anonmyization_process_text(text: str,
                          registry: RecognizerRegistry,
                          engine: AnonymizerEngine) -> EngineResult:


    analyzer_results = analyzer.analyze(text=text, language='en')
    return analyzer_results
    # Ignore for now
    # return engine.anonymize(text=text, analyzer_results=analyzer_results)

def anonymize_text_fields(text: str,
                          registry: RecognizerRegistry,
                          engine: AnonymizerEngine) -> str:

    anonmyized_txt = anonmyization_process_text(text=text, registry=registry, engine=engine)
    new_text = text[:] # create new string
    for item in anonmyized_txt:
        """No need to hash user names from text because it will be done in different field."""
        original_text = text[item.start: item.end]
        if item.entity_type in FIELDS_TO_ANONYMIZE and original_text in new_text: # Currently, not handling overlaps
            new_text = new_text.replace(original_text, FIELDS_TO_ANONYMIZE[item.entity_type](original_text))
    return new_text
    
def anonymize_data(row: pd.Series,
                   registry: RecognizerRegistry,
                   engine: AnonymizerEngine) -> pd.DataFrame:
    def process_nested_tweet(tweet):
        temp_hash_dict = {}
        # Handle URLs
        if tweet.get('new_full_entities', {}).get('urls'):
            for url in tweet['new_full_entities']['urls']:
                tweet['text'] = tweet['text'].replace(url['url'], url['expanded_url'])

        # Anonymize specified text fields
        for text_field in ANONMYIZE_TEXT_FIELDS:
            if not isinstance(tweet.get(text_field, ''), float):
                tweet[text_field] = anonymize_text_fields(tweet[text_field], registry, engine)

        # Handle user information
        if 'user' in tweet:
            _hash = get_hash(tweet['user']['name'])
            value = (tweet['user']['name'], tweet['user']['screen_name'])
            # update_hash_dict(_hash, value=value) # too slow
            temp_hash_dict[_hash] = value
            tweet['user']['name'] = _hash
            tweet['user']['screen_name'] = _hash

        # Handle user mentions
        if tweet.get('entities', {}).get('user_mentions'):
            new_list = []
            for mention in tweet['entities']['user_mentions']:
                _name = mention.pop('name')
                _screen_name = mention.pop('screen_name')
                _hash = get_hash(_name)
                value = (_name, _screen_name)
                # update_hash_dict(_hash, value=value)
                temp_hash_dict[_hash] = value
                mention['name'] = _hash
                mention['screen_name'] = _hash

        # Handle user mentions
        if tweet.get('extended_entities', {}).get('user_mentions'):
            new_list = []
            for mention in tweet['extended_entities']['user_mentions']:
                _name = mention.pop('name')
                _screen_name = mention.pop('screen_name')
                _hash = get_hash(_name)
                value = (_name, _screen_name)
                # update_hash_dict(_hash, value=value)
                temp_hash_dict[_hash] = value
                mention['name'] = _hash
                mention['screen_name'] = _hash

        # Recursively process retweeted or quoted tweets
        if 'retweeted_status' in tweet and not (isinstance(tweet['retweeted_status'],float) and pd.isna(tweet['retweeted_status'])):
            tweet['retweeted_status'], temp_hash_dict = process_nested_tweet(pd.Series(tweet['retweeted_status']))
        if 'quoted_status' in tweet and not (isinstance(tweet['quoted_status'],float) and pd.isna(tweet['quoted_status'])):
            tweet['quoted_status'], temp_hash_dict = process_nested_tweet(pd.Series(tweet['quoted_status']))

        return tweet.to_dict(), temp_hash_dict

    row, temp_hash_dict = process_nested_tweet(row)

    return row,temp_hash_dict
    
def process_row(args: Tuple[pd.Series, RecognizerRegistry, AnonymizerEngine]) -> Tuple[Dict, Dict]:
    row, registry, engine = args
    row, temp_hash_dict = anonymize_data(row, registry, engine=engine)

    return row, temp_hash_dict

def iterate_dataframe(data: pd.DataFrame) -> pd.DataFrame:
    registry = RecognizerRegistry() # initiate recongizer registery 
    engine = AnonymizerEngine() # initiate anonymizer engine
    json_list = []
    global_hash_dict = {}

    args = [(row, registry, engine) for index,row in data.iterrows()]
    with mp.Pool() as pool:
        results = list(tqdm(pool.imap(process_row, args), total=data.shape[0]))

    for row_dict, temp_hash_dict in results:
        json_list.append(row_dict)
        global_hash_dict.update(temp_hash_dict)

    return json_list, global_hash_dict
    
if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Process a path argument.")
    # parser.add_argument("path", type=str, help="The path to a file or directory.")
    # args = parser.parse_args()

    # if not args.path:
    #     raise FileNotFoundError('No data input was found')
    
    # path_to_file = args.path
    path_to_file = "Data/data.jsonl"

    data = read_ndjson(path_to_file, return_as_df=True)
    data = preprocess_data(data)
    data = iterate_dataframe(data)

    sys.exit(1)
    json_list, global_hash_dict = iterate_dataframe(data=data)
    dump_hashed_dict(global_hash_dict)

    with open('output/anonymized_jsonl.jsonl', 'w') as f:
        for entry in json_list:
            f.write(json.dumps(entry) + '\n')

