import re
from typing import Tuple
import pyspark
from tqdm import tqdm
import multiprocessing as mp
import argparse
import time 

import numpy as np
from presidio_analyzer import PatternRecognizer, AnalyzerEngine, RecognizerRegistry
from presidio_analyzer.pattern import Pattern
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities.engine.result.engine_result import EngineResult
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import pandas_udf, PandasUDFType

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
                        #   engine: AnonymizerEngine
                          ) -> EngineResult:


    analyzer_results = analyzer.analyze(text=text, language='en')
    return analyzer_results
    # Ignore for now
    # return engine.anonymize(text=text, analyzer_results=analyzer_results)




def anonymize_text_fields(text: str,
                          registry: RecognizerRegistry,
                        #   engine: AnonymizerEngine
                          ) -> str:

    anonmyized_txt = anonmyization_process_text(text=text, registry=registry)#, engine=engine)
    new_text = text[:] # create new string
    for item in anonmyized_txt:
        """No need to hash user names from text because it will be done in different field."""
        original_text = text[item.start: item.end]
        if item.entity_type in FIELDS_TO_ANONYMIZE and original_text in new_text: # Currently, not handling overlaps
            new_text = new_text.replace(original_text, FIELDS_TO_ANONYMIZE[item.entity_type](original_text))
    return new_text
    
def anonymize_data(row: pyspark.sql.Row,
                   registry: RecognizerRegistry,
                #    engine: AnonymizerEngine
                   ) -> pd.DataFrame:
    def process_nested_tweet(tweet):
        tweet = tweet.asDict()
        temp_hash_dict = {}
        # Handle URLs
        if isinstance(tweet['entities'], Row):
            tweet['entities'] = tweet['entities'].asDict()
            if tweet.get('entities', {}).get('urls'):
                for url in tweet['entities']['urls']:
                    tweet['text'] = tweet['text'].replace(url['url'], url['expanded_url'])

        if isinstance(tweet['extended_entities'], Row):
            tweet['extended_entities'] = tweet['extended_entities'].asDict()
            if tweet.get('extended_entities', {}).get('urls'):
                for url in tweet['extended_entities']['urls']:
                    tweet['text'] = tweet['text'].replace(url['url'], url['expanded_url'])

        # Anonymize specified text fields
        for text_field in ANONMYIZE_TEXT_FIELDS:
            if not isinstance(tweet.get(text_field, ''), float):
                tweet[text_field] = anonymize_text_fields(tweet[text_field], registry)

        # Handle user information
        if 'user' in tweet:
            tweet['user'] = tweet['user'].asDict()
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
                mention = mention.asDict()
                _name = mention.pop('name')
                _screen_name = mention.pop('screen_name')
                _hash = get_hash(_name)
                value = (_name, _screen_name)
                # update_hash_dict(_hash, value=value)
                temp_hash_dict[_hash] = value
                mention['name'] = _hash
                mention['screen_name'] = _hash
                new_list.append(mention)
            tweet['entities']['user_mentions'] = new_list
                
        if 'extended_entities' in tweet and tweet['extended_entities'] and 'user_mentions' in tweet['extended_entities']:
            for mention in tweet['extended_entities']['user_mentions']:
                mention = mention.asDict()
                _name = mention.pop('name')
                _screen_name = mention.pop('screen_name')
                _hash = get_hash(_name)
                value = (_name, _screen_name)
                # update_hash_dict(_hash, value=value)
                temp_hash_dict[_hash] = value
                mention['name'] = _hash
                mention['screen_name'] = _hash
                new_list.append(mention)
            tweet['extended_entities']['user_mentions'] = new_list

        # Recursively process retweeted or quoted tweets
        if 'retweeted_status' in tweet and tweet['retweeted_status'] and len(tweet['retweeted_status']) != 0:
            tweet['retweeted_status'], temp_hash_dict = process_nested_tweet(tweet['retweeted_status'])
        if 'quoted_status' in tweet and tweet['quoted_status'] and len(tweet['quoted_status']) != 0:
            tweet['quoted_status'], temp_hash_dict = process_nested_tweet(tweet['quoted_status'])

        return tweet, temp_hash_dict

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

def process_partition(iterator):
    # Initialize heavy objects here
    registry = RecognizerRegistry()
    create_mention_as_entity(registry=registry)
    analyzer = AnalyzerEngine(registry=registry)
    counter = 0
    for row in iterator:
        counter += 1
        if counter % 100 == 0:
            print(f"Processed {counter} records in this partition.")
        # Process the tweet
        processed_tweet, temp_hash_dict = anonymize_data(row, analyzer)
        yield processed_tweet, temp_hash_dict
    
if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Process a path argument.")
    # parser.add_argument("path", type=str, help="The path to a file or directory.")
    # args = parser.parse_args()
    # if not args.path:
    #     raise FileNotFoundError('No data input was found')
    
    # path_to_file = args.path
    # path_to_file = "/Users/arielblobstein/Documents/phd/tweet_anonymzation/Data/07-04-2020.ndjson"
    path_to_file = "/Users/arielblobstein/Documents/phd/tweet_anonymzation/Data/data.jsonl"

    # df = spark.read.json(path)
    # data = read_ndjson(path_to_file, return_as_df=True)
    schema_path = 'Data/decahose_schema.json'
    
    data = read_ndjson_spark(path_to_file,schema_path, spark=spark)
    data = data.limit(100)
    start_time = time.time()
    
    rdd = data.rdd.mapPartitions(process_partition)
    
    output = rdd.collect()
    print(f"Time took: {time.time() - start_time}")
    json_list, global_hash_dict = [], {}
    for processed_tweet, temp_hash_dict in output:
        json_list.append(processed_tweet)
        global_hash_dict.update(temp_hash_dict)

    dump_hashed_dict(global_hash_dict)

    with open('output/anonymized_jsonl.jsonl', 'w') as f:
        for entry in json_list:
            f.write(json.dumps(entry) + '\n')

