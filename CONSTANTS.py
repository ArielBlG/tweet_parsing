from urllib.parse import urlparse


ANONMYIZE_TEXT_FIELDS = ['text']#,'extended_tweet']

FIELDS_LIST = ['created_at', 'id_str',' truncated', 'user', 'geo','coordinates', 'place'
       'contributors', 'is_quote_status', 'quote_count', 'reply_count', 'retweet_count',
       'favorite_count', 'entities', 'favorited', 'retweeted',
       'possibly_sensitive', 'filter_level', 'lang', 'timestamp_ms',  
       'display_text_range', 'extended_tweet', 'extended_entities',
       'withheld_in_countries']

REPLY_FIELDS = ['in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id',
                 'in_reply_to_user_id_str','in_reply_to_screen_name']
RETWEET_FIELDS = ['retweeted_status', 'quoted_status_id', 'quoted_status_id_str',
                  'quoted_status', 'quoted_status_permalink']


def parse_url(url_text: str) -> str:
    parsed_url = urlparse(url_text)
    text_to_return  = f"cleaned_url: {parsed_url.netloc} " \
                      f"{parsed_url.path} {parsed_url.params} {parsed_url.query} {parsed_url.fragment}" 
    return text_to_return


FIELDS_TO_ANONYMIZE = {
    'PHONE_NUMBER': lambda x: '<PHONE_NUMBER>',
    'CREDIT_CARD': lambda x: '<CREDIT_CARD>',
    'DATE_TIME': lambda x: '<DATE_TIME>',
    'EMAIL_ADDRESS': lambda x: '<EMAIL_ADDRESS>',
    'URL': lambda x: parse_url(x),
    'MENTION': lambda x: '<MENTION>',
    'LOCATION': lambda x: '<LOCATION>'
}

FIELDS_TO_HASH = {'MENTION'}
