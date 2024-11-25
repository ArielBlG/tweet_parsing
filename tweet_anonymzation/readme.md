# Tweet anonymizer

The package is responsible for anonymizing tweets.

The following rules are being applied

| **Key**           | **Mapping/Explanation**                                                                                                                                   |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `PHONE_NUMBER`    | Maps a phone number to the placeholder string `<PHONE_NUMBER>`.                                                                                           |
| `CREDIT_CARD`     | Maps a credit card number to the placeholder string `<CREDIT_CARD>`.                                                                                      |
| `DATE_TIME`       | Maps a date and time value to the placeholder string `<DATE_TIME>`.                                                                                       |
| `EMAIL_ADDRESS`   | Maps an email address to the placeholder string `<EMAIL_ADDRESS>`.                                                                                        |
| `URL`             | Maps a URL to a cleaned version of the URL by extracting its components using the `parse_url` function. The cleaned URL includes:                         |
|                    | - `netloc` (domain or network location)                                                                                                                  |
|                    | - `path` (specific path in the URL)                                                                                                                      |
|                    | - `params` (optional parameters in the path)                                                                                                             |
|                    | - `query` (query string of the URL)                                                                                                                      |
|                    | - `fragment` (fragment identifier in the URL)                                                                                                            |
| `MENTION`         | Maps a mention (e.g., username or tagged user) to the placeholder string `<MENTION>`.                                                                     |
| `LOCATION`        | Maps a location to the placeholder string `<LOCATION>`.                                                                                                   |
## In order to run:

### Configure python env

```python -m venv myenv ```

```source activate myenv ```


```pip install -r requirements.txt```

### configure data
Add ndjson file to ```Data\*ndjson```(Need to add multiple files handling)
You can add user hash dict to update as ```Data\user_hash_dict.json``` and the code will autoaticaly update it. 


### Run
```python annonymize_script.py \path\to\data

