from tamr_unify_client import Client
from tamr_unify_client.auth import UsernamePasswordAuth
from tamr_unify_client.dataset.collection import DatasetCollection
import os
import yaml
import pandas as pd

def remove_lists(record):
  # convert array of strings to strings, empty list to empty string, and do nothing to strings
  for key, val in record.items():
    if isinstance(val, list):
      if len(val) > 0:
        record[key] = val[0]
      else:
        record[key] = np.nan
  return record


creds = yaml.full_load_all("credentials.yaml")
# replace with your credentials.yaml path
auth = UsernamePasswordAuth('admin', 'dt')
host = '10.20.255.167' # replace with your Tamr host
tamr = Client(auth, host=host)
console_print=0

if console_print:
  for dataset in tamr.datasets:
    print(dataset.name)
    print("Dataset names printed successfully")

src_dt_name="customer_000005_20191119.csv"
enrich_dt_name="customer_000005_191119_enrich"

print("Pulling the datasets "+src_dt_name+" and "+enrich_dt_name)
datasets = DatasetCollection(tamr) #get dataset collections object
src_dataset = datasets.by_name(src_dt_name) #get the source dataset object by the name
enrich_dataset = datasets.by_name(enrich_dt_name) #get the enrich source dataset object by the name

#converting record field values in strings
src_records = [remove_lists(rec) for rec in src_dataset.records()]
enrich_records = [remove_lists(rec) for rec in enrich_dataset.records()]

#passing the records to a DataFrame
df_src=pd.DataFrame(src_records)
df_enrich=pd.DataFrame(enrich_records)

print("merging the enrich fields with source dataset")
#doing a left join between the source dataset and the enrich dataset
df_merged_left = pd.merge(left=df_src, right=df_enrich, how='left', left_on='name', right_on='original_company_name')

if console_print:
  print('Number of colums in Dataframe : ', len(df_merged_left.columns))
  print('Number of rows in Dataframe : ', len(df_merged_left.index))
  pd.set_option('display.max_rows', 10)
  pd.set_option('display.max_columns', None)
  pd.set_option('display.width', None)
  print(df_merged_left)

df_merged_left = df_merged_left.sort_values(by='id')
list(df_merged_left)

#organize the enrich dataset
df_merged_left = df_merged_left[
    ['id',
      'name',
      'url',
      'ingest_id',
      'trbc',
      'city',
      'state',
      'street_1',
      'street_2',
      'zipcode',
      'phone',
      'country_prescrub',
      'country',
      'external_status',
      'bing_domain_name_primary_1',
      'bing_domain_name_primary_2',
      'bing_domain_name_primary_3',
      'bing_domain_name_primary_4',
      'bing_domain_name_primary_5',
      'bing_domain_name_primary_6',
      'bing_domain_name_primary_7',
      'bing_domain_name_primary_8',
      'bing_domain_name_primary_9',
      'bing_domain_name_primary_10'
    ]]

if console_print:
  print(df_merged_left)
  df_merged_left.to_csv(src_dt_name.replace('.csv', '') + 'plus_enrichFields.csv', encoding ='utf-8', index= False)

print("Uploading source dataset with enrich bing fields to the Dataset Catalog")
dataset_result = tamr.datasets.create_from_dataframe(df_merged_left, "id", src_dt_name.replace('.csv', '')+'_enrichFields')

if console_print:
  for rec in dataset_result.records():
    print(rec)


