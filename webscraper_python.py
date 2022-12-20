import time
import pandas as pd
import warnings
import requests
import json
import io

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', '{:.2f}'.format)

print('libraries imported')

#login if needed
def login(mail, password):
    url = 'https://prod-api.phpbamboo.com/v1/agents/login'
    s = requests.Session()
    payload = {
        "php_code": mail,
        "password": password
    }
    res = s.post(url,json=payload)
    s.headers.update({'authorization': json.loads(res.content)['token']})
    # print(res.content)
    return s

# session = login(mail='user'
#                 ,password='pass')

#print out session
# session

#flatten json columns when needed
def flatten_json(nested_json, exclude=['']):
    out = {}
 
    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude: flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
 
    flatten(nested_json)
    return out

#data/policy urls
data_url = 'https://prod-api.phpbamboo.com/v1/agents/new-business/md-base/policies?start=0&limit=20000&status=&carrier_ids=&search_term=&startDate=2000-01-01&endDate=2022-12-18&search_query=policy'
access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpYXQiOjE2NzEzODcwNDAwODEsImV4cCI6MTY3MjU5NjY0MDA4MSwidXNlciI6eyJpZCI6MjYyNTg1LCJjdXJyZW50X2xldmVsX2lkIjo1fX0.gBQxAmAi0F47TOf7J4Dxa-kavS4viNGnmzpGLY-oF_tMRO6MIRlm2gepNx6Jnpc-s3U8qsS5eIaVaVT7I5TuOg'

result = requests.get(data_url,
                      headers={'Content-Type':'application/json'
                               ,'Authorization': 'Bearer {}'.format(access_token)})

df = pd.DataFrame.from_dict(result.json())
print('main dataframe complete...')

#new df of json col
A = pd.DataFrame([flatten_json(x) for x in df['policies_json']])

#merge to original df
df_merge = pd.concat([df,A], axis=1).fillna(0)

#drop columns
df_merge.drop(['count','policies_json'],axis=1,inplace=True)
print('json columns parsed...')

#add in email, dob, policy date
primary_urls = []
secondary_urls = []
df_merge['id'] = df_merge['id'].astype(str)

#get primary and secondary info tabs' data
for i in df_merge['id']:
    primary_url = 'https://prod-api.phpbamboo.com/v1/new-business/policies/'+i+'/primary-info'
    primary_urls.append(primary_url)
    secondary_url = 'https://prod-api.phpbamboo.com/v1/new-business/policies/'+i+'/secondary-info'
    secondary_urls.append(secondary_url)

results = []
for i in primary_urls:
    result = requests.get(i
                          ,headers={'Content-Type':'application/json'
                                   ,'Authorization': 'Bearer {}'.format(access_token)}).json()
    results.append(result)
df1 = pd.DataFrame.from_dict(results)
print('primary tab data collected...')

results = []
for i in secondary_urls:
    result = requests.get(i
                          ,headers={'Content-Type':'application/json'
                                   ,'Authorization': 'Bearer {}'.format(access_token)}).json()
    results.append(result)
    results
df2 = pd.DataFrame.from_dict(results)
print('secondary tab data collected...')

#merge primary and secondary info data to df_info
df_info = df1.merge(df2,how='left',left_on='id',right_on='id')

#join primary and secondary data to maine df
df_info = df_info[['policy_number_x','client_x','created_at_x']]
df_merge = df_merge.merge(df_info,how='left',left_on='policy_number',right_on='policy_number_x')
df_client = pd.DataFrame([flatten_json(x) for x in df_info['client_x']])
df_client = pd.concat([df_info,df_client],axis=1)
df_client.drop(['client_x','created_at_x'],axis=1,inplace=True)
df_merge = df_merge.merge(df_client,how='left',left_on='policy_number',right_on='policy_number_x')
df_merge.drop(['client_x','policy_number_x_x','policy_number_x_y','created_at_x','id_y'],axis=1,inplace=True)
print('ugly columns dropped...')

#save data to your computer
print('df shape:', df_merge.shape)
df_merge.to_csv('julie_data.csv')
print('csv saved to your computer Julie!')