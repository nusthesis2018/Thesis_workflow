# -*- coding: utf-8 -*-
"""
MIT License

Copyright (c) 2019 nusthesis2018

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
@author: -
"""

#import modules 
import searchtweets, yaml, os, tweet_parser, ast, flatten_dict,emoji,re,googletrans
import pandas as pd
import arcpy
from aylienapiclient import textapi
from datetime import datetime 
from time import sleep
from faker import Faker


#define functions 

def pd_df(df):
    for i in range(len(df['{data}'])):
        if i == 0:
            dict1 = df['{data}'].get_value(i)
            df1dict = ast.literal_eval(dict1)
            dfldict1 = flatten_dict.flatten(df1dict, reducer = 'path')
            df1 = pd.DataFrame.from_dict(dfldict1, orient = 'index')
            df11 = df1.transpose()
            global df_result 
            df_result = df11.copy()
        else: 
            dict1 = df['{data}'].get_value(i)
            df1dict = ast.literal_eval(dict1)
            dfldict1 = flatten_dict.flatten(df1dict, reducer = 'path')
            df1 = pd.DataFrame.from_dict(dfldict1, orient = 'index')
            df11 = df1.transpose()
            df_result = df_result.append(df11)

def unify(subset1):
    for i in range(len(subset1)):
     
        if len(subset1['extended_tweet\\full_text'].get_value(i)) < len(subset1['text'].get_value(i)):
            subset1['unified_text'][i]= str(subset1['text'].get_value(i))
        else:
            subset1['unified_text'][i]= str(subset1['extended_tweet\\full_text'].get_value(i))



def SimulSub ( dict_ , string_ ):
    #Jeff Hykin (http://code.activestate.com/recipes/81330-single-pass-multiple-replace/)
  if len(dict_) == 0:
    return string_
  def repl_(regex_):
    match_ = regex_.group()
    for x in (sorted(dict_.keys(), key=lambda x: len(x))):
      # the below line could cause problems for lookahead / lookbehind / beginning / end regex 
      if (re.search( x.lower() , match_ ) != None): return dict_[ x ] 
    print( "error with SimulSub")
    print( "check lookahead/lookbehind/beginning/end regex")
    return match_
  return re.sub( "(" + "|".join(sorted(dict_.keys(), key=lambda x: len(x),reverse=True)) + ")"  , repl_ , string_ )

def replacedict(abbrtest1):
    for row in range(len(abbrtest1)):
            text = str(abbrtest1['unified_text'].get_value(row)).lower()
            abbrtest1['unified_text'][row]=  SimulSub( comb_dict,text  )



def sentiment(dfs):
    startTime = datetime.now()
    dfs['sentiment_polarity'] = '' 
    for row in range(len(dfs)):
        text = str(dfs['unified_text'].get_value(row))
        sentiment = client.Sentiment({'text': text})
        dfs['sentiment_polarity'][row] = sentiment
        if row%60 is 0:
            sleep(60)
    print(datetime.now() - startTime)    

def translate_unified_text2(df):
    for row in range(len(df)):
        translator = googletrans.Translator()
        text = str(df['unified_text'].get_value(row))
        text1 = emoji.demojize(text)
        trans = translator.translate(text1, dest='en')
        df['unified_text'][row]= trans.text






outfolder = arcpy.env.scratchFolder

data = dict(
        search_tweets_api = dict(
            account_type = 'premium',
            endpoint = "https://api.twitter.com/1.1/tweets/search/fullarchive/dev.json",
            consumer_key = "bHtfixh93nVQfirGwfqgfe6fA",
            consumer_secret = "1Cnp5sfkCkg25TaAC8tNxcy5GJVKdxquUwXNM9GJsAYpZhiI1w",
)
        ) 
                
with open(os.path.join(outfolder,'creddata.yml'), 'w') as outfile:
    yaml.dump(data, outfile, default_flow_style=False)        

premium_search_args = searchtweets.load_credentials(os.path.join(outfolder,'creddata.yml'),
                                       yaml_key="search_tweets_api",
                                       env_overwrite=False)        

rule = searchtweets.gen_rule_payload("(MRT OR train) (fail OR breakdown OR fault OR signal OR fix OR delay OR late OR problem OR stuck OR stopped OR disrupt OR collision OR collide OR accident OR bang OR smash) place_country: SG", from_date="2017-01-01",
                        to_date="2017-12-31 23:59",results_per_call=100) # testing with a sandbox account



rs = searchtweets.ResultStream(rule_payload=rule,
                  max_results=2000,
                  max_pages=2,
                  max_requests=20,
                  **premium_search_args)

#print(rs)

tweets = list(rs.stream())

#####
project_name = input('project name or date:') + str('.txt')
tweet_out = os.path.join(outfolder,project_name)
with open(tweet_out, 'w', encoding='utf-8') as outfile:
    for tweet in tweets:
        tweet1 = tweet_parser.tweet.Tweet(tweet)
        print(tweet1, file=outfile)        

#add header to data
src=open(tweet_out,"r",encoding='utf-8')
fline="{data} \n"  #Prepending string
oline=src.readlines()
oline.insert(0,fline)
src.close()
src=open(tweet_out,"w",encoding='utf-8')
src.writelines(oline)
src.close()        
        
#conversion to pandas dataframe 

data = tweet_out
try:
    df = pd.read_csv(eval(data), sep = '} {', encoding = 'utf-8')
except SyntaxError:
    df = pd.read_csv(data, sep = '} {', encoding = 'utf-8')
        

pd_df(df)

Dataframe1 = df_result 

Dataframe1 = Dataframe1.drop_duplicates('text')   

outpandas = os.path.join(outfolder, project_name.split('.')[0] + str('_pandas.txt'))

try:
    Dataframe1.to_csv(eval(outpandas))
except SyntaxError:
    Dataframe1.to_csv(outpandas)


#sentiment analysis function 
#check data before continuing 
client = textapi.Client("c631e421", "aadb8fe259efcbecb6a7e9f4cc5855b8")

dfs = pd.read_csv(outpandas)
dfs['unified_text']= ''
dfs=dfs.reset_index()
dfs['extended_tweet\\full_text'] = dfs['extended_tweet\\full_text'].astype(str)
dfs['text'] = dfs['text'].astype(str)
      
unify(dfs)

translate_unified_text2(dfs)


abbr_dict1= {"wtf": "what the fuck",
  "wth": "what the hell",
  "omg": 'oh my god', 
  'focking ell' : 'fucking hell' ,
   'fml': 'fuck my life',
   ' tf': 'the fuck',
   'fak': 'fuck',
   'fkn': 'fucking',
   'fck': 'fuck',
   'bruh': 'brother',
   'fking': 'fucking',
   'wts':'what the shit',
   'jeez':'jesus',
   'wtfffffff': 'what the fuck',
   'daayyuumm': 'damn',
   'stfu': 'shut the fuck up',
   'mygawwdd': 'my god', 
   'daayyuummm': 'damn ',
   "fk" : "fuck",
   "Fk" : "fuck",
   'fxck': 'fuck',
   'lmao': 'laughing my ass off',
   'lol': 'laughing out loud',
   'pls': 'please',
   'fkin': 'fucking',
   'lul': 'laughing out loud',
   '):' : 'sad face',
    'asf': 'as fuck'
}

vernac_dict= {"cb": "cunt",
  'ccb': 'rotten cunt',            
  "kena": "got",
  'knn': 'fuck your mother',
  'alamak': 'oh my god',
  'gile': 'mad',
  'babi': 'pig', 
  'merepek': 'gibberish',
  'wayang': 'fake',
  'nabei':'your father',
  'liao':'already',
  'walao':'oh my god',
  'kimak': "your mother's cunt",
  'kimek': "your mother's cunt",
  'kmk': "your mother's cunt",
  'aiya': 'oh dear',
  'aiyo': 'oh no',
  'bapak kau': 'your father',
  'bodoh': 'idiot',
  'sial': 'damn it ',
  'lanjiao': 'dick',
  'sian': 'bummer',
  'sienz': 'bummer',
  'swee':'great',
  'ya allah': 'oh god',
  'haihh': 'sigh',
  'cibai': 'cunt' 
  }

comb_dict= abbr_dict1.copy()
comb_dict.update(vernac_dict)

replacedict(dfs)
    
sentiment(dfs)
    
df1 = pd.concat([dfs.drop(['sentiment_polarity'], axis=1), dfs['sentiment_polarity'].apply(pd.Series)], axis=1)

outsentiment = os.path.join(outfolder, project_name.split('.')[0] + str('_sentiment.txt'))

try:
    df1.to_csv(eval(outsentiment))
except SyntaxError:
    df1.to_csv(outsentiment)


fake = Faker()
for row in range(len(df1)):
    df1['user\screen_name'][row] = fake.name()
    df1['user\\name'][row] =df1['user\screen_name'][row]

outdataset = os.path.join(outfolder, project_name.split('.')[0] + str('_dataset.txt'))

try:
    df1.to_csv(eval(outdataset))
except SyntaxError:
    df1.to_csv(outdataset)




        