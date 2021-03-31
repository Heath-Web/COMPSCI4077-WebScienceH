





<h1 align = 'center'>Web Science H coursework Report</h1> 











































<h4 align = 'center'>Ze Wang - 2550208W</h4>

<h4 align = 'center'>31 March 2021</h4>









### Section 1: Introduction

##### 1.1 Overview

The objective of this course work is to develop a Twitter crawler for data collection in English and to conduct social media analytics. File structure as follows:

`requirements.txt` - program dependencies

`Credentails.py` - stores the credentials from twitter Developer account

`StreamingCrawler.py` - sets up and runs streaming API for 5 minutes

`DataGrouping.py` -  groups the tweets collected in `StreamingCrawler.py`

`HybridCrawler.py` - runs hybrid architecture crawler for 30 minutes

`Imagex.jpg` - 5 images downloaded in `StreamingCrawler.py`

- The Github repository with the source code : https://github.com/Heath-Web/COMPSCI4077-WebScienceH
- Instructions on how to run the program are specified in the **README.md**

##### 1.2 Implementation

All programs including, Streaming crawler, data grouping and hybrid architecture crawler has been implemented by using Python programming language version `anaconda python 3.7`. The data storage is supplied by  MongoDB version `MongoDB 4.4.4 Community`.

Furthermore, several additional python Library  are used:

​	`StandfordCoreNLP`  - used to generate text vector

​	`emoji` - used to remove emoji from tweet text

##### 1.3  Data Collected 

Data was stored for both Streaming API and REST API in a database called "TwitterDB" of MongoDB and two collections called "Streaming_1" and "Hybrid_3". "Streaming_1" collection was used to store data collected by `StreamingCrawler.py`  through Streaming API only and "Hybrid_3" collection is for data collected by `HybirdCrawler.py` which is  a hybrid architecture twitter crawler.

For the streaming API of part 1, I ran it on March 29th from 08:51 to 08:56 UK Time and collected 7839 tweets. These data was stored in a collection "Streaming_1" in "TwitterDB" database. The grouped tweets of the streaming API in part 1 was saved in "GroupedTwitter_2". Moving onto hybrid architecture twitter crawler of part 3, 29173 tweets was collected from 06:59 to 7:29 UK Time on March 31th. The data collected by  `HybirdCrawler.py` was stored in "Hybrid_3" collection. Of those 9820 tweets, 19353 was from Streaming API and 2000 was from REST API. The grouped tweets of the streaming crawler in part 3 was saved in "GroupedTwitter_3".

### Section 2: Data crawl

##### 2.1  Streaming API 

Twitter Streaming API was used here for collection 1% data lasting for 5 minutes.

###### Filtering

All tweets were filtered with English,  the coordinates of  UK and Ireland, and the track words which are COVID relevant. The following code shows the tracking words, location and the  filtering function.

```python
# UK and Ireland
Loc_UK = [-10.392627, 49.681847, 1.055039, 61.122019]
Words_UK = ["COVID-19", "COVID", "Corona", "Virus", "Disease", "Case", "Quarantine", "Isolation", "Infection", "NHS", "Positive", "Pandemic", "Restrictions", "Lockdown", "Hospital", "Vaccine", "Infection Rate", "Variants"]

streamer.filter(locations= Loc_UK, track = Words_UK, languages = ['en'], is_async=True) #locations = Loc_UK, track = Words_UK
```

###### Text cleaning and fields extraction

The collection of tweet was done in the `on_data(self, data)` function of an  `StreamListener` class instance provided by tweepy Library . Before storing the tweets into Mongo database,  I extracted relevant twitter fields including tweet_id , created date, text, user, coordinates, place object hashtags, mentions and multimedia etc., and remove the emoji form tweet text by using the emoji Library. The process of counting, cleaning text and extracting fields was completed in the function `processTweets(tweet)`.

```python
# remove emoji form tweet text
new_text = re.sub(emoji.get_emoji_regexp(), r"", text)
```

###### Download multimedia

When processing tweet , the program will record the first five images and videos URL appeared in tweets and download them at the end.  By using urllib Library, I acquired the response of the URL and write it in the current folder. The  file name is constituted  by"Image" or "Video" ,  number (0 to 4) and  the format of multimedia which obtained through the regular expression.

```python
# Dowload images or videos through url
def download(url,Num,type): # url: the url of images or videos; Num : mark number of image or vedio; type : 'image' or 'video'
    try:
        request = urllib.request.Request(url) # format request of the url
        response = urllib.request.urlopen(request) # acquire the response
        result = response.read()
        if type == 'image':
            with open('.\\Image' + str(Num) + str(re.search(r'\.(\w*)$',url,re.I | re.M).group()),'wb') as fp: # creat file and match the format of image
                fp.write(result) # store the image in  local
            print('Download image:','Figure', str(Num), "   url:" , url)
        if type == 'video':
            with open('.\\Video' + str(Num) + str(re.search(r'\.(\w*)$',url,re.I | re.M).group()) ,'wb') as fp:# creat file and match the format of video
                fp.write(result) # store the video in  local
            print('Download image:','Figure', str(Num), "   url:" , url)
    except Exception as e:
        print("Some error occurred when download images and videos :", e)
```

###### Streaming API Crawler `StreamCrawler.py`

This program will automatically disconnect Streaming API 5 minutes after it started. The main thread will sleep for 5 minutes whereas the filtering, text cleaning and twitter fields extracting process is in the sub-thread. It also counted the collected tweet and outputted those number  in the terminal which are shown in the following tabular form.

| Total | Streaming API | No of re-tweets | No of quotes | No of Images | No of Videos | How many verified? | No of geo-tagged data | How many with locations/place Object |
| ----- | ------------- | --------------- | ------------ | ------------ | ------------ | ------------------ | --------------------- | ------------------------------------ |
| 7839  | 7839          | 5105            | 1454         | 360          | 0            | 148                | 38                    | 385                                  |

##### 2.2 Data Grouping

In this stage,  7229 tweets were grouped in 4480 clusters by Single Pass Clustering method according to Cosine Similarity, and the other 610 tweets was considered as noisy tweets based on Quality Score of the tweet user. 7839 tweets in total are all came form the previous part, collected by Streaming API crawler.

###### Quality Score

Quality score is the average of  five different weights which are User Verified Weight, User Profile image Weight, User Followers Weight, Account Age Weight and User Description Weight. If the user account is verified, did not use default profile image, has more followers,  was active for longer periods of time and has some News & Journalism relevant terms in user description,  then the higher quality score will be. When matching terms in user description ,  I implemented root forms by using the regular expression. The following codes shows how I calculate description weight.  The process of computing quality score was completed in the function `GetQualityScore(tweet)`.

```python
    # List of useful terms
    listTerms = ['news', 'report', 'journal', 'write', 'editor','media', 
                 'official', 'NHS', 'health', 'care', 'COVID', 'hospital']
    # List of Spam terms
    listSpam = ['ebay', 'review', 'shopping', 'deal','sale', 'sales','link', 
                'click', 'marketing','promote','discount', 'products', 'store',
                'diet', 'weight', 'porn', 'followback', 'follow back','lucky', 
                'winners', 'prize', 'hiring']
    if tweet['user']['description'] == None:
        descriptionweight = 0.1 # Null description
    else:
        for term in listTerms:  # match word in ListTerms
            match_res = re.search(r'\s?' + term + r'(\w*)\s?', tweet['user']['description'],re.I | re.M)  # use root forms
            if match_res != None:  # if match
                descriptionweight += 1
                match_counter += 1
        for term in listSpam:  # match word in ListSpam
            match_res = re.search(r'(\s?)' + term + r'(\w*)(\.*)\s?', tweet['user']['description'],re.I | re.M)  # use root forms
            if match_res != None:  # if match
                descriptionweight += 0.1
                match_counter += 1
        if match_counter == 0: # do not match any terms but user still has non-null description
            descriptionweight = 0.4
        else:
            descriptionweight = descriptionweight / match_counter
```

If the quality score of the tweet below 0.5, which means the user is more like a spammer or marketer etc.,  and  this tweet will be consider as noisy tweet with low-quality text and will not be grouped in later stages.

###### Text Vector

Text Vector generation is realized by using Stanford CoreNLP Library. Firstly, I derive the  Part-of-speech Tagging for tweet text and only remain URL, Adjective, Adverb, Verb and Noun words in vector so as to remove noise terms and stop words in tweet text. Otherwise, it will be an error posted by stanfordcoreNLP if text contains symbol %. And Mongodb does not allow any keys contain a '.' and keys start with '$'. So I replace them with other symbol like a space or a '~' in this step.  The process of Text vector generation was completed in the function `Generate_vector(tweeet)`

###### Similarity

The measure used to computing similarity between tweet and cluster is Cosine Similarity Measure. Following the formula below, A represent text vector and B is the cluster vector.

![](C:\Users\Administrator\Desktop\屏幕截图 2021-03-31 092247.png)

Here is the main part of function `Calculate_SIM(representation, text_vector)` and demonstrate the process of Cosine Similarity Calculation.

```python
        sum1 = 0 # numerator, product of cluster vector and text vector
        sum2 = 0 # square of cluster vector length
        sum3 = 0 # square of text vector length
        if len(text_vector) == 0:# deal with zero
            sim = 0
        else:
            for key in representation.keys():
                sum2 += (representation[key] * representation[key])
                if key in text_vector:
                    # assume all terms in text appear once
                    sum1 += (representation[key] * (1 / len(text_vector)))  # Compute numerator
            sum3 = 1 / len(text_vector)
            sim = sum1 / ((sum2 ** 0.5) * (sum3 ** 0.5))  # Cosine similarity
```

###### Single pass clustering

In terms of clustering algorithm, i use Single pass clustering. Briefly, it perform a single and sequential  pass over tweets. And for each tweet, the algorithm decides whether it should be added in an already defined cluster or a new cluster. The main flow process of Single Pass clustering is shown below where the Quality score threshold and Cosine Similarity Threshold are 0.5. The tweet text vector is empty means all terms in the text is stop words, which will also be consider as a noise tweet. The process of Single pass clustering was completed in the function `Single_Pass_Clustering(tweet)`

```python
for each tweet t in the sequence loop
	if GetQualityScore(tweet) < 0.5 then moving onto next tweet
	else if Generate_vector(tweet) is empety then moving onto next tweet
	else find a cluster c maximises Calculate_SIM(c,t vector)
		 if Calculate_SIM(c, t vector) > 0.5, add tweet into c
		 else create a new cluster whose only doucument is t
```

When clustering, I found that if Cosine Similarity Threshold was set under 0.5, then Two unrelated tweets would also be grouped in one cluster. However, The higher threshold value means there will be more cluster created. When threshold equals to 0.5, the average group size will be 1.6 roughly. So One main weakness of Single Pass clustering algorithm is that the time complexity is approach to `O(n^2)`.   With the amount of cluster growing, the time of finding maximal similarity for each tweet will also increase. From several data grouping test previously, when the amount of cluster exceeds 3000,  the speed of group a tweet will be significantly slower. One way to improve this problem is that once the similarity over 0.9,which means tweets are the same on a great probability, there is no need to compute similarity with other cluster anymore.

The structure of group representation is shown below. For quick access, this will be kept in the memory as a python dictionary first and be stored into "group_representation" collection in "GroupedTwitter_2" database at the end when grouping completed.

```json
{'_id': 1,  # cluster id   
 'cluster_id' : 1 , # cluster id   
 'count' : 10 , # amount of Tweets
 'representation' : # groupe representation / cluster vector
 	{'term' : weight,...}  # terms and their weight
}
```

###### DataGrouping `DataGrouping.py`

This program counted the clusters and outputted those number in the terminal which are shown in the following tabular form.

| Total | Groups formed | Min size | Max size | Avg size | Noisy tweet |
| ----- | ------------- | -------- | -------- | -------- | ----------- |
| 7839  | 4481          | 1        | 154      | 1.61     | 610         |

##### 2.3 Hybrid architecture of Twitter Streaming & REST APIs

In this stage,  29173 effective tweets were collected, 9820 from Streaming API and 19353 from REST API.

###### Prioritize the groups

The main idea of prioritizing the groups is to look how fast the cluster is growing instead of the cluster size. Once a tweet is collected in the Stream Listener,  the program will perform a single pass clustering to this tweet. The cluster number which the tweet is add in will be store in a 1000 length queue. This queue represents which cluster the last 1000 tweets were added in. In other words, by counting the cluster number in the queue and sort them, we can know which cluster raises the fastest over a period of time. And the counting and sort process is realized by  `Counter` Library of Python.

```python
ClusterNum_Queue = [] # a queue with latest 1000 Cluster number where tweet (form Streaming API) was added
start_index = 0 # The start index (pointer) of the cluster number queue
MAXLENGTH = 1000 # MAXLENGTH　1000 of the queue

aim_coll = DataGrouping.Single_Pass_Clustering(tweet)  # grouping
if aim_coll != -1: # if aim_coll is -1 means this tweet is a noisy tweet
   # add cluster id into the queue
   if len(ClusterNum_Queue) < MAXLENGTH:
        ClusterNum_Queue.append(aim_coll)
   elif len(ClusterNum_Queue) == MAXLENGTH:
        ClusterNum_Queue[start_index] = aim_coll
        # move pointer
        if start_index < (MAXLENGTH - 1):
             start_index += 1
        else:
             start_index = 0

TopGrowingCluster = Counter(ClusterNum_Queue).most_common(50) # Top 50 fastest growing cluster
```

###### Query Generation

After prioritizing the groups, i move onto the query generation for each cluster. The priority of things is hashtags, user mentions and terms in cluster vector.  Find all hashtags in one cluster, if zero then find all mentions, and  same to the terms. The query of REST API Search would be one of them not all or both of them.  If one tweet in the cluster is geo-tagged, then the searching of tweets will base on 10km radius of the tweet coordinate. The process of Query generation was completed in the function `GetQueries(ClusterNum)`

```python
 for tweet in  collection.find(): # find all tweet in the collection
        if tweet['coordinates'] != None: # if tweet is geo-tagged, generate geo term
            geoTerm = str(tweet['coordinates'][0]) + str(tweet['coordinates'][1]) + '10km'
        for hashtag in tweet['hashtags']: # find all hashtags of each tweet
            hashtag = '#' + hashtag
            if hashtag not in hashtags: # avoid repetition
                hashtags.append(hashtag)
        for mention in tweet['mentions']:# find all user mentions of each tweet
            mention = '@' + mention
            if mention not in mentions: # avoid repetition
                mentions.append(mention)

    # priority hashtags > user mentions > text terms
    # query ca onnly be one of them
    if hashtags != []:
        query = ' OR '.join(hashtags)
    elif mentions != []:
        query = ' OR '.join(mentions)
    else:
        query = ' '.join(DataGrouping.group_rep_list[ClusterNum]['representation'])
    return query, geoTerm
```

###### Hybrid Architecture Crawler`HybridCrawler.py`

This program will automatically disconnect Streaming API and terminate REST crawler 30 minutes after it started by set a timer in sub thread ( `Class Timer(threading.Thread)`).

 It also counted the collected tweet for both Streaming and REST APIs ,and outputted those number in the terminal which are shown in the following tabular form.

| Total | Streaming API | REST API data | Redundant | No of Quotes | No of Re-tweets | No of geo-tagged data | No of Images | No of videos |
| ----- | ------------- | ------------- | --------- | ------------ | --------------- | --------------------- | ------------ | ------------ |
| 35184 | 9820          | 19353         | 6011      | 3253         | 22213           | 30                    | 2284         | 0            |

Output:
![](C:\Users\Administrator\Desktop\屏幕截图 2021-03-31 073422.png)

###### Effectiveness  and Scheduler/ranker

At first the strategy of scheduler is that executing 180 queries and then sleep 15 minutes. However, when crawling, the top of fastest growing cluster did not change too much which meas the REST crawler search the same things from the same clusters. And the tweets returned were also almost the same (about 30000 tweets in total and 20000 redundant tweets). 

One of the solutions is that allocate the 15 minutes rest time into each query which means the crawler will sleep (15*60)/180 = 5 seconds after each query. This will leave enough time for Streaming API crawler collecting data and the top of fastest growing cluster will be more changeable.

Meanwhile, I extended the period of REST crawler. In previous,  getting the top 5 fastest growing cluster and querying  5 times for each cluster were considered as one period. And now in one period 50 queries to the top 50 fastest growing cluster are performed. The aim of this strategy is to cover more clusters.

```python
if (counter % 50) == 0: # Every fifty queries
    TopGrowingCluster = Counter(ClusterNum_Queue).most_common(50) # Top 50 fastest growing cluster
ClusterNum = list(list(zip(*TopGrowingCluster))[0])[int(counter % 50)]
query, geoterm = GetQueries(ClusterNum) # get query form cluster (hashtags, mentions or terms)

# query
if geoterm != '':
    results = api.search(q=query, geocode=geoterm, count=80, lang="en", tweet_mode='extended')
else:
    results = api.search(q=query, count=100, lang="en", tweet_mode='extended')
# process results and insert into database
```















































### Appendix

##### Crendentails.py

```python
#credentials below form Twitter Developer Account
consumer_key = "fJL8wGt3jDgOzKA1RHsBCkqAx"
consumer_secret ="BR78KxnWCxhU53C5zP7y71ckgwnvPAHBiioK8IfxlP21DmIPVp"
access_token ="1362986785907896328-x5u7ac9eoxBy7f76wRnGRpcagGGKdF"
access_token_secret ="nnyFavROip70vVghKpMKWyr7kZirXUkf72gQw9EPGtr1Z"
```

##### StreamCrawler.py

```python
import tweepy
import json
from pymongo import MongoClient
import time
import emoji
import re
import Credentails
import urllib
import urllib.request

# count
count_total_processed = 0
count_RT = 0  # Number of retweets
count_quotes = 0  # Number of quotes
count_Images = 0  # Number of Images
count_Videos = 0  # Number of Videos
count_verified = 0  # How many verified
count_geotagged = 0  # Number of geo-tagged data
count_place = 0  # How many with locations/place Object

# image url and video url
image_url = [] # image url list
video_url = [] # video url list
MAXLENGTH = 5 # MAX length of url list, down load first five images and videos

# set DB DETAILS
# this is to setup local Mongodb
client = MongoClient('127.0.0.1', 27017)  # is assigned local port
db = client["TwitterDB"]  # set-up a MongoDatabase
collection = db['Streaming_1'] #  the Collection for Streaming API of part 1

# location and tracking words
Loc_UK = [-10.392627, 49.681847, 1.055039, 61.122019]  # UK and Ireland
Words_UK = ["COVID-19", "COVID", "Corona", "Virus", "Disease", "Case", "Quarantine", "Isolation", "Infection",
            "NHS", "Positive", "Pandemic", "Restrictions", "Lockdown", "Hospital", "Vaccine", "Infection Rate", "Variants"]

# remove emoji it works
def cleanList(text):
    new_text = re.sub(emoji.get_emoji_regexp(), r"", text)
    new_text.encode("ascii", errors="ignore").decode()
    return new_text

# this module is for cleaning text and also extracting relevant twitter fields
def processTweets(tweet):
    # Pull important data from the tweet to store in the database.
    try:
        created = tweet['created_at']
        tweet_id = tweet['id_str']  # The Tweet ID from Twitter in string format
        user = {'username': tweet['user']['screen_name'], # The username of the Tweet author
                'description': tweet['user']['description'], # The user description of he Tweet author
                'followers': tweet['user']['followers_count'], # The number of followers the Tweet author has
                'verified': tweet['user']['verified'], # Verified status of the Tweet author
                'created_at': tweet['user']['created_at'], # The UTC datetime that the Tweet author account was created
                'default_profile_image': tweet['user']['default_profile_image'] # When true, A default image is used
                }
        text = tweet['text']  # The entire body of the Tweet
    except Exception as e:
        # if this happens, there is something wrong with JSON, so ignore this tweet
        # print("There is something wrong with JSON. Ignore this tweet")
        return None

    #Text
    try:
        #  deal with truncated
        if(tweet['truncated'] == True):
            text = tweet['extended_tweet']['full_text']
        elif(text.startswith('RT') == True): # deal with retweet
            try:
                if( tweet['retweeted_status']['truncated'] == True):
                    text = tweet['retweeted_status']['extended_tweet']['full_text']
                else:
                    text = tweet['retweeted_status']['full_text']
            except Exception as e:
                pass
    except Exception as e:
        print("There is something wrong with Text. Ignore this tweet")
        return None
    # remove emoji form tweet text
    text = cleanList(text)

    # entities
    entities = tweet['entities']

    # mentions
    mentions =entities['user_mentions']
    mList = []
    for x in mentions:
        mList.append(x['screen_name'])

    # Any hashtags used in the Tweet
    hashtags = entities['hashtags']
    hList =[]
    for x in hashtags:
        hList.append(x['text'])

    # source
    source = tweet['source']

    # coordinates
    exactcoord = tweet['coordinates']
    coordinates = None
    if(exactcoord):
        coordinates = exactcoord['coordinates']

    # location
    location = tweet['user']['location']

    # Geoenable and place
    place = None
    place_name = None
    place_country= None
    place_countrycode = None
    place_coordinates = None
    geoenabled = tweet['user']['geo_enabled']
    if ((geoenabled) and (text.startswith('RT') == False)):
        try:
            if(tweet['place']):
                place = tweet['place']
                place_name = tweet['place']['full_name']
                place_country = tweet['place']['country']
                place_countrycode = tweet['place']['country_code']
                place_coordinates = tweet['place']['bounding_box']['coordinates']
        except Exception as e:
            print(e)
            print('error from place details - maybe AttributeError: ... NoneType ... object has no attribute ..full_name ...')

    #media
    global image_url, video_url,MAXLENGTH
    media = []
    try:
        for med in entities['media']:
            media.append({'media_url':med['media_url'],'type':med['type']})
            if med['type'] == 'photo' and len(image_url) < MAXLENGTH: # add image url to url list
                image_url.append(med['media_url'])
            if med['type'] == 'video' and len(video_url) < MAXLENGTH: # add video url to url list
                video_url.append(med['media_url'])
    except Exception as e:
        pass

    # count
    global count_total_processed, count_RT, count_quotes, count_Images, count_Videos, count_verified, count_geotagged, count_place
    count_total_processed += 1  # count the amount of data collected
    if (tweet['text'].startswith('RT') == True):
        count_RT += 1  # count re-tweets and qouts
    if (tweet['is_quote_status']):
        count_quotes += 1  # count quots
    if (tweet['user']['verified']):
        count_verified += 1  # count verified
    if (tweet['geo'] != None):
        count_geotagged += 1  # count geo-tagged
    try:
        if (tweet['place']):
            count_place += 1  # count place object
    except Exception:
        pass
    try:  # count images and videos
        for m in entities['media']:
            if m['type'] == 'photo' or m['type'] == 'animated_gif':
                count_Images += 1
            if m['type'] == 'video':
                count_Videos += 1
    except Exception:
        pass

    tweet1 = {'_id' : tweet_id, 'date': created,
              'user': user,  'text' : text,
              'geoenabled' : geoenabled,  'coordinates' : coordinates,
              'location' : location,  'place_name' : place_name,
              'place_country' : place_country, 'country_code': place_countrycode,
              'place_coordinates' : place_coordinates, 'hashtags' : hList,
              'mentions' : mList, 'source' : source, 'media' : media}
    return tweet1

# This is a class provided by tweepy to access the Twitter Streaming API.
class StreamListener(tweepy.StreamListener):
    global geoEnabled
    global geoDisabled

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")
        print("Start crawling for 5 minutes.....")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False

    def on_data(self, data):
        # This is where each tweet is collected
        # Load the  json data
        t = json.loads(data)
        #  Process the tweet so that we will deal with cleaned and extracted JSON
        tweet = processTweets(t)

        # now insert it
        try:
            collection.insert_one(tweet)
        except Exception as e:
            if (tweet == None):
                pass  # Pass if there is something wrong with JSON. Ignore this tweet
            else:
                print(e)

# Dowload images or videos through url
def download(url,Num,type): # url: the url of images or videos; Num : mark number of image or vedio; type : 'image' or 'video'
    try:
        request = urllib.request.Request(url) # format request of the url
        response = urllib.request.urlopen(request) # acquire the response
        result = response.read()
        if type == 'image':
            with open('.\\Image' + str(Num) + str(re.search(r'\.(\w*)$',url,re.I | re.M).group()),'wb') \
                    as fp: # creat file and match the format of image
                fp.write(result) # store the image in  local
            print('Download image:','Figure', str(Num), "   url:" , url)
        if type == 'video':
            with open('.\\Video' + str(Num) + str(re.search(r'\.(\w*)$',url,re.I | re.M).group()) ,'wb') \
                    as fp:# creat file and match the format of video
                fp.write(result) # store the video in  local
            print('Download image:','Figure', str(Num), "   url:" , url)
    except Exception as e:
        print("Some error occurred when download images and videos :", e)


if __name__ == '__main__':
    # Set twitter API
    auth = tweepy.OAuthHandler(Credentails.consumer_key, Credentails.consumer_secret)
    auth.set_access_token(Credentails.access_token, Credentails.access_token_secret)
    api = tweepy.API(auth)
    if (not api):
        print('Can\'t authenticate')
        print('failed cosumeer id ----------: ', Credentails.consumer_key)

    print("Tracking: " + str(Words_UK))

    # Streaming API
    # Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
    listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))
    streamer = tweepy.Stream(auth=auth, listener=listener)
    streamer.filter(locations=Loc_UK, track=Words_UK, languages=['en'],
                    is_async=True)  # locations= Loc_UK, track = Words_UK,

    # disconnect the streamer after 5 minutes and print the count
    time.sleep(60 * 5)
    streamer.disconnect()

    # download the first five images and videos
    for image in image_url:
        download(image, image_url.index(image), 'image')
    for video in video_url:
        download(video, video_url.index(video), 'video')


    # outputs
    print(" Total: ", str(count_total_processed), "\n Retweets:", str(count_RT), "\n Quotes:", str(count_quotes),
          "\n Images: ", str(count_Images), "\n Videos: ", str(count_Videos), "\n Verified: ", str(count_verified),
          "\n Geo-tagged: ", str(count_geotagged), "\n Locations/Place: ", str(count_place))
```

##### DataGrouping.py

```python
from stanfordcorenlp import StanfordCoreNLP
from datetime import datetime
from pymongo import MongoClient
import sys
import re

"""
The group representation structure in MongoDB:
{'_id': cluster id , 'cluster_id' : cluster id , 'count' : amount of Tweets , 'representation' : {'word' : weight,...} }
Type: Dictionary
"""
# Set DB DETAILS
# This is to setup local Mongodb
client = MongoClient('127.0.0.1', 27017)  # is assigned local port
before_group_db = client['TwitterDB']  # Source tweets database
before_group_coll = before_group_db['Streaming_1']  # Source tweets collection
after_group_db = client['GroupedTwitter_2']  # db of tweets after grouping
group_representation_coll = after_group_db['group_representaion']  # collection used to store group representation

# Load stanford model for NER
stanford_model = StanfordCoreNLP('E:\software\StanfordNER\stanford-corenlp-latest\stanford-corenlp-4.2.0')

# Initialize the global variable
clusterCounter = 0  # Number of cluster
noisyTextCounter = 0  # Number of noisy text
group_rep_list = []  # list of all group representations

# Compute quality score
def GetQualityScore(tweet):
    # Compute Verified Weight
    verifiedWeight = 0
    if(tweet['user']['verified']):
        verifiedWeight = 1
    else:
        verifiedWeight = 0.3

    # Compute Profile Weight
    profileWeight = 0
    if(tweet['user']['default_profile_image']):
        profileWeight = 0.2
    else:
        profileWeight = 1

    # Compute Followers Weight
    followersWeight = 0
    if tweet['user']['followers'] <= 50:
        followersWeight = 0.5 / 3
    elif tweet['user']['followers'] <= 5000:
        followersWeight = 1 / 3
    elif tweet['user']['followers'] <= 10000:
        followersWeight = 1.5 / 3
    elif tweet['user']['followers'] <= 100000:
        followersWeight = 2 / 3
    elif tweet['user']['followers'] <= 200000:
        followersWeight = 2.5 / 3
    elif tweet['user']['followers'] > 200000:
        followersWeight = 3 / 3

    # Compute account age weight
    # Compute number of days since account is created
    ageweight = 0
    daysScince = (datetime.now() - datetime.strptime(tweet['user']['created_at'], '%a %b %d %H:%M:%S +0000 %Y')).days
    if daysScince < 30:
        ageweight = 0.5 / 2
    elif daysScince < 120:
        ageweight = 1 / 2
    elif daysScince < 365:
        ageweight = 1.5 / 2
    elif daysScince >= 365:
        ageweight = 2 / 2

    # Compute Description weight
    descriptionweight = 0
    match_counter = 0
    # List of useful terms
    listTerms = ['news', 'report', 'journal', 'write', 'editor','media', 'official',
                 'NHS', 'health', 'care', 'COVID', 'hospital']
    # List of Spam terms
    listSpam = ['ebay', 'review', 'shopping', 'deal','sale', 'sales','link', 'click', 'marketing', 'promote',
                'discount', 'products', 'store', 'diet', 'weight', 'porn', 'followback', 'follow back',
                'lucky', 'winners', 'prize', 'hiring']
    if tweet['user']['description'] == None:
        descriptionweight = 0.1 # Null description
    else:
        for term in listTerms:  # match word in ListTerms
            match_res = re.search(r'\s?' + term + r'(\w*)\s?', tweet['user']['description'],
                                  re.I | re.M)  # use root forms
            if match_res != None:  # if match
                descriptionweight += 1
                match_counter += 1
        for term in listSpam:  # match word in ListSpam
            match_res = re.search(r'(\s?)' + term + r'(\w*)(\.*)\s?', tweet['user']['description'],
                                  re.I | re.M)  # use root forms
            if match_res != None:  # if match
                descriptionweight += 0.1
                match_counter += 1
        if match_counter == 0: # no match any terms but user still has description
            descriptionweight = 0.4
        else:
            descriptionweight = descriptionweight / match_counter

    # Compute Quality Score
    QualityScore = (descriptionweight + followersWeight + verifiedWeight + ageweight + profileWeight)/5

    return QualityScore

# Using stanfordcoreNLP generate vector
def Generate_vector(tweet):
    vec = []
    try:
        res = stanford_model.pos_tag(tweet['text']) # Part-of-speech Tagging
    except Exception as e:
        # it will be an error posted by stanfordcoreNLP if text contains symbol %
        tweet['text'] = str(tweet['text']).replace('%',' ') # replace % by a Space
        res = stanford_model.pos_tag(tweet['text'])

    if tweet['text'].startswith('RT'): # deal with RT
        res = res[2:] # remove RT and screen name
    for i in res:
        # remain URL, Adjective, Adverb, Verb and Noun words in vector
        if i[1] in ['ADD','FW','JJ','JJR','JJS','NN','NNS','NNP','NNPS','RR','RBR','RBS','RP','SYM','VB','VBD','VBG','VBN','VBP','VBZ']:
            if i[0] not in vec: # remove repetitive terms
                buffer = i[0]
                # Mongo db does not allow any keys contain a '.' and keys start with '$'
                buffer = str(buffer).replace('.','~') # replace '.' by '~'
                if buffer.startswith('$'):
                    buffer = str(buffer).replace('$', '~') # replace '$' by '~'
                vec.append(buffer) # add words to vector
    return vec

# Compute the similarity between one tweet text and a cluster by Cosine Similarity Measure
def Calculate_SIM(representation, text_vector):
    sim = -1 # if sim = -1, it means similarity computation  failed
    try:
        sum1 = 0 # numerator, product of cluster vector and text vector
        sum2 = 0 # square of cluster vector length
        sum3 = 0 # square of text vector length
        if len(text_vector) == 0:# deal with zero
            sim = 0
        else:
            for key in representation.keys():
                sum2 += (representation[key] * representation[key])
                if key in text_vector:
                    # assume all terms in text appear once
                    sum1 += (representation[key] * (1 / len(text_vector)))  # Compute numerator
            sum3 = 1 / len(text_vector)
            sim = sum1 / ((sum2 ** 0.5) * (sum3 ** 0.5))  # Cosine similarity
    except Exception as e:
        print("Some error occurred when compute similarity :", e)
    return sim

# Create a new cluster in Mongodb for a tweet and add group representation
def CreateNewCluster(cluster_id,tweet,text_vector):
    # Create a new collection in MongoDB
    global after_group_db
    collName = 'cluster' + str(cluster_id)
    cluster_coll = after_group_db[collName]

    # Generate and compute group representation
    global group_rep_list
    group_representation = {'_id': cluster_id , 'cluster_id' : cluster_id , 'count' : 1 , 'representation' : {} }
    for i in text_vector:
        group_representation['representation'][i] = (1/len(text_vector)) ** 0.5 # Normalisation
    group_rep_list.append(group_representation)

    # Insert tweet into mongodb
    try:
        cluster_coll.insert_one(tweet)
    except Exception as e:
        print("Some error occurred when creating a new cluster :", e)

# Insert one tweet to a cluster and update group representation
def AddTweetToCluster(cluster_id, tweet, text_vector):
    # set collection
    collName = 'cluster' + str(cluster_id)
    cluster_coll = after_group_db[collName]
    # Insert tweet into mongodb
    try:
        cluster_coll.insert_one(tweet) # insert tweet to the cluster
    except Exception as e:
        print("Some error occurred when insert tweet into a cluster :", e)

    # update group representation (cluster vector)
    global group_rep_list
    vec_len = 0 # vector length
    try:
        group_representation = group_rep_list[cluster_id]
        # Sum two vector
        rep_keys = group_representation['representation'].keys()
        for word in text_vector:
            if word in rep_keys:
                group_representation['representation'][word] += (1/len(text_vector))
            else:
                group_representation['representation'][word] = 1/len(text_vector)
        # Normalise cluster vector
        for key in rep_keys:
            vec_len += group_representation['representation'][key] * group_representation['representation'][key]
        vec_len = (vec_len ** 0.5) # compute  vector length
        for k in rep_keys:
            group_representation['representation'][k] = group_representation['representation'][k] / vec_len # Normalise

        group_representation['count'] += 1
        group_rep_list[cluster_id] = group_representation # update in group representation list
    except Exception as e:
        print("Some error occurred when updating group representation :", e) # update representation failed

# Single Pass Clustering
def Single_Pass_Clustering(tweet):
    global after_group_db
    global clusterCounter
    global group_rep_list
    global noisyTextCounter
    aim_coll = 0 # Initiate aim collection number
    maxsim = 0 # Max similarity

    if GetQualityScore(tweet) < 0.5:# threshold 0.5
        noisyTextCounter += 1 # remove noisy tweet
        aim_coll = -1
    else:
        # get text vector
        text_vector = Generate_vector(tweet)
        if text_vector == []: # noisy text
            noisyTextCounter += 1
            aim_coll = -1
        else:
            # find the most similar cluster
            for cluster_id in range(0, clusterCounter):
                sim = Calculate_SIM(group_rep_list[cluster_id]['representation'], text_vector)  # Compute similarity
                if (sim > maxsim):
                    maxsim = sim
                    aim_coll = cluster_id
                # if similarity over 0.9, it means they are the same on a great probability and do not need to compare with other cluster anymore
                if (maxsim > 0.9):
                    break
            if (maxsim < 0.5):  # threshold 0.5
                CreateNewCluster(clusterCounter, tweet, text_vector)
                aim_coll = clusterCounter
                clusterCounter += 1
            else:
                AddTweetToCluster(aim_coll, tweet, text_vector)
    return aim_coll # Return the cluster id where this tweet was added


if __name__ == '__main__':
    tweets = before_group_coll.find()  # Load all tweets from mongodb
    total = len(list(tweets))  # Total tweets
    remain = total  # Number of ungrouped tweets
    print('Total tweets :', total, '  on processing...')

    # Grouping
    for tweet in before_group_coll.find():
        Single_Pass_Clustering(tweet)
        remain -= 1  # count remain
        sys.stdout.write( '\r' + "Complete : " + str(round((total - remain) / total, 4) * 100) + '%' + "      Rest : " +
            str(remain) + "       Remove Noisy Text:" + str(noisyTextCounter))
        sys.stdout.flush()
    print("\nData Grouping  finished.")

    # Count
    max_size = 0
    min_size = 10
    for rep in group_rep_list:
        # store group representation in database
        try:
            group_representation_coll.insert_one(rep)
        except Exception as e:
            print("Some error occurred when store group representation :", e)
        # compute max size of cluster
        if rep['count'] > max_size:
            max_size = rep['count']
        # compute min size of cluster
        if rep['count'] < min_size:
            min_size = rep['count']
    print("Total : ", total, "\nGroups formed : ", clusterCounter, "\nMin size : ", min_size, "\nMax size : ", max_size,
          "\nAvg size : ", round(( (total-noisyTextCounter) / clusterCounter), 2))
```

##### HybridCrawler.py

```python
import tweepy
import Credentails
import StreamCrawler
import json
import DataGrouping
import threading
from collections import Counter
import time
import sys

# Initialize some global variables
ClusterNum_Queue = [] # a queue with latest 1000 Cluster number where tweet (form Streaming API) was added
start_index = 0 # The start index (pointer) of the cluster number queue
MAXLENGTH = 1000 # MAXLENGTH　1000 of the queue

StreamingCounter = 0 # count tweet from Streaming API
RESTCounter = 0 # count tweet from REST API
redundantCounter = 0 # count redundant tweet
ID_List = [] # Tweet id list, used to check redundant tweet
Exit = False # used to terminate REST crawler

# Rewirte  StreamListener in Streaming Cramlwer.
class StreamListener(StreamCrawler.StreamListener):
    global geoEnabled
    global geoDisabled

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")
        print("Start crawling through Streaming API")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False

    def on_exception(self, exception):
        # Called when an unhandled exception occurs.
        global streamer
        global auth
        # restart a streamer adn fliter here if an unhandled exception occurs
        # mostly caused by http.client when reading data
        streamer = tweepy.Stream(auth=auth, listener=self)
        streamer.filter(locations=StreamCrawler.Loc_UK, track=StreamCrawler.Words_UK, languages=['en'],
                        is_async=True)  # locations= Loc_UK, track = Words_UK,
        print('There are some unkonwn Exception, Restart Streamer:', exception)
        return

    # Rewirte on_data function of StreamListener
    def on_data(self, data):
        # This is where each tweet is collected
        t = json.loads(data) #  Load the json data
        tweetID = None
        try:
            tweetID = t['id_str']
        except: # if this happens, there is something wrong with JSON, so ignore this tweet
            pass

        global redundantCounter, StreamingCounter, ID_List
        # check redundant here
        if tweetID not in ID_List:
            #  Process the tweet so that we will deal with cleaned and extracted JSON
            tweet = StreamCrawler.processTweets(t)

            # Group tweet and update the queue of cluster number
            global ClusterNum_Queue, start_index,MAXLENGTH
            if tweet != None : # tweet is None means there is something wrong with JSON, so ignore this tweet
                aim_coll = DataGrouping.Single_Pass_Clustering(tweet)  # grouping
                if aim_coll != -1: # if aim_coll is -1 means this tweet is a noisy tweet
                    # add cluster id into the queue
                    if len(ClusterNum_Queue) < MAXLENGTH:
                        ClusterNum_Queue.append(aim_coll)
                    elif len(ClusterNum_Queue) == MAXLENGTH:
                        ClusterNum_Queue[start_index] = aim_coll
                        # move pointer
                        if start_index < (MAXLENGTH - 1):
                            start_index += 1
                        else:
                            start_index = 0

            #now insert it into database
            try:
                if tweet != None:
                    StreamCrawler.collection.insert_one(tweet) # insert in 'TwitterDB' database
                    ID_List.append(tweet['_id']) # add cluster id into id list
                    StreamingCounter += 1
            except Exception as e:
                print("Some error occurred when store StreamingAPI tweet into database :", e)
        else:
            redundantCounter += 1

# Generate a query from a cluster in Grouped twitter database
def GetQueries(ClusterNum):
    geoTerm = '' # initialize geo term
    hashtags = [] # all hashtags in the cluster
    mentions = [] # all user mentions in the cluster
    collection = DataGrouping.after_group_db['cluster' + str(ClusterNum)] # get the corresponding collection in MongoDB
    for tweet in  collection.find(): # find all tweet in the collection
        if tweet['coordinates'] != None: # if tweet is geo-tagged, generate geo term
            geoTerm = str(tweet['coordinates'][0]) + str(tweet['coordinates'][1]) + '10km'
        for hashtag in tweet['hashtags']: # find all hashtags of each tweet
            hashtag = '#' + hashtag
            if hashtag not in hashtags: # avoid repetition
                hashtags.append(hashtag)
        for mention in tweet['mentions']:# find all user mentions of each tweet
            mention = '@' + mention
            if mention not in mentions: # avoid repetition
                mentions.append(mention)

    # priority hashtags > user mentions > text terms
    # query ca onnly be one of them
    if hashtags != []:
        query = ' OR '.join(hashtags)
    elif mentions != []:
        query = ' OR '.join(mentions)
    else:
        query = ' '.join(DataGrouping.group_rep_list[ClusterNum]['representation'])
    return query, geoTerm

# This is a sub thread used to timing for hybrid architecture crawler
class Timer (threading.Thread):
    min = 0 # minutes of duration
    def __init__(self, min):
        threading.Thread.__init__(self)
        print("Timer active, start hybrid architecture crawler for ", min, "minutes")
        self.min = min

    def run(self):
        time.sleep(60 * self.min)
        global Exit
        Exit = True # set True, terminate REST Crawler

#REST API Crawler
def RESTCrawler():
    global ClusterNum_Queue, RESTCounter
    # Here is to wait for enough clusters (MAXLENGTH)
    time.sleep(20)
    while 1:
        if len(ClusterNum_Queue) < MAXLENGTH:
            time.sleep(1)
        else:
            print("Start crawling through REST API")
            break

    # prioritise groups and query through REST API
    TopGrowingCluster = None # Top 5 fastest growing cluster
    counter = 0 # Number of REST API Queries

    global Exit
    while (Exit != True): # Exit  when 'Exit' is true
        try:
            if (counter % 50) == 0: # Every five queries
                TopGrowingCluster = Counter(ClusterNum_Queue).most_common(50) # Top 30 fastest growing cluster
            ClusterNum = list(list(zip(*TopGrowingCluster))[0])[int(counter % 50)]
            query, geoterm = GetQueries(ClusterNum) # get query form cluster (hashtags, mentions or terms)
            print("Query Counter: ", counter, " Query:",query, " From cluster:", ClusterNum)

            # query
            if geoterm != '':
                results = api.search(q=query, geocode=geoterm, count=80, lang="en", tweet_mode='extended')
            else:
                results = api.search(q=query, count=100, lang="en", tweet_mode='extended')
            # process results and insert into database
            for result in results:
                tweet = result._json # get json format
                tweetID = None
                try:
                    tweet['text'] = tweet['full_text']
                    tweetID = tweet['id_str']
                except:
                    # if this happens, there is something wrong with JSON, so ignore this tweet
                    pass
                global redundantCounter, RESTCounter
                if tweetID not in ID_List: # check redundant
                    #  Process the tweet so that we will deal with cleaned and extracted JSON
                    tweet = StreamCrawler.processTweets(tweet)
                    # now insert it
                    try:
                        if tweet != None:
                            StreamCrawler.collection.insert_one(tweet)# insert in 'TwitterDB' database
                            ID_List.append(tweet['_id']) # add cluster id into id list
                            RESTCounter += 1
                    except Exception as e:
                        print("Some error occurred when store RESTAPI tweet into database :", e)
                else:
                    redundantCounter += 1

            # let the crawler to sleep for 15/180 minutes each query; to meet the Tiwtter 15 minute restriction
            time.sleep((60*15)/180)
        except Exception as e:
            print("Some error occurred when crawl tweets through RESTAPI :", e)
        counter += 1

if __name__ == '__main__':
    # Set twitter API
    auth = tweepy.OAuthHandler(Credentails.consumer_key, Credentails.consumer_secret)
    auth.set_access_token(Credentails.access_token, Credentails.access_token_secret)
    api = tweepy.API(auth)
    if (not api):
        print('Can\'t authenticate')
        print('failed cosumeer id ----------: ', Credentails.consumer_key)

    # Reset the collection for hybrid architecture of part 3
    StreamCrawler.collection = StreamCrawler.db['Hybrid_3']
    DataGrouping.after_group_db = DataGrouping.client['GroupedTwitter_3'] # db of tweets after grouping
    DataGrouping.group_representation_coll = DataGrouping.after_group_db['group_representaion']  # collection used to store group representation

    # Start crawling through Streaming API
    listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))
    streamer = tweepy.Stream(auth=auth, listener=listener)
    streamer.filter(locations=StreamCrawler.Loc_UK, track=StreamCrawler.Words_UK, languages=['en'],
                    is_async=True)  # locations= Loc_UK, track = Words_UK,

    # Time 30 minutes in sub thread
    Exit = False # used to finish REST crawler
    timer = Timer(30) # duration
    timer.start() # start timer

    # Start REST API Crawler
    RESTCrawler()

    # Terminate Streaming when REST crawler finished
    print('####  Crawling complete ####')
    streamer.disconnect()

    # Count and  store group representation in database
    max_size = 0
    min_size = 10
    for rep in DataGrouping.group_rep_list:
        # store group representation in database
        try:
            DataGrouping.group_representation_coll.insert_one(rep)
        except Exception as e:
            print("Some error occurred when store group representation :", e)
        # compute max size of cluster
        if rep['count'] > max_size:
            max_size = rep['count']
        # compute min size of cluster
        if rep['count'] < min_size:
            min_size = rep['count']

    print("Total: ", str(StreamCrawler.count_total_processed + redundantCounter),
          "\nRedundant:", str(redundantCounter),
          "\nEffective tweet:", str(StreamCrawler.count_total_processed),
          "\nRetweets:", str(StreamCrawler.count_RT),
          "\nQuotes:", str(StreamCrawler.count_quotes),
          "\nImages: ", str(StreamCrawler.count_Images),
          "\nVideos: ", str(StreamCrawler.count_Videos),
          "\nVerified: ", str(StreamCrawler.count_verified),
          "\nGeo-tagged: ", str(StreamCrawler.count_geotagged),
          "\nLocations/Place: ", str(StreamCrawler.count_place),
          "\nStreaming API --------------------------------",
          "\nTotal: ", StreamingCounter,
          "\nNoisy tweets when grouping: ", str(DataGrouping.noisyTextCounter),
          "\nEffective grouped tweet: ", str(StreamingCounter-DataGrouping.noisyTextCounter),
          "\nGroups formed : ", DataGrouping.clusterCounter,
          "\nMax size : ", max_size,
          "\nMin size : ", min_size,
          "\nAvg size : ", round((StreamingCounter / DataGrouping.clusterCounter), 2),
          "\nREST API -------------------------------------",
          "\nTotal: ", RESTCounter)
    sys.exit()
```

