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