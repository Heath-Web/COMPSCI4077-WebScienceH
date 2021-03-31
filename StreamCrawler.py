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