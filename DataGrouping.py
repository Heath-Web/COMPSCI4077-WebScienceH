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