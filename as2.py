import twitter
from functools import partial
from sys import maxint
import io
import sys
import time
from urllib2 import URLError 
from httplib import BadStatusLine 
import json
import twitter
import networkx as nx
import matplotlib.pyplot as plt
import pymongo
import ast

#login function containing consumer key.
def oauth_login():    
    CONSUMER_KEY = 'Qhw9SmyDS4ugTSYLmPBPUqebc'
    CONSUMER_SECRET = '76PTSiepUdaphQls5t2teDTY5TPlTldQF6gZrP9YIexTwGasic'
    OAUTH_TOKEN = '24551258-jFooCTHzfbacGH5xrPDHWRBm38aHdaqgfk3DEl5yN'
    OAUTH_TOKEN_SECRET = 'PAJx2Uft2UinVF0Bcwy4dFk5OPUSl46sXq7hfKYdWYHSp'
    auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)   
    twitter_api = twitter.Twitter(auth=auth)
    return twitter_api

#Function for making requests, as seen in the twitter cookbook
def make_twitter_request(twitter_api_func, max_errors=10, *args, **kw): 
    
    # A nested helper function that handles common HTTPErrors. Return an updated
    # value for wait_period if the problem is a 500 level error. Block until the
    # rate limit is reset if it's a rate limiting issue (429 error). Returns None
    # for 401 and 404 errors, which requires special handling by the caller.
    def handle_twitter_http_error(e, wait_period=2, sleep_when_rate_limited=True):
    
        if wait_period > 3600: # Seconds
            print >> sys.stderr, 'Too many retries. Quitting.'
            raise e
    
        # See https://dev.twitter.com/docs/error-codes-responses for common codes
    
        if e.e.code == 401:
            print >> sys.stderr, 'Encountered 401 Error (Not Authorized)'
            return None
        elif e.e.code == 404:
            print >> sys.stderr, 'Encountered 404 Error (Not Found)'
            return None
        elif e.e.code == 429: 
            print >> sys.stderr, 'Encountered 429 Error (Rate Limit Exceeded)'
            if sleep_when_rate_limited:
                print >> sys.stderr, "Retrying in 15 minutes...ZzZ..."
                sys.stderr.flush()
                time.sleep(60*8 + 5)
                print >> sys.stderr, '...ZzZ...Awake now and trying again.'
                return 2
            else:
                raise e # Caller must handle the rate limiting issue
        elif e.e.code in (500, 502, 503, 504):
            print >> sys.stderr, 'Encountered %i Error. Retrying in %i seconds' % \
                (e.e.code, wait_period)
            time.sleep(wait_period)
            wait_period *= 1.5
            return wait_period
        else:
            raise e

    # End of nested helper function
    
    wait_period = 2 
    error_count = 0 

    while True:
        try:
            return twitter_api_func(*args, **kw)
        except twitter.api.TwitterHTTPError, e:
            error_count = 0 
            wait_period = handle_twitter_http_error(e, wait_period)
            if wait_period is None:
                return
        except URLError, e:
            error_count += 1
            print >> sys.stderr, "URLError encountered. Continuing."
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise
        except BadStatusLine, e:
            error_count += 1
            print >> sys.stderr, "BadStatusLine encountered. Continuing."
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise

def get_friends_followers_ids(twitter_api, screen_name=None, user_id=None, friends_limit=maxint, followers_limit=maxint):
    
    # Must have either screen_name or user_id (logical xor)
    assert (screen_name != None) != (user_id != None), \
    "Must have screen_name or user_id, but not both"
    
    # See https://dev.twitter.com/docs/api/1.1/get/friends/ids and
    # https://dev.twitter.com/docs/api/1.1/get/followers/ids for details
    # on API parameters
    
    get_friends_ids = partial(make_twitter_request, twitter_api.friends.ids, 
                              count=5000)
    get_followers_ids = partial(make_twitter_request, twitter_api.followers.ids, 
                                count=5000)

    friends_ids, followers_ids = [], []
    
    for twitter_api_func, limit, ids, label in [
                    [get_friends_ids, friends_limit, friends_ids, "friends"], 
                    [get_followers_ids, followers_limit, followers_ids, "followers"]
                ]:
        
        if limit == 0: continue
        
        cursor = -1
        while cursor != 0:
        
            # Use make_twitter_request via the partially bound callable...
            if screen_name: 
                response = twitter_api_func(screen_name=screen_name, cursor=cursor)
            else: # user_id
                response = twitter_api_func(user_id=user_id, cursor=cursor)

            if response is not None:
                ids += response['ids']
                cursor = response['next_cursor']
        
            print >> sys.stderr, 'Fetched {0} total {1} ids for {2}'.format(len(ids), 
                                                    label, (user_id or screen_name))
        
            # XXX: You may want to store data during each iteration to provide an 
            # an additional layer of protection from exceptional circumstances
        
            if len(ids) >= limit or response is None:
                break
    return friends_ids[:friends_limit], followers_ids[:followers_limit]

def save_to_mongo(data, mongo_db, mongo_db_coll, **mongo_conn_kw):

    client = pymongo.MongoClient(**mongo_conn_kw)
    
    # Get a reference to a particular database
    
    db = client[mongo_db]
    
    # Reference a particular collection in the database
    
    coll = db[mongo_db_coll]
    
    # Perform a bulk insert and  return the IDs
    
    return coll.insert(data)

def load_from_mongo(mongo_db, mongo_db_coll, return_cursor=False, criteria=None, projection=None, **mongo_conn_kw):
    
    # Optionally, use criteria and projection to limit the data that is 
    # returned as documented in 
    # http://docs.mongodb.org/manual/reference/method/db.collection.find/
    
    # Consider leveraging MongoDB's aggregations framework for more 
    # sophisticated queries.
    
    client = pymongo.MongoClient(**mongo_conn_kw)
    db = client[mongo_db]
    coll = db[mongo_db_coll]
    
    if criteria is None:
        criteria = {}
    
    if projection is None:
        cursor = coll.find(criteria)
    else:
        cursor = coll.find(criteria, projection)

    # Returning a cursor is recommended for large amounts of data
    
    if return_cursor:
        return cursor
    else:
        return [ item for item in cursor ]

#Modified the crawl_followers function to search for reciprocal friends
def crawl_followers(twitter_api, screen_name, limit=1000, depth=50):
    
    # Resolve the ID for screen_name and start working with IDs for consistency 
    # in storage

    seed_id = str(twitter_api.users.show(screen_name=screen_name)['id'])
    
    friends_ids, next_queue = get_friends_followers_ids(twitter_api, user_id=seed_id, friends_limit=limit, followers_limit=limit)

    # Store a seed_id => _follower_ids mapping in MongoDB
    intersection = [val for val in friends_ids if val in next_queue]    
    
    save_to_mongo({'followers' : [ _id for _id in next_queue ], 'friends' : [_id for _id in friends_ids], 'reciprocal_friends': intersection}, 'final','{0}'.format(seed_id))

    #Get information about intersection set
    intersection_info = make_twitter_request(twitter_api.users.lookup, user_id = intersection)

    #find top five users
    list_of_tuples = []
    for rfriend in intersection_info:        
        list_of_tuples.append( (rfriend['screen_name'], rfriend['followers_count'], rfriend['id']) )
    list_of_tuples.sort(key=lambda tup: tup[1])
    list_of_tuples = list_of_tuples[::-1] 
    list_of_tuples = list_of_tuples[0:5]

    for_queue = []

    for tup in list_of_tuples:
        for_queue.append(tup[2])

    
    next_queue = for_queue
    
    d = 1

    while d < depth:
        d += 1
        (queue, next_queue) = (next_queue, [])
        for fid in queue:
            friends_ids, follower_ids = get_friends_followers_ids(twitter_api, user_id=fid, friends_limit=limit, followers_limit=limit)

            intersection = [val for val in friends_ids if val in follower_ids]   

            if (len(intersection) == 0):
                continue
            else:
                if (len(intersection) > 100):
                    intersection = intersection[0:85]
            

            intersection_info = make_twitter_request(twitter_api.users.lookup, user_id = intersection)

            if (intersection_info == None):
                continue

            list_of_tuples = []
            for rfriend in intersection_info:        
                list_of_tuples.append( (rfriend['screen_name'], rfriend['followers_count'], rfriend['id']) )
            
            list_of_tuples.sort(key=lambda tup: tup[1])
            list_of_tuples = list_of_tuples[::-1] 
            list_of_tuples = list_of_tuples[0:5]
            for_queue = []

            for tup in list_of_tuples:
                 for_queue.append(tup[2])

            save_to_mongo({'reciprocal_friends' : for_queue}, 'final', '{0}'.format(fid))
            next_queue += for_queue



if __name__ == '__main__':
    
    twitter_api = oauth_login()



    root_node = load_from_mongo('final', '24551258')
    intersection = ast.literal_eval(json.dumps([val for val in root_node[0]['friends']if val in root_node[0]['followers']]))
    intersection_info = make_twitter_request(twitter_api.users.lookup, user_id = intersection)

    # get D1 Friends, the Rest will be iterable
    # Get the top five reciprocal friends and put them into the list of tuples
    list_of_tuples = []
    list_of_ids = []


    final_graph = {}
    client = pymongo.MongoClient()
    db = client.final
    for val in db.collection_names():
        final_graph[int(val)] = load_from_mongo('final', str(val))[0]['reciprocal_friends']
    final_graph[24551258] = intersection


    # Start creating the graph peice by peice.
    G=nx.from_dict_of_lists(final_graph)
    pos=nx.spring_layout(G) # positions for all nodes
    nx.draw_networkx_nodes(G,pos,node_size=3)
    nx.draw_networkx_edges(G,pos,width=1)
    nx.draw_networkx_labels(G,pos,font_size=2,font_family='sans-serif')
    plt.show()

    
    print("the number of nodes of the network is " + str(G.size()))
    print("The diameter of the network is: " + str(nx.diameter(G)))
    print("The average distance of the network is " + str(nx.center(G)))









