#!/usr/bin/python

import ConfigParser
import codecs
import datetime
import logging
from lxml import html
import os
import requests
import urllib
import traceback
import tweepy


logger = logging.getLogger('tw_search')
handler = logging.FileHandler('tw_search.log')
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def do_search(api, phrase, max_page, max_tweets, since_id=None):
    tot_results = []
    max_id = -1L
    tweet_count = 0

    # Encode the phrase to URL format
    encoded_phrase = urllib.quote(phrase.encode('utf-8'))

    while tweet_count < max_tweets:
        try:
            if max_id <= 0:
                if not since_id:
                    new_tweets = api.search(q=encoded_phrase, count=max_page)
                else:
                    new_tweets = api.search(q=encoded_phrase, count=max_page, since_id=since_id)
            else:
                if not since_id:
                    new_tweets = api.search(q=encoded_phrase, count=max_page, max_id=str(max_id - 1))
                else:
                    new_tweets = api.search(q=encoded_phrase, count=max_page, max_id=str(max_id - 1), since_id=since_id)

            if not new_tweets:
                break

            for tweet in new_tweets:
                if tweet.lang == "en":
                    if tweet.place:
                        if tweet.place.country_code == "US":
                            tot_results.append(tweet)
                    else:
                        if tweet.geo:
                            if not tweet.geo["coordinates"][0] == 0 and not tweet.geo["coordinates"][1] == 0:
                                tot_results.append(tweet)

            tweet_count += len(new_tweets)
            max_id = new_tweets[-1].id
        except (tweepy.TweepError, AttributeError) as e:
            logger.error("Tweepy Error: %s." % str(e))
            continue

    return tot_results


def get_place_name(id_tweet, latitude, longitude, app_location_token, app_location_token_secret):
    # Authenticate in user context
    auth = tweepy.OAuthHandler(app_location_key, app_location_secret)
    auth.set_access_token(app_location_token, app_location_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    # Search at region/state level (granularity=admin)
    try:
        result = api.reverse_geocode(lat=latitude, long=longitude, granularity="admin")
        country_code = result[0].country_code
        if country_code == "US":
            return result[0].name
        else:
            logger.info("Tweet %s posted from outside US (%s)" % (id_tweet, country_code))
            return None
    except (tweepy.TweepError, AttributeError) as e:
        logger.error("Problem when trying to obtain the location of the tweet %s. Lat: %s, Lon: %s" % (id_tweet, latitude, longitude))
        logger.error("Error: %s." % e)
        return None


def is_rt(tweet):
    try:
        return tweet.retweeted_status.id
    except AttributeError:
        return None


def tweet_already_saved(db_tweets, topic, tweet_id):
    if db_tweets:
        return tweet_id in db_tweets[topic]
    else:
        return False


def to_unicode(obj, encoding="utf-8"):
    if isinstance(obj, basestring):
        if not isinstance(obj, unicode):
            obj = unicode(obj, encoding)
    return obj


def format_text(text):
    u_text = to_unicode(text)
    tw_text = u_text.encode("utf-8", "ignore")
    tw_text = tw_text.replace('\n', ' ').replace('\r', '').replace('\"', '\'')
    return tw_text


def get_num_comments(url_tweet):
    try:
        page = requests.get(url_tweet)
        doc = html.fromstring(page.text)
        replies_list = doc.xpath('//ol[@id="stream-items-id"]/li')
        return len(replies_list)
    except Exception:
        return 0


def prepare_tweet(tweet, topic, state_name):
    tw_text = format_text(tweet.text)
    author_bio = format_text(tweet.author.description)
    author_location = format_text(tweet.author.location)
    author_name = format_text(tweet.author.screen_name)
    url_tweet = "http://www.twitter.com/" + tweet.author.screen_name + "/status/" + tweet.id_str
    source_tweet = format_text(tweet.source)
    num_comments = get_num_comments(url_tweet)
    tw_lat = tweet.geo["coordinates"][0]
    tw_lon = tweet.geo["coordinates"][1]
    tweet_dict = {"date": tweet.created_at, "id": tweet.id_str, "topic": topic, "state": state_name,
                  "text": tw_text, "language": tweet.lang, "source": source_tweet, "rt_count": tweet.retweet_count,
                  "fv_count": tweet.favorite_count, "comment_count": num_comments, "url": url_tweet,
                  "latitude": tw_lat, "longitude": tw_lon, "author_name": author_name,
                  "author_followers": tweet.author.followers_count, "author_friends": tweet.author.friends_count,
                  "author_statuses_count": tweet.author.statuses_count, "author_location": author_location,
                  "author_bio": author_bio}
    return tweet_dict


def save_tweet_file(output_file, tweet_dict):
    try:
        output_file.write("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\","
                          "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n" %
                          (tweet_dict["date"], tweet_dict["id"], tweet_dict["topic"], tweet_dict["state"],
                           tweet_dict["text"], tweet_dict["language"], tweet_dict["source"], tweet_dict["rt_count"],
                           tweet_dict["fv_count"], tweet_dict["comment_count"], tweet_dict["url"], tweet_dict["latitude"],
                           tweet_dict["longitude"], tweet_dict["author_name"], tweet_dict["author_followers"],
                           tweet_dict["author_friends"], tweet_dict["author_statuses_count"], tweet_dict["author_location"],
                           tweet_dict["author_bio"]))
    except UnicodeEncodeError as e:
        logger.error("Error %s." % e)
        logger.error("Impossible to save the tweet %s" % tweet_dict)


def open_output_file(fname):
    if os.path.isfile(fname):
        return codecs.open(fname,"a", encoding="utf-8")
    else:
        f = codecs.open(fname,"w", encoding="utf-8")
        f.write("\"date\",\"id\",\"keyword\",\"location\",\"text\",\"lang\",\"source\",\"rt_count\",\"fv_count\","
                "\"comments_count\",\"url\",\"latitude\",\"longitude\",\"author_handler\",\"followers\",\"friends\","
                "\"author_tw_count\",\"author_location\",\"author_description\"\n")
        return f


def get_tweets_db(fname):
    try:
        f = open(fname, "r")
        tweets_file = f.readlines()
        f.close()
        if len(tweets_file) == 1:
            return None
        else:
            db_tweets = {'obamacare': [], 'k_12': [], 'college': [], 'gay': [], 'marijuana': [], 'immigrant': []}
            for tweet_line in tweets_file[2:len(tweets_file)]:
                topic = tweet_line.split(",")[2].replace("\"", "")
                id_tweet = tweet_line.split(",")[1].replace("\"", "")
                db_tweets[topic].append(id_tweet)
            return db_tweets
    except IOError:
        return None


def print_summary(file, summary, total_tweets):
    today = datetime.date.today().strftime("%d/%m/%y")
    file.write("Date: %s ---------------------------\n" % today)
    tot_ca_tw_collected = 0
    tot_nonca_tw_collected = 0
    for topic in summary:
        tot_ca_tw_collected += topic["ca_tweets"]
        tot_nonca_tw_collected += topic["nonca_tweets"]
    file.write("Total Tweets: %s\n" % total_tweets)
    file.write("Total Tweets from California: %s\n" % tot_ca_tw_collected)
    file.write("Total Tweets from Outside CA: %s\n" % tot_nonca_tw_collected)
    file.write("---------------\n")
    for topic in summary:
        file.write("Topic: %s\n" % topic["keyword"])
        file.write("Total Tweets: %s\n" % topic["total_tweets"])
        file.write("Total Tweets from California: %s\n" % topic["ca_tweets"])
        file.write("Total Tweets from Outside CA: %s\n" % topic["nonca_tweets"])
        file.write("\n")
    file.write("------------------------------------------\n")
    file.write("\n")


def read_us_states_table():
    f_states = open("us-states.csv", "r")
    states = f_states.readlines()
    f_states.close()
    return states


def get_state_name(states_table, state_code):
    for state_row in states_table:
        state_row = state_row.replace("\n","")
        name, code = state_row.split(",")[0].replace("\"",""), state_row.split(",")[1].replace("\"","")
        if code == state_code:
            return name
    return None


def save_ca_tweets(fname_ca, ca_tweets):
    if os.path.isfile(fname_ca):
        f= codecs.open(fname_ca,"a", encoding="utf-8")
    else:
        f = codecs.open(fname_ca,"w", encoding="utf-8")
        f.write("\"date\",\"id\",\"keyword\",\"text\",\"lang\",\"source\",\"rt_count\",\"fv_count\",\"comments_count\","
                "\"location\",\"url\",\"latitude\",\"longitude\",\"author_handler\",\"followers\",\"friends\","
                "\"author_tw_count\",\"author_location\",\"author_description\"\n")
    for ca_tweet in ca_tweets:
        f.write("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\","
                "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n" %
                (ca_tweet["date"], ca_tweet["id"], ca_tweet["topic"], ca_tweet["state"],
                 ca_tweet["text"], ca_tweet["language"], ca_tweet["source"], ca_tweet["rt_count"],
                 ca_tweet["fv_count"], ca_tweet["comment_count"], ca_tweet["url"], ca_tweet["latitude"],
                 ca_tweet["longitude"], ca_tweet["author_name"], ca_tweet["author_followers"],
                 ca_tweet["author_friends"], ca_tweet["author_statuses_count"], ca_tweet["author_location"],
                 ca_tweet["author_bio"]))

    f.close()

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('config')

    # Read configuration
    app_search_key = config.get('app_search', 'key')
    app_search_secret = config.get('app_search', 'secret')
    app_location_key = config.get('app_location', 'key')
    app_location_secret = config.get('app_location', 'secret')
    app_location_token = config.get('app_location', 'token')
    app_location_token_secret = config.get('app_location', 'token_secret')
    output_path = config.get('output', 'path')
    output_fname = config.get('output','name')
    output_fname_ca = config.get('output','ca')
    output_summary = config.get('output','summary')
    max_page = config.getint('search_engine', 'max_page')
    max_tweets = config.getint('search_engine', 'max_tweets')

    print("Starting to collect tweets related with CRC topics, please wait, this can take several minutes...")

    try:
        fname_output = os.path.join(output_path, output_fname)
        fname_output_ca = os.path.join(output_path, output_fname_ca)
        # Open the dictionary
        f_dict = open("filters.csv", "r")
        # Get db of tweets
        db_tweets = get_tweets_db(fname_output)
        # Open output file to save tweets
        f_output = open_output_file(fname_output)
        # Read US States table
        states_table = read_us_states_table()
        try:
            # Authenticate
            auth = tweepy.AppAuthHandler(app_search_key, app_search_secret)

            # Instantiate the API obj
            api = tweepy.API(auth, wait_on_rate_limit=True)

            # Iterate over the dictionary and search for tweets containing the phrase
            summary = []
            total_tweets = 0
            for line in f_dict:
                ca_tweets = []
                keyword, phrase = line.split(",")
                print("Searching tweets related to %s" % keyword)
                # Get id of the last saved tweet related to the topic
                id_last_tweet = db_tweets[keyword][-1] if db_tweets and db_tweets[keyword] else None
                results = do_search(api, phrase, max_page, max_tweets, id_last_tweet)
                print("Found %s tweets that are going to be processed" % len(results))
                summary_keyword = {"keyword": keyword}
                num_ca_tweets = 0
                num_nonca_tweets = 0
                for tweet in results:
                    if not tweet_already_saved(db_tweets, keyword, tweet.id_str):
                        id_rt = is_rt(tweet)
                        if id_rt:
                            if db_tweets:
                                if not tweet_already_saved(db_tweets, keyword, id_rt):
                                    tweet = api.get_status(id_rt)
                                else:
                                    logger.info("The tweet %s was discarded because it is RT and the original tweet is "
                                                 "already in the dataset" % tweet.id_str)
                                    continue
                            else:
                                tweet = api.get_status(id_rt)

                        if tweet.place:
                            country_code = tweet.place.country_code
                            if country_code == "US":
                                if tweet.place.place_type == "country" or tweet.place.place_type == "neighborhood" or \
                                   tweet.place.place_type == "poi":
                                    lat = tweet.place.bounding_box.coordinates[0][0][1]
                                    lon = tweet.place.bounding_box.coordinates[0][0][0]
                                    state_name = get_place_name(tweet.id_str, lat, lon, app_location_token,
                                                                app_location_token_secret)
                                elif tweet.place.place_type == "admin":
                                    state_name = tweet.place.name
                                else:
                                    state_code = tweet.place.full_name.split(",")[1].strip()
                                    state_name = get_state_name(states_table, state_code)
                            else:
                                state_name = None
                        else:
                            lat = tweet.geo["coordinates"][0]
                            lon = tweet.geo["coordinates"][1]
                            if lat == 0 and lon == 0:
                                state_name = None
                                logger.info("Tweet with unreachable location (0,0). %s" % tweet)
                            else:
                                state_name = get_place_name(tweet.id_str, lat, lon, app_location_token,
                                                            app_location_token_secret)

                        if state_name:  # Discard tweets from outside US
                            tweet_dict = prepare_tweet(tweet, keyword, state_name)
                            if state_name == "California":
                                num_ca_tweets += 1
                                ca_tweets.append(tweet_dict)
                            else:
                                num_nonca_tweets += 1
                            save_tweet_file(f_output, tweet_dict)
                        else:
                            logger.info("The tweet %s was discarded because it seems to be posted from outside US" %
                                        tweet.id_str)
                summary_keyword["ca_tweets"] = num_ca_tweets
                summary_keyword["nonca_tweets"] = num_nonca_tweets
                summary_keyword["total_tweets"] = num_ca_tweets + num_nonca_tweets
                summary.append(summary_keyword)
                total_tweets += summary_keyword["total_tweets"]
                print("Found %s tweets (%s from CA) related with the topic %s" % (summary_keyword["total_tweets"],
                                                                                  summary_keyword["ca_tweets"],
                                                                                  keyword))
                # Save in another file ca-related tweets
                save_ca_tweets(fname_output_ca, ca_tweets)

            # Print a summary
            f_summary = open(output_summary,"a")
            print_summary(f_summary, summary, total_tweets)
            print("Done!. We found in total %s new tweets!. Look at %s for more details" % (total_tweets, output_summary))
        except Exception as e:
            logger.error("Error: %s. %s" % (str(e), traceback.format_exc()))
        finally:
            f_dict.close()
            f_output.close()
    except IOError as e:
        logger.error("Error: %s. %s" % (str(e), traceback.format_exc()))