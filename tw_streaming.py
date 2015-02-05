#!/usr/bin/python

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import codecs
import ConfigParser
import sys
import signal
import os.path


config = ConfigParser.ConfigParser()
config.read('config')

consumer_key = config.get('apikey', 'key')
consumer_secret = config.get('apikey', 'secret')
access_token = config.get('token', 'token')
access_token_secret = config.get('token', 'secret')

class StdOutListener(StreamListener):
    
    def __init__(self, f):
        super(StdOutListener, self).__init__()
        self.f = f
        self.url = "http://www.twitter.com"
        self.keywords = [{"id":"obamacare","terms":["obamacare"]},
                         {"id":"college","terms":["tuition price cost", "colleges universities college university"]},
                         {"id":"k-12","terms":["k-12", "public education"]},
                         {"id":"immigrant","terms":["undocumented immigrants immigrant", "service services", "access"]},
                         {"id":"marijuana","terms":["law laws regulation regulations", "marijuana", "recreational"]},
                         {"id":"gay","terms":["marriage", "rights law laws", "gay same-sex partners"]}]
    
    def on_status(self, status):
        for keyword in self.keywords:
            arr_terms = keyword.get("terms")
            found_terms = True
            for terms in arr_terms:
                if not any(term in status.text.lower().split() for term in terms.split()):
                    found_terms = False
            if found_terms:
                tw_text = status.text.encode("ascii","ignore")
                tw_text = tw_text.replace('\n', ' ').replace('\r', '')
                tw_text = tw_text.replace('\"', '\'')
                url_status = self.url + "/" + status.author.screen_name + "/status/" + status.id_str
                self.f.write("\"%s\",\"%s\",\"%s\",\"@%s\",\"%s\",\"%s\"\n" % (status.created_at,status.id_str,keyword.get("id"),
                                                                               status.author.screen_name,
                                                                               status.text.encode("ascii","ignore"),
                                                                               url_status))
                break
        return True

    def on_error(self, status):
        print "Error, code: %s" % status
        return True  # To continue listening

if __name__ == '__main__':
    try:
        fname = "output.csv"
        file_exists = False
        if os.path.isfile(fname):
            file_exists = True
        f_output = codecs.open("output.csv", "a", encoding="ascii")
        if !file_exists:
            f_output.write("\"date\",\"id\",\"keyword\",\"user_handler\",\"text\",\"url\"\n")
        l = StdOutListener(f_output)
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        stream = Stream(auth, l)
        stream.filter(locations=[-124.24,32.30,-120,42])  # Only tweets from California
    except KeyboardInterrupt:
        f_output.close()

#California Latitude-Longitude
#SW: -124.24, 32.30
#NE: -120, 42
