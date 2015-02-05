#!/usr/bin/python

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import codecs
import sys
import signal

# Go to http://dev.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key="PVBkth1IJfit6XhVSdxXQwa3u"
consumer_secret="CrA8TVEnr3IUJ3idHerTOkY9ApZRfKmOjRzyfG7OghvMEOMmFY"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token="2733258272-akrGnoZLYKJ5srQNFORgYHNfnOo5JivVZBjI8Bv"
access_token_secret="yyNdrOOOWC7eXOsA8gsI6Tke0ZQnHIX0aGzdVWzucGZpK"

class StdOutListener(StreamListener):
    
    def __init__(self, f):
        super(StdOutListener, self).__init__()
        self.f = f
        self.f.write("\"date\",\"id\",\"keyword\",\"user_handler\",\"text\",\"url\"\n")
        self.url = "http://www.twitter.com"
        self.keywords = [{"id":"obamacare","terms":["obamacare"]},
                         {"id":"college","terms":["tuition price cost", "colleges universities college university"]},
                         {"id":"k-12","terms":["k-12", "public education"]},
                         {"id":"immigrant","terms":["undocumented immigrants immigrant", "service services", "access"]},
                         {"id":"marijuana","terms":["law laws regulation regulations", "marijuana", "recreational"]},
                         {"id":"gay","terms":["marriage", "rights law laws", "gay same-sex partners"]}]
    
    def on_status(self, status):
	#print("@%s: %s" % (status.author.screen_name, status.text.encode("ascii","ignore")))
        for keyword in self.keywords:
            arr_terms = keyword.get("terms")
            found_terms = True
            for terms in arr_terms:
                if not any(term in status.text.lower().split() for term in terms.split()):
                    found_terms = False
            if found_terms:
                #print("@%s: %s" % (status.author.screen_name, status.text.encode("ascii","ignore")))
                #print("Found a tweet from California containg the keyword %s!" % keyword.get("id"))
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
        f_output = codecs.open("output.csv", "a", encoding="ascii")
        l = StdOutListener(f_output)
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        stream = Stream(auth, l)
        stream.filter(locations=[-124.24,32.30,-120,42])  # Only tweets from California
        #stream.filter(track=['obamacare'], locations=[-124.24,32.30,-120,42])
        #stream.filter(track=['obamacare', 'k-12 public education', 'immigrant service access', 'marijuana recreation law', 'gay marriage rights'])
    except KeyboardInterrupt:
        f_output.close()

#California Latitude-Longitude
#SW: -124.24, 32.30
#NE: -120, 42
