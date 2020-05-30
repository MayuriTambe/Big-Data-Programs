#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API
access_token = "1266030289244979200-HGDlLqR7Ou3O3ZfEPRa7vPCFatuk3G"
access_token_secret = "yi65BpGeMtK1gNqTb5AygVySpjTWn2dH6Ib2Pe2ogyh25"
consumer_key = "zNv9D90bV6oIR43WrhMGvT21U"
consumer_secret = "4vqO08O04O2Bp5fU4S9E4aViuGMGqACsBu8txeHQpOFnfiUjSi"


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print (status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['python', 'javascript', 'ruby'])
