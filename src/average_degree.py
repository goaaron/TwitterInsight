#!/usr/bin/env/python
import os
import re
import sys, json
import time, datetime
from collections import deque

class Tweet_Graph(object):

    """
    Class that holds our tweet input. The object stores data concerning text and time
    along with methods for generating and summarizing an updating graph of related hashtags.

    """

    # instantiate the tweet instance with the message data we parse from the json
    def __init__(self, inputfile, outputfile):
        self.inputfile = inputfile
        self.outputfile = outputfile
        self.tweet_data = deque()
        self.nodes = {}
        self.edges = set()
        self.flag = 0


    def acquire_tags(self, tweet_data):
        """
        Input: A line of JSON holding data and metadata associated with a tweet.
        Output: The set of hashtags contained in a single instance of a tweet.

        Finds the relevant hashtag data associated with a line of tweet json.

        """
        hashtags = []
        hashlist = tweet_data.get("hashtags")
        try:
            hashtags = [hashes['text'] for hashes in hashlist]
            clean_tags = [fix.encode('ascii', 'ignore') for fix in hashtags]  #need to ignore special characters
            #return sorted(set(clean_tags)) #kill dupes
            hashtags = sorted(list(set(hashtags))) #kill dupes
            return hashtags
        except IOError:
            print("Tweet did not contain any hashtag data")


    def acquire_time(self, tweet_data):
        """
        Input: A line of JSON holding data and metadata associated with a tweet.
        Output: The time associated with the creation of a tweet. Datetime.

        Finds the relevant timsestap associated with a line of tweet json.

        """
        create_time = datetime.datetime.strptime(tweet_data, "%a %b %d %H:%M:%S %z %Y")
        return create_time


    def add_to_graph(self, hash_tags, creation_time):
        """
        Input: A list of hashtags and time of creation.
        Output: None. Append to the existing object's dictionary and set of edges.

        Adds hashtag nodes to the dictionary and appends edges to the set for hash_tags that are referenced in the same tweet.

        """
        edgelist = []
        #generate pairs for the hashtags in a tweet
        for i in range(0, len(hash_tags)):
            for j in range(i + 1, len(hash_tags)):
        #this is effectively a list of possible edges given our tags
                if ( (hash_tags[i], hash_tags[j]) not in self.edges and (hash_tags[j], hash_tags[i]) not in self.edges):
                    self.edges.add((hash_tags[i], hash_tags[j]))
                    edgelist.append((hash_tags[i], hash_tags[j]))
                    if hash_tags[i] in self.nodes:
                        self.nodes[hash_tags[i]] = self.nodes.get(hash_tags[i]) + 1
                    else:
                        self.nodes[hash_tags[i]] = 1
                    if hash_tags[j] in self.nodes:
                        self.nodes[hash_tags[j]] = self.nodes.get(hash_tags[j]) + 1
                    else:
                        self.nodes[hash_tags[j]] = 1
        if edgelist:
            #push this onto the deque of tweets
            self.tweet_data.append((creation_time, edgelist))


    def remove_expired_tweets(self, time_of_creation):
        """
        Input: Time of the creation of the latest input tweet.
        Output: None. Side-effect.

        Compares the latest input tweet to the time of the oldest tweets in the deque and pops from the left to clear out tweets older than the hardcoded minute-long duration. On deletion, the function calls the remove_edges function which takes care of degree decrementation and node deletion.

        """
        while len(self.tweet_data):
          #need to test for prolonged delay between tweets
            (time, old_edges) = self.tweet_data[0]
            newtime = self.tweet_data[-1][0]
            #account for disorderly tweets which should not be included
            if((newtime - time_of_creation).seconds < 0):
                self.flag = 1  #a little message to ignore this tweet
                break
            if((time_of_creation - time).seconds <= 60):
                break
            self.remove_edges(old_edges)
            self.tweets.popleft()  #clear the deque from the oldest tweets


    def remove_edges(self, old_edges):
        """
        Input: A list of edges to be removed.
        Output: None. Side-effect.

        Iterates through the set of edges in the object and deletes entries matching pairs in the "old_edges" list. With each deletion of a node representing a hashtag, the function also decrements the degree of the node--in the case that the degree becomes zero, the node is removed from the dictionary of nodes.

        """
        # Given that this in an undirected graph, this actually goes both ways.
        try:
            for (start, finish) in old_edges:
                if (start,finish) in self.edges:
                    self.edges.discard((start,finish))
                elif (finish,start) in self.edges:
                    self.edges.discard((finish,start))   # A bit redundant.
                if start in self.nodes:
                    new_degree = self.nodes[start] - 1
                    if new_degree == 0 :
                        del self.nodes[start]
                    else:
                        self.nodes[start] = new_degree
                #repeating myself here, looking at something cleaner
                if finish in self.nodes:
                    new_degree2 = self.nodes[finish] - 1
                    if new_degree2 == 0 :
                        del self.nodes[finish]
                    else:
                        self.nodes[finish] = new_degree2
        except:
            pass


    def calc_average_degree(self, output):

        """
        Input: Self object.
        Output: Text file writing the average degree.

        Simple calculation that runs through the nodes in the object and finds the average degree and writes it to a file.

        """
        avg_degree = 0
        num_nodes = len(self.nodes.keys())
        if num_nodes:
            for edges in self.nodes.values():
                avg_degree += edges
            avg_degree = round(avg_degree * 1. / num_nodes, 2)
            output.write(str("{:.2f}".format(avg_degree)) + "\n")
        else:
            output.write("0.00\n")


    def run(self):
        """
        Input: Path to text file of tweets. In our case it's project/tweet_input/tweets.txt

        Output: Writes to output text through calc_average_degree.

        The main function that runs the process of parsing tweets for calculation of the average degree.

        """
        with open(self.outputfile, 'w') as text_output:
            with open(self.inputfile, 'r') as text_input:
                for line in text_input:
                    json_input = json.loads(line)
                    #filter because of issues with the Twitter api rate limit
                    invalid = filter(lambda limit: re.search(r'^{"limit', limit), json_input)
                    if json_input in invalid:
                        continue
                    entities = json_input.get("entities")
                    if entities is None:
                        continue
                    hash_tag_for_calc = self.acquire_tags(entities)
                    created_at = json_input.get("created_at")
                    if created_at is None:
                        continue
                    creation_time = self.acquire_time(created_at)
                    self.remove_expired_tweets(creation_time)
                    if self.flag == 0:
                        self.add_to_graph(hash_tag_for_calc, creation_time)
                        self.calc_average_degree(text_output)
                    else:
                        self.flag = 0
                        continue


if __name__ == "__main__":
    file_dir  = os.path.abspath(__file__)
    source_path = os.path.dirname(file_dir)
    directory_path = os.path.dirname(source_path)
    input_path = os.path.join(directory_path, 'tweet_input/tweets.txt')
    output_path = os.path.join(directory_path, 'tweet_output/output.txt')
    TweetCalc = Tweet_Graph(input_path, output_path)
    TweetCalc.run()
