#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import ast

class MRTopicPageRankPre(MRJob):
    def mapper(self, _, line):
        node, topic_outlinks = line.split('\t')
        node = node.replace('"', '')
        topic_outlinks = topic_outlinks.replace('"', '')
        out_links, topic = topic_outlinks.split('|')
        pr = [ 1.0 / 100 ] * (10 + 1)

        yield node, out_links + "|" + topic + "|" + str(pr)

if __name__ == '__main__':
    MRTopicPageRankPre.run()