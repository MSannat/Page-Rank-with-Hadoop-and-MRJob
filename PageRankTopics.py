# START STUDENT CODE 9.4
# (ADD CELLS AS NEEDED)

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
import sys

class PageRankTopics(MRJob):
    
    def steps(self):
        JOBCONF_STEP1 = {
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            #'mapreduce.text.key.comparator.options': '-k2,2nr -k1,1',
            #'mapred.text.key.comparator.options': '-k2,2nr -k1,1', 
            'mapreduce.job.reduces': 1
        }
        
      
        return [MRStep(jobconf=JOBCONF_STEP1, 
                       mapper=self.mapper, 
                       reducer=self.reducer),
                MRStep(reducer_init=self.reducer_pagerank_init,
                       reducer=self.reducer_pagerank)
                ]
        
        
    
    
    def configure_options(self):
        super(PageRankTopics, self).configure_options()
        self.add_passthrough_option('--dampingfactor', default='0.85', type=str, help='damping factor')
        self.add_passthrough_option('--numtotalnodes', default=None, type=str, help='total number of nodes in the graph')
        self.add_passthrough_option('--numtopics', default='10', type=str, help='total number of topics')
        self.add_passthrough_option('--beta', default='0.98', type=str, help='beta factor')
        self.add_file_option('--topic_counts', help='topic counts file')
    
    
    def mapper(self, _, line):
        pass
        
        node = None
        adjacency_list = None
        initial_page_rank = 0.0
        neighbors = 0
        page_rank_list = []
        
        topic = ""
        
        if line!= "":
            node, graph_pr_out_links = line.strip().split('\t')
            node =  node.replace('"', '')
        
            adjacency_list, topic, page_rank_list, = map(ast.literal_eval, graph_pr_out_links.strip("\"").strip('"').strip("'").split('|'))
            neighbors = len(adjacency_list)
             
            if neighbors> 0:
                page_rank_updated =  [page_rank*1.0/neighbors for page_rank in page_rank_list]
                for key in adjacency_list.keys():
                    yield (key),  (page_rank_updated)
                    
            if neighbors== 0:
                yield '**dangling', (page_rank_list) 
                
            yield (node), (graph_pr_out_links)
        
        
    
    def reducer(self, key, values):
        node = None
        adjacency_list = {}
        total = 0.0
        node =  key.strip("'")
        lost_mass = [0] * (int(self.options.numtopics) + 1)
        updated_page_rank = [0] * (int(self.options.numtopics) + 1)
        
        if key == '**dangling':
            for value in values:
                for j in range(len(value)):
                    lost_mass[j]+= float(value[j])
            
            for i in range(1, int(self.options.numtotalnodes) + 1):
                yield str(i), lost_mass
        
        else:
            for value in values:
                if type(value) == list:
                    for k in range(len(value)):
                        updated_page_rank[k]+= float(value[k])
                else:
                    adjacency_list, topic, page_rank_ori = map(ast.literal_eval, value.strip("\"").strip('"').strip("'").split('|'))

            yield node , (str(adjacency_list) + '|' + str(topic)  + '|' + str(updated_page_rank) )
        
    
    def reducer_pagerank_init(self):
        self.topic_counts = {}        
        with open('topic_counts.txt', 'r') as topic_counts:
            for line in topic_counts.readlines():
                count, topic  = line.strip().split("\t")
                self.topic_counts[int(topic)] = int(count)
        
    
    def reducer_pagerank(self, node, pagerank_adj_list):
        lost_mass = [0] * (int(self.options.numtopics) + 1)

        page_rank_updated = []
        adj_list = {}
        num_topics = int(self.options.numtopics) + 1
        
        alpha = 1 - float(self.options.dampingfactor)
        total_nodes = float(self.options.numtotalnodes)
        beta = float(self.options.beta)
        
        for pagerank_link in pagerank_adj_list:
            if type(pagerank_link) == list:
                lost_mass = pagerank_link
            else:
                adj_list, topic, pagerank = map(ast.literal_eval, pagerank_link.strip("\"").split('|'))
        
        for topic_num in range(1, num_topics):
            curr_topic = self.topic_counts[topic_num]
            #yield node, self.topic_counts[topic_num]
            
            if (topic_num) == int(topic):
                #pass
                page_rank_updated.append(alpha * (beta/curr_topic) + (1-alpha) * (lost_mass[topic_num-1]/total_nodes + float(pagerank[topic_num-1])))
            else:
                page_rank_updated.append(alpha * ((1-beta)/(total_nodes-curr_topic)) + (1-alpha) * (lost_mass[topic_num-1]/total_nodes + float(pagerank[topic_num-1])))

        page_rank_updated.append(alpha * (1/total_nodes) + (1-alpha) * (lost_mass[topic_num-1]/total_nodes + float(pagerank[topic_num-1])))
                
        yield node, str(adj_list) + "|" + str(topic) + "|" + str(page_rank_updated)
                
        
            
if __name__ == '__main__':
    PageRankTopics.run()    
# END STUDENT CODE 9.4     