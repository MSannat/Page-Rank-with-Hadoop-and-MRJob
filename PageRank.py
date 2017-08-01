# START STUDENT CODE 9.1
# (ADD CELLS AS NEEDED)

#Working
from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
import sys

class PageRank(MRJob):
    
    def steps(self):
        JOBCONF_STEP1 = {
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            #'mapreduce.text.key.comparator.options': '-k2,2nr -k1,1',
            #'mapred.text.key.comparator.options': '-k2,2nr -k1,1', 
            'mapreduce.job.reduces': 16
        }
    
        return [MRStep(jobconf=JOBCONF_STEP1, 
                       mapper=self.mapper, 
                       reducer=self.reducer),
                MRStep(reducer=self.reducer_pagerank)
                ]
       
    
    
    def configure_options(self):
        super(PageRank, self).configure_options()
        self.add_passthrough_option('--dampingfactor', default='0.85', type=str, help='damping factor')
        self.add_passthrough_option('--numtotalnodes', default=None, type=str, help='total number of nodes in the graph')
    
    
    def mapper(self, _, line):
        node = None
        adjacency_list = None
        initial_page_rank = 0.0
        neighbors = 0 
        if line!= "":
            node, graph_pr_out_links = line.strip().split('\t')
            node =  node.replace('"', '')
        
            adjacency_list, initial_page_rank = map(ast.literal_eval, graph_pr_out_links.strip("\"").strip('"').strip("'").split('|'))
            neighbors = len(adjacency_list)
            
            if neighbors> 0:
                for key in adjacency_list.keys():
                    yield (key),  (1.0* initial_page_rank /neighbors)
                    
            if neighbors== 0:
                yield '**dangling', (1.0* initial_page_rank) 
                
            yield (node), (adjacency_list)
        
    
    def reducer(self, key, values):
        node = None
        adjacency_list = {}
        total = 0.0
        node =  key.strip("'")
        #values = [v  for v in values]
        redistribute_mass = 0.0
        
        
        if node == '**dangling':
            redistribute_mass= sum([float(v) for v in values])
            for i in range(1, int(self.options.numtotalnodes) + 1):
                yield str(i), redistribute_mass
        
        else:
            for pr_out_link in values:
                if type(pr_out_link) == float:
                    total = total + float(pr_out_link)

                elif type(pr_out_link) == dict:
                    adjacency_list = pr_out_link

            yield node , (str(adjacency_list) + '|' + str(total) )
        
    
            
    def reducer_pagerank(self, node, page_rank_adj_lists):
        lost_mass = 0.0
        page_rank = 0.0
        adj_lists = {}
        
        alpha = 1 - float(self.options.dampingfactor)
        total_nodes = float(self.options.numtotalnodes)
        
        for page_rank_adj_list in page_rank_adj_lists:
            if type(page_rank_adj_list) == float:
                lost_mass = page_rank_adj_list
            else:
                adj_lists, page_rank = map(ast.literal_eval, page_rank_adj_list.strip("\"").split('|'))
                    
        page_rank_updated = alpha * (1/total_nodes) + (1-alpha) * (float(lost_mass)/total_nodes + float(page_rank))
                
        yield node, str(adj_lists) + "|" + str(round(page_rank_updated, 6))
                
        
if __name__ == '__main__':
    PageRank.run()    
# END STUDENT CODE 9.1      