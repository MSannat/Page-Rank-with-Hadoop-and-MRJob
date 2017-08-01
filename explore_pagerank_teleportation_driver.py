####
# Using HW9.1 implmentation of the iterative PageRank algorithm, run several MRJobs with different
#damping factors: [0,0.25,0.5,0.75, 0.85, 1] and collect resulting pagerank probablities.
####
# START STUDENT CODE 9.2
# (ADD CELLS AS NEEDED)

from PageRank import PageRank
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import ast
import subprocess
from shutil import copyfile
import time

SOURCE_GRAPH = './wikipedia_example_updated.txt'
DAMPING_FACTOR = [0, 0.25, 0.5, 0.75, 0.85, 1]
TOTAL_NODES = 11
MAX_ITERATION = 30
MODE = 'local'

for d in DAMPING_FACTOR:
    print 'Started job with damping factor:', d
    start_time = time.time()
    INPUT_GRAPH = './wikipedia_example_input_with_'+str(d)+'.txt'
    copyfile(SOURCE_GRAPH, INPUT_GRAPH)
   
    mr_job = PageRank(args=[INPUT_GRAPH, '--dampingfactor', str(d), '--numtotalnodes', str(TOTAL_NODES),  '-r', 'local' ])
    num_interation = 0
    while(num_interation<MAX_ITERATION):
        
        with mr_job.make_runner() as runner: 
            
            num_interation += 1
            #print "Iteration: ", num_interation
            runner.run()
        
            f = open(INPUT_GRAPH, 'w+')
            for line in runner.stream_output():
                f.writelines(line)

        f.close()
        
    end_time = time.time()
    print "Time taken = {:.2f} seconds for d = {}".format(end_time - start_time, d)

# END STUDENT CODE 9.2