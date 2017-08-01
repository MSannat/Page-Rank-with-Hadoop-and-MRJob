# START STUDENT CODE 9.1 DRIVER
from PageRankTopics import PageRankTopics
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import ast
import subprocess

INPUT_GRAPH = sys.argv[1]
DAMPING_FACTOR = sys.argv[2]
TOTAL_NODES = sys.argv[3]
MAX_ITERATION = int(sys.argv[4])
MODE = sys.argv[5]

num_interation = 0

if MODE == 'hadoop':
    mr_job = PageRankTopics(args=[INPUT_GRAPH, '--dampingfactor', DAMPING_FACTOR, '--numtotalnodes', TOTAL_NODES,  '-r', 'hadoop', 
                                  '--topic_counts', 'topic_counts.txt', '--no-output', '--cleanup', 'NONE', '--no-check-input-paths', '--output-dir',  '/tmp/msannat/temp-output' ])
    while (num_interation<MAX_ITERATION):
        with mr_job.make_runner() as runner: 
            num_interation += 1
            print "Iteration: ", num_interation
            subprocess.Popen(["hadoop", "fs", "-rm", "-r", "/tmp/msannat/temp-output/"], stdout=None)
            runner.run()
            f = open(INPUT_GRAPH, 'w+')
            cat = subprocess.Popen(["hadoop", "fs", "-cat", "/tmp/msannat/temp-output/part-*"], stdout=subprocess.PIPE)
            for line in cat.stdout:
                #print line
                f.writelines(line)

        f.close()
        
        
else:
    mr_job = PageRankTopics(args=[INPUT_GRAPH, '--dampingfactor', DAMPING_FACTOR, '--numtotalnodes', TOTAL_NODES, 
                                  '--topic_counts', './topic_counts.txt',  '-r', 'local' ])
    while (num_interation<MAX_ITERATION):
        with mr_job.make_runner() as runner: 
            num_interation += 1
            print "Iteration: ", num_interation
            runner.run()
            f = open(INPUT_GRAPH, 'w+')
            for line in runner.stream_output():
                #print line
                f.writelines(line)

        f.close()
        
# END STUDENT CODE 9.1 DRIVER