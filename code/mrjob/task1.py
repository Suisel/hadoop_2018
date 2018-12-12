from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"[\w']+")


class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield None, [len(word), word.lower()]


    def reducer(self, _, length_word):
        yield max(length_word)


if __name__ == '__main__':
    MRWordFreqCount.run()
    
#result:
# root@quickstart:/# hadoop fs -cat "hdfs:///user/root/task1_res/*"
# 63	"rindfleischetikettierungs\u00fcberwachungsaufgaben\u00fcbertragungsgesetz"
