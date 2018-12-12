from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"[a-zA-Z]+")


class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def combiner(self, word, counts):
        yield None, (sum(counts), word)

    def reducer(self, _, counts):
        yield max(counts)


if __name__ == '__main__':
    MRWordFreqCount.run()

# result:
# 2985	"formula"