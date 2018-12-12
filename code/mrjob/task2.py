from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re
from statistics import mean

WORD_RE = re.compile(r"[\w']+")


class MRAvgWordLength(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield 1, len(word)


    def reducer(self, _, length_word):
        length = 0
        count = 0
        for i in length_word:
            length += i
            count += 1
        yield None, length / count

if __name__ == '__main__':
    MRAvgWordLength.run()

# result:
# null  6.057031234327786