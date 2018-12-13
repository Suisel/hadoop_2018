from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

WORD_RE = re.compile(r"[A-Za-zА-Яа-я]+")
UPPER_RE = re.compile(r"A-ZА-Я")


class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            if 'Я' >= word[0] >= 'А' or 'Z' >= word[0] >= 'A':
                yield word.lower(), True
            else:
                yield word.lower(), False

    def reducer(self, word, counts):
        upper_count = 0
        lower_count = 0

        for i in counts:

            if i:
                upper_count += 1
            else:
                lower_count += 1

        if (lower_count + upper_count) > 10 and upper_count > (lower_count + upper_count) / 2:
            yield word, (upper_count, lower_count)

if __name__ == '__main__':
    MRWordFreqCount.run()