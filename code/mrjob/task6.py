from mrjob.job import MRJob
import re
from mrjob.protocol import TextProtocol, ReprProtocol

WORD_RE = re.compile(r"[a-zа-я]\. ?[a-zа-я]\.")
threshold = 40

class MRFindAbbreviation(MRJob):
    OUTPUT_PROTOCOL = ReprProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield ''.join(word.split()), 1

    def reducer(self, word, count):
        s = sum(count)
        if s > threshold:
            yield word, s


if __name__ == '__main__':
    MRFindAbbreviation.run()

# result
# "а.е."    70
# "в.д."    46
# "в.м."    68
# "в.н."    113
# "г.н."    63
# "г.р."    56
# "д.ч."    50
# "л.с."    79
# "н.э."  1117
# "с.ш."    117
# "т.д."    172
# "т.е."    138
# "т.н."    53
# "т.п."    82
# "ю.ш."    80