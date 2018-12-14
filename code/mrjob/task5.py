from mrjob.job import MRJob
import re

from mrjob.protocol import TextProtocol, ReprProtocol

WORD_RE = re.compile(r" [a-zа-я][a-zа-я]\.")
threshold = 50

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
# "вв." 211
# "га." 190
# "гг." 1240
# "др." 2399
# "ед." 82
# "им." 914
# "их." 158
# "кв." 247
# "кг." 59
# "км." 367
# "мл." 56
# "мм." 96
# "ок." 208
# "пр." 223
# "св." 316
# "см." 325
# "ср." 80
# "ст." 415
# "ул." 326
# "юг." 52