# This script operates on the wikipedia already in json format, as
# created by WikiExtractor.py
#

import multiprocessing as mp
import numpy as np
import gensim
from gensim.models.ldamulticore import LdaMulticore
from gensim.corpora.textcorpus import TextCorpus
import json
import os
import sys
from pprint import pprint
import bz2

import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

NCORE = 8

raw_path = sys.argv[1]
glove_path = sys.argv[2]
output_path = sys.argv[3]

class WikiCorpus(TextCorpus):

    def getstream(self):
        num_texts = 0
        for root, dirs, files in os.walk(self.input):
            for fname in files:
                path = os.path.join(root, fname)
                with bz2.open(os.path.join(root, fname)) as fp:
                #with bz2.open(os.path.join(root, fname), encoding='utf8') as fp:
                    for line in fp.readlines():
                        data = json.loads(line)
                        yield data
                        num_texts += 1
        self.length = num_texts

    def get_texts(self):
        docs = self.getstream()
        for doc in docs:
            if self.metadata:
                text = doc['text']
                del doc['text']
                yield (self.preprocess_text(text), doc)
            else:
                yield self.preprocess_text(doc['text'])

class GloveMap(object):
    def __init__(self, path):
        self.mapping = {}
        self.dimension = None
        with open(path, encoding="utf8") as fp:
            for line in fp.readlines():
                tokens = line.split()
                self.mapping[tokens[0]] = np.array([float(t) for t in tokens[1:]])
                if self.dimension is None:
                    self.dimension = len(tokens) - 1

    def get(self, word):
        return self.mapping.get(word)

    def map_bow(self, mapping, bow):
        vec = np.zeros(self.dimension)
        cnt = 0
        for (word_idx, count) in bow:
            wordvec = self.get(mapping[word_idx])
            if wordvec is not None:
                vec += (wordvec * count)
                cnt += 1
        if cnt > 0:
            return vec / np.float(cnt)
        else:
            return None


def load_lda():
    if not os.path.exists("wiki.ldamodel"):
        corpus = WikiCorpus(raw_path, metadata=False)
        print("Training LDA")
        lda = LdaMulticore(corpus, num_topics=100, 
                          # update_every=1, 
                           passes=1)
        print("Saving LDA")
        lda.save('wiki.ldamodel')
    else:
        print("Model file found, loading")
        lda = LdaMulticore.load("wiki.ldamodel")
    return lda

# Force loading before multiprocessing
lda = load_lda()

def process(queue, dictionary, out_fp):
    if out_fp is None:
        proc_id = mp.current_process().pid
        fname = output_path + "." + str(proc_id)
        print("Writing process output to ", fname)
        out_fp = bz2.open(fname, "wb")
    lda = load_lda()
    while True:
        pair = queue.get()
        if pair is None:
            out_fp.close()
            print("Closed output file")
            break
        (bow, meta) = pair
        vector = glove.map_bow(dictionary, bow)
        # Encode the sparse vector in a format amenable
        # for Spark processing
        if vector is not None:
            topics = lda.get_document_topics(bow, minimum_probability=0.1)
            vector = " ".join([str(c) for c in vector])
            outdata = {
                'id': int(meta['id']),
                'title': meta['title'],
                'topic': [p[0] for p in topics],
                'vector': vector
            }
            jstr = json.dumps(outdata)
            out_fp.write(bytes(jstr, encoding="utf8"))
            out_fp.write(bytes('\n', encoding="utf8"))


print("Building gloveMap")
glove = GloveMap(glove_path)
print("Writing output, with remapping to Glove vectors")
corpus = WikiCorpus(raw_path, metadata=True)

# queue = mp.Queue(maxsize=NCORE)
# pool = mp.Pool(NCORE, 
#                initializer=process, 
#                initargs=(queue, corpus.dictionary))

out_fp = bz2.open(output_path, "wb")
dictionary = corpus.dictionary

for (bow, meta) in corpus:
    vector = glove.map_bow(dictionary, bow)
    # Encode the sparse vector in a format amenable
    # for Spark processing
    if vector is not None:
        topics = lda.get_document_topics(bow, minimum_probability=0.1)
        vector = " ".join([str(c) for c in vector])
        outdata = {
            'id': int(meta['id']),
            'title': meta['title'],
            'topic': [p[0] for p in topics],
            'vector': vector
        }
        jstr = json.dumps(outdata)
        out_fp.write(bytes(jstr, encoding="utf8"))
        out_fp.write(bytes('\n', encoding="utf8"))

out_fp.close()

# for _ in range(NCORE):  # tell workers we're done
#     queue.put(None)
# pool.close()
# pool.join()


