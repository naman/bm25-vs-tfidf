import os
import re
import sys
import math
import json
import string
from porter_stemmer import PorterStemmer
from stop_words import get_stop_words
from operator import itemgetter


def add_to_dictionary(docid, word):
    if word not in dictionary.keys():
        posting = {
            'id': docid,
            'frequency': 1,
            'tf_idf_weight': 0,
            # 'positions': [position]
        }

        dictionary[word] = [posting]  # creating postings list
    else:
        # word already in dictionary
        present = False
        for doc in dictionary[word]:  # iterate over postings of that word
            if doc['id'] == docid:  # doc already present as a posting
                doc['frequency'] += 1  # found another use in the same doc
                # doc['positions'].append(position)

                present = True
                break

        if not present:  # doc not present in the term index
            posting = {
                'id': docid,
                'frequency': 1,  # first use of the word in this doc
                'tf_idf_weight': 0,
                # 'positions': [position]
            }

            dictionary[word].append(posting)


def calc_words_in_doc(docid):
    c = 0
    for x in doc_lengths:
        if x['id'] == docid:
            c = x['count']
            break
    return c


def normalize_index():
    print "Calculating idf weights for each term(in all documents)!"
    idf_weights = {}

    for term in dictionary:
        # document frequency, how many docs does the term lie in
        df = len(dictionary[term])
        idf = math.log10(N / float(df))
        idf_weights[term] = idf

    print "Calculating term frequencies for a term in each doc."

    for docid in xrange(1, N + 1):
        for term in dictionary:
            for doc in dictionary[term]:
                if doc['id'] == docid:
                    frequency = doc['frequency']
                    D = calc_words_in_doc(docid)
                    tf = frequency / float(D)
                    # idf is constant for all the docs a term appears in
                    idf = idf_weights[term]
                    # score for each term in doc
                    doc['tf_idf_weight'] = tf * idf
                    break


def build_index():
    f = open(path_to_document)
    # position = 0
    doc_id = 0
    word_count = 0

    for line in f:
        if "#" in line:
            # position = 0
            tmp = {
                'id': doc_id,
                'count': word_count
            }
            doc_lengths.append(tmp)

            doc_id += 1  # next doc now
            word_count = 0  # reset
        else:
            words_in_line = special_split(line)

            for word in words_in_line:
                # position += 1
                word_count += 1

                if word not in stop_words and not word.isdigit():
                    # already stemmed
                    # word = porter.stem(word, 0, len(word) - 1)
                    add_to_dictionary(doc_id, word)
                    # add_to_dictionary(doc_id, position, word)
    f.close()


def MultiWordQ(query):
    focus_terms = clean_words(special_split(query))

    results_dict = {}
    scores_doc = {}
    for query in focus_terms:
        if query in dictionary:
            results_dict[query] = []
            for x in dictionary[query]:
                tmp = {
                    'doc': x['id']
                }
                results_dict[query].append(tmp)

            for doc in dictionary[query]:
                doc_score = doc['tf_idf_weight']
                # print query, "\t", doc['id'], "\t", doc_score
                if doc['id'] in scores_doc.keys():
                    scores_doc[doc['id']] += doc_score
                else:
                    scores_doc[doc['id']] = doc_score
    ranked = reversed(sorted(scores_doc.items(), key=itemgetter(1)))
    rank = 1
    print "Rank", "\tdocID", "\tScore"
    for x in ranked:
        if rank == 11:
            break
        print rank, "\t", x[0], "\t", x[1]
        rank += 1


def load_stop_words():
    # x = stopwords.words("english")
    x = get_stop_words("en")
    return [s.encode('ascii') for s in x] + list(string.printable)


def clean_split(string):
    return re.split('|'.join(map(re.escape, delimiters)), string.lower().strip())


def special_split(string):
    x = clean_split(string)
    return filter(lambda a: a != "", x)


def write_to_file(text, path):
    with open(path, 'w') as outfile:
        json.dump(text, outfile, sort_keys=True, indent=4)


def load_index_in_memory_list(path):
    with open(path) as data_file:
        var = list(json.load(data_file))
    return var


def load_index_in_memory(path):
    with open(path) as data_file:
        var = dict(json.load(data_file))
    return var


def clean_words(array):
    cleaned_words = []
    for word in array:
        if (word is '') or (word in stop_words) or (word.isdigit()):
            continue
        else:
            word = porter.stem(word, 0, len(word) - 1)
            cleaned_words.append(word)
    return cleaned_words


def calc_avgdl():
    num = len(doc_lengths)
    total = sum(x['count'] for x in doc_lengths)
    return total / float(num)


def BM(query):
    focus_terms = clean_words(special_split(query))
    avgdl = calc_avgdl()

    results_dict = {}  # relevant docs too!
    scores_doc = {}

    for query in focus_terms:
        if query in dictionary:
            results_dict[query] = []
            for doc in dictionary[query]:
                tmp = {
                    'doc': doc['id']
                }
                results_dict[query].append(tmp)

            # the number of docs query term is in
            num_relevant_docs = len(results_dict[query])

            # calculate idf for query term
            idf = math.log10(N - num_relevant_docs + 0.5 /
                             float(num_relevant_docs + 0.5))

            # print "Query Term", "\t", "docID", "\t", "Score"
            for doc in dictionary[query]:
                D = calc_words_in_doc(doc['id'])
                tf = doc['frequency'] / float(D)
                num = tf * (k + 1)
                denom = tf + k * (1 - b + b * (D / float(avgdl)))
                doc_score = idf * (num / float(denom))
                # print query, "\t", doc['id'], "\t", doc_score
                if doc['id'] in scores_doc.keys():
                    scores_doc[doc['id']] += doc_score
                else:
                    scores_doc[doc['id']] = doc_score

    ranked = reversed(sorted(scores_doc.items(), key=itemgetter(1)))
    rank = 1
    print "Rank", "\tdocID", "\tScore"
    for x in ranked:
        if rank == 11:
            break
        print rank, "\t", x[0], "\t", x[1]
        rank += 1


def run_query(query):
    print "TF-IDF\n"
    MultiWordQ(query)
    print "\nBM25\n"
    BM(query)


def take_commands():
    print "Please enter your query at the prompt!\n"
    while 1:
        sys.stdout.write("> ")
        query = raw_input().strip()
        run_query(query)


if len(sys.argv) < 4:
    print "USAGE: python search_engine.py <inverted_index> <stop_words> <path_to_doc>\n"
    print "PLEASE USE STOP WORDS IF YOU ALREADY HAVE IT."
    print "PLEASE USE PATH TO DOCUMENTATION IF YOU ALREADY HAVE IT."

    exit(1)

N = 3204
k = 1
b = 0.5
path_to_document = sys.argv[3]
doc_lengths = []

stop_words = []
delimiters = ['\n', ' ', ',', '.', '?', '!', ':', ';', '#', '$', '[', ']',
              '(', ')', '-', '=', '@', '%', '&', '*', '_', '>', '<',
              '{', '}', '|', '/', '\\', '\'', '"', '\t', '+', '~',
              '^', '\u']
dictionary = {
    'code': [{
        'id': 0,  # primary_key
        'frequency': 1,
        'tf_idf_weight': 0,
        # 'positions': [1]
    }]  # postings_list
}

porter = PorterStemmer()

os.system("clear")

print ".........................................................."
print "\t\tWelcome to End Sem!"
print "..........................................................\n"

print "Do you want to update/build inverted index?[y/n]"
if raw_input() == 'y':
    print "Loading Stop Words..."
    stop_words = load_stop_words()

    print "Building inverted index..."
    build_index()

    print "Normalizing!"
    normalize_index()
    print "Normalizing complete!"

    print "Writing the inverted word index to", sys.argv[1]
    write_to_file(dictionary, sys.argv[1])
    write_to_file(doc_lengths, "doc_lengths.json")

    print "Data munching complete!"
    print "Complete!"
    take_commands()
else:
    print "Congrats! You just saved 5 minutes in your life.\n"
    print "Loading Stop Words..."
    stop_words = load_stop_words()

    print "Loading inverted index in memory..."
    dictionary = load_index_in_memory(sys.argv[1])
    doc_lengths = load_index_in_memory_list("doc_lengths.json")

    if dictionary == {} or doc_lengths == []:
        print "error"
        exit(-1)
    print "Loaded inverted index in memory!"

    take_commands()
