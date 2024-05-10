# -*- coding: utf-8 -*-
import itertools
import operator

from six.moves import zip

first = operator.itemgetter(0)

def windows(iterable, length):
    args = itertools.tee(iterable, length)
    # advance each iterator as many steps as its rank-1
    # thus, advance the 1st iterator none, 2nd iterator once. etc.
    for i in range(len(args)-1, 0, -1):
        for iter_ in args[i:]:
            next(iter_, None)
    return zip(*args)


def kmer_set(iterable, kmer_lengths=(2,)):
    ret = set()
    for k in kmer_lengths:
        ret.update(windows(iterable, k))
    return ret


def distance(a_str, b_str, kmer_lengths=(2,)):
    a_chunks = kmer_set(a_str, kmer_lengths)
    b_chunks = kmer_set(b_str, kmer_lengths)
    return sum((len(a_chunks.difference(b_chunks)),
                len(b_chunks.difference(a_chunks))))


def similarity(a_str, b_str, kmer_lengths=(2,)):
    a_chunks = kmer_set(a_str, kmer_lengths)
    b_chunks = kmer_set(b_str, kmer_lengths)
    return len(a_chunks.intersection(b_chunks))


def min_with_ties(iterable, key=None):
    iterable = iter(iterable)
    if not key:
        key = lambda val : val

    try:
        ret = [next(iterable)]
    except StopIteration:
        return ret

    for item in iterable:
        if key(item) < key(ret[0]):
            ret = [item]
        elif key(item) == key(ret[0]):
            ret.append(item)

    return ret
    

def closest(needle_str, haystack, kmer_lengths=(2,)):
    distances = [ (distance(needle_str, s, kmer_lengths), s)
                  for s in haystack ]
    return min_with_ties(distances, key=first)


def find_match(needle_str, haystack_strs, kmer_lengths=(2,)):
    haystack = set(haystack_strs)
    distances = [ (distance(needle_str, s, kmer_lengths), s)
                  for s in haystack ]
    d, match = min(distances, key=first)
    return match
