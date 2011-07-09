#!/usr/bin/env python

from datetime import datetime, timedelta
import simplejson as json
import sys
from random import triangular, choice, random


class Posting(object):
    def __init__(self, account, amount=None):
        self.account = account
        self.amount = amount

    def generate(self):
        if self.amount == None:
            amount = ''
        elif isinstance(self.amount, list):
            amount = '$%.2f' % triangular(*self.amount)
        else:
            amount = '$%.2f' % self.amount
            
        return "    %s    %s" % (
            self.account,
            amount)


class Entry(object):

    def __init__(self, **kwargs):

        self.postings = []
        self._has_generated = False

        for k,v in kwargs.items():
            setattr(self, k, v)

        if not isinstance(self.payee, list):
            self.payee = [self.payee]

        if hasattr(self, 'start'):
            self.start = datetime.strptime(self.start, '%Y/%m/%d')

        if hasattr(self, 'end'):
            self.end = datetime.strptime(self.end, '%Y/%m/%d')
            
    def _should_generate_for_date(self, gendate):

        if hasattr(self, 'start'):
            if gendate < self.start:
                return False

        if hasattr(self, 'end'):
            if gendate > self.end:
                return False

        if hasattr(self, 'once') and self._has_generated == False:
            self._has_generated = True
            return True

        if hasattr(self, 'day'):
            if type(self.day) == list:
                return any([d == gendate.day for d in self.day])
            elif self.day == gendate.day:
                return True

        if hasattr(self, 'dow'):
            if isinstance(self.dow, list):
                return any([d == gendate.weekday() for d in self.dow])
            elif self.dow == gendate.weekday():
                return True

        return False

    def _should_generate_for_probability(self):
        if hasattr(self, 'probability'):
            return self.probability >= random()
        return True

    def should_generate(self, gendate):
        return self._should_generate_for_probability() \
            and self._should_generate_for_date(gendate)

    def generate(self, date):
        return "%s * %s\n%s" % (
            date.strftime('%Y/%m/%d'),
            choice(self.payee),
            "\n".join([p.generate() for p in self.postings]))

    def add_posting(self, account, amount=None):
        self.postings.append(Posting(account, amount))

entries = []
with open(sys.argv[1]) as f:
    for entry in json.loads(f.read()):
        postings = entry.pop('postings')
        e = Entry(**entry)

        for p in postings:
            e.add_posting(*p)

        entries.append(e)

start = datetime(year=2008, month=01, day=1)
end = datetime(year=2011, month=7, day=7)

while start <= end:
    for entry in entries:
        if entry.should_generate(start):
            print entry.generate(start)

    start = start + timedelta(1)
    
