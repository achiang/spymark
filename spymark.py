#!/usr/bin/python

import sys
import sqlite3
sys.path.append('ofxparse')
from ofxparse import OfxParser
sys.path.append('ystockquote')
import ystockquote
import datetime
from datetime import timedelta

class Spymark(object):
    def __init__(self, db_name):
        # Assume we're running under test if db_name isn't supplied
        if not db_name:
            print "Using in-memory SQL database"
            db_name = ":memory:"

        self.db = sqlite3.connect(db_name, detect_types=sqlite3.PARSE_DECLTYPES)
        c = self.db.cursor()

        # "if not exists" requires sqlite 3.3+
        c.execute('''CREATE TABLE IF NOT EXISTS stocks
                  (id text, ticker text, uniqueid text, tradeDate timestamp,
                  settleDate timestamp, units real, unit_price real,
                  spy_units real, spy_price real)''')

    def import_ofx(self, ofx_file):
        ofx = OfxParser.parse(file(ofx_file))
        idx = {}
        for s in ofx.security_list:
            idx[s.uniqueid] = s.ticker

        c = self.db.cursor()

        for t in ofx.account.statement.transactions:
            c.execute("SELECT id FROM stocks WHERE id = ?", [t.id])
            row = c.fetchone()
            if row:
                print "Skipping duplicate transaction:", t.id
                continue

            spydate = t.tradeDate
            # Fidelity transactions can "close" on a weekend?!?
            if spydate.weekday() == 5:
                spydate = spydate - timedelta(days=1)
            elif spydate.weekday() == 6:
                spydate = spydate - timedelta(days=2)
            spy = ystockquote.get_historical_prices('SPY',
                    spydate.strftime("%Y%m%d"), spydate.strftime("%Y%m%d"))
            spy_price = float(spy[1][4])
            spy_units = (float(t.units) * float(t.unit_price)) / spy_price

            c.execute("INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (t.id, idx[t.security], t.security, t.tradeDate, t.settleDate,
                float(t.units), float(t.unit_price), spy_units, spy_price))

        self.db.commit()

    def display(self):
        c = self.db.cursor()

        now = datetime.datetime.now()
        if now.weekday() == 5:
            now = now - timedelta(days=1)
        elif now.weekday() == 6:
            now = now - timedelta(days=2)
        now = now.strftime("%Y%m%d")

        spynow = ystockquote.get_historical_prices('SPY', now, now)
        spynow = float(spynow[1][4])

        for row in c.execute("SELECT * FROM stocks ORDER BY tradeDate"):
            cur = ystockquote.get_historical_prices(row[1], now, now)
            cur = float(cur[1][4])
            orig = row[5] * row[6]
            spy_orig = row[7] * row[8]
            if row[5] < 0:
                continue
            print "Ticker:", row[1]
            print "  Date:", row[3]
            print "  Unit Price:", row[6]
            print "  SPY Price:", row[8]
            print "  Current Price:", cur
            print "  Current SPY:", spynow
            print "  Return:", 100 * ((cur * row[5]) - orig) / orig
            print "  SPY Return:", 100 * ((spynow * row[7]) - spy_orig) / spy_orig

def main(args):
    spy = Spymark("spymark.db")

    if (len(args) > 1):
        spy.import_ofx(args[1])
    else:
        spy.display()

if __name__ == "__main__":
    main(sys.argv)
