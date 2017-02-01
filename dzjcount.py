#!python3
"""DJZ Char Counter

Usage:
  dzjcount.py [--dir <folder>]

Options:
  -h --help     Show this screen.
  --version     Show version.
  -d --dir      Specify the folder to store DZJ files
"""

import os
import sys
import glob
import re
import math
import statistics as sta


# Global Data
DATA = {
    "name": "大藏经",
    "zizhong_count": 0,
    "pstdev_total": 1.0,
    "all_zizhong": {
        # sample: "大": 101,
    },
    "all_sutra": [
        # sample: { "name": "abc", "book": "10", "order" : "20", "zizhong": { "大": 10, ...}, "pstdev": 1.0},
    ]
}

# -------------- ORM BEGIN --------------
from peewee import SqliteDatabase
from peewee import Model
from peewee import CharField, ForeignKeyField, IntegerField, DoubleField

DB_FP = 'sutra_chars.db'
if os.path.exists(DB_FP):
    os.remove(DB_FP)
db = SqliteDatabase(DB_FP)
db.set_autocommit(False)

class Zangjing_tongji(Model):
    zangjing_ming = CharField(max_length=100, default='大藏经')
    zizhong_shu = IntegerField()
    ping_heng_xing = DoubleField()

    class Meta:
        database = db


class Zangjing_zizhong_tongji(Model):
    zangjing_tongji_id = ForeignKeyField(Zangjing_tongji, related_name='all_zizhong_tongji')
    zizhong = CharField(max_length=100)
    ci_shu = IntegerField()

    class Meta:
        database = db


class Jingwen_tongji(Model):
    zangjing_tongji_id = ForeignKeyField(Zangjing_tongji, related_name='all_jingwen_tongji')
    ce_shu = CharField(max_length=4)
    xu_hao = CharField(max_length=5)
    zizhong_shu = IntegerField()
    jingwen_ming = CharField(max_length=100)
    ping_heng_xing = DoubleField()

    class Meta:
        database = db


class Jingwen_zizhong_tongji(Model):
    jingwen_tongji = ForeignKeyField(Jingwen_tongji, related_name='all_zizhong')
    zizhong = CharField(max_length=100)
    ci_shu = IntegerField()

    class Meta:
        database = db


# -------------- SCAN BEGIN --------------

RE_TITLE_LINE = re.compile(r'.*第\s*(\d+)\s*(?:冊|卷)\s*No.\s*(\w+)\s*(.*)$')

PUNCTUATIONS = set(['　'] # full-corner space
                  )


# prepare punctations
def prep_punctations_list():
    global PUNCTUATIONS
    PUNCTUATIONS.update([chr(v) for v in range(1, 255)] +
                        [chr(v) for v in range(ord('０'), ord('９') + 1)] +
                        [chr(v) for v in range(ord('ａ'), ord('ｚ') + 1)] +
                        [chr(v) for v in range(ord('Ａ'), ord('Ｚ') + 1)] +
                        [chr(v) for v in range(ord('─'), ord('╿') + 1)] # all table symbols
                        )
    with open('fuhao.txt', encoding='utf-8') as f:
        PUNCTUATIONS.update([p.strip() for p in f.readlines()])

def dump_result():
    """ Dump to DATABASE """

    # calc each sutra variance and merge all zizhong to one dict
    all_zizhong = {}
    for sutra in DATA['all_sutra']:
        sutra['pstdev'] = sta.pstdev(sutra['zizhong'].values())
        for ch, count in sutra['zizhong'].items():
            if ch in all_zizhong:
                all_zizhong[ch] += count
            else:
                all_zizhong[ch] = count

    # all zizhong pstdev value
    DATA['pstdev_total'] = sta.pstdev(all_zizhong.values())
    DATA['all_zizhong'] = all_zizhong
    DATA['zizhong_count'] = len(all_zizhong.keys())

    ################# DATABASE ###################
    # initialize DB
    db.connect()
    db.create_tables([Jingwen_tongji, Jingwen_zizhong_tongji, Zangjing_tongji, Zangjing_zizhong_tongji])
    db.begin()

    r_zangjing = Zangjing_tongji(zangjing_ming='大正藏',
                               zizhong_shu=DATA['zizhong_count'],
                               ping_heng_xing=DATA['pstdev_total'])
    r_zangjing.save()

    for sch, count in DATA['all_zizhong'].items():
        r_zangjing_zizhong = Zangjing_zizhong_tongji(zangjing_tongji_id=r_zangjing,
                                                     zizhong=sch,
                                                     ci_shu=count)
        r_zangjing_zizhong.save()

    for sutra in DATA['all_sutra']:
        r_sutra = Jingwen_tongji(zangjing_tongji_id=r_zangjing,
                                    ce_shu=sutra['book'],
                                    xu_hao=sutra['order'],
                                    zizhong_shu=len(sutra['zizhong']),
                                    jingwen_ming=sutra['name'],
                                    ping_heng_xing=sutra['pstdev'])
        r_sutra.save()

        for sch, count in sutra['zizhong'].items():
            zizhong = Jingwen_zizhong_tongji(jingwen_tongji_id=r_sutra,
                                             zizhong=sch,
                                             ci_shu=count)
            zizhong.save()

    print('DB writing start ...',)
    db.commit()
    print('DONE')


def scan_sutras(folder):

    RE_DIR_PART = re.compile(r'\d+-(\w+)$', re.I)
    RE_DIR_BOOK = re.compile(r'T(\d+)-f$', re.I)
    RE_FP_SUTRA = re.compile(r'T(\d+)n(\w+).txt$', re.I)

    skip_parts = {"18-xujingshu", "19-xulushu", "20-xuzhuzong", "21-xitan"}

    for d_part in glob.glob(os.path.join(folder, '??-*')):
        if not os.path.isdir(d_part):
            continue

        if d_part.split(os.path.sep)[-1] in skip_parts:
            continue

        m = RE_DIR_PART.search(d_part)
        if not m:
            continue

        for d_book in glob.glob(os.path.join(d_part, 'T*')):
            if not os.path.isdir(d_book):
                continue
            m = RE_DIR_BOOK.search(d_book)
            if not m:
                continue

            for f_sutra in glob.glob(os.path.join(d_book, 'T*.txt')):
                if not os.path.isfile(f_sutra):
                    continue
                m = RE_FP_SUTRA.search(f_sutra)
                if not m:
                    print('WARN: Found unexpected file:', f_sutra)
                    continue
                s_book, s_order = m.groups()
                #print(f_sutra, s_book, s_order)
                scan_single_sutra(f_sutra, s_book, s_order)



def scan_single_sutra(fp, s_book, s_order):
    chs = {}
    first_line = None
    with open(fp, encoding='utf8') as f:
        try:
            for line in f:
                line = line.strip()
                if first_line is None:
                    first_line = line
                    rem = re.match(RE_TITLE_LINE, line)
                    if rem:
                        v_book, v_order, s_name = rem.groups()
                        if not s_name.strip():
                            s_name = 'Unknown'
                    else:
                        print('WARN: file %s first line unmatched: \"%s\"' % (fp, line))
                        s_name = 'Unknown'
                        v_book='0000'
                        v_order='0000'

                if not line or 'No.' in line or 'NO.' in line or 'no.' in line:
                    continue

                # only appear in 'SKIPPED' parts
                if '暫未輸入' in line:
                    print('WARN: %s/%s/%s 暫未輸入, skip' %(sbook, s_order, fp))
                    return

                to_wait_bracket = False
                for ch in line:
                    if ch == '[':
                        to_wait_bracket = True
                        combo_ch = ''
                        continue
                    if to_wait_bracket:
                        if ch == ']':
                            ch = combo_ch
                            to_wait_bracket = False

                            if combo_ch in {"中阿含 (98)",
                                            "中阿含 (59)",
                                            "燉煌出 S. 700",
                                            "āmlam",
                                            "a-",
                                            "adhyāśsya*",
                                            "ta",
                                            "ka",
                                            "Paramārtha-deva",
                                            "Moksa-deva",
                                            "Mahāyāna-deva"}:
                                # skip it, TODO save these 3 chars if to be better than 99.99% perfect
                                continue
                        else:
                            combo_ch += ch
                            continue

                    if ch == '' or ch in PUNCTUATIONS:
                        continue

                    if ch not in chs:
                        chs[ch] = 1
                    else:
                        chs[ch] += 1
        except UnicodeDecodeError as err:
            print(err)


    if v_book != s_book or v_order != s_order:
        pass
        # it's only one exception: 0220 大般若波羅蜜多經(第1卷-第200卷)
        #print('WARN: first line unmatching pathinfo',)
        #print(fp, s_book, v_book, s_order, v_order, s_name, len(chs.keys()))

    # merge 大般若波羅蜜多經
    if s_order == '0220a':
        s_order = '0220'
        s_name = '大般若波羅蜜多經'
    elif s_order == '0220b' or s_order == '0220c':
        # merge to LAST one
        great_boruo = DATA['all_sutra'][-1]
        for ch, cnt in chs.items():
            if ch in great_boruo['zizhong']:
                great_boruo['zizhong'][ch] += cnt
            else:
                great_boruo['zizhong'][ch] = cnt
        return # on purpose

    DATA['all_sutra'].append({
        'name': s_name,
        'book': s_book,
        'order': s_order,
        'zizhong': chs})


if __name__ == '__main__':
    from docopt import docopt
    args = docopt(__doc__, version='DZJ char counter version 0.1')

    if args['--dir'] and args['dir']:
        folder = args['dir']
    else:
        #folder = 'samples'
        folder = '..\\T'

    prep_punctations_list()
    scan_sutras(folder)
    dump_result()
