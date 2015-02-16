#!/usr/local/bin/python
import time
from string import strip
from pandas import Series
import re
from hashlib import md5
from  argparse import ArgumentParser
import os
from mongoHelper import Mongo

FILE_LOCATION  = "hash_dict"
HOME_PATH = os.environ['HOME']
LOCATION = os.path.join(HOME_PATH+"/Documents",FILE_LOCATION)
db =None
if  not os.path.exists(LOCATION):
    os.mkdir(LOCATION)
    print "mkdir directory to save hash dict"

class Counter:
    count = 0
    
    @staticmethod
    def co():
        Counter.count += 1

    @staticmethod
    def ze():
        Counter.count = 0

def save_db(h,p):
#    if db.find_one("hash",**{'hash':h}):
#        db.update("hash",{
#            'hash':h,
#            },**{
#                'hash':h,
#                'plain':p,
#                })
#    else:
        db.insert('hash',**{
            'hash':h,
            'plain':p,
            })


def generate (* strings ):
    
    Counter.ze()

    def _single_generate(plain):
        try:
            md5_gen = md5()
            md5_gen.update(plain.encode("utf8"))
            Counter.co() 
            return md5_gen.hexdigest()
        except UnicodeDecodeError:
            try:
                md5_gen = md5()
                md5_gen.update(plain)
                Counter.co()
                return md5_gen.hexdigest()
            except:
                print "error in line : {} => {} ".format(Counter.count,plain)
                Counter.co()
                return "0"*32
    return map(_single_generate , strings)

def get_plain(path):
    if os.path.exists(path):
        print "load file ",
        f = file(path)

        res = map(strip,f.readlines())
        print "ok"
        f.close()
        return res
    else :
        print "not csv file load {}".format (path)
def divide_word(string_pri):
    pattern = re.compile(r'[a-zA-Z0-9]+')
    return tuple(pattern.findall(string_pri))

def log(H_strings,P_strings):
    hash_dict = dict(zip(P_strings,H_strings))
    for i in hash_dict:
        print "{} , {} ".format(i,hash_dict[i])
        if db:    save_db(hash_dict[i],i)
    return hash_dict

def save(dict_hash):
    print "save ...",
    file_name = "_".join(time.asctime().split())
    structure = Series(dict_hash)
    structure.to_csv( os.path.join(LOCATION,file_name))
    print "ok"


def args():
    desc = """
    this is write by QinglUan
    Feb 16/2015 night
    """
    parser = ArgumentParser(usage="for generate hash by string or csv file",description=desc)
    parser.add_argument("-f","--file",help="csv file path ",default=None)
    parser.add_argument("-s","--strings",default=None,help="strings can divide by any syn")
    parser.add_argument("-sf","--save-file",action="store_true",default=False,help="to decide not save file to location")
    parser.add_argument("-d","--divide-if",action="store_true",default=False,help="to decide not divide word by ' '(space) ")
    parser.add_argument("-sm","--save-mongo-db",default=True,action="store_false",help="save  hash result in mongo db") 
    parser.add_argument("-fm","--find-mongo-db",default=None,help="find result in mongo db ")
    return parser.parse_args()


if __name__ == "__main__":
    start = time.time()
    log_time = lambda x :  time.time() - x
    args = args()
    if  args.save_mongo_db:
        print "save to mongo db [on]"
        db = Mongo("hashlib")
    if args.find_mongo_db :
        db = Mongo("hashlib")
        print args.find_mongo_db
        print db.find("hash",**{"plain":args.find_mongo_db})
    if args.strings :
        strings = args.strings
        db = Mongo("hashlib")
        if   args.divide_if:
            strings = divide_word(strings)
        else :
            strings = [strings]
        res = generate(*strings)
        hash_dict = log(list(strings),res)
        if args.save_file:

            save(hash_dict)
    elif args.file:
        strings = get_plain(args.file)
        res = generate(*strings)
        hash_dict = log(strings,res)
        if args.save_file:
            save(hash_dict)
    print "used : ",log_time(start)
    del db
