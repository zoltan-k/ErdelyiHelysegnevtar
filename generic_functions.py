import json
import pymongo
import wikipediaapi
import pywikibot
import httpx
import html
from time import sleep
from config import *


"""a mongoDB connection is always established first. Wikipedia too"""
client = pymongo.MongoClient(MONGODB_URI)    # defined in config.py
db = client["ErdelyHNT"]
collection = db["Places"]
w = wikipediaapi.Wikipedia('ro')
pwb_site = pywikibot.Site('ro', "wikipedia")
mindenfalu_urls = []
my_wdi = {}


def readme(place_id, read_db=True):
    """Reads a single item preferably from the db, falls back to file, if not. assumes file exists"""
    if read_db:
        blob = collection.find_one({"_id": place_id})
        if blob:
            return blob
    with open('JsonDumps/' + place_id + '.json', encoding="utf-8") as user_file:
        blob = json.load(user_file)
    return blob


def write_to_file(data, show=False, *_, **__):
    """write back the data into json files"""
    with open("JsonDumps/" + data["_id"] + ".json", 'w', encoding='utf8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)
        if show:
            print("saved JsonDumps/" + data["_id"] + ".json")
    return data


def write_to_db(data, show=False, *_, **__):
    """update the data in the DB"""
    collection.replace_one({"_id": data["_id"]}, data, upsert=True)
    if show:
        print("written " + data["_id"] + " to db")
    return data


def process_all(limit=None, start_at=None, write=False, function=None, functions=None, show=False):
    """the original idea to bulk process data blobs, before DB was added. Probably redundant now"""
    if functions is None:
        functions = []
    start = True
    if start_at:
        start = False
    counter = 0
    with open('falu_ids.txt') as mindenfalu:
        for falu in mindenfalu:
            falu = falu.strip()
            if falu == start_at:
                start = True
            if start:
                if show:
                    print('Working on ' + falu)
                counter += 1
                temp = readme(falu)
                if function:
                    temp = function(temp, show=show)
                else:
                    for f in functions:
                        temp = f(temp, show=show)
                if write:
                    write_to_file(temp, show)
                    write_to_db(temp, show)
            if limit and counter >= limit:
                break
    if show:
        print('Processed: %s' % counter)


def process_all_db(
        find='', limit=0, write=False, functions=None, attributes=None,
        show=False, throttle=None, sort_field="_id", sort_asc=True):
    """
    Parse all documents in mongodb (optionally, matching a filter) and execute one or more functions on each.
    Arguments:
        find: mongodb filter
        limit: only parse maximum this many at once
        write: write back (to DB AND to the files) the processed data blob
        functions: array of functions to process each data blob
                    each function takes a data blob and the show argument, then returns the processed data blob
                    consecutive functions are chained (second feeds on output of first etc.)
                    only the final output is written (if write is True)
        attributes: dictionary of attributes that can be passed to functions.
                    it is up to the functions to interpret these correctly.
        show: sort of debug feature
        throttle: in seconds. slow down processing (potentially useful if some sort of data scraping is involved)
        sort_field:  sort by field name
        sort_asc: sort ascending
    """
    if functions is None:
        functions = []
    if attributes is None:
        attributes = {}
    if write:
        functions.append(write_to_file)
        functions.append(write_to_db)
    errors = []
    counter = 0
    valid_data = 0
    print("DB processing starts")
    sort_dir = pymongo.DESCENDING
    if sort_asc:
        sort_dir = pymongo.ASCENDING
    if "table" in attributes:
        for k, v in attributes["table"].items():
            print(f"| {k:{v}} ", end='')
        print("|")
    for falu in collection.find(find).sort([(sort_field, sort_dir)]).limit(limit):
        # if show:
        #   print("Working on: %s" % (falu["_id"]))
        temp = falu
        for f in functions:
            if temp:  # expect functions with negative results. they should return False then.
                temp = f(temp, show=show, **attributes)
            else:
                errors.append(" item " + falu["_id"] + " resulted in an empty item")
                break
        counter += 1
        if temp:
            valid_data += 1
        if throttle:
            sleep(throttle)
    print(f"Processed: {counter} | valid: {valid_data}")
    if show:
        print(errors)
    return errors


def get_all_urls():
    """Reads the full URLs into memory from the list scraped from arcanum"""
    global mindenfalu_urls
    with open(r'mindenfalu_full_list.txt', 'r') as fp:
        mindenfalu_urls = fp.readlines()


def delete_from_file(file_name, text_filter='nothing'):
    """Delete a line matching _text_filter_ from a file called _file_name_. Simple search """
    with open(file_name, "r+") as f:
        d = f.readlines()
        f.seek(0)
        for i in d:
            if i.find(text_filter) == -1:
                f.write(i)
        f.truncate()


def get_item(my_dict, item_map):
    """ Return a nested value from a dictionary and/or list in a dot-notated format, where only keys are mentioned.
        if the nested element is a list, the key value should be a number
        ie:
            names.3
            sources.reference_url
            sources.fs_place.0
        Arguments:
            my_dict: dictionary (or list) to search in
            item_map: dot-separated list of keys, such as above
    """
    if type(item_map) not in (tuple, list) and "." in item_map:
        item_map = item_map.split('.')
    for i in item_map:
        if type(my_dict) is dict:
            my_dict = my_dict.get(i)
        elif type(my_dict) is list:
            if int(i) < len(my_dict):
                my_dict = my_dict[int(i)]
            else:
                my_dict = None
    return my_dict


def get_wikipedia_url_from_wikidata_id(wikidata_id, lang='en', debug=False):
    import requests

    url = (
        'https://www.wikidata.org/w/api.php'
        '?action=wbgetentities'
        '&props=sitelinks/urls'
        f'&ids={wikidata_id}'
        '&format=json')
    json_response = requests.get(url).json()
    if debug:
        print(wikidata_id, url, json_response)

    entities = json_response.get('entities')
    if entities:
        entity = entities.get(wikidata_id)
        if entity:
            sitelinks = entity.get('sitelinks')
            if sitelinks:
                if lang:
                    # filter only the specified language
                    site_link = sitelinks.get(f'{lang}wiki')
                    if site_link:
                        wiki_url = site_link.get('url')
                        if wiki_url:
                            return wiki_url
                else:
                    # return all the urls
                    wiki_urls = {}
                    for key, site_link in sitelinks.items():
                        wiki_url = site_link.get('url')
                        if wiki_url:
                            wiki_urls[key] = wiki_url
                    return wiki_urls
    return None


def count_instances():
    counter = {k: len(my_wdi[k]) for k in my_wdi.keys()}
    wd_site = pywikibot.Site("wikidata", "wikidata")
    pwb_repo = wd_site.data_repository()

    for k, v in counter.items():
        pwb_item = pywikibot.ItemPage(pwb_repo, k)
        counter[k] = {
            "uri": "https://www.wikidata.org/wiki/" + k,
            "label_en": pwb_item.labels["en"],
            "count": v}
        if k not in ["Q532", "Q16858213", "Q34841063", "Q15921247", "Q640364"]:
            counter[k]["items"] = my_wdi[k]
            counter[k]["item_URIs"] = ["https://www.wikidata.org/wiki/" + i.split("|")[0] for i in my_wdi[k]]

    print(json.dumps(counter, ensure_ascii=False, indent=4))


def get_place(name, fuzzy=True, **q_args):
    headers = {
        'Accept': 'application/json'}
    if fuzzy:
        name = name + "~"
    url = "https://api.familysearch.org/platform/places/search"
    q = "name:" + name
    for (k, v) in q_args.items():
        q += f" {k}:{v}"
    url += f"?q={html.escape(q)}"
    re = httpx.get(url, headers=headers)
    return re.json()
