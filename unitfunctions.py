import re
import os
from generic_functions import *

"""
    All of the functions here should process exactly one location document at a time 
and should return exactly one location document, as the original document looks like after processing.
Even if the function is read-only, it should return the input data to its output.

common parameters:
    data: input json array/data blob for one place
    show: boolean variable used to print/debug mostly
    all functions should accept arbitrary arguments as well, i.e. *_, **__ to allow feeding custom arguments to some
    
Function may return False, the it should brake further processing of the item.
since usually "write" is appended to the end of the list this also means changes will not be rewritten to files/db 
    """

language_map = {"ro": "ro", "hu": "hu", "g": "de", "s": "sx", "jid": "yi", "y": "yi", "srp": "sr", "hrv": "hr",
                "czk": "cs", "cz": "cs", "blg": "bg", "bg": "bg", "slk": "sk",
                "pol": "pl", "tr": "tr", "l": "la", "gr": "el", "ukr": "uk"}

languages = []
for lang in language_map.values():
    if lang not in languages:
        languages.append(lang)
all_keys = []


def fix_fields_in_atlas(data, fields='', *_, **__):
    """fix any field in the DB by re-reading it from file"""
    fromfile = readme(data["_id"], False)
    new_set = {k: v for k, v in fromfile.items() if k in fields}
    collection.update_one({"_id": data["_id"]}, {'$set': new_set})
    return data


def read_from_file(data, show=False, *_, **__):
    """Reads a single item from json file"""
    try:
        with open('JsonDumps/' + data["_id"] + '.json', encoding="utf-8") as user_file:
            data = json.load(user_file)
    except IOError:
        if show:
            print('JsonDumps/' + data["_id"] + '.json not found')
        return False
    return data


def dump(data, *_, **__):
    """just print the data blob as pretty printed json"""
    print(json.dumps(data, ensure_ascii=False, indent=4))
    return data


def table_out(data, table=None, *_, **__):
    """ try printing the data in a table form for the console
        will accept a key:value dictionary, where each key is a field name, and value is the column width.
        For fields, you can specify regular field, an array field (all entries will be printed in width size columns)
        as well as dot separated definitions for nested entries. i.e. sources.wikipedia
    """
    if data is None:
        return data
    if table is not None:
        for (t_col, col_width) in table.items():
            if "." in t_col:
                x = get_item(data, t_col.split("."))
                if type(x) in (tuple, list):
                    print("|", end='')
                    for y in x:
                        print(f":: {y:{col_width}} ", end='')
                elif x is not None:
                    print(f"| {x:{col_width}} ", end='')
            elif t_col in data:
                if type(data[t_col]) in (tuple, list):
                    print("|", end='')
                    for x in data[t_col]:
                        print(f":: {x:{col_width}} ", end='')
                else:
                    print(f"| {data.get(t_col):{col_width}} ", end='')
            else:
                print(f'| {" ":{col_width}}', end='')
        print("|")
    return data


def clear_primary(data, show=False, *_, **__):
    """strip spaces etc. from primary_name"""
    data["primary_name"] = data["primary_name"].strip()
    if show:
        print('Primary name: %s' % (data["primary_name"]))
    return data


def clear_ro(data, show=False, *_, **__):
    """Clean up Romanian name variants into an array"""
    data["ro"] = re.sub(r"\(.*\)$", "", data["ro"]).strip()
    data["ro"] = re.sub(r"\[", ",", data["ro"]).strip()
    data["ro"] = re.sub(r"]", "", data["ro"]).strip()
    data["ro"] = [x.strip() for x in data['ro'].split(',')]
    if show:
        print('data[ro] = %s' % (data['ro']))
    return data


def regions_hu(data, show=False, *_, **__):
    """extract region names from hungarian name field"""
    hug = re.match(r"^(.*)\[([^]]+)](.*)$", data.get('hu', ""))
    if hug:
        data["hu"] = hug.group(1).strip()
        if hug.group(3):
            data["hu"] += " " + hug.group(3)
        if "region" not in data.keys():
            data["region"] = {}
        data["region"]["hu"] = hug.group(2)
        if show:
            print('Region = %s' % (data['region']['hu']))
    else:
        if show:
            print('Region not found')
    return data


def regions_de(data, show=False, *_, **__):
    """extract region names from german name field"""
    de_names = ','.join(data.get('de', [])).replace("\n", "")
    deg = re.match(r"^(.*)\[ *(in )?([^]]+)]$", de_names)
    if deg:
        data["de"] = [x.strip() for x in deg.group(1).split(",")]
        if "region" not in data.keys():
            data["region"] = {}
        data["region"]["de"] = deg.group(3).replace("in ", "").strip()
        if show:
            print('Region = %s' % (data['region']['de']))
    else:
        if show:
            print('Region not found')
    return data


def clean_hu(data, *_, **__):
    """extract region names from hungarian name field"""
    hug = re.match(r"^(.*?), ([0-9]{4}.*)$", data.get('hu', ""))
    if hug:
        data["hu"] = hug.group(1).strip()
        data["names"][1] = hug.group(1).strip()
        data["names"].insert(2, hug.group(2).strip())
    return data


def split_hu(data, *_, **__):
    """split hungarian name field into array"""
    hu_array = [x.strip() for x in data["hu"].split(",")]
    data["hu"] = hu_array
    return data


def strip_all(data, *_, **__):
    for key in data.keys():
        if type(data[key]) is list:
            new_a = [v.strip() for v in data[key] if v != ""]
            data[key] = new_a
        elif type(data[key]) is dict:
            data[key] = strip_all(data[key])
        else:
            data[key] = data[key].strip()
    return data


def language_variants(data, show=False, *_, **__):
    """ Should break out language variant from name array."""
    for i, n in enumerate(data["names"]):
        language = re.search(r"^([^0-9][^(]*)\(([a-z]+\.?)\)$", n)
        if not lang:
            continue
        if language.group(2).strip(".").lower() not in language_map.keys():
            if show:
                print("Unexpected language %s" % language.group(2))
            continue
        cc = language_map[language.group(2).strip(".").lower()]
        # if show:
        # print(lang.group(1))
        data[cc] = re.split(",", language.group(1))

    return data


def get_part_of(data, *_, **__):
    for language in languages:
        for k, nv in enumerate(data.get(lang, [])):
            part_of = re.search(r"^(.*)∩(.*)$", nv)
            if not part_of:
                continue
            data[language][k] = part_of.group(1).strip()
            if "part-of" not in data:
                data["part-of"] = {}
            data["part-of"][language] = part_of.group(2).strip()
    return data


def commune(data, *_, **__):
    for language in languages:
        for k, nv in enumerate(data.get(lang, [])):
            part_of = re.search(r"^(.*)#(.*)$", nv)
            if not part_of:
                continue
            data[language][k] = part_of.group(1).strip()
            if "commune" not in data:
                data["commune"] = {}
            data["commune"][language] = part_of.group(2).strip()
    return data


def reset_c(data, *_, **__):
    if "commune" not in data:
        return None
    if "ro" not in data["commune"]:
        return None
    data["commune"] = data["commune"]["ro"]
    return data


def get_wiki(data, show=False, *_, **__):
    """ tries to find wikipedia entry for the place in the current data blob"""
    url = 'not found'
    data['wikipedia_response_status'] = 404
    if type(data["primary_name"]) != str:
        return data

    if 'wikipedia' in data['sources']:
        del data['sources']['wikipedia']
    if "commune" in data:
        page = w.page(data["primary_name"] + "_(" + data["commune"] + "),_" + data["county"])
    else:
        page = w.page(data["primary_name"] + ", " + data["county"])
    if not page.exists():
        page = w.page(data["primary_name"].replace(" ", "-") + ", " + data["county"])
    if not page.exists():
        page = w.page(data["primary_name"].replace("-", " ") + ", " + data["county"])
    if not page.exists():
        page = w.page(data["primary_name"].replace(" ", "_") + ",_" + data["county"].replace(" ", "_"))
    if (not page.exists()) and ("commune" in data):
        page = w.page(data["primary_name"] + ", " + data["county"])

    if not page.exists():
        for v in data["names"]["ro"]:
            sleep(1)
            page = w.page(v + ", " + data["county"])
            if (not page.exists()) and ("commune" in data):
                sleep(1)
                page = w.page(v + "_(" + data["commune"] + "),_" + data["county"])
            if page.exists():
                break
    if page.exists():
        data['sources']['wikipedia'] = page.canonicalurl
        url = data['sources']['wikipedia']
        data['wikipedia_response_status'] = 200
    if show:
        print('URL: %s\nResponse: %d' % (url, data['wikipedia_response_status']))
    if data['wikipedia_response_status'] == 404:
        return None
    return data


def wiki_was_moved(data, *_, **__):
    data['wikipedia_response_status'] = 300
    return data


def wikidata(data, show=False, try_wiki_url=False, *_, **__):
    if show:
        print(data["primary_name"] + ", " + data["county"])
        print(data["sources"].get("wikipedia"))
    if try_wiki_url:
        if "wikipedia" not in data["sources"]:
            return data
        pwb_page = pywikibot.Page(pwb_site, data["sources"]["wikipedia"].replace("https://ro.wikipedia.org/wiki/", ""))
    else:
        pwb_page = pywikibot.Page(pwb_site, data["primary_name"] + ", " + data["county"])
    try:
        pwb_item = pywikibot.ItemPage.fromPage(pwb_page)
    except Exception as error:
        if show:
            print(error)
        if ("wikipedia" in data["sources"]) and (not try_wiki_url):
            data = wikidata(data, show, try_wiki_url=True)
        return data
    if "wikidata" not in data.keys():
        data["wikidata"] = {}
    data["wikidata"]["id"] = pwb_item.id
    pwb_prop = pwb_item.get('properties')

    instanceof = pwb_prop["claims"].get("P31")
    if instanceof:
        data["wikidata"]["instance-of"] = [i.getTarget().id for i in instanceof]
        if ("Q4167410" in data["wikidata"]["instance-of"]) and ("wikipedia" in data["sources"]) and (not try_wiki_url):
            return wikidata(data, show, try_wiki_url=True)
    elif "instance-of" in data["wikidata"]:
        del data["wikidata"]["instance-of"]

    located_in_array = pwb_prop["claims"].get("P131")
    if located_in_array:
        data["wikidata"]["located-in"] = [i.getTarget().id for i in located_in_array]
    elif "located-in" in data["wikidata"]:
        del data["wikidata"]["located-in"]

    coordinates = pwb_prop["claims"].get("P625")
    if coordinates:
        data["wikidata"]["lat"] = coordinates[0].getTarget().lat
        data["wikidata"]["lon"] = coordinates[0].getTarget().lon
    if show:
        print(json.dumps(data["wikidata"], ensure_ascii=False, indent=4))
    return data


def wikidata_commune_or_county(data, show=False, *_, **__):
    if "wikidata" not in data.keys():
        return None
    if "located-in" not in data["wikidata"]:
        return None
    for inside in data["wikidata"]["located-in"]:
        pwd_item = pywikibot.ItemPage(pwd_site.data_repository(), inside)
        if show:
            print("%s, %s: located-in QID: %s" % (data["primary_name"], data["county"], pwd_item.id))
        claims = pwd_item.get('properties').get("claims", [])
        for i in claims.get("P31", []):
            instances_of = i.getTarget().id
            if "Q659103" == instances_of:
                data["wikidata"]["commune_id"] = inside
            elif "Q1776764" == instances_of:
                data["wikidata"]["county_id"] = inside
    if show:
        print(data['wikidata'])
    return data


def wiki_from_data(data, *_, **__):
    if "wikipedia" in data["sources"]:
        return data
    if "wikidata" not in data:
        return data
    w_url = get_wikipedia_url_from_wikidata_id(data["wikidata"]["id"], 'ro')
    if w_url:
        data["sources"]["wikipedia"] = w_url
        data['wikipedia_response_status'] = 200
    return data


def get_all_keys(data, *_, **__):
    if "part-of" not in data:
        return data
    for k in data["part-of"].keys():
        if k not in all_keys:
            all_keys.append(k)
    print(all_keys)
    return data


def wikidata_instances(data, wdi=None, *_, **__):
    if "wikidata" not in data:
        return None
    for v in data["wikidata"].get("instance-of", []):
        if v not in wdi:
            wdi[v] = []
        tag = data["wikidata"]["id"] + "|" + data["primary_name"] + ", " + data["county"]
        if tag not in wdi[v]:
            wdi[v].append(tag)
    return data


def fs_place_from_wd(data, show=False,  *_, **__):
    fs_wd_place_type_map = {
        'Q532': "Village",
        'Q659103': "Village",
        "Q15921247": "Village",
        "Q3257686": "Village",
        "Q486972": "Village",
        'Q640364': "Municipality",
        'Q1549591': "City",
        'Q16858213': "Town",
        "Q123705": "Neighborhood or suburb",
        "Q2983893": "Neighborhood or suburb",
    }
    if show:
        print(data["primary_name"])
    if "wikidata" not in data:
        return data
    if "instance-of" not in data["wikidata"]:
        return data
    if "fs" not in data:
        data["fs"] = {}

    for k in fs_wd_place_type_map:
        if k in data["wikidata"]["instance-of"]:
            data["fs"]["place_type"] = fs_wd_place_type_map[k]
            break

    if show:
        print(data["primary_name"] + ": FS place type: " +
              data["fs"].get("place_type", "Not found Type:" + str(data["wikidata"]["instance-of"])))
    return data


def q_data(data, qd=None, *_, **__):
    if "wikidata" not in data:
        return data

    if data["wikidata"].get("id") not in qd:
        qd[data["wikidata"].get("id")] = []
    qd[data["wikidata"].get("id")] += data["wikidata"].get("located-in", [])
    return data


def reorg(data, order=None, *_, **__):
    if order is None:
        order = ['_id', 'primary_name', 'county', 'names', 'part-of', 'commune', 'region',
                 'names_raw', 'administrative', 'sources', 'wikipedia_response_status']
    reordered_data = {k: data.get(k) for k in order if data.get(k)}
    return reordered_data


def names_reorg(data, *_, **__):
    n_raw = data["names"]

    data["names"] = {}
    for language in languages:
        if data.get(language):
            data["names"][language] = data.get(language)
            del data[language]
    data["names_raw"] = n_raw

    return data


def data_filter(data, d_filter=None, regex=False, case_sensitive=False, *_, **__):
    if (d_filter is None) or (data is None):
        return data
    for key, value in data.items():
        if isinstance(value, str):
            if regex:
                if re.search(d_filter, value, re.IGNORECASE * (not case_sensitive)):
                    return data
            elif case_sensitive:
                if d_filter in value:
                    return data
            else:
                if d_filter.casefold() in value.casefold():
                    return data
        elif isinstance(value, dict):
            data_filter(value, d_filter, regex, case_sensitive)
        elif isinstance(value, list):
            for val in value:
                if isinstance(val, str):
                    if regex:
                        if re.search(d_filter, val, re.IGNORECASE * (not case_sensitive)):
                            return data
                    elif case_sensitive:
                        if d_filter in val:
                            return data
                    else:
                        if d_filter.casefold() in val.casefold():
                            return data
                elif isinstance(val, list):
                    data_filter({i: val[i] for i in range(len(val))}, d_filter, regex, case_sensitive)
                else:
                    data_filter(val, d_filter, regex, case_sensitive)
    return None


def get_arcanum_url(data, show=False, *_, **__):
    """add the original, arcanum URL as a source to the data blob"""
    base_url = 'https://www.arcanum.com'
    for line in mindenfalu_urls:
        if line.find(data['_id']) != -1:
            if 'sources' not in data:
                data['sources'] = {}
            data['sources']['reference_URL'] = base_url + line.strip()
            if show:
                print(data['sources']['reference_URL'])
            return data
    return data


def eliminate(data, show=False, *_, **__):
    """Clean out every occurrence of the data blob from all intermediary sources
    (json files, URL lists and from the DB)"""
    if show:
        print("Removing " + data["_id"])
    # delete from falu_ids
    delete_from_file("falu_ids.txt", data["_id"])
    # delete from mindenfalu_full_list
    delete_from_file("mindenfalu_full_list.txt", data["_id"])
    # delete json dump file
    os.remove("JsonDumps/" + data["_id"] + ".json")
    # delete from database
    collection.delete_one({"_id": data["_id"]})


def clear_metadata(data, *_, **__):
    """Remove existing metadata from the entry. Wikipedia, wikidata, fs fields"""
    if "wikidata" in data:
        del data["wikidata"]
    if "fs" in data:
        del data["fs"]
    if "wikipedia" in data["sources"]:
        del data["sources"]["wikipedia"]
    if "wikipedia_response_status" in data:
        del data["wikipedia_response_status"]
    if "wikipedia_redirected" in data:
        del data["wikipedia_redirected"]
    return data


def aggregate_looper(agg_data, show=False, agg_functions=None, agg_attributes=None, agg_write=False, *_, **__):
    process_all_db(find={"wikidata.id": agg_data["_id"]},
                   functions=agg_functions,
                   attributes=agg_attributes,
                   show=show,
                   write=agg_write)
    return agg_data


def aggregate_reduce_duplicate_wdids(agg_data, eliminate_it=False, show=False, *_, **__):
    print(agg_data["prtof"])
    for k in agg_data["villages"]:
        if k["primary_name"] not in agg_data["prtof"]:
            data = readme(k["village_id"])
            answer = 'N'
            write = False
            if eliminate_it:
                answer = input("Clear metadata from %s, %s (%s)? Y/[N]: " % (data["primary_name"], data["county"], data["_id"])).upper()
            if answer == 'Y':
                data = clear_metadata(data)
                data = wiki_was_moved(data)
                answer = 'N'
                write = True
            elif eliminate_it:
                answer = input("Try to reset metadata from? Y/[N]:").upper()
            if answer == 'Y':
                data = clear_metadata(data)
                data = get_wiki(data)
                data = wikidata(data, try_wiki_url=True)
                data = wikidata_commune_or_county(data)
                data = fs_place_from_wd(data)
                write = True
            if write:
                data = write_to_db(data, show=show)
                data = write_to_file(data, show=show)
            print(data)
    return agg_data


def get_places_primary(data, show=False, fuzzy=True, *_, **__):
    fs_place = get_place(data["primary_name"], fuzzy)
    if show:
        print(fs_place)
