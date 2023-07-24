from unitfunctions import *

# import re

"""
process_all_db(find={
    "hu": {"$regex": "[^a-zöüóőúűéáí, -]", "$options": 'i'},
    "wikipedia_response_status": 404
}, functions=[eliminate], show=True, )


process_all_db(find={
        "wikipedia_response_status": {
            "$exists": False
        }
    }, functions=[get_wiki], throttle=1.1, show=True, write=True)
    
backup db to json:
     make_clean_json_dumps_from_db()


restore db from json:
    restore_db_from_json_dumps()

process_all(functions=[read_from_file, write_to_db], start_at="abucea-1D1", limit=1, show=True)

process_all_db(find={"_id": "harghita-madaras-B5A",}, functions=[eliminate], show=True, )

count the wikidata instance-of attributes:

process_all_db(find={'wikipedia_response_status': 200},
               functions=[wikidata_instances],
               attributes={"wdi": my_wdi},
               show=False,
               write=False)
count_instances()



process_all_db(find={'wikidata': {"$exists": True}},
               functions=[table_out],
               attributes={"table":{"primary_name": 20, "county": 15, "wikidata.located-in":15, "commune":20}})


process_all_db(aggregate=[{'$match': {'wikidata.id': {'$exists': True}}},
                          {'$group': {'_id': '$wikidata.id', 'instances': {'$count': {}},
                                      'villages': {'$push': {
                                          'village_id': '$_id', 'primary_name': '$primary_name', 'county': '$county'}}}
                           }, {'$match': {'instances': {'$gt': 1}}},
                          {'$project': {'URI': {'$concat': ['http://wikidata.org/wiki/', '$_id']},
                                        'instances': 1, 'villages': 1}},
                          {'$sort': {'instances': -1}}],
               functions=[aggregate_looper],
               attributes={"agg_attributes": {"table": {
                   "primary_name": 20, "county": 15, "wikidata.located-in": 15, "commune": 20, "part-of.ro": 20}},
                   "agg_functions": [table_out]}
               )

process_all_db(find={"primary_name":"Fânațe", "county":"Mureș"},
               functions=[fs_place_from_wd, table_out],
               attributes={"table":{"primary_name": 13, "county": 12, "commune":12, "part-of.ro":20,  "sources.wikipedia":40, "wikidata.located-in":15,}},
               show=False, write=True)

"""

process_all_db(aggregate=[{'$match': {'wikidata.id': {'$exists': True}}},
                          {'$group': {'_id': '$wikidata.id', 'instances': {'$count': {}},
                                      'villages': {'$push': {
                                          'village_id': '$_id',
                                          'primary_name': '$primary_name',
                                          'county': '$county',
                                          'partof':'$part-of.ro'}},
                                      'prtof':{'$addToSet': '$part-of.ro'}}},
                          {'$match': {'instances': {'$gt': 1}}},
                          {'$project': {'URI': {'$concat': ['http://wikidata.org/wiki/', '$_id']},
                                        'instances': 1, 'villages': 1, 'prtof':1 }},
                          {'$sort': {'instances': -1, 'villages.0.county':1}},
                          # {'$skip': 1},
                          # {'$limit': 3}
                          ],
               functions=[aggregate_looper, aggregate_reduce_duplicate_wdids],
               attributes={"agg_attributes": {
                   "table": {"wikidata.id":11, "_id": 22, "primary_name": 15, "county": 15,
                             "names.hu.0": 17, "commune": 20, "part-of.ro": 20},
                   # "try_wiki_url": True
               #    "no_header": True
               },
                   #
                   "agg_functions": [
                       #clear_metadata, get_wiki, wikidata, wikidata_commune_or_county, fs_place_from_wd,
                       #write_to_db, write_to_file,
                       table_out],
                   "eliminate_it": True},
               #show = True
               )


# process_all_db(find={'_id': 'bucerdea-vinoasa-4DA'}, functions=[clear_metadata, wiki_was_moved], show=True, write=True)
# process_all_db(find={'_id': 'sancraiu-almasului-1328'},
#               functions=[clear_metadata, get_wiki, wikidata, wikidata_commune_or_county, fs_place_from_wd],
#               show=True, write=True)
# process_all_db(find={"_id": "luncasprie-D97"}, functions=[eliminate], show=True, )
