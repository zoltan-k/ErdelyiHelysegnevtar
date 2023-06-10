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

"""

process_all_db(find={'wikidata': {"$exists": True}},
               functions=[wikidata_commune_or_county],
               #  attributes={"table":{"primary_name": 20, "county": 15, "wikidata.located-in":15, "commune":20}},
               show=True, write=True)
