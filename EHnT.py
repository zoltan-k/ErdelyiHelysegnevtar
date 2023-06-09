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
[write_to_file]

restore db from json:
    if ids are still in place:
        [read_from_file,write_to_db]
    if ids are lost:
        everything:
        process_all(functions=[read_from_file,write_to_db])
        single:
        process_all(functions=[read_from_file,write_to_db],start_at="id',limit=1)

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

"""

qd_dict = {}
nfound = []

process_all_db(find={'wikidata': {"$exists": True}},
               functions=[q_data],
               attributes={"qd": qd_dict})

for k, v in qd_dict.items():
    for e in v:
        if e not in qd_dict.keys():
            if e not in nfound:
                nfound.append("https://www.wikidata.org/wiki/"+e)

print("not found the following: "+str(nfound))
print(qd_dict)
