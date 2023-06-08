from unitfunctions import *
import re

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
"""
process_all_db(find={'wikipedia_response_status': 200},
               functions=[wikidata_instances],
               attributes={"wdi": mywdi},
               show=False,
               write=False)
count_instances()





