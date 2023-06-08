import json
import requests
from bs4 import BeautifulSoup
import re


def download_page(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def scrape_location(url):

    content = download_page(url)
    soup = BeautifulSoup(content, 'html.parser')

    span = soup.select("div.ErdelyHelysegnevTar_LE_C_msor_2:nth-child(1) > span:nth-child(1)")
    if span[0].a:
        span[0].a.decompose()
    text = span[0].get_text()
    nevek = re.split('✪', text)

    result = {}

    result["id"] = re.sub(r".*\/([a-zA-Z0-9-]*)\/[^\/]*$", "\\1", url)
    result["primary_name"] = span[0].span.find_next("span").get_text().strip()

    helyseg = re.split(";\s*(?![^()]*\))", nevek[0])

    result["county"] = re.sub(r"^.*\(([^)]*)\).*$", "\\1", helyseg[0].strip())
    result["ro"] = helyseg[0].strip()
    if len(helyseg) > 1:
        result["hu"] = helyseg[1].strip()
    result["names"] = []
    for hn in helyseg:
        result["names"].append(hn.strip())

    if len(nevek)>1:
        kozigazgatas = re.split(";\s*(?![^()]*\))", nevek[1])
        result["administrative"] = []
        for an in kozigazgatas:
            result["administrative"].append(an.strip())

    with open("JsonDumps/"+result["id"]+".json", 'w', encoding='utf8') as json_file:
        json.dump(result, json_file, ensure_ascii=False, indent=4)


def get_all_links(base):
    villages = []
    abc_content = download_page(base)
    abc_soup = BeautifulSoup(abc_content, 'html.parser')
    abc_links = abc_soup.select("div.list-group")[0].find_all("a")
    for betu_link in abc_links:
        betu = betu_link.get_text()
        print("Parsing " + betu)
        betu_base = betu_link.attrs["href"]
        betu_content = download_page(base_url + betu_base)
        betu_soup = BeautifulSoup(betu_content, 'html.parser')
        pages = betu_soup.find("ul", class_="pagination-sm")


        for falu in betu_soup.find("div", class_="list-group").find_all("a"):
            villages.append(falu.attrs["href"])

        if pages:
            subpages = int(pages.find_all("a")[-1].get_text())
            for p in range(2, subpages+1):
                bt_c = download_page(base_url + betu_base + "?page=" + str(p))
                bt_s = BeautifulSoup(bt_c, 'html.parser')
                for falu in bt_s.find("div", class_="list-group").find_all("a"):
                    villages.append(falu.attrs["href"])
                print("page "+str(p)+"done")
    return villages


base_url = 'https://www.arcanum.com'
betuk = '/en/online-kiadvanyok/' \
           'ErdelyHelysegnevTar-erdely-bansag-es-partium-torteneti-es-kozigazgatasi-helysegnevtara-1/telepulesek-1C9/'

# faluk = get_all_links(base_url+betuk)
with open('mindenfalu.txt', 'r') as f:
    for falu in f.readlines():
        print("Reading "+falu)
        #scrape_location(base_url+falu.strip())



