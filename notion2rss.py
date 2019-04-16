#! /usr/bin/env python3
import time
import configparser

import pyatom
import requests
import dateparser


def get_record_values(json_data):
    api_url = "https://www.notion.so/api/v3/getRecordValues"
    resp = requests.post(api_url, json=json_data)
    return resp.json()


def load_page_chunk(json_data):
    api_url = "https://www.notion.so/api/v3/loadPageChunk"
    resp = requests.post(api_url, json=json_data)
    return resp.json()


def query_collection(json_data):
    api_url = "https://www.notion.so/api/v3/queryCollection"
    resp = requests.post(api_url, json=json_data)
    return resp.json()


def add_dashes_to_id(id_string):
    id_without_dashes = id_string.replace('-', '')

    tokens = [
        id_without_dashes[0:8],
        id_without_dashes[8:12],
        id_without_dashes[12:16],
        id_without_dashes[16:20],
        id_without_dashes[20:],
    ]

    return '-'.join(tokens)


def parse_table_info(page_id, limit=50):
    # chunks = load_page_chunk({
    #     "chunkNumber": 0,
    #     "limit": limit,
    #     "pageId": add_dashes_to_id(page_id),
    #     "verticalColumns": False,
    # })
    chunks = get_record_values({
        "requests": [{
            "table": "block",
            "id": add_dashes_to_id(page_id)
        }]
    })

    block_value = chunks["results"][0]["value"]
    collection_id = block_value["collection_id"]
    collection_view_id = block_value["view_ids"][0]

    data = query_collection({
        "collectionId": collection_id,
        "collectionViewId": collection_view_id,
        "query": {
            "aggregate": [{
                "id": "count",
                "type": "title",
                "property": "title",
                "view_type": "table",
                "aggregation_type": "count"
            }],
            "filter": [],
            "sort": [],
            "filter_operator": "and"
        },
        "loader": {
            "type": "table",
            "limit": 70,
            "userTimeZone": "Europe/Prague",
            "userLocale": "en",
            "loadContentCover": True
        }
    })

    collection_view = data["recordMap"]["collection_view"]
    collection = data["recordMap"]["collection"]

    collection_view_value = list(collection_view.values())[0]["value"]
    if collection_view_value["type"] != "table":
        raise ValueError("Uknown type: `%s`" % collection_view_value["type"])

    ordering = [
        x["property"]
        for x in collection_view_value["format"]["table_properties"]
        if x["visible"]
    ]

    collection_value = list(collection.values())[0]["value"]
    readable_names_of_columns = {
        key: val["name"]
        for key, val in collection_value["schema"].items()
    }

    data_block = data["recordMap"]["block"]

    for row_data in data_block.values():
        if not row_data.get("value", {}).get("properties"):
            continue

        properties = row_data["value"]["properties"]

        records = {}
        for key, item in properties.items():
            name = readable_names_of_columns[key]
            if len(item[0]) == 1:
                # [['Bystroushaak']]
                content = item[0][0]
            elif len(item) == 0 or len(item[0]) == 0:
                continue
            elif item[0][1][0][0] == "d":
                # [['‣', [['d', {'type': 'datetime', 'time_zone': 'Europe/Prague',
                # 'start_date': '2019-04-16', 'start_time': '11:59'}]]]]
                date_info = item[0][1][0][1]
                content = date_info["start_date"]
                if "start_time" in date_info:
                    content += " " + date_info["start_time"]
            elif item[0][1][0][0] == "a":
                records["URL"] = item[0][1][0][1]
                content = item[0][0]
            else:
                content = item

            records[name] = content

        if len(records.keys()) > 1:
            yield records


def convert_to_rss():
    config = configparser.ConfigParser()
    config.read("notion2rss.conf")
    channel_config = config["channel"]

    feed = pyatom.AtomFeed(
        title=channel_config["blog_name"],
        feed_url=channel_config["feed_url"],
        url=channel_config["blog_url"],
        author=channel_config["author"]
    )

    # parsed_data = None
    # for _ in range(2):
    #     try:
    parsed_data = list(parse_table_info(config["channel"]["blog_id"]))
        #     break
        # except Exception:
        #     time.sleep(1)

    if not parsed_data:
        raise ValueError("Can't parse notion data!")

    item_mapping = config["mapping"]
    for item in parsed_data:
        updated = item.get(item_mapping.get("updated", "-"), "")
        updated = dateparser.parse(updated)

        feed.add(
            title=item.get(item_mapping.get("title", "-"), "Update"),
            content=item.get(item_mapping.get("content", "-"), ""),
            content_type="text",
            author=item.get(item_mapping.get("author", "-"), channel_config["author"]),
            url=item.get(item_mapping.get("URL", "-"), None),
            updated=updated
        )

    return feed.to_string()


if __name__ == "__main__":
    assert add_dashes_to_id('89c7c5f0ab804edf99a4985cc0c11168') == "89c7c5f0-ab80-4edf-99a4-985cc0c11168"
    print(convert_to_rss())