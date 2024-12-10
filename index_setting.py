index_settings = {
    "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1,
    "analysis": {
      "analyzer": {
        "keyword_analyzer": {
          "type": "custom",
          "tokenizer": "keyword"
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "post_id": {
        "type": "integer"
      },
      "keyword": {
        "type": "text",
        "analyzer": "keyword_analyzer",
        "fielddata": True
      }
    }
  }
}