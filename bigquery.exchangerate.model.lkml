connection: "web_projects"

include: "bigquery.explore"

datagroup: 24hours {
  max_cache_age: "24 hours"
  sql_trigger: SELECT CURRENT_DATETIME();;
}

persist_with: 24hours
