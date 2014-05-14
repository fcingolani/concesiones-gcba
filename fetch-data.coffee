util    = require 'util'

async   = require 'async'
request = require 'request'
_       = require 'underscore'
$       = require 'cheerio'

GRANT_ROWS_PER_PAGE = 10
PARALLEL_REQUESTS_LIMIT = 20

grants_page_url_template = "http://www.buenosaires.gob.ar/areas/des_economico/concesiones/?offset=%d"
grant_url_template = "http://www.buenosaires.gob.ar/areas/des_economico/concesiones/pop.php?IdLegajo=%d"

create_requester = (url)->
  (callback)->
    request url, (error, response, body)->
      callback error, body

create_grants_page_requester = (offset = 0)->
  create_requester util.format( grants_page_url_template, offset )

cheerize = (html, callback)->
  callback null, $.load(html)

extract_max_offset = (doc, callback)->
  max_offset = doc(".paginador a")
              .last()
              .attr('href')
              .match(/\d+/)[0]

  callback null, parseInt(max_offset)

fetch_max_offset = (callback)->
  async.waterfall [
    create_grants_page_requester()
    cheerize
    extract_max_offset
  ], callback

generate_offset_list = (max_offset, callback)->
  offset_list = (offset for offset in [0..max_offset] by GRANT_ROWS_PER_PAGE)
  callback null, offset_list

extract_grant_ids = (doc, callback)->
  grant_ids = []

  doc("table.contenido tbody tr td:nth-child(3) span").each ()->
    grant_ids.push($(this).text())

  callback null, grant_ids

create_grant_ids_extractor = (offset)->
  (callback)->
    async.waterfall [
      create_grants_page_requester offset
      cheerize
      extract_grant_ids
    ], callback

fetch_grant_ids = (offset_list, callback)->
  ids_extractors = (create_grant_ids_extractor offset for offset in offset_list)
  async.parallelLimit ids_extractors, PARALLEL_REQUESTS_LIMIT, (err, grant_ids)->
    callback null, _.flatten(grant_ids)

create_grant_requester = (grant_id)->
  create_requester util.format( grant_url_template, grant_id )

extract_grant = (doc, callback)->
  grant = {}

  doc("table.contenido tr").each ()->
    tds = $(this).children()
    if tds.length is 2
      grant[tds.first().text()] = tds.last().text()

  callback null, grant

create_grant_extractor = (grant_id)->
  (callback)->
    async.waterfall [
      create_grant_requester grant_id
      cheerize
      extract_grant
    ], callback

fetch_grants = (grant_ids, callback)->
  grant_extractors = (create_grant_extractor grant_id for grant_id in grant_ids)
  async.parallelLimit grant_extractors, PARALLEL_REQUESTS_LIMIT, (err, grants)->
    callback null, _.flatten(grants)

async.waterfall [
  fetch_max_offset
  generate_offset_list
  fetch_grant_ids
  fetch_grants
], (err, grants)->
  console.log JSON.stringify(grants)