#' URLs and other information for accessing the database.
#'
#' @format A list containing information about the database.
#' @description A list of lists containing endpoint addresses for the various views in the database.
#'   These are intended to be appended to the URL of the database itself.
#'
#'  @field base_url The base url, to which the endpoints may be appended,
#'    to the database
#'
#' @field endpoints A list containing endpoint addresses for various views in the database.
#'   These endpoints are intended to be appended to the URL of the database.
#'   The currently configured endpoints are
#'
#'   <li><code><b>auth_token</b></code>: Authentication token endpoint address.</li></br>
#'   <li><code><b>chrmap</b></code>: Chromosome map endpoint address.</li></br>
#'   <li><code><b>genes</b></code>: Genes endpoint address.</li></br>
#'   <li><code><b>promoterregions</b></code>: Promoter regions endpoint address.</li></br>
#'   <li><code><b>harbisonchip</b></code>: Harbison chip endpoint address.</li></br>
#'   <li><code><b>kemmerentfko</b></code>: Kemmeren TF knockout endpoint address.</li></br>
#'   <li><code><b>mcisaaczev</b></code>: McIsaac Zev endpoint address.</li></br>
#'   <li><code><b>background</b></code>: Background endpoint address.</li></br>
#'   <li><code><b>cctf</b></code>: CCTF endpoint address.</li></br>
#'   <li><code><b>ccexperiment</b></code>: CC experiment endpoint address.</li></br>
#'   <li><code><b>hops</b></code>: HOPS endpoint address.</li></br>
#'   <li><code><b>hopsreplicatesig</b></code>: HOPS replicate signature endpoint address.</li></br>
#'   <li><code><b>qcmetrics</b></code>: QC metrics endpoint address.</li></br>
#'   <li><code><b>qcmanualreview</b></code>: QC manual review endpoint address.</li></br>
#'   <li><code><b>qcr1tor2</b></code>: QC R1 to R2 endpoint address.</li></br>
#'   <li><code><b>qcr2tor1</b></code>: QC R2 to R1 endpoint address.</li></br>
#'   <li><code><b>qctftotransposon</b></code>: QC TF to transposon endpoint address.</li></br>
#'   <li><code><b>pagination_info</b></code>: Pagination info endpoint address.
#'     Note that this should be appended to one of the data view URLS above</li></br>
#'   <li><code><b>row_count</b></code>: Row count endpoint address.
#'      Note that this should be appended to one of the data view URLS above</li>
#'
#' @examples
#' data(database_info)
#' database_info$endpoints$genes
#' paste0('www.some-url.com',
#'        database_info$endpoints$genes,
#'        database_info$endpoints$pagination_info)
#'
#' @export
database_info <- list(
  base_url = "http://127.0.0.1:8000",
  endpoints = list(
    auth_token = "/api-token-auth/",
    chrmap = "/api/v1/chrmap/",
    genes = "/api/v1/genes/",
    promoterregions = "/api/v1/promoterregions/",
    harbisonchip = "/api/v1/harbisonchip/",
    kemmerentfko = "/api/v1/kemmerentfko/",
    mcisaaczev = "/api/v1/mmcisaaczev/",
    background = "/api/v1/background/",
    cctf = "/api/v1/cctf/",
    ccexperiment = "/api/v1/ccexperiment/",
    hops = "/api/v1/hops/",
    hopsreplicatesig = "/api/v1/hopsreplicatesig/",
    qcmetrics = "/api/v1/qcmetrics/",
    qcmanualreview = "/api/v1/qcmanualreview/",
    qcr1tor2 = "/api/v1/qcr1tor2/",
    qcr2tor1 = "/api/v1/qcr2tor1/",
    qctftotransposon = "/api/v1/qctftotransposon/",
    pagination_info = "pagination_info/",
    row_count = "count/"
  )
)
