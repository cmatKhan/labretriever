#' Database Information for the Yeast Compendium API
#'
#' A list containing the base URL and various API endpoints to interact
#'   with the Yeast Compendium API.
#'
#' @format A list containing the following elements:
#' \describe{
#'   \item{base_url}{The base URL for the API (a character string).}
#'   \item{endpoints}{
#'     A list of API endpoints (character strings) for various resources:
#'     \describe{
#'       \item{auth_token}{Authentication token endpoint.}
#'       \item{chrmap}{Chromosome map endpoint.}
#'       \item{genes}{Genes endpoint.}
#'       \item{genes_with_effects}{Genes with effects endpoint.}
#'       \item{promoterregions}{Promoter regions endpoint.}
#'       \item{promoterregions_targets}{Promoter regions targets endpoint.}
#'       \item{harbisonchip}{Harbison ChIP endpoint.}
#'       \item{harbisonchip_with_annote}{Harbison ChIP with
#'         annotations endpoint.}
#'       \item{kemmerentfko}{Kemmeren TF knockout endpoint.}
#'       \item{mcisaaczev}{McIsaac ZEV endpoint.}
#'       \item{background}{Background endpoint.}
#'       \item{cctf}{CC TF endpoint.}
#'       \item{ccexperiment}{CC experiment endpoint.}
#'       \item{hops}{HOPS endpoint.}
#'       \item{hopsreplicatesig}{HOPS replicate significance endpoint.}
#'       \item{hopsreplicatesig_with_annote}{HOPS replicate significance
#'         with annotations endpoint.}
#'       \item{qcmetrics}{QC metrics endpoint.}
#'       \item{qcmanualreview}{QC manual review endpoint.}
#'       \item{qcr1tor2}{QC R1 to R2 endpoint.}
#'       \item{qcr2tor1}{QC R2 to R1 endpoint.}
#'       \item{qctftotransposon}{QC TF to transposon endpoint.}
#'       \item{expression}{Expression endpoint.}
#'       \item{pagination_info}{Pagination information endpoint.}
#'       \item{row_count}{Row count endpoint.}
#'     }
#'   }
#' }
#'
#' @examples
#' # Accessing the base_url from the database_info object
#' database_info$base_url
#' # Accessing the genes endpoint from the database_info object
#' database_info$endpoints$genes
#'
#' @export
database_info <- list(
  base_url = "http://127.0.0.1:8000",
  endpoints = list(
    auth_token = "/api-token-auth/",
    chrmap = "/api/v1/chrmap/",
    genes = "/api/v1/genes/",
    genes_with_effects = "/api/v1/genes/with-effects/",
    promoterregions = "/api/v1/promoterregions/",
    promoterregions_targets = "/api/v1/promoterregions/targets/",
    harbisonchip = "/api/v1/harbisonchip/",
    harbisonchip_with_annote = "/api/v1/harbisonchip/with_annote/",
    kemmerentfko = "/api/v1/kemmerentfko/",
    mcisaaczev = "/api/v1/mcisaaczev/",
    background = "/api/v1/background/",
    cctf = "/api/v1/cctf/",
    ccexperiment = "/api/v1/ccexperiment/",
    hops = "/api/v1/hops/",
    hopsreplicatesig = "/api/v1/hopsreplicatesig/",
    hopsreplicatesig_with_annote = "api/v1/hopsreplicatesig/with_annote/",
    qcmetrics = "/api/v1/qcmetrics/",
    qcmanualreview = "/api/v1/qcmanualreview/",
    qcr1tor2 = "/api/v1/qcr1tor2/",
    qcr2tor1 = "/api/v1/qcr2tor1/",
    qctftotransposon = "/api/v1/qctftotransposon/",
    expression = "/api/v1/expression/",
    pagination_info = "pagination_info/",
    row_count = "count/"
  )
)
