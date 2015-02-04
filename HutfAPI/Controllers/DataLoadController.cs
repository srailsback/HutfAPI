using HuftAPI.Logging;
using HutfAPI.Infrastructure.Repositories;
using HutfAPI.Infrastructure.Security;
using System;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace HutfAPI.Controllers
{
    // api auth filter
    // https://github.com/cuongle/WebAPI.Hmac/tree/master/WebAPI.Hmac/Filters

    public class DataLoadController : BaseApiController
    {
        protected IDataLoadRepository _repo;

        public DataLoadController(INLogger logger, IDataLoadRepository repo) : base(logger) {
            this._repo = repo;
        }

        [RequireHttps]
        [HttpGet]
        public HttpResponseMessage Index(string q = "")
        {
            return Request.CreateResponse(HttpStatusCode.OK, new { msg = "silence is golden" });
        }

        [RequireHttps]
        [HttpGet]
        [BasicAuthenticationFilter]
        [Route("api/dataload/sql/get/{q}", Name = "GetSQL")]
        public HttpResponseMessage GetSQL(string q)
        {
            try
            {
                // C76D3AA947774485B097329B84FB39A6
                dynamic data = _repo.sqlGetCICOOFF(q);
                var result = new 
                {
                    segmentKey = data.SegmentKey,
                    fips = data.FIPS,
                    route = data.ROUTE,
                    segmId = data.SEGMID,
                    updateYr = data.UPDATEYR.ToString()
                };
                return Request.CreateResponse(HttpStatusCode.OK, result);

            }
            catch (Exception ex)
            {
                return NotFound("Not found.");
               
            }
        }

        [RequireHttps]
        [HttpGet]
        [BasicAuthenticationFilter]
        [Route("api/dataload/orcl/get/{q}", Name = "GetORCL")]
        public HttpResponseMessage GetOrcl(string q)
        {
            try
            {
                // C76D3AA947774485B097329B84FB39A6
                dynamic data = _repo.orclGetCICOOFF(q);
                var result = new
                {
                    guid = data.GUID,
                    fips = data.FIPS,
                    route = data.ROUTE,
                    segmId = data.SEGMID,
                    updateYr = data.UPDATEYR
                };
                return Request.CreateResponse(HttpStatusCode.OK, result);

            }
            catch (Exception ex)
            {
                return NotFound("Not found.");

            }
        }

        [HttpGet]
        [RequireHttps]
        [Route("api/dataload/load/fips/{q}", Name = "Load")]
        [BasicAuthenticationFilter]
        public HttpResponseMessage Load(string q)
        {
            var fips = q == "99999" ? "" : q;
            _logger.Info(string.Format("Loading data from Oracle to SQL for FIPS => {0}", fips));
            var success = _repo.BulkCopyFromOrcleToSql(fips);
            var js = new { success = success };
            if (success)
            {
                return Request.CreateResponse(HttpStatusCode.OK, js);
            }
            return Request.CreateResponse(HttpStatusCode.InternalServerError, js);


            //try
            //{

            //    if (success)
            //    {
            //        var js = new { success = success };
            //        return Request.CreateResponse(HttpStatusCode.OK, js);
            //    }

            //    throw new InvalidOperationException("Could not load data");

            //}
            //catch (Exception ex)
            //{
            //    return Error(ex.Message);
            //}
        }

        [HttpGet]
        [RequireHttps]
        [Route("api/dataload/Commit/fips/{q}", Name = "Commit")]
        [BasicAuthenticationFilter]
        public HttpResponseMessage Commit(string q)
        {
            var fips = q == "99999" ? "" : q;
            try
            {

                var success = _repo.BulkCopyFromSqlToOracle(fips);

                if (success)
                {
                    var js = new { success = success };
                    return Request.CreateResponse(HttpStatusCode.OK, js);
                }

                throw new InvalidOperationException("Could not load data");

            }
            catch (Exception ex)
            {
                return Error(ex.Message);
            }
        }
    }
}
