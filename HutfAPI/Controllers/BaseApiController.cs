using System;
using HuftAPI.Logging;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace HutfAPI.Controllers
{
    public class BaseApiController : ApiController
    {
        protected INLogger _logger;

        public BaseApiController(INLogger logger)
        {
            this._logger = logger;
        }

        public HttpResponseMessage NotFound(string message)
        {
            var ex = new Exception(message);
            _logger.Error(ex);
            var js = new { error = true, msg = ex };
            return Request.CreateResponse(HttpStatusCode.NotFound, js);
        }


        public HttpResponseMessage Error(string message)
        {
            var ex = new Exception(message);
            _logger.Fatal(ex);
            var js = new { error = true, msg = ex };
            return Request.CreateResponse(HttpStatusCode.InternalServerError, js);
        }
    }
}