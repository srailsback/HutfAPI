using Newtonsoft.Json.Serialization;
using System.Web.Http;

namespace HutfAPI
{
    public static class SettingsConfig
    {
        /// <summary>
        /// JSON the formatting
        /// </summary>
        public static void JsonFormatters()
        {

            var formatters = GlobalConfiguration.Configuration.Formatters;

            // Remove default XML formatter
            formatters.XmlFormatter.SupportedMediaTypes.Clear();

            // JSON formatting
            var json = GlobalConfiguration.Configuration.Formatters.JsonFormatter;
            json.SerializerSettings.Formatting = Newtonsoft.Json.Formatting.Indented;
            json.SerializerSettings.ContractResolver = new CamelCasePropertyNamesContractResolver();

            // Fix JSON.NET self referencing issue 
            json.SerializerSettings.ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Ignore;
        }
    }
}