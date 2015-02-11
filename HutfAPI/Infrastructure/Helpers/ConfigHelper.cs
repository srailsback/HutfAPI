using HuftAPI.Logging;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Web;

namespace HutfAPI.Infrastructure.Helpers
{
    public  static class ConfigHelper
    {
        private static INLogger _logger;

        /// <summary>
        /// Gets an application settings.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static string GetAppSetting(string key)
        {
            string value = "";
            try
            {
                value = ConfigurationManager.AppSettings[key];
            }
            catch (InvalidOperationException ex)
            {
                _logger.Fatal(string.Format("{0} app setting key not found."));
            }
            return value;
        }
    }
}