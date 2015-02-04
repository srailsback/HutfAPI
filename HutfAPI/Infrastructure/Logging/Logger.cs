using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace HuftAPI.Logging {

    /// <summary>
    /// NLog interface
    /// </summary>
    public interface INLogger
    {
        /// <summary>
        /// Information the specified message.
        /// </summary>
        /// <param name="message">The message.</param>
        void Info(string message);

        /// <summary>
        /// Warns the specified message.
        /// </summary>
        /// <param name="message">The message.</param>
        void Warn(string message);

        /// <summary>
        /// Debugs the specified message.
        /// </summary>
        /// <param name="message">The message.</param>
        void Debug(string message);

        /// <summary>
        /// Errors the specified message.
        /// </summary>
        /// <param name="message">The message.</param>
        void Error(string message);

        /// <summary>
        /// Errors the specified ex.
        /// </summary>
        /// <param name="ex">The ex.</param>
        void Error(Exception ex);

        void ErrorException(string message, Exception ex);

        /// <summary>
        /// Fatals the specified message.
        /// </summary>
        /// <param name="message">The message.</param>
        void Fatal(string message);

        /// <summary>
        /// Fatals the specified ex.
        /// </summary>
        /// <param name="ex">The ex.</param>
        void Fatal(Exception ex);

        void FatalException(string message, Exception ex);
    }

    /// <summary>
    /// NLog class
    /// </summary>
    public class NLogger : INLogger
    {
        /// <summary>
        /// The _logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="NLogger"/> class.
        /// </summary>
        public NLogger()
        {
            //_logger = LogManager.GetLogger("ODOTActiveDirectoryTest");
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// Information the specified message.
        /// </summary>
        /// <param name="message">The message.</param>
        public void Info(string message)
        {
            _logger.Info(message);
        }

        /// <summary>
        /// Warns the specified message.
        /// </summary>
        /// <param name="message">The message.</param>
        public void Warn(string message)
        {
            _logger.Warn(message);
        }

        /// <summary>
        /// Debugs the specified message.
        /// </summary>
        /// <param name="message">The message.</param>
        public void Debug(string message)
        {
            _logger.Debug(message);
        }

        /// <summary>
        /// Errors the specified message.
        /// </summary>
        /// <param name="message">The message.</param>
        public void Error(string message)
        {
            //_logger.Error(message);
            this.ErrorException(message, null);
        }

        /// <summary>
        /// Errors the specified ex.
        /// </summary>
        /// <param name="ex">The ex.</param>
        public void Error(Exception ex)
        {
            //_logger.Error(LogUtility.BuildExceptionMessage(ex));
            this.ErrorException(ex.Message, ex);
        }

        /// <summary>
        /// Errors the specified ex.
        /// </summary>
        /// <param name="ex">The ex.</param>
        public void ErrorException(string message, Exception ex)
        {
            _logger.ErrorException(message, ex);
        }

        /// <summary>
        /// Fatals the specified message.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <exception cref="System.NotImplementedException"></exception>
        public void Fatal(string message)
        {
            //_logger.Fatal(message);
            //throw new NotImplementedException();
            this.FatalException(message, null);
        }

        /// <summary>
        /// Fatals the specified ex.
        /// </summary>
        /// <param name="ex">The ex.</param>
        public void Fatal(Exception ex)
        {
            //_logger.Fatal(LogUtility.BuildExceptionMessage(ex));
            this.FatalException(ex.Message, ex);
        }

        /// <summary>
        /// Logs a fatal exception.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="ex">The ex.</param>
        public void FatalException(string message, Exception ex)
        {
            _logger.FatalException(message, ex);
        }
    }

    /// <summary>
    /// Log utility helper
    /// </summary>
    public class LogUtility
    {
        /// <summary>
        /// Builds the exception message.
        /// </summary>
        /// <param name="x">The x.</param>
        /// <returns></returns>
        public static string BuildExceptionMessage(Exception x)
        {

            Exception logException = x;
            if (x.InnerException != null)
                logException = x.InnerException;

            //string strErrorMsg = Environment.NewLine + "Error in Path :" + System.Web.HttpContext.Current.Request.Path;
            string strErrorMsg = Environment.NewLine + "Error in Path : NA";

            // Get the QueryString along with the Virtual Path
            //strErrorMsg += Environment.NewLine + "Raw Url :" + System.Web.HttpContext.Current.Request.RawUrl;
            strErrorMsg += Environment.NewLine + "Raw Url : NA";


            // Get the error message
            strErrorMsg += Environment.NewLine + "Message :" + logException.Message;

            // Source of the message
            strErrorMsg += Environment.NewLine + "Source :" + logException.Source;

            // Stack Trace of the error

            strErrorMsg += Environment.NewLine + "Stack Trace :" + logException.StackTrace;

            // Method where the error occurred
            strErrorMsg += Environment.NewLine + "TargetSite :" + logException.TargetSite;
            return strErrorMsg;
        }
    }

}
