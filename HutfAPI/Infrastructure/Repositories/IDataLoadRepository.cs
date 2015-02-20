using HuftAPI.Logging;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using Massive.Oracle;
using System.Data.Common;
using System.Dynamic;
using HutfAPI.Infrastructure.Helpers;

namespace HutfAPI.Infrastructure.Repositories
{
    public interface IDataLoadRepository
    {
        #region SQL

        /// <summary>
        /// Get CICOOFF by SegmentKey
        /// </summary>
        /// <param name="segmentKey">The segment key.</param>
        /// <returns></returns>
        object sqlGetCICOOFF(string segmentKey);


        /// <summary>
        /// SQL get all CICOOFF with FIPS. Returns all CICOOFFs if no FIPS
        /// </summary>
        /// <param name="fips">The FIPS.</param>
        /// <returns></returns>
        IEnumerable<dynamic> sqlGetAllCICOOOFF(string fips = "");


        /// <summary>
        /// SQL get HOVT by FIPS, ROUTE, and SEGMID
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <param name="route">The route.</param>
        /// <param name="segmId">The segm identifier.</param>
        /// <returns></returns>
        dynamic sqlGetHOVT(string fips, string route, int segmId);


        /// <summary>
        /// SQL get all HOVT with FIPS. Returns all HOVTs if no FIPS. 
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <returns></returns>
        IEnumerable<dynamic> sqlGetAllCICOHOVT(string fips = "");


        /// <summary>
        /// SQL get all HPMS by FIPS, ROUTE, and SEGMID
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <param name="route">The route.</param>
        /// <param name="segmId">The segm identifier.</param>
        /// <returns></returns>
        dynamic sqlGetHPMS(string fips, string route, int segmId);


        /// <summary>
        /// SQL get all HPMS with FIPS. Returns all HMPSs if no FIPS,
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <returns></returns>
        IEnumerable<dynamic> sqlGetAllCICOHPMS(string fips = "");

        dynamic sqlGetLiveCICOOFF(string segmentKey);
        IEnumerable<dynamic> sqlGetAllLiveCICOOFF(string fips = "");

        dynamic sqlGetLiveCICOHOVT(string fips, string route, int segmId);
        IEnumerable<dynamic> sqlGetAllLiveCICOHOVT(string fips = "");

        #endregion



        #region ORCL

        /// <summary>
        /// Oracle get CICOOFF by SegmentKey
        /// </summary>
        /// <param name="segmentKey">The segment key.</param>
        /// <returns></returns>
        object orclGetCICOOFF(string segmentKey);


        /// <summary>
        /// Oracle get all CICOOFF with FIPS. Returns all CICOOFFs if no FIPS
        /// </summary>
        /// <param name="fips">The FIPS.</param>
        /// <returns></returns>        
        IEnumerable<dynamic> orclGetAllCICOOOFF(string fips = "");


        /// <summary>
        /// Oracle get HOVT by FIPS, ROUTE, and SEGMID
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <param name="route">The route.</param>
        /// <param name="segmId">The segm identifier.</param>
        /// <returns></returns>
        dynamic orclGetCICOHOVT(string fips, string route, int segmId);


        /// <summary>
        /// Oracle get all HOVT with FIPS. Returns all HOVTs if no FIPS. 
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <returns></returns>
        IEnumerable<dynamic> orclGetAllCICOHOVT(string fips = "");


        /// <summary>
        /// Oracle get all HPMS by FIPS, ROUTE, and SEGMID
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <param name="route">The route.</param>
        /// <param name="segmId">The segm identifier.</param>
        /// <returns></returns>
        dynamic orclGetCICOHPMS(string fips, string route, int segmId);


        /// <summary>
        /// Oracle get all HPMS with FIPS. Returns all HMPSs if no FIPS,
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <returns></returns>
        IEnumerable<dynamic> orclGetAllCICOHPMS(string fips = "");
        #endregion



        #region Load Oracle into SQL

        bool CopyFromSqlToOracle(string segmentKey);

        /// <summary>
        /// Copy data from Oracle to SQL for a FIPS, all if no FIPS is provided. Do this inside a transaction we we can roll it back on an error.
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <returns></returns>
        bool BulkCopyFromOrcleToSql(string fips = "");



        #endregion



        #region Load SQL into Oracle

        /// <summary>
        /// Copy data from Sql to Oracle for a FIPS, all if no FIPS is provided. Do this inside a transaction we we can roll it back on an error.
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <returns></returns>
        bool BulkCopyFromSqlToOracle(string fips = "");

        #endregion

    }

    public class DataLoadRepository : IDataLoadRepository
    {
        #region members

        private INLogger _logger;

        private string ORCL_CONNECTION_STRING_NAME;
        private string SQL_CONNECTION_STRING_NAME;

        // oracle tables
        private string ORCL_CICOOFF;
        private string ORCL_CICOHPMS;
        private string ORCL_CICOHOVT;

        // sql tables
        private string SQL_CICOOFF;
        private string SQL_CICOHPMS;
        private string SQL_CICOHOVT;
        private string SQL_LIVE_CICOOFF;
        private string SQL_LIVE_CICOHOVT;


        #endregion

        public DataLoadRepository(INLogger logger)
        {
            this._logger = logger;

            // connection string names
            this.ORCL_CONNECTION_STRING_NAME = GetConnectionName(true);
            this.SQL_CONNECTION_STRING_NAME = GetConnectionName();

            // orcl tables
            this.ORCL_CICOOFF = ConfigHelper.GetAppSetting("ORCL_CICOOFF_TABLE");
            this.ORCL_CICOHPMS = ConfigHelper.GetAppSetting("ORCL_CICOHPMS_TABLE");
            this.ORCL_CICOHOVT = ConfigHelper.GetAppSetting("ORCL_CICOHOVT_TABLE");

            // sql tables
            this.SQL_CICOOFF = ConfigHelper.GetAppSetting("SQL_CICOOFF_TABLE");
            this.SQL_CICOHPMS = ConfigHelper.GetAppSetting("SQL_CICOHPMS_TABLE");
            this.SQL_CICOHOVT = ConfigHelper.GetAppSetting("SQL_CICOHOVT_TABLE");
            this.SQL_LIVE_CICOOFF = ConfigHelper.GetAppSetting("SQL_LIVE_CICOOFF_TABLE");
            this.SQL_LIVE_CICOHOVT = ConfigHelper.GetAppSetting("SQL_LIVE_CICOHOVT_TABLE");

        }

        #region helpers


        /// <summary>
        /// Gets name of the connection string.
        /// </summary>
        /// <param name="isOracle">if set to <c>true</c> [is oracle].</param>
        /// <returns></returns>
        private string GetConnectionName(bool isOracle = false)
        {
            // get all connection strings
            var connectionStrings = ConfigurationManager.ConnectionStrings.Cast<ConnectionStringSettings>().ToList();

            if (isOracle)
            {
                return connectionStrings.Single(x => x.Name.ToLower().Contains("orcl")).Name;
            }
            return connectionStrings.Single(x => x.Name.ToLower().Contains("sql") && !x.Name.ToLower().Contains("local")).Name;
        }

        #endregion






        #region SQL


        /// <summary>
        /// Get CICOOFF by SegmentKey
        /// </summary>
        /// <param name="segmentKey">The segment key.</param>
        /// <returns></returns>
        public object sqlGetCICOOFF(string segmentKey)
        {
            var db = new Massive.DynamicModel(SQL_CONNECTION_STRING_NAME, tableName: SQL_CICOOFF);
            var where = "SegmentKey = @0";
            return db.Single(where, args: segmentKey);
        }


        /// <summary>
        /// SQL get all CICOOFF with FIPS. Returns all CICOOFFs if no FIPS
        /// </summary>
        /// <param name="fips">The FIPS.</param>
        /// <returns></returns>
        public IEnumerable<dynamic> sqlGetAllCICOOOFF(string fips = "")
        {

            var db = new Massive.DynamicModel(SQL_CONNECTION_STRING_NAME, tableName: SQL_CICOOFF);
            if (!string.IsNullOrWhiteSpace(fips))
            {
                var where = "FIPS = @0";
                return db.All(where: where, args: fips);
            }
            return db.All();
        }


        /// <summary>
        /// SQL get HOVT by FIPS, ROUTE, and SEGMID
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <param name="route">The route.</param>
        /// <param name="segmId">The segm identifier.</param>
        /// <returns></returns>
        public dynamic sqlGetHOVT(string fips, string route, int segmId)
        {
            var db = new Massive.DynamicModel(SQL_CONNECTION_STRING_NAME, tableName: SQL_CICOHOVT);
            var where = "FIPS = @0 AND ROUTE = @1 AND SEGMID = @2";
            var args = new List<object>();
            args.Add(fips);
            args.Add(route);
            args.Add(segmId);
            return db.Single(where, args: args.ToArray());
        }


        /// <summary>
        /// SQL get all HOVT with FIPS. Returns all HOVTs if no FIPS.
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <returns></returns>
        public IEnumerable<dynamic> sqlGetAllCICOHOVT(string fips = "")
        {
            var db = new Massive.DynamicModel(SQL_CONNECTION_STRING_NAME, tableName: SQL_CICOHOVT);
            if (!string.IsNullOrWhiteSpace(fips))
            {
                var where = "FIPS = @0";
                return db.All(where: where, args: fips);
            }
            return db.All();
        }


        /// <summary>
        /// SQL get all HPMS by FIPS, ROUTE, and SEGMID
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <param name="route">The route.</param>
        /// <param name="segmId">The segm identifier.</param>
        /// <returns></returns>
        public dynamic sqlGetHPMS(string fips, string route, int segmId)
        {
            var db = new Massive.DynamicModel(SQL_CONNECTION_STRING_NAME, tableName: SQL_CICOHPMS);
            var where = "FIPS = @0 AND ROUTE = @1 AND SEGMID = @2";
            var args = new List<object>();
            args.Add(fips);
            args.Add(route);
            args.Add(segmId);
            return db.Single(where, args: args.ToArray());
        }


        /// <summary>
        /// SQL get all HPMS with FIPS. Returns all HMPSs if no FIPS,
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <returns></returns>
        public IEnumerable<dynamic> sqlGetAllCICOHPMS(string fips = "")
        {
            var db = new Massive.DynamicModel(SQL_CONNECTION_STRING_NAME, tableName: SQL_CICOHPMS);
            if (!string.IsNullOrWhiteSpace(fips))
            {
                var where = "FIPS = @0";
                return db.All(where: where, args: fips);
            }
            return db.All();
        }

        public dynamic sqlGetLiveCICOOFF(string segmentKey)
        {
            var db = new Massive.DynamicModel(SQL_CONNECTION_STRING_NAME, tableName: SQL_LIVE_CICOOFF);
            var where = "SegmentKey = @0";
            return db.Single(where: where, args: segmentKey);
        }

        public IEnumerable<dynamic> sqlGetAllLiveCICOOFF(string fips = "")
        {
            var db = new Massive.DynamicModel(SQL_CONNECTION_STRING_NAME, tableName: SQL_LIVE_CICOOFF);
            if (!string.IsNullOrWhiteSpace(fips))
            {
                var where = "FIPS = @0";
                return db.All(where: where, args: fips);
            }
            return db.All();
        }

        public dynamic sqlGetLiveCICOHOVT(string fips, string route, int segmId)
        {
            var db = new Massive.DynamicModel(SQL_CONNECTION_STRING_NAME, tableName: SQL_LIVE_CICOHOVT);
            var where = "FIPS = @0 AND ROUTE = @1 AND SEGMID = @2";
            var args = new List<object>();
            args.Add(fips);
            args.Add(route);
            args.Add(segmId);
            return db.Single(where, args: args.ToArray());
        }

        public IEnumerable<dynamic> sqlGetAllLiveCICOHOVT(string fips = "")
        {
            var db = new Massive.DynamicModel(SQL_CONNECTION_STRING_NAME, tableName: SQL_LIVE_CICOHOVT);
            if (!string.IsNullOrWhiteSpace(fips))
            {
                var where = "FIPS = @0";
                return db.All(where: where, args: fips);
            }
            return db.All();
        }


        #endregion





        #region Oracle

        /// <summary>
        /// Oracle get CICOOFF by SegmentKey
        /// </summary>
        /// <param name="segmentKey">The segment key.</param>
        /// <returns></returns>
        public object orclGetCICOOFF(string segmentKey)
        {
            var db = new Massive.Oracle.DynamicModel(ORCL_CONNECTION_STRING_NAME, tableName: ORCL_CICOOFF);
            var where = "GUID = :0";
            return db.Single(where, args: segmentKey);
        }


        /// <summary>
        /// Oracle get all CICOOFF with FIPS. Returns all CICOOFFs if no FIPS
        /// </summary>
        /// <param name="fips">The FIPS.</param>
        /// <returns></returns>
        public IEnumerable<dynamic> orclGetAllCICOOOFF(string fips = "")
        {
            _logger.Info(string.Format("Getting CICOOFF for FIPS => {0}", fips));
            var db = new Massive.Oracle.DynamicModel(ORCL_CONNECTION_STRING_NAME, tableName: ORCL_CICOOFF);
            if (!string.IsNullOrWhiteSpace(fips))
            {
                var where = "FIPS = :0";
                return db.All(where: where, args: fips);
            }
            return db.All();
        }


        /// <summary>
        /// Oracle get HOVT by FIPS, ROUTE, and SEGMID
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <param name="route">The route.</param>
        /// <param name="segmId">The segm identifier.</param>
        /// <returns></returns>
        public dynamic orclGetCICOHOVT(string fips, string route, int segmId)
        {
            var db = new Massive.Oracle.DynamicModel(ORCL_CONNECTION_STRING_NAME, tableName: ORCL_CICOHOVT);
            var where = "FIPS = :0 AND ROUTE = :1 AND SEGMID = :2";
            var args = new List<object>();
            args.Add(fips);
            args.Add(route);
            args.Add(segmId);
            return db.Single(where, args: args.ToArray());
        }


        /// <summary>
        /// Oracle get all HOVT with FIPS. Returns all HOVTs if no FIPS.
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <returns></returns>
        public IEnumerable<dynamic> orclGetAllCICOHOVT(string fips = "")
        {
            _logger.Info(string.Format("Getting CICOHOVT for FIPS => {0}", fips));
            var db = new Massive.Oracle.DynamicModel(ORCL_CONNECTION_STRING_NAME, tableName: ORCL_CICOHOVT);
            if (!string.IsNullOrWhiteSpace(fips))
            {
                var where = "FIPS = :0";
                return db.All(where: where, args: fips);
            }
            return db.All();
        }


        /// <summary>
        /// Oracle get all HPMS by FIPS, ROUTE, and SEGMID
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <param name="route">The route.</param>
        /// <param name="segmId">The segm identifier.</param>
        /// <returns></returns>
        public dynamic orclGetCICOHPMS(string fips, string route, int segmId)
        {
            var db = new Massive.Oracle.DynamicModel(ORCL_CONNECTION_STRING_NAME, tableName: ORCL_CICOHPMS);
            var where = "FIPS = :0 AND ROUTE = :1 AND SEGMID = :2";
            var args = new List<object>();
            args.Add(fips);
            args.Add(route);
            args.Add(segmId);
            return db.Single(where, args: args.ToArray());
        }


        /// <summary>
        /// Oracle get all HPMS with FIPS. Returns all HMPSs if no FIPS,
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <returns></returns>
        public IEnumerable<dynamic> orclGetAllCICOHPMS(string fips = "")
        {
            _logger.Info(string.Format("Getting CICOHPMS for FIPS => {0}", fips));
            var db = new Massive.Oracle.DynamicModel(ORCL_CONNECTION_STRING_NAME, tableName: ORCL_CICOHPMS);
            if (!string.IsNullOrWhiteSpace(fips))
            {
                var where = "FIPS = :0";
                return db.All(where: where, args: fips);
            }
            return db.All();
        }


        private DbCommand orclDeleteCICOOFF(string fipsOrGuid)
        {
            var table = new ORCL_CICOOFF_TABLE();
            var args = new List<object>();
            if (fipsOrGuid.Length <= 5)
            {
                _logger.Info(string.Format("Deleting records from TABLE => {0} for FIPS => ", ORCL_CICOOFF, fipsOrGuid));
                args.Add(fipsOrGuid);
                return table.CreateDeleteCommand(where: "FIPS = :0", args: args.ToArray());
            }
            else
            {
                fipsOrGuid = fipsOrGuid.Replace(@"-", "").ToUpper();

                _logger.Info(string.Format("Deleting record from TABLE => {0} for GUID => ", ORCL_CICOOFF, fipsOrGuid));
                args.Add(fipsOrGuid);
                return table.CreateDeleteCommand(where: "GUID = :0", args: args.ToArray());
            }
        }

        private DbCommand orclInsertCICOOFF(dynamic cicooffToInsert)
        {
            string segmentKey = cicooffToInsert.SegmentKey.ToString().ToLower().Replace(@"-", "");

            IDictionary<string, object> map = cicooffToInsert;
            map.Remove("SegmentKey");
            map.Remove("LASTEDITEDATUTC");
            map.Add("GUID", segmentKey.ToUpper());

            var expando = new ExpandoObject();
            var cols = (ICollection<KeyValuePair<string, object>>)expando;
            var colsToIgnore = new string[] { "segmentkey", "lasteditedatutc", "routesuffix" };
            foreach (var kvp in map)
            {
                if (!colsToIgnore.Any(x => x == kvp.Key.ToLower()))
                {
                    cols.Add(kvp);
                }
            }
            dynamic toCommit = expando;

            var table = new ORCL_CICOOFF_TABLE();
            return table.CreateInsertCommand(expando);
        }

        private DbCommand orclUpdateCICOHOVT(dynamic newCICOHOVT, dynamic oldCICOHOVT, dynamic newCICOOFF)
        {
            var args = new List<object>();
            args.Add(newCICOHOVT.LENGTH_); // 0
            args.Add(newCICOHOVT.UPDATEYR); // 1
            args.Add(newCICOHOVT.FIPSCOUNTY); // 2
            args.Add(newCICOHOVT.HOVTYPE); // 3
            args.Add(newCICOHOVT.HOVLNQTY); // 4
            args.Add(oldCICOHOVT.TOLLID); // 5
            args.Add(oldCICOHOVT.TOLLTYPE); // 6
            args.Add(oldCICOHOVT.TOLLCHARGED); // 7
            args.Add(newCICOOFF.LRSROUTE); // 8
            args.Add(newCICOOFF.TOMEAS); // 9
            args.Add(newCICOOFF.FROMMEAS); // 10
            args.Add(newCICOHOVT.FIPS); // 11
            args.Add(newCICOHOVT.ROUTE); // 12
            args.Add(newCICOHOVT.SEGMID); // 13

            var sql = @"UPDATE {0} SET 
                        LENGTH_ = :0, 
                        UPDATEYR = :1, 
                        FIPSCOUNTY = :2,
                        HOVTYPE = :3,
                        HOVLNQTY = :4,
                        TOLLID = :5,
                        TOLLTYPE = :6,
                        TOLLCHARGED = :7,
                        LRSROUTE = :8,
                        TOMEAS = :9,
                        FROMMEAS = :10
                        WHERE FIPS = :11 AND ROUTE = :12 AND SEGMID = :13";
            sql = string.Format(sql, ORCL_CICOHOVT, newCICOHOVT.FIPS, newCICOHOVT.ROUTE, newCICOHOVT.SEGMID);

            var table = new ORCL_CICOHOVT_TABLE();
            return table.CreateCommand(sql, null, args: args.ToArray());
        }

        private DbCommand orclInsertCICOHOVT(dynamic newCICOHOVT, dynamic cicooff)
        {
            var args = new List<object>();
            args.Add(newCICOHOVT.FIPS); // 0
            args.Add(newCICOHOVT.ROUTE); // 1
            args.Add(newCICOHOVT.SEGMID); // 2
            args.Add(newCICOHOVT.LENGTH_); // 3
            args.Add(newCICOHOVT.UPDATEYR); // 4
            args.Add(newCICOHOVT.FIPSCOUNTY); // 5
            args.Add(newCICOHOVT.HOVTYPE); // 6
            args.Add(newCICOHOVT.HOVLNQTY); // 7
            args.Add(cicooff.LRSROUTE); // 8
            args.Add(cicooff.TOMEAS); // 9
            args.Add(cicooff.FROMMEAS); // 10

            var sql = "INSERT INTO {0} (FIPS, ROUTE, SEGMID, LENGTH_, UPDATEYR, FIPSCOUNTY, HOVTYPE, HOVLNQTY, LRSROUTE, TOMEAS, FROMMEAS) VALUES (:0, :1, :2, :3, :4, :5, :6, :7, :8, :9, :10)";
            sql = string.Format(sql, ORCL_CICOHOVT);

            var table = new ORCL_CICOHOVT_TABLE();
            return table.CreateCommand(sql, null, args: args.ToArray());

        }

        private IDbCommand orclDeleteCICOHOVT(dynamic cicohovtToDelete)
        {
            var args = new List<object>();
            args.Add(cicohovtToDelete.FIPS);
            args.Add(cicohovtToDelete.ROUTE);
            args.Add(cicohovtToDelete.SEGMID);
            var table = new ORCL_CICOHOVT_TABLE();
            var sql = string.Format("DELETE FROM {0} WHERE FIPS = :0 AND ROUTE = :1 AND SEGMID = :2", ORCL_CICOHOVT);
            return table.CreateCommand(sql, null, args: args.ToArray());
        }

        private bool orclValidateCICOOFF()
        {
            // data from Oralce is inconsistent.
            // need to have guid and gisid property formatted.

            var db = new ExtendedDynamicModel(ORCL_CONNECTION_STRING_NAME);
            using (var orclConnection = db.Connection = db.OpenConnection())
            {
                _logger.Info(string.Format("Validating and updating TABLE => {0}", ORCL_CICOOFF));
                using (var orclTransaction = db.Transaction = orclConnection.BeginTransaction())
                {
                    try
                    {
                        // doing this all under transactions, create a todo list
                        // add every update, insert, and delete into the toDo queue
                        var toDo = new List<DbCommand>();

                        var table = new ORCL_CICOOFF_TABLE();
                        // insert new guid where it's null
                        var sql1 = string.Format("UPDATE {0} SET GUID = UPPER(SYS_GUID()) WHERE GUID IS NULL", ORCL_CICOOFF);
                        toDo.Add(table.CreateCommand(sql1, null));

                        // convert any new guids from lower case
                        var sql2 = string.Format("UPDATE {0} SET GUID = UPPER(GUID) WHERE GUID = lower(GUID)", ORCL_CICOOFF);
                        toDo.Add(table.CreateCommand(sql2, null));

                        // correct for poorly formatted gisid
                        var sql3 = string.Format("UPDATE {0} SET GISID = RPAD(FIPS, 5, ' ') || RPAD(ROUTE, 11, ' ') || LPAD(SEGMID, 5, ' ') WHERE (GISID <> RPAD(FIPS, 5, ' ') || RPAD(ROUTE, 11, ' ') || LPAD(SEGMID, 5, ' ')) OR LENGTH(TRIM(GISID)) = 0 OR GISID IS NULL", ORCL_CICOOFF);
                        toDo.Add(table.CreateCommand(sql3, null));

                        var result = table.Execute(toDo);
                        orclTransaction.Commit();
                        _logger.Info(string.Format("TABLE => {0} was validated and updated", ORCL_CICOOFF));
                        return true;
                    }
                    catch (Exception ex)
                    {
                        _logger.ErrorException(string.Format("Could not validate and update TABLE => {0}", ORCL_CICOOFF), ex);
                        orclTransaction.Rollback();
                        return false;
                    }
                }

            }
        }

        #endregion



        #region Load Oracle into SQL

        /// <summary>
        /// Copy data from Oracle to SQL for a FIPS, all if no FIPS is provided. Do this inside a transaction we we can roll it back on an error.
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <returns></returns>
        public bool BulkCopyFromOrcleToSql(string fips = "")
        {
            _logger.Info(string.Format("Performing bulk copy from Oracle to SQL for FIPS => {0}", fips));

            // validate and update cicooff, because their data is inconsistent and webhutf expects it right
           
            // if not fixed, stop here!!!!
            var isValidCICOOFF = orclValidateCICOOFF();

            if (!isValidCICOOFF)
            {
                _logger.Info("Bulk copy from Oracle to SQL process stopped. This could be due to null or improperly formatted GUID and GISID");
                return false;
            }

            // using sql connection
            using (var sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings[SQL_CONNECTION_STRING_NAME].ConnectionString))
            {
                sqlConnection.Open();

                // do all this work inside a transaction so we can roll it back
                var sqlTransaction = sqlConnection.BeginTransaction();

                try
                {
                    // delete fips from destination table, truncate them if no fips
                    SqlDeleteOrTruncate(sqlConnection, sqlTransaction, SQL_CICOOFF, fips);
                    SqlDeleteOrTruncate(sqlConnection, sqlTransaction, SQL_CICOHOVT, fips);
                    SqlDeleteOrTruncate(sqlConnection, sqlTransaction, SQL_CICOHPMS, fips);

                    // sql bulk copy
                    SqlBulkCopyFromOracle(sqlConnection, sqlTransaction, SQL_CICOOFF, fips);
                    SqlBulkCopyFromOracle(sqlConnection, sqlTransaction, SQL_CICOHOVT, fips);
                    SqlBulkCopyFromOracle(sqlConnection, sqlTransaction, SQL_CICOHPMS, fips);

                    sqlTransaction.Commit();
                    _logger.Info("Bulk copy from Oracle to SQL success");
                    return true;
                }
                catch (Exception ex)
                {
                    _logger.ErrorException(ex.Message, ex);
                    sqlTransaction.Rollback();
                    return false;
                }

            }
        }

        /// <summary>
        /// Delete or truncate a SQL table
        /// We include the sql connection and transaction so it may be rolled back.
        /// </summary>
        /// <param name="sqlConnection">The SQL connection.</param>
        /// <param name="sqlTransaction">The SQL transaction.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="fips">The fips.</param>
        private void SqlDeleteOrTruncate(SqlConnection sqlConnection, SqlTransaction sqlTransaction, string tableName, string fips = "")
        {
            var sql = "";
            if (string.IsNullOrWhiteSpace(fips))
            {
                _logger.Info(string.Format("TRUNCATING table {0}", tableName));
                sql = string.Format("TRUNCATE TABLE {0}", tableName);
            }
            else
            {
                _logger.Info(string.Format("DELETING from TABLE => {0} for FIPS => {1}", tableName, fips));
                sql = string.Format("DELETE FROM {0} WHERE FIPS = @FIPS", tableName, fips);
            }

            var cmd = new SqlCommand(sql, sqlConnection, sqlTransaction);
            if (!string.IsNullOrWhiteSpace(fips))
            {
                cmd.Parameters.AddWithValue("@FIPS", fips);
            }
            cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Bulk copy dynamic data from Oracle into SQL. SqlBulkCopy works with with datatables and readers. 
        /// We pass in the connection, transaction, oracle cico* table to query and the fips
        /// then toss the collection of dynamic objects into a datatable.
        /// </summary>
        /// <param name="sqlConnection">The SQL connection.</param>
        /// <param name="sqlTransaction">The SQL transaction.</param>
        /// <param name="destinationTable">The destination table.</param>
        /// <param name="fips">The fips.</param>
        /// <exception cref="System.InvalidOperationException">Invalid destination table.</exception>
        private void SqlBulkCopyFromOracle(SqlConnection sqlConnection, SqlTransaction sqlTransaction, string destinationTable, string fips = "")
        {

            // get the datatable source
            DataTable source;
            if (destinationTable == SQL_CICOOFF)
            {
                _logger.Info(string.Format("Creating DATATABLE from ORACLE TABLE => {0} for FIPS => {1}", ORCL_CICOOFF, fips));
                source = CICOOFF(orclGetAllCICOOOFF(fips).ToList());
            }
            else if (destinationTable == SQL_CICOHOVT)
            {
                _logger.Info(string.Format("Creating DATATABLE from ORACLE TABLE => {0} for FIPS => {1}", ORCL_CICOHOVT, fips));
                source = CICOHOVT(orclGetAllCICOHOVT(fips).ToList());
            }
            else if (destinationTable == SQL_CICOHPMS)
            {
                _logger.Info(string.Format("Creating DATATABLE from ORACLE TABLE => {0} for FIPS => {1}", ORCL_CICOHPMS, fips));
                source = CICOHPMS(orclGetAllCICOHPMS(fips).ToList());
            }
            else
            {
                throw new InvalidOperationException("Invalid destination table.");
            }


            using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, sqlTransaction))
            {
                _logger.Info("Mapping columns");
                sqlBulkCopy.DestinationTableName = destinationTable;
                sqlBulkCopy.ColumnMappings.Clear();
                foreach (DataColumn col in source.Columns)
                {
                    sqlBulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                sqlBulkCopy.BulkCopyTimeout = 500;
                sqlBulkCopy.BatchSize = 5000;

                _logger.Info("Writing to server");
                sqlBulkCopy.WriteToServer(source);
            }
        }

        /// <summary>
        /// CICOOFF datatable.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <returns></returns>
        private DataTable CICOOFF(IList<dynamic> data, bool toOracle = false)
        {
            var dt = new DataTable();
            try
            {


                // columns
                // segment key is called GUID on the Oracle side
                if (toOracle)
                {
                    dt.Columns.Add("GUID", typeof(string));
                }
                else
                {
                    dt.Columns.Add("SegmentKey", typeof(string));
                }

                dt.Columns.Add("FIPS", typeof(string));
                dt.Columns.Add("ROUTE", typeof(string));
                dt.Columns.Add("SEGMID", typeof(decimal));
                dt.Columns.Add("LENGTH_", typeof(decimal));
                dt.Columns.Add("UPDATEYR", typeof(string));
                dt.Columns.Add("FIPSCOUNTY", typeof(string));
                dt.Columns.Add("FUNCCLASSID", typeof(string));
                dt.Columns.Add("GOVLEVEL", typeof(string));
                dt.Columns.Add("ADMINCLASS", typeof(string));
                dt.Columns.Add("POPULATION", typeof(string));
                dt.Columns.Add("URBAN", typeof(string));
                dt.Columns.Add("NAAQSID", typeof(string));
                dt.Columns.Add("NHSDESIG", typeof(string));
                dt.Columns.Add("SPECIALSYS", typeof(string));
                dt.Columns.Add("ACCESS_", typeof(string));
                dt.Columns.Add("TRKRESTRICT", typeof(string));
                dt.Columns.Add("PRIIRI", typeof(decimal));
                dt.Columns.Add("PRIIRIDATE", typeof(DateTime));
                dt.Columns.Add("PRIPSI", typeof(decimal));
                dt.Columns.Add("PROJYR", typeof(string));
                dt.Columns.Add("BUILTYR", typeof(string));
                dt.Columns.Add("INSPYR", typeof(string));
                dt.Columns.Add("PRITREATMENTDEPTH", typeof(decimal));
                dt.Columns.Add("PRISURF", typeof(string));
                dt.Columns.Add("PRISURFWD", typeof(decimal));
                dt.Columns.Add("THRULNQTY", typeof(decimal));
                dt.Columns.Add("PRITHRULNWD", typeof(decimal));
                dt.Columns.Add("OPERATION", typeof(string));
                dt.Columns.Add("RRXID", typeof(string));
                dt.Columns.Add("STRID", typeof(string));
                dt.Columns.Add("REGION", typeof(string));
                dt.Columns.Add("TPRID", typeof(string));
                dt.Columns.Add("TERRAIN", typeof(string));
                dt.Columns.Add("FORESTROUTE", typeof(string));
                dt.Columns.Add("ROUTESIGN", typeof(string));
                dt.Columns.Add("ROUTESIGNQUAL", typeof(string));
                dt.Columns.Add("JURSPLIT", typeof(string));
                dt.Columns.Add("ROUTENAME", typeof(string));
                // routesuffix was created SQL side
                if (!toOracle)
                {
                    dt.Columns.Add("ROUTESUFFIX", typeof(string));
                }
                dt.Columns.Add("FROMFEATURE", typeof(string));
                dt.Columns.Add("TOFEATURE", typeof(string));
                dt.Columns.Add("SEGMDIR", typeof(string));
                dt.Columns.Add("SEGMPREFIX", typeof(string));
                dt.Columns.Add("GISID", typeof(string));
                dt.Columns.Add("COUNTSTATIONID", typeof(string));
                dt.Columns.Add("PRIYRREHAB", typeof(string));
                dt.Columns.Add("ISDIVIDED", typeof(string));
                dt.Columns.Add("LRSROUTE", typeof(string));
                dt.Columns.Add("FROMMEAS", typeof(decimal));
                dt.Columns.Add("TOMEAS", typeof(decimal));

                if (!toOracle)
                {
                    // this one is on SQL side
                    dt.Columns.Add("LastEditedAtUTC", typeof(DateTime));
                }

                // rows
                foreach (var item in data)
                {
                    DataRow row = dt.NewRow();
                    if (toOracle)
                    {
                        row["GUID"] = item.SegmentKey;
                    }
                    else
                    {
                        row["SegmentKey"] = item.GUID;
                    }
                    row["FIPS"] = item.FIPS;
                    row["ROUTE"] = item.ROUTE;
                    row["SEGMID"] = item.SEGMID;
                    row["LENGTH_"] = item.LENGTH_;
                    row["UPDATEYR"] = item.UPDATEYR;
                    row["FIPSCOUNTY"] = item.FIPSCOUNTY;
                    row["FUNCCLASSID"] = item.FUNCCLASSID;
                    row["GOVLEVEL"] = item.GOVLEVEL;
                    row["ADMINCLASS"] = item.ADMINCLASS;
                    row["POPULATION"] = item.POPULATION;
                    row["URBAN"] = item.URBAN;
                    row["NAAQSID"] = item.NAAQSID;
                    row["NHSDESIG"] = item.NHSDESIG;
                    row["SPECIALSYS"] = item.SPECIALSYS;
                    row["ACCESS_"] = item.ACCESS_;
                    row["TRKRESTRICT"] = item.TRKRESTRICT;
                    row["PRIIRI"] = item.PRIIRI;
                    row["PRIIRIDATE"] = item.PRIIRIDATE != null ? item.PRIIRIDATE : DBNull.Value;
                    row["PRIPSI"] = item.PRIPSI;
                    row["PROJYR"] = item.PROJYR;
                    row["BUILTYR"] = item.BUILTYR;
                    row["INSPYR"] = item.INSPYR;
                    row["PRITREATMENTDEPTH"] = item.PRITREATMENTDEPTH != null ? item.PRITREATMENTDEPTH : 0;
                    row["PRISURF"] = item.PRISURF;
                    row["PRISURFWD"] = item.PRISURFWD;
                    row["THRULNQTY"] = item.THRULNQTY;
                    row["PRITHRULNWD"] = item.PRITHRULNWD;
                    row["OPERATION"] = item.OPERATION;
                    row["RRXID"] = item.RRXID;
                    row["STRID"] = item.STRID;
                    row["REGION"] = item.REGION;
                    row["TPRID"] = item.TPRID;
                    row["TERRAIN"] = item.TERRAIN;
                    row["FORESTROUTE"] = item.FORESTROUTE;
                    row["ROUTESIGN"] = item.ROUTESIGN;
                    row["ROUTESIGNQUAL"] = item.ROUTESIGNQUAL;
                    row["JURSPLIT"] = item.JURSPLIT;
                    row["ROUTENAME"] = item.ROUTENAME;
                    if (!toOracle)
                    {
                        row["ROUTESUFFIX"] = "";
                    }
                    row["FROMFEATURE"] = item.FROMFEATURE;
                    row["TOFEATURE"] = item.TOFEATURE;
                    row["SEGMDIR"] = item.SEGMDIR;
                    row["SEGMPREFIX"] = item.SEGMPREFIX;
                    row["GISID"] = item.GISID;
                    row["COUNTSTATIONID"] = item.COUNTSTATIONID;
                    row["PRIYRREHAB"] = item.PRIYRREHAB;
                    row["ISDIVIDED"] = item.ISDIVIDED;
                    row["LRSROUTE"] = item.LRSROUTE != null ? item.LRSROUTE : "";
                    row["FROMMEAS"] = item.FROMMEAS != null ? item.FROMMEAS : 0;
                    row["TOMEAS"] = item.TOMEAS != null ? item.TOMEAS : 0;
                    if (!toOracle)
                    {
                        row["LastEditedAtUTC"] = DBNull.Value;
                    }
                    dt.Rows.Add(row);
                }
                return dt;
            }
            catch (Exception ex)
            {
                _logger.ErrorException(ex.Message, ex);
                return dt;
            }

        }

        /// <summary>
        /// CICOHOVT datatable.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <returns></returns>
        private DataTable CICOHOVT(IList<dynamic> data, bool toOracle = false)
        {
            var dt = new DataTable();
            try
            {
                // columns
                dt.Columns.Add("FIPS", typeof(string));
                dt.Columns.Add("ROUTE", typeof(string));
                dt.Columns.Add("SEGMID", typeof(decimal));
                dt.Columns.Add("LENGTH_", typeof(decimal));
                dt.Columns.Add("UPDATEYR", typeof(string));
                dt.Columns.Add("FIPSCOUNTY", typeof(string));
                dt.Columns.Add("HOVTYPE", typeof(string));
                dt.Columns.Add("HOVLNQTY", typeof(string));
                dt.Columns.Add("TOLLID", typeof(string));
                dt.Columns.Add("TOLLTYPE", typeof(string));
                dt.Columns.Add("TOLLCHARGED", typeof(string));

                // rows
                foreach (var item in data)
                {
                    DataRow row = dt.NewRow();
                    row["FIPS"] = item.FIPS;
                    row["ROUTE"] = item.ROUTE;
                    row["SEGMID"] = item.SEGMID;
                    row["LENGTH_"] = item.LENGTH_;
                    row["UPDATEYR"] = item.UPDATEYR;
                    row["FIPSCOUNTY"] = item.FIPSCOUNTY;
                    row["HOVTYPE"] = item.HOVTYPE;
                    row["HOVLNQTY"] = item.HOVLNQTY;
                    row["TOLLID"] = item.TOLLID;
                    row["TOLLTYPE"] = item.TOLLTYPE;
                    row["TOLLCHARGED"] = item.TOLLCHARGED;

                    dt.Rows.Add(row);
                }

                return dt;

            }
            catch (Exception ex)
            {
                _logger.ErrorException(ex.Message, ex);
                return dt;
            }
        }

        /// <summary>
        /// CICOHPMS DataTable
        /// </summary>
        /// <param name="data">The data.</param>
        /// <returns></returns>
        private DataTable CICOHPMS(IList<dynamic> data, bool toOracle = false)
        {
            var dt = new DataTable();
            try
            {
                // columns
                dt.Columns.Add("HPMSID", typeof(string));
                dt.Columns.Add("FIPS", typeof(string));
                dt.Columns.Add("ROUTE", typeof(string));
                dt.Columns.Add("SEGMID", typeof(decimal));
                dt.Columns.Add("LENGTH_", typeof(decimal));
                dt.Columns.Add("UPDATEYR", typeof(string));
                dt.Columns.Add("FIPSCOUNTY", typeof(string));
                dt.Columns.Add("YRADD", typeof(string));
                dt.Columns.Add("YRINSPECT", typeof(string));
                dt.Columns.Add("PRIRUTTING", typeof(decimal));
                dt.Columns.Add("PRIFAULTING", typeof(decimal));
                dt.Columns.Add("PRIFATIGUECRACK", typeof(decimal));
                dt.Columns.Add("PRITRANSCRACK", typeof(decimal));
                dt.Columns.Add("RIGIDTHICK", typeof(decimal));
                dt.Columns.Add("FLEXTHICK", typeof(decimal));
                dt.Columns.Add("BASETYPE", typeof(decimal));
                dt.Columns.Add("BASETHICK", typeof(decimal));
                dt.Columns.Add("MEDIAN", typeof(string));
                dt.Columns.Add("MEDIANWD", typeof(decimal));
                dt.Columns.Add("PRIINSHLDWD", typeof(decimal));
                dt.Columns.Add("PRIOUTSHLD", typeof(string));
                dt.Columns.Add("PRIOUTSHLDWD", typeof(decimal));
                dt.Columns.Add("CURVES_A", typeof(decimal));
                dt.Columns.Add("CURVES_B", typeof(decimal));
                dt.Columns.Add("CURVES_C", typeof(decimal));
                dt.Columns.Add("CURVES_D", typeof(decimal));
                dt.Columns.Add("CURVES_E", typeof(decimal));
                dt.Columns.Add("CURVES_F", typeof(decimal));
                dt.Columns.Add("GRADES_A", typeof(decimal));
                dt.Columns.Add("GRADES_B", typeof(decimal));
                dt.Columns.Add("GRADES_C", typeof(decimal));
                dt.Columns.Add("GRADES_D", typeof(decimal));
                dt.Columns.Add("GRADES_E", typeof(decimal));
                dt.Columns.Add("GRADES_F", typeof(decimal));
                dt.Columns.Add("PKPARK", typeof(decimal));
                dt.Columns.Add("WIDENOBSTACLE", typeof(string));
                dt.Columns.Add("WIDENFEAS", typeof(decimal));
                dt.Columns.Add("SIGHTDIST", typeof(decimal));
                dt.Columns.Add("SPEEDLIM", typeof(decimal));
                dt.Columns.Add("INTERSECTION", typeof(string));
                dt.Columns.Add("LTTURNLN", typeof(decimal));
                dt.Columns.Add("RTTURNLN", typeof(decimal));
                dt.Columns.Add("SIGNALTYPE", typeof(decimal));
                dt.Columns.Add("GREENTIME", typeof(decimal));
                dt.Columns.Add("SIGNALQTY", typeof(decimal));
                dt.Columns.Add("STOPQTY", typeof(decimal));
                dt.Columns.Add("NONCTRLQTY", typeof(decimal));
                dt.Columns.Add("ROADTERRAIN", typeof(string));
                dt.Columns.Add("COMMENTS", typeof(string));
                dt.Columns.Add("PRISLABCRACK", typeof(decimal));
                dt.Columns.Add("MEDIANSF", typeof(string));

                // rows
                foreach (var item in data)
                {
                    DataRow row = dt.NewRow();
                    row["HPMSID"] = item.HPMSID;
                    row["FIPS"] = item.FIPS;
                    row["ROUTE"] = item.ROUTE;
                    row["SEGMID"] = item.SEGMID;
                    row["LENGTH_"] = item.LENGTH_;
                    row["UPDATEYR"] = item.UPDATEYR;
                    row["FIPSCOUNTY"] = item.FIPSCOUNTY;
                    row["YRADD"] = item.YRADD != null ? item.YRADD : DBNull.Value;
                    row["YRINSPECT"] = item.YRINSPECT != null ? item.YRINSPECT : DBNull.Value;
                    row["PRIRUTTING"] = item.PRIRUTTING != null ? item.PRIRUTTING : DBNull.Value;
                    row["PRIFAULTING"] = item.PRIFAULTING != null ? item.PRIFAULTING : DBNull.Value;
                    row["PRIFATIGUECRACK"] = item.PRIFATIGUECRACK != null ? item.PRIFATIGUECRACK : DBNull.Value;
                    row["PRITRANSCRACK"] = item.PRITRANSCRACK != null ? item.PRITRANSCRACK : DBNull.Value;
                    row["RIGIDTHICK"] = item.RIGIDTHICK != null ? item.RIGIDTHICK : DBNull.Value;
                    row["FLEXTHICK"] = item.FLEXTHICK != null ? item.FLEXTHICK : DBNull.Value;
                    row["BASETYPE"] = item.BASETYPE != null ? item.BASETYPE : DBNull.Value;
                    row["BASETHICK"] = item.BASETHICK != null ? item.BASETHICK : DBNull.Value;
                    row["MEDIAN"] = item.MEDIAN != null ? item.MEDIAN : DBNull.Value;
                    row["MEDIANWD"] = item.MEDIANWD != null ? item.MEDIANWD : DBNull.Value;
                    row["PRIINSHLDWD"] = item.PRIINSHLDWD != null ? item.PRIINSHLDWD : DBNull.Value;
                    row["PRIOUTSHLD"] = item.PRIOUTSHLD != null ? item.PRIOUTSHLD : DBNull.Value;
                    row["PRIOUTSHLDWD"] = item.PRIOUTSHLDWD != null ? item.PRIOUTSHLDWD : DBNull.Value;
                    row["CURVES_A"] = item.CURVES_A != null ? item.CURVES_A : DBNull.Value;
                    row["CURVES_B"] = item.CURVES_B != null ? item.CURVES_B : DBNull.Value;
                    row["CURVES_C"] = item.CURVES_C != null ? item.CURVES_C : DBNull.Value;
                    row["CURVES_C"] = item.CURVES_D != null ? item.CURVES_D : DBNull.Value;
                    row["CURVES_C"] = item.CURVES_E != null ? item.CURVES_E : DBNull.Value;
                    row["CURVES_C"] = item.CURVES_F != null ? item.CURVES_F : DBNull.Value;
                    row["GRADES_A"] = item.GRADES_A != null ? item.GRADES_A : DBNull.Value;
                    row["GRADES_A"] = item.GRADES_B != null ? item.GRADES_B : DBNull.Value;
                    row["GRADES_A"] = item.GRADES_C != null ? item.GRADES_C : DBNull.Value;
                    row["GRADES_A"] = item.GRADES_D != null ? item.GRADES_D : DBNull.Value;
                    row["GRADES_A"] = item.GRADES_E != null ? item.GRADES_E : DBNull.Value;
                    row["GRADES_A"] = item.GRADES_F != null ? item.GRADES_F : DBNull.Value;
                    row["PKPARK"] = item.PKPARK != null ? item.PKPARK : DBNull.Value;
                    row["WIDENOBSTACLE"] = item.WIDENOBSTACLE != null ? item.WIDENOBSTACLE : DBNull.Value;
                    row["WIDENFEAS"] = item.WIDENFEAS != null ? item.WIDENFEAS : DBNull.Value;
                    row["SIGHTDIST"] = item.SIGHTDIST != null ? item.SIGHTDIST : DBNull.Value;
                    row["SPEEDLIM"] = item.SPEEDLIM != null ? item.SPEEDLIM : DBNull.Value;
                    row["INTERSECTION"] = item.INTERSECTION != null ? item.INTERSECTION : DBNull.Value;
                    row["LTTURNLN"] = item.LTTURNLN != null ? item.LTTURNLN : DBNull.Value;
                    row["RTTURNLN"] = item.RTTURNLN != null ? item.RTTURNLN : DBNull.Value;
                    row["SIGNALTYPE"] = item.SIGNALTYPE != null ? item.SIGNALTYPE : DBNull.Value;
                    row["GREENTIME"] = item.GREENTIME != null ? item.GREENTIME : DBNull.Value;
                    row["SIGNALQTY"] = item.SIGNALQTY != null ? item.SIGNALQTY : DBNull.Value;
                    row["STOPQTY"] = item.STOPQTY != null ? item.STOPQTY : DBNull.Value;
                    row["NONCTRLQTY"] = item.NONCTRLQTY != null ? item.NONCTRLQTY : DBNull.Value;
                    row["ROADTERRAIN"] = item.ROADTERRAIN != null ? item.ROADTERRAIN : DBNull.Value;
                    row["COMMENTS"] = item.COMMENTS;
                    row["PRISLABCRACK"] = item.PRISLABCRACK != null ? item.PRISLABCRACK : DBNull.Value;
                    row["MEDIANSF"] = item.MEDIANSF != null ? item.MEDIANSF : DBNull.Value;

                    dt.Rows.Add(row);
                }
                return dt;
            }
            catch (Exception ex)
            {
                _logger.ErrorException(ex.Message, ex);
                return dt;
            }
        }

        #endregion



        #region Load SQL into Oracle

        public bool CopyFromSqlToOracle(string segmentKey)
        {
            var cicooff = sqlGetLiveCICOOFF(segmentKey);
            if (cicooff != null)
            {
                _logger.Info(string.Format("Performing copy from SQL to Oracle for GUID => {0}", segmentKey));

                // spin up massive and do this under transactions.
                // first we will delete the segment key
                // the we will insert a new one from sql
                // spin up a dynamic model, open an oracle connection and create a transaction instance
                var db = new ExtendedDynamicModel(ORCL_CONNECTION_STRING_NAME);
                using (var orclConnection = db.Connection = db.OpenConnection())
                {
                    using (var orclTransaction = db.Transaction = orclConnection.BeginTransaction())
                    {
                        try
                        {
                            // doing this all under transactions, create a todo list
                            // add every update, insert, and delete into the toDo queue
                            // so all our queries are under a transaction and can be rolled back should things get sideways
                            var toDo = new List<DbCommand>();

                            // delete cicooffs from oracle
                            toDo.Add(orclDeleteCICOOFF(segmentKey));

                            _logger.Info(string.Format("Inserting record into TABLE => {0} for SegmentKey => {1}", ORCL_CICOOFF, segmentKey));
                            toDo.Add(orclInsertCICOOFF(cicooff));


                            var table = new ORCL_CICOOFF_TABLE();
                            var result = table.Execute(toDo);
                            if (result > 0)
                            {
                                orclTransaction.Commit();
                                _logger.Info(string.Format("Successfully inserted record into TABLE => {0} for SegmentKey => {1}", ORCL_CICOOFF, segmentKey));
                                return true;
                            }
                            else
                            {
                                orclTransaction.Rollback();
                                _logger.Error(string.Format("Could not insert record into TABLE => {0} for SegmentKey => {1}", ORCL_CICOOFF, segmentKey));
                                return false;
                            }
                        }
                        catch (Exception ex)
                        {
                            orclTransaction.Rollback();
                            _logger.ErrorException(string.Format("Could not insert record into TABLE => {0} for SegmentKey => {1}", ORCL_CICOOFF, segmentKey), ex);
                            return false;
                        }
                    }
                }
            }
            _logger.Error(string.Format("Could not copy segment from SQL to Oracle for Segment Key => {0}, does not exist", segmentKey));
            return false;
        }


        /// <summary>
        /// Copy data from Sql to Oracle for a FIPS, all if no FIPS is provided. Do this inside a transaction we we can roll it back on an error.
        /// </summary>
        /// <param name="fips">The fips.</param>
        /// <returns></returns>
        /// <remarks>
        /// Transactions are not supported with OracleBulkCopy... so we will do it this way
        /// 1. Select the CICOOFF and CICOHOVT for the FIPS into temp tables. These will be our rollback which will be done on exception
        /// 2. Use Massive to:
        ///     a. Delete CICOOFF for fips
        ///     b. Update or insert a CICOOFF record. These are transaction based
        /// 4. If successful, drop the temp tables
        /// 3. If not successful, catch the exception and 'roll back' the data from the temp tables 
        /// </remarks>
        public bool BulkCopyFromSqlToOracle(string fips)
        {
            _logger.Info(string.Format("Performing copy from SQL to SQL for FIPS => {0}", fips));

            // get cicooffs and cicohovts from live data
            var cicooffsFoFips = sqlGetAllLiveCICOOFF(fips);
            if (cicooffsFoFips == null || cicooffsFoFips.Count() == 0)
            {
                throw new ArgumentException("FIPS not found.");
            }

            // hey - cool news from CDOT, according to them we don't have to maintain the hovt record. 
            // comment out for now should they change their minds
            //var cicohovtsForFips = sqlGetAllLiveCICOHOVT(fips);

            // get the original oracle cicoohovts which will be used in updating an existing cicohovt record
            //var cicohovtsSource = orclGetAllCICOHOVT(fips);

            // spin up a dynamic model, open an oracle connection and create a transaction instance
            var db = new ExtendedDynamicModel(ORCL_CONNECTION_STRING_NAME);
            using (var orclConnection = db.Connection = db.OpenConnection())
            {
                using (var orclTransaction = db.Transaction = orclConnection.BeginTransaction())
                {
                    try
                    {
                        // doing this all under transactions, create a todo list
                        // add every update, insert, and delete into the toDo queue
                        // so all our queries are under a transaction and can be rolled back should things get sideways
                        var toDo = new List<DbCommand>();

                        // delete cicooffs from oracle
                        toDo.Add(orclDeleteCICOOFF(fips));

                        // since we deleted all oracle cicooff records previously, we just need to insert the live ones
                        // hey - cool news from CDOT, according to them we don't have to maintain the hovt record. 
                        // comment out for now should they change their minds.
                        _logger.Info(string.Format("Inserting records info TABLE => {0} for FIPS => {1}", ORCL_CICOOFF, fips));
                        foreach (var cicooff in cicooffsFoFips)
                        {
                            toDo.Add(orclInsertCICOOFF(cicooff));

                            // insert or update cicohovt
                            // hey - cool news from CDOT, according to them we don't have to maintain the hovt record. 
                            // comment out for now should they change their minds.
                            //var cicohovt = cicohovtsForFips.SingleOrDefault(x => x.FIPS == cicooff.FIPS && x.ROUTE == cicooff.ROUTE && x.SEGMID == cicooff.SEGMID);
                            //if (cicohovt != null)
                            //{
                            //    // does cicohovt exist in oracle?
                            //    var orclCICOHOVT = cicohovtsSource.SingleOrDefault(x => x.FIPS == cicohovt.FIPS && x.ROUTE == cicohovt.ROUTE && x.SEGMID == cicohovt.SEGMID);
                            //    if (orclCICOHOVT != null)
                            //    {
                            //        // update it
                            //        toDo.Add(orclUpdateCICOHOVT(cicohovt, orclCICOHOVT, cicooff));
                            //    }
                            //    else
                            //    {
                            //        // insert it
                            //        toDo.Add(orclInsertCICOHOVT(cicohovt, cicooff));
                            //    }
                            //}
                        }


                        // hey - cool news from CDOT, according to them we don't have to maintain the hovt record. 
                        // comment out for now should they change their minds.
                        // almost done, delete leftover oracle cicohovts
                        //var cicohovtsToDelete = cicohovtsSource.Where(x => !cicohovtsForFips.Any(y => y.FIPS == x.FIPS && y.ROUTE == x.ROUTE && y.SEGMID == x.SEGMID));
                        //foreach (var cicohovtToDelete in cicohovtsToDelete)
                        //{
                        //    toDo.Add(orclDeleteCICOHOVT(cicohovtToDelete));
                        //}

                        var table = new ORCL_CICOOFF_TABLE();
                        var result = table.Execute(toDo);
                        if (result > 0)
                        {
                            orclTransaction.Commit();
                            _logger.Info(string.Format("Successfully inserted records into TABLE => {0} for FIPS => {1}", ORCL_CICOOFF, fips));
                            return true;
                        }
                        else
                        {
                            orclTransaction.Rollback();
                            _logger.Error(string.Format("Could not insert records into TABLE => {0} for FIPS => {1}", ORCL_CICOOFF, fips));
                            return false;
                        }
                    }
                    catch (Exception ex)
                    {
                        orclTransaction.Rollback();
                        _logger.ErrorException(string.Format("Could not insert records into TABLE => {0} for FIPS => {1}", ORCL_CICOOFF, fips), ex);
                        return false;
                    }
                }
            }
        }
        #endregion


        internal class ORCL_CICOOFF_TABLE : DynamicModel
        {
            public ORCL_CICOOFF_TABLE() : base("ORCLConnectionString",  ConfigHelper.GetAppSetting("ORCL_CICOOFF_TABLE"), primaryKeyField: "GUID") { }
        }

        internal class ORCL_CICOHOVT_TABLE : DynamicModel
        {
            public ORCL_CICOHOVT_TABLE() : base("ORCLConnectionString", ConfigHelper.GetAppSetting("ORCL_CICOHIOVT_TABLE")) { }
        }
    }

}