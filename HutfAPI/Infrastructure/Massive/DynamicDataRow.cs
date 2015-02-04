using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Web;

namespace Massive
{
    /// <summary>
    /// This class provides an easy way to turn a DataRow 
    /// into a Dynamic object that supports direct property
    /// access to the DataRow fields.
    /// 
    /// The class also automatically fixes up DbNull values
    /// (null into .NET and DbNUll to DataRow)
    /// </summary>
    /// <remarks>
    /// http://weblog.west-wind.com/posts/2011/Nov/24/Creating-a-Dynamic-DataRow-for-easier-DataRow-Syntax#CreatingyourowncustomDynamicObjects
    /// </remarks>
    public class DynamicDataRow : DynamicObject
    {
        /// <summary>
        /// Instance of object passed in
        /// </summary>
        DataRow DataRow;

        /// <summary>
        /// Pass in a DataRow to work off
        /// </summary>
        /// <param name="instance"></param>
        public DynamicDataRow(DataRow dataRow)
        {
            DataRow = dataRow;
        }

        /// <summary>
        /// Returns a value from a DataRow items array.
        /// If the field doesn't exist null is returned.
        /// DbNull values are turned into .NET nulls.
        /// 
        /// </summary>
        /// <param name="binder"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = null;

            try
            {
                result = DataRow[binder.Name];

                if (result == DBNull.Value)
                    result = null;

                return true;
            }
            catch { }

            result = null;
            return false;
        }


        /// <summary>
        /// Property setter implementation tries to retrieve value from instance 
        /// first then into this object
        /// </summary>
        /// <param name="binder"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            try
            {
                if (value == null)
                    value = DBNull.Value;

                DataRow[binder.Name] = value;
                return true;
            }
            catch { }

            return false;
        }
    }



    /// <summary>
    /// 
    /// </summary>
    public static class DataTableDynamicExtensions
    {
        /// <summary>
        /// Returns a dynamic DataRow instance that can be accessed
        /// with the field name as a property
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>taTab
        public static dynamic DynamicRow(this DataTable dt, int index)
        {
            var row = dt.Rows[index];
            return new DynamicDataRow(row);
        }

        /// <summary>
        /// Returns a dynamic list of rows so you can reference them with
        /// row.fieldName
        /// </summary>
        /// <param name="dt"></param>
        /// <returns></returns>
        public static List<dynamic> DynamicRows(this DataTable dt)
        {
            List<dynamic> drows = new List<dynamic>();

            foreach (DataRow row in dt.Rows)
                drows.Add(new DynamicDataRow(row));

            return drows;
        }

    }
}