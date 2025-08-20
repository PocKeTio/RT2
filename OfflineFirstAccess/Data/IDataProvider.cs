using System;
using System.Collections.Generic;
using System.Threading.Tasks;

// Represents a record in a generic way
using GenericRecord = System.Collections.Generic.Dictionary<string, object>;

namespace OfflineFirstAccess.Data
{
    /// <summary>
    /// Defines atomic operations for reading and writing data from a data source.
    /// </summary>
    public interface IDataProvider
    {
        /// <summary>
        /// Retrieves records that have changed since a specific point in time.
        /// </summary>
        Task<IEnumerable<GenericRecord>> GetChangesAsync(string tableName, DateTime? since);

        /// <summary>
        /// Applies a set of changes (inserts, updates, deletes) to a table.
        /// </summary>
        Task ApplyChangesAsync(string tableName, IEnumerable<GenericRecord> changesToApply);

        /// <summary>
        /// Retrieves full records based on their primary key IDs.
        /// </summary>
        Task<IEnumerable<GenericRecord>> GetRecordsByIds(string tableName, IEnumerable<string> ids);

        /// <summary>
        /// Retrieves a configuration parameter value from the database.
        /// </summary>
        Task<string> GetParameterAsync(string key);

        /// <summary>
        /// Sets a configuration parameter value in the database.
        /// </summary>
        Task SetParameterAsync(string key, string value);
    }
}
