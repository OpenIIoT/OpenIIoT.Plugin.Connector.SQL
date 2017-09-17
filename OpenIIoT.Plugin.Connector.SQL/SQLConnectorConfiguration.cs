using System.Collections.Generic;

namespace OpenIIoT.Plugin.Connector.SQL
{
    public class SQLConnectorConfiguration
    {
        #region Public Constructors

        public SQLConnectorConfiguration()
        {
            Databases = new List<SQLConnectorConfigurationDatabase>();
        }

        #endregion Public Constructors

        #region Public Properties

        public List<SQLConnectorConfigurationDatabase> Databases { get; set; }

        #endregion Public Properties
    }

    public class SQLConnectorConfigurationDatabase
    {
        #region Public Constructors

        public SQLConnectorConfigurationDatabase()
        {
            Queries = new List<SQLConnectorConfigurationDatabaseQuery>();
        }

        #endregion Public Constructors

        #region Public Properties

        public string ConnectionString { get; set; }
        public string Name { get; set; }
        public List<SQLConnectorConfigurationDatabaseQuery> Queries { get; set; }

        #endregion Public Properties
    }

    public class SQLConnectorConfigurationDatabaseQuery
    {
        #region Public Properties

        public int? ChecksumInternval { get; set; }
        public string ChecksumSQL { get; set; }
        public string Name { get; set; }
        public string SQL { get; set; }

        #endregion Public Properties
    }
}