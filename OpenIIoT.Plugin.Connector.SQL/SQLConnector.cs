using System;
using System.Collections.Generic;
using System.Linq;
using System.Timers;
using System.Threading.Tasks;
using NLog;
using NLog.xLogger;
using OpenIIoT.SDK.Common.OperationResult;
using OpenIIoT.SDK.Configuration;
using OpenIIoT.SDK;
using System.Data;
using System.Drawing;
using System.IO;
using System.Text;
using OpenIIoT.SDK.Common;
using OpenIIoT.SDK.Common.Provider.ItemProvider;
using OpenIIoT.SDK.Plugin;
using OpenIIoT.SDK.Plugin.Connector;
using System.Runtime.InteropServices;
using Newtonsoft.Json;
using System.Data.SqlClient;

namespace OpenIIoT.Plugin.Connector.SQL
{
    /// <summary>
    ///     Provides simulation data.
    /// </summary>
    public class SQLConnector : IConnector, ISubscribable, IConfigurable<SQLConnectorConfiguration>
    {
        #region Private Fields

        /// <summary>
        ///     The main counter.
        /// </summary>
        private int counter;

        /// <summary>
        ///     The root node for the item tree.
        /// </summary>
        private Item itemRoot;

        /// <summary>
        ///     the logger for the Connector.
        /// </summary>
        private xLogger logger;

        /// <summary>
        ///     The main timer.
        /// </summary>
        private Timer timer;

        #endregion Private Fields

        #region Public Constructors

        /// <summary>
        ///     Initializes a new instance of the <see cref="SQLConnector"/> class.
        /// </summary>
        /// <param name="manager">The ApplicationManager instance.</param>
        /// <param name="instanceName">The assigned name for this instance.</param>
        /// <param name="logger">The logger for this instance.</param>
        public SQLConnector(IApplicationManager manager, string instanceName, xLogger logger)
        {
            InstanceName = instanceName;
            this.logger = logger;

            Name = "SQL";
            FQN = "OpenIIoT.Plugin.Connector.SQL";
            Version = "1.0.0.0";
            PluginType = PluginType.Connector;

            Manager = manager;

            ItemProviderName = FQN;

            logger.Info("Initializing " + PluginType + " " + FQN + "." + instanceName);

            Configure();

            InitializeItems();

            timer = new System.Timers.Timer(500);
            timer.Elapsed += Timer_Elapsed;

            Subscriptions = new Dictionary<Item, List<Action<object>>>();
            TriggerCache = new Dictionary<Item, string>();
        }

        #endregion Public Constructors

        #region Public Events

        public event EventHandler<StateChangedEventArgs> StateChanged;

        #endregion Public Events

        #region Public Properties

        public bool AutomaticRestartPending { get; private set; }
        public SQLConnectorConfiguration Configuration { get; private set; }
        public string Fingerprint { get; private set; }

        /// <summary>
        ///     The Connector FQN.
        /// </summary>
        public string FQN { get; private set; }

        /// <summary>
        ///     The name of the Connector instance.
        /// </summary>
        public string InstanceName { get; private set; }

        public string ItemProviderName { get; private set; }
        public IApplicationManager Manager { get; set; }

        /// <summary>
        ///     The Connector name.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        ///     The Connector type.
        /// </summary>
        public PluginType PluginType { get; private set; }

        /// <summary>
        ///     The State of the Connector.
        /// </summary>
        public State State { get; private set; }

        /// <summary>
        ///     The <see cref="Dictionary{TKey, TValue}"/> keyed on subscribed Item and containing a <see cref="List{T}"/> of the
        ///     <see cref="Action{T}"/> delegates used to update the subscribers.
        /// </summary>
        public Dictionary<Item, List<Action<object>>> Subscriptions { get; protected set; }

        /// <summary>
        ///     The Connector Version.
        /// </summary>
        public string Version { get; private set; }

        private IDictionary<Item, string> TriggerCache { get; set; }

        #endregion Public Properties

        #region Public Methods

        /// <summary>
        ///     The GetConfigurationDefinition method is static and returns the ConfigurationDefinition for the Endpoint.
        ///
        ///     This method is necessary so that the configuration defintion can be registered with the ConfigurationManager prior
        ///     to any instances being created. This method MUST be implemented, however it is not possible to specify static
        ///     methods in an interface, so implementing IConfigurable will not enforce this.
        /// </summary>
        /// <returns>The ConfigurationDefinition for the Endpoint.</returns>
        public static IConfigurationDefinition GetConfigurationDefinition()
        {
            ConfigurationDefinition retVal = new ConfigurationDefinition();

            // to create the form and schema strings, visit http://schemaform.io/examples/bootstrap-example.html use the example to
            // create the desired form and schema, and ensure that the resulting model matches the model for the endpoint. When you
            // are happy with the json from the above url, visit http://www.freeformatter.com/json-formatter.html#ad-output and
            // paste in the generated json and format it using the "JavaScript escaped" option. Paste the result into the methods below.

            retVal.Form = "[\"templateURL\",{\"type\":\"submit\",\"style\":\"btn-info\",\"title\":\"Save\"}]";
            retVal.Schema = "{\"type\":\"object\",\"title\":\"XMLEndpoint\",\"properties\":{\"templateURL\":{\"title\":\"Template URL\",\"type\":\"string\"}},\"required\":[\"templateURL\"]}";

            // this will always be typeof(YourConfiguration/ModelObject)
            retVal.Model = typeof(SQLConnectorConfiguration);
            retVal.DefaultConfiguration = new SQLConnectorConfiguration();
            return retVal;
        }

        public Item Browse()
        {
            return itemRoot;
        }

        public IList<Item> Browse(Item root)
        {
            return (root == null ? itemRoot.Children : root.Children);
        }

        public async Task<Item> BrowseAsync()
        {
            return await Task.Run(() => Browse());
        }

        public async Task<IList<Item>> BrowseAsync(Item root)
        {
            return await Task.Run(() => Browse(root));
        }

        /// <summary>
        ///     The parameterless Configure() method calls the overloaded Configure() and passes in the instance of the model/type
        ///     returned by the GetConfiguration() method in the Configuration Manager.
        ///
        ///     This is akin to saying "configure yourself using whatever is in the config file"
        /// </summary>
        /// <returns></returns>
        public IResult Configure()
        {
            logger.EnterMethod();
            logger.Debug("Attempting to Configure with the configuration from the Configuration Manager...");
            Result retVal = new Result();

            IResult<SQLConnectorConfiguration> fetchResult = Manager.GetManager<IConfigurationManager>().Configuration.GetInstance<SQLConnectorConfiguration>(GetType());

            // if the fetch succeeded, configure this instance with the result.
            if (fetchResult.ResultCode != ResultCode.Failure)
            {
                logger.Debug("Successfully fetched the configuration from the Configuration Manager.");
                Configure(fetchResult.ReturnValue);
            }
            else
            {
                // if the fetch failed, add a new default instance to the configuration and try again.
                logger.Debug("Unable to fetch the configuration.  Adding the default configuration to the Configuration Manager...");
                IResult<SQLConnectorConfiguration> createResult = Manager.GetManager<IConfigurationManager>().Configuration.AddInstance<SQLConnectorConfiguration>(GetType(), GetConfigurationDefinition().DefaultConfiguration);
                if (createResult.ResultCode != ResultCode.Failure)
                {
                    logger.Debug("Successfully added the configuration.  Configuring...");
                    Configure(createResult.ReturnValue);
                }

                retVal.Incorporate(createResult);
            }

            retVal.LogResult(logger.Debug);
            logger.ExitMethod(retVal);
            return retVal;
        }

        /// <summary>
        ///     The Configure method is called by external actors to configure or re-configure the Endpoint instance.
        ///
        ///     If anything inside the Endpoint needs to be refreshed to reflect changes to the configuration, do it in this method.
        /// </summary>
        /// <param name="configuration">The instance of the model/configuration type to apply.</param>
        /// <returns>A Result containing the result of the operation.</returns>
        public IResult Configure(SQLConnectorConfiguration configuration)
        {
            Configuration = configuration;

            return new Result();
        }

        public Item Find(string fqn)
        {
            return Find(itemRoot, fqn);
        }

        public async Task<Item> FindAsync(string fqn)
        {
            return await Task.Run(() => Find(fqn));
        }

        /// <summary>
        ///     Returns true if any of the specified <see cref="State"/> s match the current <see cref="State"/>.
        /// </summary>
        /// <param name="states">The list of States to check.</param>
        /// <returns>True if the current State matches any of the specified States, false otherwise.</returns>
        public virtual bool IsInState(params State[] states)
        {
            return states.Any(s => s == State);
        }

        public object Read(Item item)
        {
            object retVal = new object();

            SQLConnectorConfigurationDatabase db = Configuration?.Databases?.Where(d => d.Name == "Demo").FirstOrDefault();
            SQLConnectorConfigurationDatabaseQuery query = db?.Queries?.Where(q => item.FQN.EndsWith(q.Name)).FirstOrDefault();

            retVal = Fetch(db.ConnectionString, query.Query);

            return retVal;
        }

        public async Task<object> ReadAsync(Item item)
        {
            return await Task.Run(() => Read(item));
        }

        public IResult Restart(StopType stopType = StopType.Stop)
        {
            return Start().Incorporate(Stop(stopType | StopType.Restart));
        }

        public IResult SaveConfiguration()
        {
            return Manager.GetManager<IConfigurationManager>().Configuration.UpdateInstance(this.GetType(), Configuration);
        }

        public IResult Start()
        {
            logger.Debug("Starting SQL Connector...");
            timer.Start();

            logger.Debug("SQL Connector started.");
            return new Result();
        }

        public IResult Stop(StopType stopType = StopType.Stop)
        {
            timer.Stop();
            return new Result();
        }

        /// <summary>
        ///     Creates a subscription to the specified Item.
        /// </summary>
        /// <remarks>
        ///     <para>
        ///         Upon the addition of the initial subscriber, an entry is added to the <see cref="Subscriptions"/> Dictionary
        ///         keyed with the specified Item with a new <see cref="List{T}"/> of type <see cref="Action{T}"/> containing one
        ///         entry corresponding to the specified callback delegate.
        ///     </para>
        ///     <para>
        ///         Successive additions add each of the specified callback delegates to the <see cref="Subscriptions"/> dictionary.
        ///     </para>
        /// </remarks>
        /// <param name="item">The <see cref="Item"/> to which the subscription should be added.</param>
        /// <param name="callback">The callback delegate to be invoked upon change of the subscribed Item.</param>
        /// <returns>A value indicating whether the operation succeeded.</returns>
        public bool Subscribe(Item item, Action<object> callback)
        {
            bool retVal = false;

            try
            {
                if (!Subscriptions.ContainsKey(item))
                {
                    Subscriptions.Add(item, new List<Action<object>>());
                }

                Subscriptions[item].Add(callback);

                retVal = true;
            }
            catch (Exception ex)
            {
                logger.Exception(ex);
            }

            return retVal;
        }

        /// <summary>
        ///     Removes a subscription from the specified ConnectorItem.
        /// </summary>
        /// <remarks>
        ///     <para>
        ///         Upon the removal of a subscriber the specified callback delegate is removed from the corresponding Dictionary
        ///         entry for the specified <see cref="Item"/>.
        ///     </para>
        ///     Upon removal of the final subscriber, the Dictionary key corresponding to the specified <see cref="Item"/> is
        ///     completely removed.
        /// </remarks>
        /// <param name="item">The <see cref="Item"/> for which the subscription should be removed.</param>
        /// <param name="callback">The callback delegate to be invoked upon change of the subscribed Item.</param>
        /// <returns>A value indicating whether the operation succeeded.</returns>
        public bool UnSubscribe(Item item, Action<object> callback)
        {
            bool retVal = false;

            try
            {
                if (Subscriptions.ContainsKey(item))
                {
                    Subscriptions[item].Remove(callback);

                    if (Subscriptions[item].Count == 0)
                    {
                        Subscriptions.Remove(item);
                    }

                    retVal = true;
                }
            }
            catch (Exception ex)
            {
                logger.Exception(ex);
            }

            return retVal;
        }

        #endregion Public Methods

        #region Private Methods

        public void SetFingerprint(string fingerprint)
        {
            throw new NotImplementedException();
        }

        private object Fetch(string connectionString, string query)
        {
            object retVal;
            SqlConnection connection = new SqlConnection(connectionString);
            SqlDataAdapter adapter = new SqlDataAdapter(query, connection);

            try
            {
                connection.Open();

                DataTable table = new DataTable();
                adapter.Fill(table);

                retVal = table;
            }
            catch (Exception ex)
            {
                retVal = ex.Message;
            }
            finally
            {
                adapter.Dispose();
                connection.Close();
                connection.Dispose();
            }

            return retVal;
        }

        private Item Find(Item root, string fqn)
        {
            if (root.FQN == fqn) return root;

            Item found = default(Item);
            foreach (Item child in root.Children)
            {
                found = Find(child, fqn);
                if (found != default(Item)) break;
            }
            return found;
        }

        private string GetCachedTrigger(Item item)
        {
            if (TriggerCache.ContainsKey(item))
            {
                return TriggerCache[item];
            }
            return string.Empty;
        }

        private void InitializeItems()
        {
            itemRoot = new Item(InstanceName, this);

            logger.Info(JsonConvert.SerializeObject(Configuration));

            foreach (var database in Configuration.Databases)
            {
                Item dbRoot = itemRoot.AddChild(new Item(database.Name, this)).ReturnValue;

                foreach (var query in database.Queries)
                {
                    dbRoot.AddChild(new Item(query.Name, this));
                }
            }
        }

        private void Timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            foreach (Item key in Subscriptions.Keys)
            {
                if (key.FQN.EndsWith("Batches"))
                {
                    // check to see if the trigger has updated. ignore the polling time for demo purposes.
                    SQLConnectorConfigurationDatabase db = Configuration.Databases.Where(d => d.Name == "Demo").FirstOrDefault();
                    SQLConnectorConfigurationDatabaseQuery query = db.Queries.Where(q => q.Name == "Batches").FirstOrDefault();
                    SQLConnectorConfigurationDatabaseQueryTrigger trigger = query.Trigger;

                    if (trigger != default(SQLConnectorConfigurationDatabaseQueryTrigger))
                    {
                        if (!string.IsNullOrEmpty(trigger.Query))
                        {
                            DataTable triggerTable = (DataTable)Fetch(db.ConnectionString, trigger.Query);
                            string triggerValue = triggerTable.Rows[0].ItemArray[0].ToString();

                            if (string.IsNullOrEmpty(triggerValue))
                            {
                            }
                            else
                            {
                                if (GetCachedTrigger(key) != triggerValue)
                                {
                                    logger.Debug("Trigger changed. Firing update.");

                                    foreach (Action<object> callback in Subscriptions[key]) { callback.Invoke(Read(key)); }

                                    UpdateTriggerCache(key, triggerValue);
                                }
                            }
                        }
                    }
                }
            }
        }

        private void UpdateTriggerCache(Item item, string value)
        {
            if (TriggerCache.ContainsKey(item))
            {
                TriggerCache[item] = value;
            }
            else
            {
                TriggerCache.Add(item, value);
            }
        }

        #endregion Private Methods
    }
}