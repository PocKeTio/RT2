using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RecoTool.Services
{
    /// <summary>
    /// Tracks active user sessions viewing/editing TodoList items to prevent conflicts
    /// and provide multi-user awareness.
    /// Uses the country-specific Lock database for session tracking.
    /// </summary>
    public class TodoListSessionTracker : IDisposable
    {
        // Static registry of all active trackers for cleanup coordination
        private static readonly List<WeakReference<TodoListSessionTracker>> _activeTrackers = new List<WeakReference<TodoListSessionTracker>>();
        private static readonly object _staticLock = new object();
        
        private readonly string _lockDbConnectionString;
        private readonly string _currentUserId;
        private readonly OfflineFirstService _offlineFirstService;
        private readonly Timer _heartbeatTimer;
        private readonly HashSet<int> _trackedTodoIds = new HashSet<int>();
        private readonly Dictionary<int, DateTime> _lastActivityTime = new Dictionary<int, DateTime>(); // Track last edit activity per TodoId
        private readonly object _lock = new object();
        private bool _disposed = false;
        private int _heartbeatTickCounter = 0; // Counter to track ticks

        // Cache for session data to avoid constant DB access
        private readonly Dictionary<int, List<TodoSessionInfo>> _sessionCache = new Dictionary<int, List<TodoSessionInfo>>();
        private readonly Dictionary<int, DateTime> _cacheExpiry = new Dictionary<int, DateTime>();
        private OleDbConnection _persistentConnection = null;
        private DateTime _lastConnectionCheck = DateTime.MinValue;
        private bool _connectionHealthy = true;

        private const int CHECK_INTERVAL_MS = 10000; // Check every 10 seconds
        private const int HEARTBEAT_WRITE_INTERVAL_TICKS = 6; // Write heartbeat every 6 ticks (1 minute)
        private const int SESSION_TIMEOUT_SECONDS = 180; // Consider session dead after 3 minutes without heartbeat
        private const int EDITING_TIMEOUT_SECONDS = 300; // Auto-unmark editing after 5 minutes of inactivity
        private const int CACHE_LIFETIME_SECONDS = 15; // Cache sessions for 15 seconds
        private const int CONNECTION_CHECK_INTERVAL_SECONDS = 30; // Check connection health every 30 seconds

        /// <summary>
        /// Creates a new TodoListSessionTracker using the country-specific Lock database
        /// </summary>
        /// <param name="lockDbConnectionString">Connection string to the country's Lock database</param>
        /// <param name="currentUserId">Current user identifier (e.g., Windows username)</param>
        /// <param name="offlineFirstService">Optional OfflineFirstService to check for import operations</param>
        public TodoListSessionTracker(string lockDbConnectionString, string currentUserId, OfflineFirstService offlineFirstService = null)
        {
            if (string.IsNullOrWhiteSpace(lockDbConnectionString))
                throw new ArgumentException("Connection string is required", nameof(lockDbConnectionString));
            if (string.IsNullOrWhiteSpace(currentUserId))
                throw new ArgumentException("User ID is required", nameof(currentUserId));

            _lockDbConnectionString = lockDbConnectionString;
            _currentUserId = currentUserId;
            _offlineFirstService = offlineFirstService;

            // Start heartbeat timer - check every 10 seconds
            _heartbeatTimer = new Timer(HeartbeatCallback, null, CHECK_INTERVAL_MS, CHECK_INTERVAL_MS);
            
            // Register in static list for cleanup coordination
            lock (_staticLock)
            {
                _activeTrackers.Add(new WeakReference<TodoListSessionTracker>(this));
            }
        }

        public async Task EnsureTableAsync()
        {
            const string table = "T_TodoList_Sessions";
            using (var conn = new OleDbConnection(_lockDbConnectionString))
            {
                await conn.OpenAsync().ConfigureAwait(false);
                try
                {
                    var schema = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, table, "TABLE" });
                    if (schema == null || schema.Rows.Count == 0)
                    {
                        // Create table with CountryId for isolation
                        var createSql = $@"
                            CREATE TABLE {table} (
                                [SessionId] AUTOINCREMENT PRIMARY KEY,
                                [TodoId] LONG NOT NULL,
                                [UserId] TEXT(255) NOT NULL,
                                [UserName] TEXT(255),
                                [SessionStart] DATETIME NOT NULL,
                                [LastHeartbeat] DATETIME NOT NULL,
                                [IsEditing] BIT NOT NULL DEFAULT 0
                            )";
                        using (var cmd = new OleDbCommand(createSql, conn))
                        {
                            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }

                        // Create indexes for faster lookups
                        try
                        {
                            var indexSql1 = $"CREATE INDEX IX_TodoSessions_CountryTodo ON {table}([CountryId], [TodoId])";
                            using (var cmd = new OleDbCommand(indexSql1, conn))
                            {
                                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                            }
                        }
                        catch { /* Index creation is optional */ }
                    }
                }
                catch { /* Table might already exist */ }
            }
        }

        /// <summary>
        /// Registers that the current user is viewing a TodoList item
        /// </summary>
        public async Task<bool> RegisterViewingAsync(int todoId, string userName = null, bool isEditing = false)
        {
            try
            {
                System.Diagnostics.Debug.WriteLine($"[TodoSessionTracker.RegisterViewing] START - TodoId={todoId}, User={_currentUserId}, UserName={userName}");
                
                lock (_lock)
                {
                    _trackedTodoIds.Add(todoId);
                }

                using (var conn = new OleDbConnection(_lockDbConnectionString))
                {
                    await conn.OpenAsync().ConfigureAwait(false);
                    System.Diagnostics.Debug.WriteLine($"[TodoSessionTracker.RegisterViewing] Connection opened");

                    // Check if session already exists for this country
                    var checkSql = "SELECT SessionId FROM T_TodoList_Sessions WHERE TodoId = ? AND UserId = ?";
                    using (var checkCmd = new OleDbCommand(checkSql, conn))
                    {
                        checkCmd.Parameters.AddWithValue("@TodoId", todoId);
                        checkCmd.Parameters.AddWithValue("@UserId", _currentUserId);
                        var existing = await checkCmd.ExecuteScalarAsync().ConfigureAwait(false);
                        System.Diagnostics.Debug.WriteLine($"[TodoSessionTracker.RegisterViewing] Existing session: {existing}");

                        if (existing != null)
                        {
                            // Update existing session
                            var updateSql = @"UPDATE T_TodoList_Sessions 
                                            SET LastHeartbeat = ?, IsEditing = ? 
                                            WHERE SessionId = ?";
                            using (var updateCmd = new OleDbCommand(updateSql, conn))
                            {
                                updateCmd.Parameters.Add("@LastHeartbeat", OleDbType.Date).Value = DateTime.Now;
                                updateCmd.Parameters.AddWithValue("@IsEditing", isEditing);
                                updateCmd.Parameters.AddWithValue("@SessionId", existing);
                                await updateCmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                                System.Diagnostics.Debug.WriteLine($"[TodoSessionTracker.RegisterViewing] Updated existing session");
                            }
                        }
                        else
                        {
                            // Insert new session
                            var insertSql = @"INSERT INTO T_TodoList_Sessions 
                                            (TodoId, UserId, UserName, SessionStart, LastHeartbeat, IsEditing) 
                                            VALUES (?, ?, ?, ?, ?, ?)";
                            using (var insertCmd = new OleDbCommand(insertSql, conn))
                            {
                                insertCmd.Parameters.AddWithValue("@TodoId", todoId);
                                insertCmd.Parameters.AddWithValue("@UserId", _currentUserId);
                                insertCmd.Parameters.AddWithValue("@UserName", userName);
                                insertCmd.Parameters.Add("@SessionStart", OleDbType.Date).Value = DateTime.Now;
                                insertCmd.Parameters.Add("@LastHeartbeat", OleDbType.Date).Value = DateTime.Now;
                                insertCmd.Parameters.AddWithValue("@IsEditing", isEditing);
                                await insertCmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                                System.Diagnostics.Debug.WriteLine($"[TodoSessionTracker.RegisterViewing] Inserted new session");
                            }
                        }
                    }
                }
                
                // Invalidate cache for this TodoId since we modified the session
                InvalidateCache(todoId);
                
                System.Diagnostics.Debug.WriteLine($"[TodoSessionTracker.RegisterViewing] SUCCESS");
                return true;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[TodoSessionTracker.RegisterViewing] ERROR: {ex.Message}\n{ex.StackTrace}");
                // Connection might be broken
                _connectionHealthy = false;
                CloseConnection();
                return false;
            }
        }

        /// <summary>
        /// Unregisters the current user from viewing a TodoList item
        /// </summary>
        public async Task UnregisterViewingAsync(int todoId)
        {
            try
            {
                lock (_lock)
                {
                    _trackedTodoIds.Remove(todoId);
                }

                using (var conn = new OleDbConnection(_lockDbConnectionString))
                {
                    await conn.OpenAsync().ConfigureAwait(false);
                    var deleteSql = "DELETE FROM T_TodoList_Sessions WHERE TodoId = ? AND UserId = ?";
                    using (var cmd = new OleDbCommand(deleteSql, conn))
                    {
                        cmd.Parameters.AddWithValue("@TodoId", todoId);
                        cmd.Parameters.AddWithValue("@UserId", _currentUserId);
                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                }
                
                // Invalidate cache for this TodoId
                InvalidateCache(todoId);
            }
            catch { /* Best effort */ }
        }

        /// <summary>
        /// Gets information about other users currently viewing/editing a TodoList item
        /// Uses cache to avoid constant DB access
        /// </summary>
        public async Task<List<TodoSessionInfo>> GetActiveSessionsAsync(int todoId)
        {
            // Check cache first
            lock (_lock)
            {
                if (_sessionCache.TryGetValue(todoId, out var cached) && 
                    _cacheExpiry.TryGetValue(todoId, out var expiry) &&
                    DateTime.Now < expiry)
                {
                    // Cache hit!
                    return new List<TodoSessionInfo>(cached);
                }
            }

            // Cache miss - fetch from DB
            var sessions = new List<TodoSessionInfo>();
            try
            {
                var conn = await GetOrCreateConnectionAsync().ConfigureAwait(false);
                if (conn == null)
                {
                    // Connection failed, return empty list
                    return sessions;
                }

                // Clean up stale sessions first
                await CleanupStaleSessionsAsync(conn).ConfigureAwait(false);

                // Get active sessions for this country (excluding current user)
                var sql = @"SELECT UserId, UserName, SessionStart, LastHeartbeat, IsEditing 
                           FROM T_TodoList_Sessions 
                           WHERE TodoId = ? AND UserId <> ?
                           ORDER BY SessionStart";
                using (var cmd = new OleDbCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@TodoId", todoId);
                    cmd.Parameters.AddWithValue("@UserId", _currentUserId);
                    using (var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync().ConfigureAwait(false))
                        {
                            sessions.Add(new TodoSessionInfo
                            {
                                UserId = reader["UserId"]?.ToString(),
                                UserName = reader["UserName"]?.ToString(),
                                SessionStart = reader["SessionStart"] as DateTime? ?? DateTime.MinValue,
                                LastHeartbeat = reader["LastHeartbeat"] as DateTime? ?? DateTime.MinValue,
                                IsEditing = reader["IsEditing"] as bool? ?? false
                            });
                        }
                    }
                }

                // Update cache
                lock (_lock)
                {
                    _sessionCache[todoId] = new List<TodoSessionInfo>(sessions);
                    _cacheExpiry[todoId] = DateTime.Now.AddSeconds(CACHE_LIFETIME_SECONDS);
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[TodoSessionTracker.GetActiveSessions] ERROR: {ex.Message}");
                // Mark connection as unhealthy
                _connectionHealthy = false;
                CloseConnection();
            }
            return sessions;
        }

        /// <summary>
        /// Checks if any other user is currently editing the specified TodoList item
        /// Uses cache from GetActiveSessionsAsync to avoid DB access
        /// </summary>
        public async Task<bool> IsBeingEditedByOtherUserAsync(int todoId)
        {
            try
            {
                // Use cached sessions if available
                var sessions = await GetActiveSessionsAsync(todoId).ConfigureAwait(false);
                return sessions.Any(s => s.IsEditing && s.IsActive);
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Updates the editing status for the current user's session
        /// </summary>
        public async Task UpdateEditingStatusAsync(int todoId, bool isEditing)
        {
            try
            {
                // Track activity time when marking as editing
                if (isEditing)
                {
                    lock (_lock)
                    {
                        _lastActivityTime[todoId] = DateTime.Now;
                    }
                }

                using (var conn = new OleDbConnection(_lockDbConnectionString))
                {
                    await conn.OpenAsync().ConfigureAwait(false);
                    var updateSql = @"UPDATE T_TodoList_Sessions 
                                    SET IsEditing = ?, LastHeartbeat = ? 
                                    WHERE TodoId = ? AND UserId = ?";
                    using (var cmd = new OleDbCommand(updateSql, conn))
                    {
                        cmd.Parameters.AddWithValue("@IsEditing", isEditing);
                        cmd.Parameters.Add("@LastHeartbeat", OleDbType.Date).Value = DateTime.Now;
                        cmd.Parameters.AddWithValue("@TodoId", todoId);
                        cmd.Parameters.AddWithValue("@UserId", _currentUserId);
                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                }
                
                // Invalidate cache for this TodoId
                InvalidateCache(todoId);
            }
            catch { /* Best effort */ }
        }

        /// <summary>
        /// Notifies that the user has performed an edit action on a TodoList.
        /// Automatically marks the session as editing and updates activity timestamp.
        /// </summary>
        public async Task NotifyEditActivityAsync(int todoId)
        {
            try
            {
                lock (_lock)
                {
                    _lastActivityTime[todoId] = DateTime.Now;
                }

                // Mark as editing if not already
                await UpdateEditingStatusAsync(todoId, true).ConfigureAwait(false);
            }
            catch { /* Best effort */ }
        }

        /// <summary>
        /// Heartbeat callback - runs every 10 seconds to check activity and write heartbeat every minute
        /// Uses persistent connection for better performance
        /// </summary>
        private void HeartbeatCallback(object state)
        {
            if (_disposed) return;

            try
            {
                // Skip heartbeat writes if an Ambre import is in progress to avoid DB lock contention
                if (_offlineFirstService != null && _offlineFirstService.IsAmbreImportInProgress())
                {
                    System.Diagnostics.Debug.WriteLine("[TodoSessionTracker.Heartbeat] Skipping heartbeat - Ambre import in progress");
                    return;
                }

                _heartbeatTickCounter++;
                bool shouldWriteHeartbeat = (_heartbeatTickCounter % HEARTBEAT_WRITE_INTERVAL_TICKS) == 0;

                int[] todoIds;
                Dictionary<int, DateTime> activitySnapshot;
                lock (_lock)
                {
                    todoIds = _trackedTodoIds.ToArray();
                    activitySnapshot = new Dictionary<int, DateTime>(_lastActivityTime);
                }

                if (todoIds.Length == 0) return;

                // Use persistent connection
                OleDbConnection conn;
                lock (_lock)
                {
                    conn = _persistentConnection;
                }

                if (conn == null || conn.State != ConnectionState.Open)
                {
                    // Try to reconnect
                    try
                    {
                        var task = GetOrCreateConnectionAsync();
                        task.Wait(5000); // Wait max 5 seconds
                        conn = task.Result;
                    }
                    catch
                    {
                        // Connection failed - mark as unhealthy and return
                        _connectionHealthy = false;
                        CloseConnection();
                        return;
                    }
                }

                if (conn == null) return;

                var now = DateTime.Now;
                bool cacheInvalidated = false;

                    foreach (var todoId in todoIds)
                    {
                        try
                        {
                            // Check if we need to auto-unmark editing due to inactivity
                            bool shouldUnmarkEditing = false;
                            if (activitySnapshot.TryGetValue(todoId, out var lastActivity))
                            {
                                var inactivityDuration = (now - lastActivity).TotalSeconds;
                                if (inactivityDuration > EDITING_TIMEOUT_SECONDS)
                                {
                                    shouldUnmarkEditing = true;
                                }
                            }

                            // Write heartbeat only every minute (every 6 ticks)
                            if (shouldWriteHeartbeat)
                            {
                                if (shouldUnmarkEditing)
                                {
                                    // Unmark editing and update heartbeat
                                    var updateSql = @"UPDATE T_TodoList_Sessions 
                                                    SET LastHeartbeat = ?, IsEditing = False 
                                                    WHERE TodoId = ? AND UserId = ?";
                                    using (var cmd = new OleDbCommand(updateSql, conn))
                                    {
                                        cmd.Parameters.Add("@LastHeartbeat", OleDbType.Date).Value = now;
                                        cmd.Parameters.AddWithValue("@TodoId", todoId);
                                        cmd.Parameters.AddWithValue("@UserId", _currentUserId);
                                        cmd.ExecuteNonQuery();
                                    }

                                    // Remove from activity tracking
                                    lock (_lock)
                                    {
                                        _lastActivityTime.Remove(todoId);
                                    }
                                    
                                    // Invalidate cache for this TodoId
                                    InvalidateCache(todoId);
                                    cacheInvalidated = true;
                                }
                                else
                                {
                                    // Normal heartbeat update
                                    var updateSql = @"UPDATE T_TodoList_Sessions 
                                                    SET LastHeartbeat = ? 
                                                    WHERE TodoId = ? AND UserId = ?";
                                    using (var cmd = new OleDbCommand(updateSql, conn))
                                    {
                                        cmd.Parameters.Add("@LastHeartbeat", OleDbType.Date).Value = now;
                                        cmd.Parameters.AddWithValue("@TodoId", todoId);
                                        cmd.Parameters.AddWithValue("@UserId", _currentUserId);
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                            }
                            else if (shouldUnmarkEditing)
                            {
                                // Even if not writing heartbeat, unmark editing if inactive
                                var updateSql = @"UPDATE T_TodoList_Sessions 
                                                SET IsEditing = False 
                                                WHERE TodoId = ? AND UserId = ?";
                                using (var cmd = new OleDbCommand(updateSql, conn))
                                {
                                    cmd.Parameters.AddWithValue("@TodoId", todoId);
                                    cmd.Parameters.AddWithValue("@UserId", _currentUserId);
                                    cmd.ExecuteNonQuery();
                                }

                                // Remove from activity tracking
                                lock (_lock)
                                {
                                    _lastActivityTime.Remove(todoId);
                                }
                                
                                // Invalidate cache for this TodoId
                                InvalidateCache(todoId);
                                cacheInvalidated = true;
                            }
                        }
                        catch (Exception ex)
                        {
                            System.Diagnostics.Debug.WriteLine($"[TodoSessionTracker.Heartbeat] Error updating TodoId {todoId}: {ex.Message}");
                            // Connection might be broken
                            _connectionHealthy = false;
                        }
                    }
                
                if (cacheInvalidated)
                {
                    System.Diagnostics.Debug.WriteLine("[TodoSessionTracker.Heartbeat] Cache invalidated due to session updates");
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[TodoSessionTracker.Heartbeat] ERROR: {ex.Message}");
                // Connection failed, close and invalidate
                CloseConnection();
            }
        }

        /// <summary>
        /// Removes sessions that haven't sent a heartbeat recently
        /// </summary>
        private async Task CleanupStaleSessionsAsync(OleDbConnection conn)
        {
            try
            {
                var cutoff = DateTime.Now.AddSeconds(-SESSION_TIMEOUT_SECONDS);
                var deleteSql = "DELETE FROM T_TodoList_Sessions WHERE LastHeartbeat < ?";
                using (var cmd = new OleDbCommand(deleteSql, conn))
                {
                    cmd.Parameters.Add("@Cutoff", OleDbType.Date).Value = cutoff;
                    await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
            catch { /* Cleanup is best-effort */ }
        }

        /// <summary>
        /// Gets or creates a persistent connection to the Lock database
        /// Handles connection failures and reconnection
        /// </summary>
        private async Task<OleDbConnection> GetOrCreateConnectionAsync()
        {
            lock (_lock)
            {
                // Check if we need to verify connection health
                if ((DateTime.Now - _lastConnectionCheck).TotalSeconds > CONNECTION_CHECK_INTERVAL_SECONDS)
                {
                    _lastConnectionCheck = DateTime.Now;
                    
                    // Test existing connection
                    if (_persistentConnection != null)
                    {
                        try
                        {
                            if (_persistentConnection.State != ConnectionState.Open)
                            {
                                System.Diagnostics.Debug.WriteLine("[TodoSessionTracker] Connection not open, closing and recreating");
                                CloseConnection();
                            }
                        }
                        catch
                        {
                            System.Diagnostics.Debug.WriteLine("[TodoSessionTracker] Connection health check failed, closing");
                            CloseConnection();
                        }
                    }
                }

                // Return existing healthy connection
                if (_persistentConnection != null && _persistentConnection.State == ConnectionState.Open)
                {
                    return _persistentConnection;
                }
            }

            // Need to create new connection
            try
            {
                var conn = new OleDbConnection(_lockDbConnectionString);
                await conn.OpenAsync().ConfigureAwait(false);
                
                lock (_lock)
                {
                    _persistentConnection = conn;
                    _connectionHealthy = true;
                    _lastConnectionCheck = DateTime.Now;
                }
                
                System.Diagnostics.Debug.WriteLine("[TodoSessionTracker] Created new persistent connection");
                return conn;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[TodoSessionTracker] Failed to create connection: {ex.Message}");
                lock (_lock)
                {
                    _connectionHealthy = false;
                }
                return null;
            }
        }

        /// <summary>
        /// Closes all persistent connections from all active TodoListSessionTracker instances.
        /// Used before database maintenance operations (e.g., compaction) to release locks.
        /// Connections will be automatically recreated on the next heartbeat tick.
        /// </summary>
        public static void CloseAllConnections()
        {
            lock (_staticLock)
            {
                // Clean up dead references and close active ones
                _activeTrackers.RemoveAll(wr => !wr.TryGetTarget(out _));
                
                foreach (var weakRef in _activeTrackers)
                {
                    if (weakRef.TryGetTarget(out var tracker))
                    {
                        try { tracker.CloseConnection(); }
                        catch { /* ignore errors */ }
                    }
                }
                
                System.Diagnostics.Debug.WriteLine($"[TodoSessionTracker.CloseAllConnections] Closed {_activeTrackers.Count} active connections");
            }
        }
        
        /// <summary>
        /// Closes the persistent connection and invalidates cache.
        /// Can be called externally to release the database lock before maintenance operations (e.g., compaction).
        /// The connection will be automatically recreated on the next heartbeat tick.
        /// </summary>
        public void CloseConnection()
        {
            lock (_lock)
            {
                try
                {
                    _persistentConnection?.Close();
                    _persistentConnection?.Dispose();
                }
                catch { }
                finally
                {
                    _persistentConnection = null;
                    _connectionHealthy = false;
                    
                    // Invalidate cache when connection is lost
                    _sessionCache.Clear();
                    _cacheExpiry.Clear();
                    
                    System.Diagnostics.Debug.WriteLine("[TodoSessionTracker] Connection closed, cache invalidated");
                }
            }
        }

        /// <summary>
        /// Invalidates the cache for a specific TodoId
        /// </summary>
        private void InvalidateCache(int todoId)
        {
            lock (_lock)
            {
                _sessionCache.Remove(todoId);
                _cacheExpiry.Remove(todoId);
            }
        }

        /// <summary>
        /// Invalidates all cached session data
        /// </summary>
        public void InvalidateAllCache()
        {
            lock (_lock)
            {
                _sessionCache.Clear();
                _cacheExpiry.Clear();
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _heartbeatTimer?.Dispose();

            // Clean up all tracked sessions
            try
            {
                using (var conn = new OleDbConnection(_lockDbConnectionString))
                {
                    conn.Open();
                    var deleteSql = "DELETE FROM T_TodoList_Sessions WHERE UserId = ?";
                    using (var cmd = new OleDbCommand(deleteSql, conn))
                    {
                        cmd.Parameters.AddWithValue("@UserId", _currentUserId);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch { /* Best effort cleanup */ }
        }
    }

    /// <summary>
    /// Information about an active TodoList viewing session
    /// </summary>
    public class TodoSessionInfo
    {
        public string UserId { get; set; }
        public string UserName { get; set; }
        public DateTime SessionStart { get; set; }
        public DateTime LastHeartbeat { get; set; }
        public bool IsEditing { get; set; }

        public TimeSpan Duration => DateTime.Now - SessionStart;
        // Consider active if heartbeat within 90 seconds (1.5x the 60s heartbeat interval)
        public bool IsActive => (DateTime.Now - LastHeartbeat).TotalSeconds < 90;
    }
}
