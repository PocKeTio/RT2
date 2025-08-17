using System;
using System.Collections.Generic;
using System.IO;

namespace OfflineFirstAccess.Models
{
    /// <summary>
    /// Configuration globale pour le système de base de données offline-first
    /// </summary>
    public class DatabaseConfiguration
    {
        /// <summary>
        /// Nom du modèle de fichier pour les bases de données locales
        /// </summary>
        public string LocalDatabasePath { get; set; } = "Local_{0}.accdb";
        
        /// <summary>
        /// Chemin d'accès au dossier de stockage des bases de données locales
        /// </summary>
        public string LocalStoragePath { get; set; }
        
        /// <summary>
        /// Nom du modèle de fichier pour les bases de données distantes (données)
        /// </summary>
        public string RemoteDataDatabasePath { get; set; } = "\\\\serveur\\share\\Data_{0}.accdb";
        
        /// <summary>
        /// Nom du modèle de fichier pour les bases de données distantes (verrous)
        /// </summary>
        public string RemoteLockDatabasePath { get; set; } = "\\\\serveur\\share\\Lock_{0}.accdb";
        
        /// <summary>
        /// Nom du modèle de fichier pour les signaux de session
        /// </summary>
        public string RemoteSignalFilePath { get; set; } = "\\\\serveur\\share\\Sessions_{0}.signal";
        
        /// <summary>
        /// Intervalle en secondes pour la vérification des modifications
        /// </summary>
        public int ChangeCheckIntervalSeconds { get; set; } = 60;
        
        /// <summary>
        /// Nombre maximum de tentatives de connexion
        /// </summary>
        public int MaxConnectionRetries { get; set; } = 3;
        
        /// <summary>
        /// Délai en millisecondes entre les tentatives de connexion
        /// </summary>
        public int ConnectionRetryDelayMs { get; set; } = 1000;
        
        /// <summary>
        /// Liste des configurations de tables
        /// </summary>
        public List<TableConfiguration> Tables { get; set; } = new List<TableConfiguration>();
        
        /// <summary>
        /// Obtient le chemin d'accès à la base de données locale pour l'identifiant spécifié
        /// </summary>
        public string GetLocalDatabasePath(string identifier)
        {
            return Path.Combine(LocalStoragePath, string.Format(LocalDatabasePath, identifier));
        }
        
        /// <summary>
        /// Obtient le chemin d'accès à la base de données distante (données) pour l'identifiant spécifié
        /// </summary>
        public string GetRemoteDataDatabasePath(string identifier)
        {
            return string.Format(RemoteDataDatabasePath, identifier);
        }
        
        /// <summary>
        /// Obtient le chemin d'accès à la base de données distante (verrous) pour l'identifiant spécifié
        /// </summary>
        public string GetRemoteLockDatabasePath(string identifier)
        {
            return string.Format(RemoteLockDatabasePath, identifier);
        }
        
        /// <summary>
        /// Obtient le chemin d'accès au fichier de signal pour l'identifiant spécifié
        /// </summary>
        public string GetRemoteSignalFilePath(string identifier)
        {
            return string.Format(RemoteSignalFilePath, identifier);
        }
        
        /// <summary>
        /// Obtient la configuration d'une table par son nom
        /// </summary>
        public TableConfiguration GetTableConfiguration(string tableName)
        {
            return Tables.Find(t => t.Name == tableName);
        }
        
        /// <summary>
        /// Crée une configuration standard avec des tables prédéfinies
        /// </summary>
        public static DatabaseConfiguration CreateDefault(string localStoragePath)
        {
            var config = new DatabaseConfiguration
            {
                LocalStoragePath = localStoragePath
            };
            
            // CORRECTION : ChangeLog supprimé de la config locale car maintenant dans la base Lock
            // ChangeLog est géré automatiquement par EnsureChangeLogTableExistsAsync dans GenericAccessService.Lock.cs
            
            // Ajouter la table des verrous (SyncLocks)
            var syncLocksTable = new TableConfiguration
            {
                Name = "SyncLocks",
                PrimaryKeyColumn = "LockID",
                PrimaryKeyType = typeof(string),
                LastModifiedColumn = "CreatedAt",
                CreateTableSql = @"CREATE TABLE SyncLocks (
                    LockID TEXT(255) PRIMARY KEY,
                    Reason TEXT(255) NOT NULL,
                    CreatedAt DATETIME NOT NULL,
                    ExpiresAt DATETIME,
                    MachineName TEXT(100),
                    ProcessId LONG
                )"
            };
            
            syncLocksTable.Columns.Add(new ColumnDefinition("LockID", typeof(string), "TEXT(255)", false, true));
            syncLocksTable.Columns.Add(new ColumnDefinition("Reason", typeof(string), "TEXT(255)", false));
            syncLocksTable.Columns.Add(new ColumnDefinition("CreatedAt", typeof(DateTime), "DATETIME", false));
            syncLocksTable.Columns.Add(new ColumnDefinition("ExpiresAt", typeof(DateTime), "DATETIME", true));
            syncLocksTable.Columns.Add(new ColumnDefinition("MachineName", typeof(string), "TEXT(100)", true));
            syncLocksTable.Columns.Add(new ColumnDefinition("ProcessId", typeof(long), "LONG", true));
            
            // Ajouter la table des sessions (Sessions)
            var sessionsTable = new TableConfiguration
            {
                Name = "Sessions",
                PrimaryKeyColumn = "SessionID",
                PrimaryKeyType = typeof(string),
                LastModifiedColumn = "LastActivity",
                CreateTableSql = @"CREATE TABLE Sessions (
                    SessionID TEXT(255) PRIMARY KEY,
                    UserID TEXT(50) NOT NULL,
                    MachineName TEXT(50) NOT NULL,
                    StartTime DATETIME NOT NULL,
                    LastActivity DATETIME NOT NULL,
                    IsActive BIT NOT NULL
                )"
            };
            
            sessionsTable.Columns.Add(new ColumnDefinition("SessionID", typeof(string), "TEXT(255)", false, true));
            sessionsTable.Columns.Add(new ColumnDefinition("UserID", typeof(string), "TEXT(50)", false));
            sessionsTable.Columns.Add(new ColumnDefinition("MachineName", typeof(string), "TEXT(50)", false));
            sessionsTable.Columns.Add(new ColumnDefinition("StartTime", typeof(DateTime), "DATETIME", false));
            sessionsTable.Columns.Add(new ColumnDefinition("LastActivity", typeof(DateTime), "DATETIME", false));
            sessionsTable.Columns.Add(new ColumnDefinition("IsActive", typeof(bool), "BIT", false));
            
            // Ajouter la table des paramètres de configuration (T_ConfigParameters)
            var configParamsTable = new TableConfiguration
            {
                Name = "T_ConfigParameters",
                PrimaryKeyColumn = "Key",
                PrimaryKeyType = typeof(string),
                LastModifiedColumn = "LastModified",
                VersionColumn = "Version",
                CreateTableSql = @"CREATE TABLE T_ConfigParameters (
                    [Key] TEXT(255) PRIMARY KEY,
                    Value TEXT(255) NOT NULL,
                    Description TEXT(255),
                    Category TEXT(50) NOT NULL,
                    LastModified DATETIME NOT NULL,
                    Version LONG DEFAULT 1,
                    IsUserEditable BIT DEFAULT 1
                )"
            };
            
            configParamsTable.Columns.Add(new ColumnDefinition("Key", typeof(string), "TEXT(255)", false, true));
            configParamsTable.Columns.Add(new ColumnDefinition("Value", typeof(string), "TEXT(255)", false));
            configParamsTable.Columns.Add(new ColumnDefinition("Description", typeof(string), "TEXT(255)", true));
            configParamsTable.Columns.Add(new ColumnDefinition("Category", typeof(string), "TEXT(50)", false));
            configParamsTable.Columns.Add(new ColumnDefinition("LastModified", typeof(DateTime), "DATETIME", false));
            configParamsTable.Columns.Add(new ColumnDefinition("Version", typeof(long), "LONG", false));
            configParamsTable.Columns.Add(new ColumnDefinition("IsUserEditable", typeof(bool), "BIT", false));

            // Ajouter la table des données Ambre (T_Data_Ambre)
            var dataAmbreTable = new TableConfiguration
            {
                Name = "T_Data_Ambre",
                PrimaryKeyColumn = "ID",
                PrimaryKeyType = typeof(string),
                LastModifiedColumn = "LastModified",
                DeleteDateColumn = "DeleteDate",
                VersionColumn = "Version",
                CreateTableSql = @"CREATE TABLE T_Data_Ambre (
        ID                           TEXT(255)    PRIMARY KEY,  
        Account_ID                   TEXT(50),                  
        CCY                          TEXT(3),                   
        Country                      TEXT(255),                   
        Event_Num                    TEXT(50),                  
        Folder                       TEXT(255),                 
        Pivot_MbawIDFromLabel        TEXT(255),                 
        Pivot_TransactionCodesFromLabel TEXT(255),              
        Pivot_TRNFromLabel           TEXT(255),                 
        RawLabel                     TEXT(255),                 
        Receivable_DWRefFromAmbre    TEXT(255),                 
        LocalSignedAmount            DOUBLE,      
        Operation_Date               DATETIME,                  
        Reconciliation_Num           TEXT(255),                 
        Receivable_InvoiceFromAmbre  TEXT(255),                 
        ReconciliationOrigin_Num     TEXT(255),                 
        SignedAmount                 DOUBLE,      
        Value_Date                   DATETIME,                  
        -- Champs BaseEntity
        CreationDate                 DATETIME,                  
        DeleteDate                   DATETIME,                  
        ModifiedBy                   TEXT(100),                 
        LastModified                 DATETIME,                  
        Version                      LONG     
    )"
            };

            // -- Colonnes métier T_Data_Ambre
            dataAmbreTable.Columns.Add(new ColumnDefinition("ID", typeof(string), "TEXT(255)", true, true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("Account_ID", typeof(string), "TEXT(50)", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("CCY", typeof(string), "TEXT(3)", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("Country", typeof(string), "TEXT(3)", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("Event_Num", typeof(string), "TEXT(50)", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("Folder", typeof(string), "TEXT(255)", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("Pivot_MbawIDFromLabel", typeof(string), "TEXT(255)", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("Pivot_TransactionCodesFromLabel", typeof(string), "TEXT(255)", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("Pivot_TRNFromLabel", typeof(string), "TEXT(255)", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("RawLabel", typeof(string), "TEXT(255)", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("Receivable_DWRefFromAmbre", typeof(string), "TEXT(255)", true));

            dataAmbreTable.Columns.Add(new ColumnDefinition("LocalSignedAmount", typeof(double), "DOUBLE", false));
            dataAmbreTable.Columns.Add(new ColumnDefinition("Operation_Date", typeof(DateTime), "DATETIME", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("Reconciliation_Num", typeof(string), "TEXT(255)", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("Receivable_InvoiceFromAmbre", typeof(string), "TEXT(255)", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("ReconciliationOrigin_Num", typeof(string), "TEXT(255)", true));

            dataAmbreTable.Columns.Add(new ColumnDefinition("SignedAmount", typeof(double), "DOUBLE", false));
            dataAmbreTable.Columns.Add(new ColumnDefinition("Value_Date", typeof(DateTime), "DATETIME", true));

            // -- Champs BaseEntity
            dataAmbreTable.Columns.Add(new ColumnDefinition("CreationDate", typeof(DateTime), "DATETIME", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("DeleteDate", typeof(DateTime), "DATETIME", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("ModifiedBy", typeof(string), "TEXT(100)", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("LastModified", typeof(DateTime), "DATETIME", true));
            dataAmbreTable.Columns.Add(new ColumnDefinition("Version", typeof(long), "LONG", false));


            // Ajouter la table de réconciliation (T_Reconciliation)
            var reconciliationTable = new TableConfiguration
            {
                Name = "T_Reconciliation",
                PrimaryKeyColumn = "ID",
                PrimaryKeyType = typeof(string),
                LastModifiedColumn = "LastModified",
                DeleteDateColumn = "DeleteDate",
                VersionColumn = "Version",
                CreateTableSql = @"CREATE TABLE T_Reconciliation (
                            ID                     TEXT(255)  PRIMARY KEY,
                            DWINGS_GuaranteeID     TEXT(255),
                            DWINGS_InvoiceID       TEXT(255),
                            DWINGS_CommissionID    TEXT(255),
                            Action                 BIT,               
                            Comments               TEXT(255),
                            InternalInvoiceReference TEXT(255),
                            FirstClaimDate         DATETIME,
                            LastClaimDate          DATETIME,
                            ToRemind               BIT,      
                            ToRemindDate           DATETIME,
                            ACK                    BIT,      
                            SwiftCode              TEXT(50),
                            PaymentReference       TEXT(255),
                            KPI                    LONG,     
                            IncidentType           LONG,     
                            RiskyItem              LONG,
                            ReasonNonRisky         TEXT(255),
                            CreationDate           DATETIME,
                            DeleteDate             DATETIME,
                            ModifiedBy             TEXT(100),
                            LastModified           DATETIME,
                            Version                LONG  DEFAULT 1
                        )"
            };

            /* ---------- Définition des colonnes ---------- */
            // Champs métier
            reconciliationTable.Columns.Add(new ColumnDefinition("ID", typeof(string), "TEXT(255)", false, true));
            reconciliationTable.Columns.Add(new ColumnDefinition("DWINGS_GuaranteeID", typeof(string), "TEXT(255)", true));
            reconciliationTable.Columns.Add(new ColumnDefinition("DWINGS_InvoiceID", typeof(string), "TEXT(255)", true));
            reconciliationTable.Columns.Add(new ColumnDefinition("DWINGS_CommissionID", typeof(string), "TEXT(255)", true));

            reconciliationTable.Columns.Add(new ColumnDefinition("Action", typeof(bool), "BIT", false));
            reconciliationTable.Columns.Add(new ColumnDefinition("Comments", typeof(string), "TEXT(255)", true));
            reconciliationTable.Columns.Add(new ColumnDefinition("InternalInvoiceReference", typeof(string), "TEXT(255)", true));

            reconciliationTable.Columns.Add(new ColumnDefinition("FirstClaimDate", typeof(DateTime), "DATETIME", true));
            reconciliationTable.Columns.Add(new ColumnDefinition("LastClaimDate", typeof(DateTime), "DATETIME", true));

            reconciliationTable.Columns.Add(new ColumnDefinition("ToRemind", typeof(bool), "BIT", false));
            reconciliationTable.Columns.Add(new ColumnDefinition("ToRemindDate", typeof(DateTime), "DATETIME", true));

            reconciliationTable.Columns.Add(new ColumnDefinition("ACK", typeof(bool), "BIT", false));
            reconciliationTable.Columns.Add(new ColumnDefinition("SwiftCode", typeof(string), "TEXT(50)", true));
            reconciliationTable.Columns.Add(new ColumnDefinition("PaymentReference", typeof(string), "TEXT(255)", true));

            reconciliationTable.Columns.Add(new ColumnDefinition("KPI", typeof(int), "LONG", true));
            reconciliationTable.Columns.Add(new ColumnDefinition("IncidentType", typeof(int), "LONG", true));
            reconciliationTable.Columns.Add(new ColumnDefinition("RiskyItem", typeof(int), "LONG", true));
            reconciliationTable.Columns.Add(new ColumnDefinition("ReasonNonRisky", typeof(string), "TEXT(255)", true));

            // Champs BaseEntity
            reconciliationTable.Columns.Add(new ColumnDefinition("CreationDate", typeof(DateTime), "DATETIME", true));
            reconciliationTable.Columns.Add(new ColumnDefinition("DeleteDate", typeof(DateTime), "DATETIME", true));
            reconciliationTable.Columns.Add(new ColumnDefinition("ModifiedBy", typeof(string), "TEXT(100)", true));
            reconciliationTable.Columns.Add(new ColumnDefinition("LastModified", typeof(DateTime), "DATETIME", true));
            reconciliationTable.Columns.Add(new ColumnDefinition("Version", typeof(long), "LONG", false));


            // CORRECTION : ChangeLog supprimé - maintenant dans la base Lock
            config.Tables.Add(syncLocksTable);
            config.Tables.Add(sessionsTable);
            config.Tables.Add(configParamsTable);
            config.Tables.Add(dataAmbreTable);
            config.Tables.Add(reconciliationTable);

            return config;
        }
    }
}
