using System;
using System.IO;
using System.Threading.Tasks;
using OfflineFirstAccess.Models;

namespace OfflineFirstAccess.Helpers
{
    /// <summary>
    /// Classe utilitaire pour créer facilement des bases de données template
    /// </summary>
    public static class DatabaseTemplateFactory
    {
        /// <summary>
        /// Crée une base de données template standard avec les tables système
        /// </summary>
        /// <param name="templatePath">Chemin où la base de données template sera créée</param>
        /// <returns>True si la création a réussi, False sinon</returns>
        public static async Task<bool> CreateStandardTemplateAsync(string templatePath)
        {
            var builder = new DatabaseTemplateBuilder(templatePath);
            return await builder.CreateTemplateAsync();
        }

        /// <summary>
        /// Crée une base de données template personnalisée
        /// </summary>
        /// <param name="templatePath">Chemin où la base de données template sera créée</param>
        /// <param name="configureAction">Action permettant de configurer la structure de la base de données</param>
        /// <returns>True si la création a réussi, False sinon</returns>
        public static async Task<bool> CreateCustomTemplateAsync(string templatePath, Action<DatabaseTemplateBuilder> configureAction)
        {
            var builder = new DatabaseTemplateBuilder(templatePath);
            
            // Appliquer la configuration personnalisée
            configureAction?.Invoke(builder);
            
            return await builder.CreateTemplateAsync();
        }

        /// <summary>
        /// Crée une base de données template à partir d'une configuration existante
        /// </summary>
        /// <param name="templatePath">Chemin où la base de données template sera créée</param>
        /// <param name="config">Configuration définissant la structure de la base de données</param>
        /// <returns>True si la création a réussi, False sinon</returns>
        public static async Task<bool> CreateFromConfigurationAsync(string templatePath, DatabaseConfiguration config)
        {
            return await DatabaseTemplateGenerator.CreateDatabaseTemplateAsync(templatePath, config);
        }

        /// <summary>
        /// Crée une base de données référentielle commune avec les tables de référence Ambre et utilisateur
        /// </summary>
        /// <param name="templatePath">Chemin où la base de données référentielle sera créée</param>
        /// <returns>True si la création a réussi, False sinon</returns>
        public static async Task<bool> CreateReferentialTemplateAsync(string templatePath)
        {
            var builder = new DatabaseTemplateBuilder(templatePath);
            
            // Table T_Ref_Ambre_ImportFields
            builder.AddTable("T_Ref_Ambre_ImportFields")
                .WithColumn("AMB_Source", typeof(string), false)
                .WithColumn("AMB_Destination", typeof(string), false);
            
            // Table T_Ref_Ambre_TransactionCodes
            builder.AddTable("T_Ref_Ambre_TransactionCodes")
                .WithPrimaryKey("ID", typeof(int), true)
                .WithColumn("CODE", typeof(string), false)
                .WithColumn("TAG", typeof(string), false);
            
            // Table T_Ref_Ambre_Transform
            builder.AddTable("T_Ref_Ambre_Transform")
                .WithColumn("AMB_Source", typeof(string), false)
                .WithColumn("AMB_Destination", typeof(string), false)
                .WithColumn("AMB_TransformationFunction", typeof(string), false)
                .WithColumn("AMB_Description", typeof(string), true);
            
            // Table T_Ref_User_Fields
            builder.AddTable("T_Ref_User_Fields")
                .WithPrimaryKey("ID", typeof(int), true)
                .WithColumn("Category", typeof(string), false)
                .WithColumn("FieldName", typeof(string), false)
                .WithColumn("FieldDescription", typeof(string), true)
                .WithColumn("Pivot", typeof(bool), false)
                .WithColumn("Receivable", typeof(bool), false)
                .WithColumn("IsClickable", typeof(bool), false);
            
            // Table T_Ref_Country
            builder.AddTable("T_Ref_Country")
                .WithPrimaryKey("CNT_Id", typeof(string), false)
                .WithColumn("CNT_Name", typeof(string), false)
                .WithColumn("CNT_AmbreCountryId", typeof(int), true)
                .WithColumn("CNT_AmbrePivot", typeof(string), true)
                .WithColumn("CNT_AmbreReceivable", typeof(string), true)
                .WithColumn("CNT_AmbrePivotCountry", typeof(int), true)
                .WithColumn("CNT_ServiceCode", typeof(string), true)
                .WithColumn("CNT_BIC", typeof(string), true);
            
            return await builder.CreateTemplateAsync();
        }

        /// <summary>
        /// Crée une base de données template à partir d'une base de données existante
        /// </summary>
        /// <param name="sourceDatabasePath">Chemin de la base de données source</param>
        /// <param name="templatePath">Chemin où la base de données template sera créée</param>
        /// <returns>True si la création a réussi, False sinon</returns>
        public static bool CreateFromExistingDatabase(string sourceDatabasePath, string templatePath)
        {
            try
            {
                // Vérifier que la base de données source existe
                if (!File.Exists(sourceDatabasePath))
                    return false;

                // Créer le répertoire de destination si nécessaire
                string directory = Path.GetDirectoryName(templatePath);
                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                // Copier la base de données source vers le template
                File.Copy(sourceDatabasePath, templatePath, true);

                return true;
            }
            catch (Exception ex)
            {
                LogManager.Error($"Erreur lors de la création du template à partir d'une base existante : {ex.Message}", ex);
                return false;
            }
        }

        /// <summary>
        /// Vérifie si une base de données existante correspond à un template standard
        /// </summary>
        /// <param name="databasePath">Chemin de la base de données à vérifier</param>
        /// <returns>True si la structure correspond, False sinon</returns>
        public static async Task<bool> ValidateStandardStructureAsync(string databasePath)
        {
            var builder = new DatabaseTemplateBuilder(Path.GetTempFileName());
            return await builder.ValidateDatabaseAsync(databasePath);
        }

        /// <summary>
        /// Met à jour la structure d'une base de données existante pour correspondre à un template standard
        /// </summary>
        /// <param name="databasePath">Chemin de la base de données à mettre à jour</param>
        /// <returns>True si la mise à jour a réussi, False sinon</returns>
        public static async Task<bool> UpdateToStandardStructureAsync(string databasePath)
        {
            var builder = new DatabaseTemplateBuilder(Path.GetTempFileName());
            return await builder.UpdateDatabaseAsync(databasePath);
        }
    }
}
