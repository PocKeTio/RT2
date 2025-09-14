using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OfflineFirstAccess.Helpers;
using OfflineFirstAccess.Models;
using RecoTool.Helpers;
using RecoTool.Models;

namespace RecoTool.Services.AmbreImport
{
    /// <summary>
    /// Validateur pour l'import Ambre
    /// </summary>
    public class AmbreImportValidator
    {
        /// <summary>
        /// Valide les fichiers d'import
        /// </summary>
        public string[] ValidateFiles(string[] filePaths, bool isMultiFile, ImportResult result)
        {
            var files = (filePaths ?? Array.Empty<string>())
                .Where(p => !string.IsNullOrWhiteSpace(p))
                .Take(2)
                .ToArray();

            if (files.Length == 0)
            {
                result.Errors.Add("No files provided");
                return Array.Empty<string>();
            }

            foreach (var filePath in files)
            {
                ValidateSingleFile(filePath, isMultiFile, result);
            }

            return files;
        }

        /// <summary>
        /// Valide que les comptes requis sont présents dans les données
        /// </summary>
        public bool ValidateRequiredAccounts(
            List<Dictionary<string, object>> filteredData,
            Country country,
            ImportResult result)
        {
            var accounts = ExtractUniqueAccounts(filteredData);
            
            bool hasPivot = ValidateAccount(accounts, country?.CNT_AmbrePivot, "Pivot");
            bool hasReceivable = ValidateAccount(accounts, country?.CNT_AmbreReceivable, "Receivable");

            if (!(hasPivot && hasReceivable))
            {
                var missing = new List<string>();
                if (!hasPivot) missing.Add($"Pivot={country?.CNT_AmbrePivot}");
                if (!hasReceivable) missing.Add($"Receivable={country?.CNT_AmbreReceivable}");
                
                result.Errors.Add($"Import aborted: both AMBRE accounts are required. Missing: {string.Join(", ", missing)}.");
                return false;
            }

            return true;
        }

        /// <summary>
        /// Valide les données transformées
        /// </summary>
        public (List<string> errors, List<DataAmbre> validData) ValidateTransformedData(
            List<DataAmbre> data,
            Country country)
        {
            var errors = new List<string>();
            var validData = new List<DataAmbre>();

            foreach (var item in data)
            {
                var itemErrors = ValidateDataCoherence(item, country);
                if (itemErrors.Any())
                {
                    errors.AddRange(itemErrors.Select(e => $"Line {item.Event_Num}: {e}"));
                }
                else
                {
                    validData.Add(item);
                }
            }

            return (errors, validData);
        }

        private void ValidateSingleFile(string filePath, bool isMultiFile, ImportResult result)
        {
            var errors = ValidationHelper.ValidateImportFile(filePath);
            
            if (errors.Any())
            {
                if (isMultiFile)
                {
                    var fileName = Path.GetFileName(filePath);
                    result.Errors.AddRange(errors.Select(e => $"{fileName}: {e}"));
                }
                else
                {
                    result.Errors.AddRange(errors);
                }
            }
        }

        private List<string> ExtractUniqueAccounts(List<Dictionary<string, object>> data)
        {
            return data
                .Select(r => r.ContainsKey("Account_ID") ? r["Account_ID"]?.ToString() : null)
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        private bool ValidateAccount(List<string> accounts, string expectedAccount, string accountType)
        {
            if (string.IsNullOrWhiteSpace(expectedAccount))
                return false;

            return accounts.Any(a => string.Equals(a, expectedAccount, StringComparison.OrdinalIgnoreCase));
        }

        private List<string> ValidateDataCoherence(DataAmbre item, Country country)
        {
            var errors = new List<string>();

            // Validation des champs obligatoires
            if (string.IsNullOrWhiteSpace(item.Account_ID))
                errors.Add("Account_ID is required");

            if (string.IsNullOrWhiteSpace(item.Event_Num))
                errors.Add("Event_Num is required");

            //if (!item.Operation_Date.HasValue && !item.Value_Date.HasValue)
            //    errors.Add("At least one date (Operation_Date or Value_Date) is required");

            // Validation de cohérence des montants
            if (item.SignedAmount == 0 && item.LocalSignedAmount == 0)
                errors.Add("Amount cannot be zero");

            // Validation du compte par rapport au pays
            if (!IsValidAccountForCountry(item.Account_ID, country))
                errors.Add($"Account {item.Account_ID} is not valid for country {country?.CNT_Id}");

            // Validation des dates
            if (item.Operation_Date.HasValue && item.Value_Date.HasValue)
            {
                if (item.Value_Date.Value < item.Operation_Date.Value.AddDays(-30))
                    errors.Add("Value date is too far before operation date");
            }

            return errors;
        }

        private bool IsValidAccountForCountry(string accountId, Country country)
        {
            if (country == null || string.IsNullOrWhiteSpace(accountId))
                return false;

            var account = accountId.Trim();
            var pivot = country.CNT_AmbrePivot?.Trim();
            var receivable = country.CNT_AmbreReceivable?.Trim();

            return (!string.IsNullOrWhiteSpace(pivot) && 
                    string.Equals(account, pivot, StringComparison.OrdinalIgnoreCase))
                   ||
                   (!string.IsNullOrWhiteSpace(receivable) && 
                    string.Equals(account, receivable, StringComparison.OrdinalIgnoreCase));
        }
    }
}