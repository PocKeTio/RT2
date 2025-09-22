using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using RecoTool.Models;

namespace RecoTool.Helpers
{
    /// <summary>
    /// Helper pour la validation des données
    /// </summary>
    public static class ValidationHelper
    {
        /// <summary>
        /// Valide les données d'une ligne Ambre
        /// </summary>
        /// <param name="data">Données à valider</param>
        /// <returns>Liste des erreurs de validation</returns>
        public static List<string> ValidateAmbreData(Dictionary<string, object> data)
        {
            var errors = new List<string>();

            // Validation des champs obligatoires
            ValidateRequiredField(data, "Account_ID", errors);
            ValidateRequiredField(data, "Event_Num", errors);
            ValidateRequiredField(data, "ReconciliationOrigin_Num", errors);

            // Validation des montants
            ValidateDecimalField(data, "SignedAmount", errors);
            ValidateDecimalField(data, "LocalSignedAmount", errors);

            // Validation des dates
            ValidateDateField(data, "Operation_Date", errors);
            ValidateDateField(data, "Value_Date", errors);

            // Validation du format des devises (3 caractères)
            ValidateCurrencyField(data, "CCY", errors);

            // Validation du pays (2 caractères)
            ValidateCountryField(data, "Country", errors);

            return errors;
        }

        /// <summary>
        /// Valide qu'un champ obligatoire est présent et non vide
        /// </summary>
        private static void ValidateRequiredField(Dictionary<string, object> data, string fieldName, List<string> errors)
        {
            if (!data.ContainsKey(fieldName) || 
                data[fieldName] == null || 
                string.IsNullOrWhiteSpace(data[fieldName].ToString()))
            {
                errors.Add($"Le champ '{fieldName}' est obligatoire.");
            }
        }

        /// <summary>
        /// Valide qu'un champ décimal est valide
        /// </summary>
        private static void ValidateDecimalField(Dictionary<string, object> data, string fieldName, List<string> errors)
        {
            if (data.ContainsKey(fieldName) && data[fieldName] != null)
            {
                var value = data[fieldName].ToString();
                if (!string.IsNullOrWhiteSpace(value) && !decimal.TryParse(value, out _))
                {
                    errors.Add($"Le champ '{fieldName}' doit être un nombre décimal valide.");
                }
            }
        }

        /// <summary>
        /// Valide qu'un champ date est valide
        /// </summary>
        private static void ValidateDateField(Dictionary<string, object> data, string fieldName, List<string> errors)
        {
            if (data.ContainsKey(fieldName) && data[fieldName] != null)
            {
                var value = data[fieldName].ToString();
                if (!string.IsNullOrWhiteSpace(value) && !DateTime.TryParse(value, out _))
                {
                    errors.Add($"Le champ '{fieldName}' doit être une date valide.");
                }
            }
        }

        /// <summary>
        /// Valide le format d'une devise (3 caractères)
        /// </summary>
        private static void ValidateCurrencyField(Dictionary<string, object> data, string fieldName, List<string> errors)
        {
            if (data.ContainsKey(fieldName) && data[fieldName] != null)
            {
                var value = data[fieldName].ToString().Trim();
                if (!string.IsNullOrEmpty(value) && (value.Length != 3 || !Regex.IsMatch(value, @"^[A-Z]{3}$")))
                {
                    errors.Add($"Le champ '{fieldName}' doit être un code devise de 3 lettres majuscules.");
                }
            }
        }

        /// <summary>
        /// Valide le format d'un pays (2 caractères)
        /// </summary>
        private static void ValidateCountryField(Dictionary<string, object> data, string fieldName, List<string> errors)
        {
            if (data.ContainsKey(fieldName) && data[fieldName] != null)
            {
                var value = data[fieldName].ToString().Trim();
                if (!string.IsNullOrEmpty(value) && (value.Length != 2 || !Regex.IsMatch(value, @"^[A-Z]{2}$")))
                {
                    errors.Add($"Le champ '{fieldName}' doit être un code pays de 2 lettres majuscules.");
                }
            }
        }

        /// <summary>
        /// Valide le format d'un ID d'invoice :
        /// BGI + année sur 4 chiffres + mois (01 à 12) + numéro sur 7 chiffres.
        /// Exemple : BGI2024010000000
        /// </summary>
        /// <param name="invoiceId">ID d'invoice à valider</param>
        /// <returns>True si le format est valide</returns>
        public static bool ValidateInvoiceIdFormat(string invoiceId)
        {
            if (string.IsNullOrEmpty(invoiceId))
                return false;

            // Pattern: BGI + année (4 chiffres) + mois (01-12) + numéro (7 chiffres)
            return Regex.IsMatch(invoiceId, @"^BGI\d{4}(0[1-9]|1[0-2])\d{7}$");
        }

        /// <summary>
        /// Valide qu'une devise est supportée
        /// </summary>
        /// <param name="currency">Code devise</param>
        /// <returns>True si la devise est supportée</returns>
        public static bool ValidateCurrency(string currency)
        {
            var supportedCurrencies = new HashSet<string>
            {
                "EUR", "USD", "GBP", "CHF", "JPY", "CAD", "AUD", "SEK", "NOK", "DKK"
            };

            return !string.IsNullOrEmpty(currency) && 
                   supportedCurrencies.Contains(currency.ToUpper());
        }

        /// <summary>
        /// Valide qu'un pays est supporté
        /// </summary>
        /// <param name="countryCode">Code pays ISO 2 lettres</param>
        /// <param name="supportedCountries">Liste des pays supportés</param>
        /// <returns>True si le pays est supporté</returns>
        public static bool ValidateCountry(string countryCode, IEnumerable<Country> supportedCountries)
        {
            if (string.IsNullOrEmpty(countryCode) || supportedCountries == null)
                return false;

            return supportedCountries.Any(c => 
                string.Equals(c.CNT_Id, countryCode, StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Valide la cohérence des données d'une ligne Ambre
        /// </summary>
        /// <param name="dataAmbre">Données Ambre</param>
        /// <param name="country">Configuration du pays</param>
        /// <returns>Liste des erreurs de cohérence</returns>
        public static List<string> ValidateDataCoherence(DataAmbre dataAmbre, Country country)
        {
            var errors = new List<string>();

            if (dataAmbre == null)
            {
                errors.Add("Les données Ambre sont nulles.");
                return errors;
            }

            if (country == null)
            {
                errors.Add("La configuration du pays est manquante.");
                return errors;
            }

            // Vérifier que le compte appartient bien au pays
            if (!dataAmbre.IsPivotAccount(country.CNT_AmbrePivot) && 
                !dataAmbre.IsReceivableAccount(country.CNT_AmbreReceivable))
            {
                errors.Add($"Le compte '{dataAmbre.Account_ID}' ne correspond ni au compte Pivot ni au compte Receivable configurés pour le pays '{country.CNT_Id}'.");
            }

            // Vérifier la cohérence des montants
            if (dataAmbre.SignedAmount == 0 && dataAmbre.LocalSignedAmount == 0)
            {
                errors.Add("Au moins un des montants (SignedAmount ou LocalSignedAmount) doit être différent de zéro.");
            }

            //// Vérifier la cohérence des dates
            //if (dataAmbre.Operation_Date.HasValue && dataAmbre.Value_Date.HasValue)
            //{
            //    if (dataAmbre.Value_Date < dataAmbre.Operation_Date)
            //    {
            //        errors.Add("La date de valeur ne peut pas être antérieure à la date d'opération.");
            //    }
            //}

            return errors;
        }

        /// <summary>
        /// Valide qu'un fichier peut être importé
        /// </summary>
        /// <param name="filePath">Chemin vers le fichier</param>
        /// <param name="maxFileSizeMB">Taille maximum en MB</param>
        /// <returns>Liste des erreurs de validation</returns>
        public static List<string> ValidateImportFile(string filePath, int maxFileSizeMB = 50)
        {
            var errors = new List<string>();

            if (string.IsNullOrEmpty(filePath))
            {
                errors.Add("Le chemin du fichier est obligatoire.");
                return errors;
            }

            if (!System.IO.File.Exists(filePath))
            {
                errors.Add("Le fichier spécifié n'existe pas.");
                return errors;
            }

            // Vérifier l'extension
            if (!ExcelHelper.ValidateExcelFormat(filePath))
            {
                errors.Add("Le fichier doit être au format Excel (.xlsx ou .xls).");
            }

            // Vérifier la taille
            var fileInfo = new System.IO.FileInfo(filePath);
            var fileSizeMB = fileInfo.Length / (1024.0 * 1024.0);
            
            if (fileSizeMB > maxFileSizeMB)
            {
                errors.Add($"Le fichier est trop volumineux ({fileSizeMB:F1} MB). Taille maximum autorisée: {maxFileSizeMB} MB.");
            }

            return errors;
        }

        /// <summary>
        /// Convertit une valeur en décimal de manière sécurisée
        /// </summary>
        /// <param name="value">Valeur à convertir</param>
        /// <returns>Décimal ou 0 si conversion impossible</returns>
        public static decimal SafeParseDecimal(object value)
        {
            if (value == null)
                return 0;

            // Direct numeric types
            if (value is decimal dec) return dec;
            if (value is double d) return Convert.ToDecimal(d);
            if (value is float f) return Convert.ToDecimal(f);
            if (value is int i) return i;
            if (value is long l) return l;

            var stringValue = value.ToString().Trim();
            return SafeParseDecimal(stringValue, null);
        }

        /// <summary>
        /// Convertit une valeur en date de manière sécurisée
        /// </summary>
        /// <param name="value">Valeur à convertir</param>
        /// <returns>DateTime ou null si conversion impossible</returns>
        public static DateTime? SafeParseDateTime(object value)
        {
            if (value == null)
                return null;

            // Direct DateTime and Excel OLE Automation numeric dates
            if (value is DateTime dt) return dt;
            if (value is double oa)
            {
                try { return DateTime.FromOADate(oa); } catch { }
            }

            var stringValue = value.ToString().Trim();
            if (string.IsNullOrWhiteSpace(stringValue))
                return null;

            // Try with French-like formats first to avoid MM/DD confusion
            var fr = CultureInfo.GetCultureInfo("fr-FR");
            string[] preferredFormats = new[]
            {
                "dd/MM/yyyy",
                "d/M/yyyy",
                "dd/MM/yyyy HH:mm",
                "d/M/yyyy HH:mm",
                "dd-MM-yyyy",
                "d-M-yyyy",
                "dd-MM-yyyy HH:mm",
                "d-M-yyyy HH:mm"
            };
            if (DateTime.TryParseExact(stringValue, preferredFormats, fr, DateTimeStyles.None, out DateTime result))
                return result;

            // Tolerate seconds
            string[] extendedFormats = new[]
            {
                "dd/MM/yyyy HH:mm:ss",
                "d/M/yyyy HH:mm:ss",
                "dd-MM-yyyy HH:mm:ss",
                "d-M-yyyy HH:mm:ss"
            };
            if (DateTime.TryParseExact(stringValue, extendedFormats, fr, DateTimeStyles.None, out result))
                return result;

            // Fallback: invariant and current culture
            if (DateTime.TryParse(stringValue, CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
                return result;
            if (DateTime.TryParse(stringValue, CultureInfo.CurrentCulture, DateTimeStyles.None, out result))
                return result;

            return null;
        }

        /// <summary>
        /// Culture-aware decimal parsing with cleaning of thousand separators.
        /// </summary>
        public static decimal SafeParseDecimal(object value, CultureInfo preferredCulture)
        {
            if (value == null) return 0;
            if (value is decimal dec) return dec;
            if (value is double d) return Convert.ToDecimal(d);
            if (value is float f) return Convert.ToDecimal(f);
            if (value is int i) return i;
            if (value is long l) return l;

            var s = value.ToString()?.Trim();
            if (string.IsNullOrEmpty(s)) return 0;

            // Try preferred culture first
            if (preferredCulture != null && decimal.TryParse(s, NumberStyles.Any, preferredCulture, out var vPref))
                return vPref;

            // Heuristic cleanup: remove non-breaking spaces and normal spaces
            var cleaned = s.Replace("\u00A0", "").Replace(" ", "");
            // If preferredCulture has distinct decimal separator, normalize to '.' for invariant parsing
            if (preferredCulture != null)
            {
                var decSep = preferredCulture.NumberFormat.NumberDecimalSeparator;
                if (!string.IsNullOrEmpty(decSep) && decSep != ".")
                    cleaned = cleaned.Replace(decSep, ".");
                var grpSep = preferredCulture.NumberFormat.NumberGroupSeparator;
                if (!string.IsNullOrEmpty(grpSep) && grpSep != ",")
                    cleaned = cleaned.Replace(grpSep, "");
            }

            if (decimal.TryParse(cleaned, NumberStyles.Any, CultureInfo.InvariantCulture, out var vInv))
                return vInv;

            // Fallbacks
            if (decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var v1)) return v1;
            if (decimal.TryParse(s, NumberStyles.Any, CultureInfo.CurrentCulture, out var v2)) return v2;
            return 0;
        }

        /// <summary>
        /// Culture-aware DateTime parsing with Excel numeric support.
        /// </summary>
        public static DateTime? SafeParseDateTime(object value, CultureInfo preferredCulture)
        {
            if (value == null) return null;
            if (value is DateTime dt) return dt;
            if (value is double oa)
            {
                try { return DateTime.FromOADate(oa); } catch { }
            }

            var s = value.ToString()?.Trim();
            if (string.IsNullOrEmpty(s)) return null;

            if (preferredCulture != null && DateTime.TryParse(s, preferredCulture, DateTimeStyles.None, out var vPref))
                return vPref;

            // Try dd/MM/yyyy style common in many EU countries
            var fr = CultureInfo.GetCultureInfo("fr-FR");
            string[] fmts = new[] { "dd/MM/yyyy", "d/M/yyyy", "dd/MM/yyyy HH:mm", "d/M/yyyy HH:mm", "dd/MM/yyyy HH:mm:ss", "d/M/yyyy HH:mm:ss" };
            if (DateTime.TryParseExact(s, fmts, fr, DateTimeStyles.None, out var exFr))
                return exFr;

            if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.None, out var inv))
                return inv;
            if (DateTime.TryParse(s, CultureInfo.CurrentCulture, DateTimeStyles.None, out var cur))
                return cur;
            return null;
        }
    }
}
