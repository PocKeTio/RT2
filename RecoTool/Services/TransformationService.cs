using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using RecoTool.Models;
using RecoTool.Helpers;

namespace RecoTool.Services
{
    /// <summary>
    /// Service pour les transformations de données Ambre
    /// Implémente les fonctions définies dans AMB_TransformationFunction
    /// </summary>
    public class TransformationService
    {
        private readonly Dictionary<string, Country> _countries;

        public TransformationService(IEnumerable<Country> countries)
        {
            _countries = countries?.ToDictionary(c => c.CNT_Id, c => c) ?? new Dictionary<string, Country>();
        }

        /// <summary>
        /// Applique une transformation sur les données d'une ligne
        /// </summary>
        /// <param name="sourceData">Données source</param>
        /// <param name="transform">Configuration de transformation</param>
        /// <returns>Valeur transformée</returns>
        public string ApplyTransformation(Dictionary<string, object> sourceData, AmbreTransform transform)
        {
            var source = GetSourceValue(sourceData, transform?.AMB_Source);

            if (transform == null || string.IsNullOrEmpty(transform.AMB_TransformationFunction))
                return source;

            switch (transform.AMB_TransformationFunction.ToUpper())
            {
                case "GETBOOKINGNAMEFROMID":
                    return GetBookingNameFromID(source);

                case "GETMBAWIDFROMLABEL":
                    return GetMbawIDFromLabel(source);

                case "GETCODESFROMLABEL":
                    return GetCodesFromLabel(source);

                case "GETTRNFROMLABEL":
                case "GETTRN FROMLABEL": // support legacy with space
                    return GetTRNFromLabel(source);

                case "EXTRACTFORRECEIVABLE":
                    return ExtractForReceivable(source);

                case "REMOVEZEROSFROMSTART":
                    return RemoveZerosFromStart(source);

                case "ADDSIGN":
                    return AddSign(source, sourceData);

                default:
                    // Si la fonction n'est pas reconnue, retourne la valeur source
                    return source;
            }
        }

        /// <summary>
        /// Récupère la valeur source en gérant les concaténations
        /// Exemple: [Account_ID]&[Event_Num]&[ReconciliationOrigin_Num]&[RawLabel]
        /// </summary>
        private string GetSourceValue(Dictionary<string, object> sourceData, string sourceExpression)
        {
            if (string.IsNullOrEmpty(sourceExpression))
                return string.Empty;

            // Si c'est une simple référence de champ
            // Back-compat: 'Label' legacy => map to RawLabel if needed
            if (string.Equals(sourceExpression, "Label", StringComparison.OrdinalIgnoreCase))
            {
                var raw = sourceData.ContainsKey("RawLabel") ? sourceData["RawLabel"]?.ToString() : null;
                if (!string.IsNullOrEmpty(raw)) return raw;
            }

            if (sourceData.ContainsKey(sourceExpression))
            {
                return sourceData[sourceExpression]?.ToString() ?? string.Empty;
            }

            // Si c'est une expression avec des champs entre crochets
            var result = sourceExpression;
            var fieldMatches = Regex.Matches(sourceExpression, @"\[([^\]]+)\]");

            foreach (Match match in fieldMatches)
            {
                var fieldName = match.Groups[1].Value;
                string fieldValue;
                if (sourceData.ContainsKey(fieldName))
                {
                    fieldValue = sourceData[fieldName]?.ToString() ?? string.Empty;
                }
                else if (string.Equals(fieldName, "Label", StringComparison.OrdinalIgnoreCase) && sourceData.ContainsKey("RawLabel"))
                {
                    // Back-compat: replace [Label] by RawLabel if present
                    fieldValue = sourceData["RawLabel"]?.ToString() ?? string.Empty;
                }
                else
                {
                    fieldValue = string.Empty;
                }
                
                result = result.Replace(match.Value, fieldValue);
            }

            return result;
        }

        /// <summary>
        /// Obtient le nom du booking depuis l'ID pays
        /// </summary>
        /// <param name="countryId">Code pays Ambre</param>
        /// <returns>Booking ID 2 lettres</returns>
        public string GetBookingNameFromID(string countryId)
        {
            if (string.IsNullOrEmpty(countryId))
                return string.Empty;

            var country = _countries.Values.FirstOrDefault(c => 
                string.Equals(c.CNT_AmbrePivotCountryId.ToString(), countryId, StringComparison.OrdinalIgnoreCase));

            return country?.CNT_Id ?? countryId;
        }

        /// <summary>
        /// Extrait l'ID MBAW depuis un libellé
        /// Pattern: recherche des références de type MBAW suivies de chiffres
        /// </summary>
        /// <param name="label">Libellé à analyser</param>
        /// <returns>ID MBAW extrait</returns>
        public string GetMbawIDFromLabel(string label)
        {
            if (string.IsNullOrEmpty(label))
                return string.Empty;

            // Pattern pour MBAW suivi de chiffres et éventuellement de lettres
            var match = Regex.Match(label, @"MBAW[A-Z0-9]+", RegexOptions.IgnoreCase);
            return match.Success ? match.Value.ToUpper() : string.Empty;
        }

        /// <summary>
        /// Extrait les codes de transaction depuis un libellé
        /// </summary>
        /// <param name="label">Libellé à analyser</param>
        /// <returns>Codes extraits</returns>
        public string GetCodesFromLabel(string label)
        {
            if (string.IsNullOrEmpty(label))
                return string.Empty;

            var trimmed = label.Trim();
            if (trimmed.Length <= 13)
                return trimmed;

            return trimmed.Substring(trimmed.Length - 13);
        }

        /// <summary>
        /// Extrait le numéro TRN (Transaction Reference Number) depuis un libellé
        /// </summary>
        /// <param name="label">Libellé à analyser</param>
        /// <returns>TRN extrait</returns>
        public string GetTRNFromLabel(string label)
        {
            if (string.IsNullOrEmpty(label))
                return string.Empty;

            // Start at character 43 (1-based) => index 42 (0-based), take 10 characters
            int startIndex = 42;
            if (label.Length <= startIndex)
                return string.Empty;

            int len = Math.Min(10, label.Length - startIndex);
            return label.Substring(startIndex, len);
        }

        /// <summary>
        /// Extrait les informations spécifiques au compte Receivable
        /// Principalement l'ID DW 
        /// </summary>
        /// <param name="label">Libellé à analyser</param>
        /// <returns>Information extraite pour Receivable</returns>
        public string ExtractForReceivable(string label)
        {
            if (string.IsNullOrEmpty(label))
                return string.Empty;

            // Utiliser les extracteurs centralisés pour garantir des patterns uniformes
            var bgi = DwingsLinkingHelper.ExtractBgiToken(label);
            if (!string.IsNullOrWhiteSpace(bgi))
                return bgi.ToUpperInvariant();

            var guaranteeId = DwingsLinkingHelper.ExtractGuaranteeId(label);
            return string.IsNullOrWhiteSpace(guaranteeId) ? string.Empty : guaranteeId.ToUpperInvariant();
        }

        /// <summary>
        /// Supprime les zéros en début de chaîne
        /// </summary>
        /// <param name="value">Valeur à traiter</param>
        /// <returns>Valeur sans zéros initiaux</returns>
        public string RemoveZerosFromStart(string value)
        {
            if (string.IsNullOrEmpty(value))
                return string.Empty;

            return value.TrimStart('0');
        }

        /// <summary>
        /// Ajoute un signe '-' au début de la valeur si le champ SS indique un débit ("D").
        /// Si la valeur est déjà négative, elle est renvoyée telle quelle. Les '+' initiaux sont supprimés.
        /// </summary>
        /// <param name="value">Valeur à signer (telle que lue des colonnes Excel selon la config)</param>
        /// <param name="sourceData">Ligne source complète pour lire le champ SS</param>
        /// <returns>Valeur potentiellement préfixée d'un '-'</returns>
        public string AddSign(string value, Dictionary<string, object> sourceData)
        {
            try
            {
                var ss = sourceData != null && sourceData.ContainsKey("")
                    ? sourceData[""]?.ToString()?.Trim()
                    : null;

                if (string.Equals(ss, "D", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(value)) return "-0";
                    var v = value.Trim();
                    if (v.StartsWith("-")) return v; // déjà négatif
                    if (v.StartsWith("+")) v = v.Substring(1);
                    return "-" + v;
                }

                return value; // crédit ou non indiqué: laisser tel quel
            }
            catch
            {
                return value;
            }
        }

        /// <summary>
        /// Détermine le type de transaction pour la catégorisation automatique
        /// </summary>
        /// <param name="label">Libellé de la transaction</param>
        /// <param name="isPivot">True si c'est le compte Pivot</param>
        /// <param name="category">Indice de type de transaction (enum TransactionType) pour Pivot</param>
        /// <returns>Type de transaction détecté</returns>
        public TransactionType? DetermineTransactionType(string label, bool isPivot, int? category = null)
        {
            // Normalize label once
            var upperLabel = (label ?? string.Empty).ToUpperInvariant();

            // If explicitly uncategorized or empty
            if (string.IsNullOrWhiteSpace(label) && !category.HasValue)
                return TransactionType.TO_CATEGORIZE;
            if (upperLabel.Contains("TO CATEGORIZE"))
                return TransactionType.TO_CATEGORIZE;

            if (isPivot)
            {
                if (category.HasValue)
                {
                    // Désormais, category encode directement un TransactionType
                    return (TransactionType)category.Value;
                }

                // Fallback: parse label for pivot (should rarely happen if Category is populated)
                if (upperLabel.Contains("COLLECTION")) return TransactionType.COLLECTION;
                if (upperLabel.Contains("AUTOMATIC REFUND") || upperLabel.Contains("PAYMENT")) return TransactionType.PAYMENT;
                if (upperLabel.Contains("ADJUSTMENT")) return TransactionType.ADJUSTMENT;
                if (upperLabel.Contains("XCL LOADER")) return TransactionType.XCL_LOADER;
                if (upperLabel.Contains("TRIGGER")) return TransactionType.TRIGGER;
                return TransactionType.TO_CATEGORIZE;
            }
            else
            {
                // For RECEIVABLE: return null - TransactionType should come from PAYMENT_METHOD (handled in BuildRuleContext)
                // Parsing label is not reliable for receivables
                return null;
            }
        }

        /// <summary>
        /// Détermine si la transaction est crédit ou débit
        /// </summary>
        /// <param name="amount">Montant de la transaction</param>
        /// <returns>"CREDIT" ou "DEBIT"</returns>
        public string DetermineCreditDebit(decimal amount)
        {
            return amount >= 0 ? "CREDIT" : "DEBIT";
        }
    }
}