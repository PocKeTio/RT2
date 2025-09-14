using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using OfflineFirstAccess.Helpers;
using OfflineFirstAccess.Models;
using RecoTool.Models;
using RecoTool.Services.DTOs;

namespace RecoTool.Services.AmbreImport
{
    /// <summary>
    /// Gestionnaire de chargement des configurations pour l'import Ambre
    /// </summary>
    public class AmbreConfigurationLoader
    {
        private readonly OfflineFirstService _offlineFirstService;
        private TransformationService _transformationService;
        private Dictionary<string, TransactionType> _codeToCategory;
        private bool _initialized;

        public AmbreConfigurationLoader(OfflineFirstService offlineFirstService)
        {
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
        }

        public TransformationService TransformationService => _transformationService;
        public Dictionary<string, TransactionType> CodeToCategory => _codeToCategory;

        public async Task EnsureInitializedAsync()
        {
            if (_initialized) return;

            var countries = await _offlineFirstService.GetCountries().ConfigureAwait(false);
            _transformationService = new TransformationService(countries);
            
            await LoadTransactionCodesAsync();
            _initialized = true;
        }

        public async Task<AmbreImportConfiguration> LoadConfigurationsAsync(string countryId, ImportResult result)
        {
            try
            {
                var country = await LoadCountryConfigurationAsync(countryId);
                var importFields = await LoadImportFieldsConfigurationAsync();
                var transforms = await LoadTransformConfigurationsAsync();

                if (country == null)
                {
                    result.Errors.Add($"Configuration not found for country: {countryId}");
                    return null;
                }

                return new AmbreImportConfiguration
                {
                    Country = country,
                    ImportFields = importFields,
                    Transforms = transforms
                };
            }
            catch (Exception ex)
            {
                result.Errors.Add($"Error loading configuration: {ex.Message}");
                LogManager.Error($"Error loading configuration for {countryId}", ex);
                return null;
            }
        }

        private async Task LoadTransactionCodesAsync()
        {
            try
            {
                var codes = _offlineFirstService.GetAmbreTransactionCodes() ?? new List<AmbreTransactionCode>();
                _codeToCategory = new Dictionary<string, TransactionType>(StringComparer.OrdinalIgnoreCase);
                
                foreach (var code in codes)
                {
                    if (string.IsNullOrWhiteSpace(code?.ATC_CODE) || string.IsNullOrWhiteSpace(code?.ATC_TAG)) 
                        continue;
                    
                    var normalized = code.ATC_TAG.Replace(" ", "_").Replace("-", "_");
                    if (Enum.TryParse<TransactionType>(normalized, ignoreCase: true, out var tx))
                    {
                        _codeToCategory[code.ATC_CODE.Trim()] = tx;
                    }
                    else
                    {
                        LogManager.Warning($"Unknown ATC_TAG '{code.ATC_TAG}' for code '{code.ATC_CODE}'");
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to load T_Ref_Ambre_TransactionCodes", ex);
                _codeToCategory = new Dictionary<string, TransactionType>(StringComparer.OrdinalIgnoreCase);
            }
        }

        private async Task<Country> LoadCountryConfigurationAsync(string countryId)
        {
            try
            {
                return await _offlineFirstService.GetCountryByIdAsync(countryId);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Error loading country configuration for {countryId}: {ex.Message}", ex);
            }
        }

        private async Task<List<AmbreImportField>> LoadImportFieldsConfigurationAsync()
        {
            try
            {
                return await Task.FromResult(_offlineFirstService.GetAmbreImportFields());
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Error loading import fields: {ex.Message}", ex);
            }
        }

        private async Task<List<AmbreTransform>> LoadTransformConfigurationsAsync()
        {
            try
            {
                return await Task.FromResult(_offlineFirstService.GetAmbreTransforms());
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Error loading transformations: {ex.Message}", ex);
            }
        }
    }

    /// <summary>
    /// Configuration complète pour un import Ambre
    /// </summary>
    public class AmbreImportConfiguration
    {
        public Country Country { get; set; }
        public List<AmbreImportField> ImportFields { get; set; }
        public List<AmbreTransform> Transforms { get; set; }
    }
}