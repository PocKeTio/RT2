using System;
using Microsoft.Extensions.DependencyInjection;

namespace RecoTool.Infrastructure.DI
{
    /// <summary>
    /// Service Locator pattern to access DI container from anywhere
    /// Provides a consistent way to resolve services throughout the application
    /// </summary>
    public static class ServiceLocator
    {
        private static IServiceProvider _serviceProvider;

        /// <summary>
        /// Initializes the service locator with the DI container
        /// Should be called once during application startup (App.xaml.cs)
        /// </summary>
        public static void Initialize(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        }

        /// <summary>
        /// Gets a required service from the DI container
        /// Throws if service is not registered
        /// </summary>
        public static T GetRequiredService<T>() where T : class
        {
            if (_serviceProvider == null)
                throw new InvalidOperationException("ServiceLocator not initialized. Call Initialize() during app startup.");

            return _serviceProvider.GetRequiredService<T>();
        }

        /// <summary>
        /// Gets an optional service from the DI container
        /// Returns null if service is not registered
        /// </summary>
        public static T GetService<T>() where T : class
        {
            if (_serviceProvider == null)
                throw new InvalidOperationException("ServiceLocator not initialized. Call Initialize() during app startup.");

            return _serviceProvider.GetService<T>();
        }

        /// <summary>
        /// Creates a new scope for scoped services
        /// Useful for background operations or dialog windows
        /// </summary>
        public static IServiceScope CreateScope()
        {
            if (_serviceProvider == null)
                throw new InvalidOperationException("ServiceLocator not initialized. Call Initialize() during app startup.");

            return _serviceProvider.CreateScope();
        }

        /// <summary>
        /// Checks if the service locator has been initialized
        /// </summary>
        public static bool IsInitialized => _serviceProvider != null;

        /// <summary>
        /// Resets the service locator (primarily for testing)
        /// </summary>
        internal static void Reset()
        {
            _serviceProvider = null;
        }
    }
}
