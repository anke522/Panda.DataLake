using System;
using Microsoft.Extensions.DependencyInjection;
using Nest;

namespace ElasticSearch.Extensions.DependencyInjection
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddElasticSearch(this IServiceCollection services, string url)
        {
            if (services == null) throw new ArgumentNullException(nameof(services));
            return BuildElasticClient(services, url);
        }

        public static IServiceCollection AddElasticSearch<TModel>(this IServiceCollection services, string url)
        {
            if (services == null) throw new ArgumentNullException(nameof(services));
            return BuildElasticClient(services, url);
        }

        private static IServiceCollection BuildElasticClient(IServiceCollection services, string url)
        {
            var connectionSettings = new ConnectionSettings(new Uri(url));

            services.AddSingleton<IConnectionSettingsValues>(connectionSettings);

            services.AddScoped(typeof(IElasticClient), typeof(ElasticClient));
            return services;
        }
    }
}