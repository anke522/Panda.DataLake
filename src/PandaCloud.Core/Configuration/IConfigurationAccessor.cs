using Microsoft.Extensions.Configuration;

namespace PandaCloud.Core.Configuration
{
    public interface IConfigurationAccessor
    {
        IConfigurationRoot Configuration { get; }
    }
}