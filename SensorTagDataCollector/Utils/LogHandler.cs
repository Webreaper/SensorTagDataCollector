using System;
using log4net;
using log4net.Appender;
using log4net.Core;
using log4net.Layout;
using log4net.Repository.Hierarchy;

namespace SensorTagDataCollector.Logging
{
    public static class LogHandler
    {
        public static string logLocation = string.Empty;

        public static void LogSetup(string fileName)
        {
            Hierarchy hierarchy = (Hierarchy)log4net.LogManager.GetRepository();

            PatternLayout patternLayout = new PatternLayout();
            patternLayout.ConversionPattern = "%date [%thread] %-5level %logger - %message%newline";
            patternLayout.ActivateOptions();

            RollingFileAppender roller = new RollingFileAppender();
            roller.AppendToFile = true;
            roller.File = fileName;
            roller.Layout = patternLayout;
            roller.MaxSizeRollBackups = 3;
            roller.MaxFileSize = 10000000;
            roller.RollingStyle = RollingFileAppender.RollingMode.Size;
            roller.StaticLogFileName = true;
            roller.ActivateOptions();

            ConsoleAppender console = new ConsoleAppender();
            console.Layout = patternLayout;
            console.ActivateOptions();

            hierarchy.Root.AddAppender(console);
            hierarchy.Root.AddAppender(roller);
            hierarchy.Root.Level = Level.Info;
            hierarchy.Configured = true;

            m_logInstance = LogManager.GetLogger("SensorTagCollector");
        }

        private static ILog m_logInstance;

        public static ILog LogInstance() 
        {
            if(m_logInstance == null )
            {
                throw new SystemException("Logging was not initialised.");
            }

            return m_logInstance;
        } 
    }
}
