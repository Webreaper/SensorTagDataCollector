using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Json;
using System.Text;
using MailKit.Net.Smtp;
using MimeKit;
using static SensorTagElastic.MainClass;
using PushoverClient;
using log4net.Repository.Hierarchy;
using SensorTagDataCollector.Logging;

namespace SensorTagElastic
{
    public class Utils
    {
        public static DateTime getFromEpoch(long epoch)
        {
            var stamp = new DateTime(1970, 1, 1, 0, 0, 0).ToUniversalTime().AddMilliseconds(Convert.ToDouble(epoch));
            return stamp;

        }

        public static long getEpochTime(DateTime time)
        {
            TimeSpan t = time.ToUniversalTime() - new DateTime(1970, 1, 1, 0, 0, 0);
            return (long)t.TotalMilliseconds;
        }


        public static void SendAlertEmail(EmailSettings settings, ICollection<Alert> alerts)
        {
            Log("Sending email with {0} warnings.", alerts.Count());

            string body = "Warnings / notifications for devices:\n";
            foreach( var alert in alerts.OrderBy( x => x.deviceName ) )
            {
                var msg = $" - {alert.deviceName}: {alert.alertText}";
                body += msg + "\n";
                Log(msg);
            }

            try
            {
                var mimeMsg = new MimeMessage();
                mimeMsg.From.Add(new MailboxAddress("Sensor Tag Process", settings.fromaddress));
                mimeMsg.To.Add(new MailboxAddress(settings.toname, settings.toaddress));
                mimeMsg.Subject = "Wireless Sensor Tag Alert";
                mimeMsg.Body = new TextPart("plain") { Text = body };

                using (var client = new SmtpClient())
                {
                    client.ServerCertificateValidationCallback = ServerCertificateValidation.CertValidationCallBack;
                    client.Timeout = 30 * 1000;
                    client.Connect(settings.smtpserver, settings.smtpport, false);

                    // Note: since we don't have an OAuth2 token, disable
                    // the XOAUTH2 authentication mechanism.
                    client.AuthenticationMechanisms.Remove("XOAUTH2");

                    // Note: only needed if the SMTP server requires authentication
                    client.Authenticate(settings.username, settings.password);

                    client.Send(mimeMsg);
                    client.Disconnect(true);
                }
            }
            catch( Exception ex )
            {
                Log("Exception sending email: {0}", ex.Message);  
            }
        }

        public static void Log(string format, params object[] args)
        {
            LogHandler.LogInstance().InfoFormat(format, args);
        }

        public static T deserializeJSON<T>(string json)
        {
            var instance = Activator.CreateInstance<T>();
            using (var ms = new MemoryStream(Encoding.Unicode.GetBytes(json)))
            {
                var serializer = new DataContractJsonSerializer(instance.GetType());
                return (T)serializer.ReadObject(ms);
            }
        }

        internal static void SendPushAlert(PushSettings pushSettings, List<Alert> alerts)
        {
            try
            {
                
                var client = new Pushover(pushSettings.applicationKey);

                string body = "Notifications for devices:\n";
                foreach (var alert in alerts.OrderBy(x => x.deviceName))
                {
                    var msg = $" - {alert.deviceName}: {alert.alertText}";
                    body += msg + "\n";
                }

                client.Push(pushSettings.alertTitle, body, pushSettings.userKey);

                Log("Push event sent successfully.");
            }
            catch( Exception ex )
            {
                Log("Exception sending push: {0}", ex.Message);
            }
        }
    }
}
