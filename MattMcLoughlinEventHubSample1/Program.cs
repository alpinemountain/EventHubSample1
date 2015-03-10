using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

#region Copyright Notice
//The MIT License (MIT)
//Copyright (c) 2015, Matthew McLoughlin
//
//Permission is hereby granted, free of charge, to any person obtaining a copy
//of this software and associated documentation files (the "Software"), to deal
//in the Software without restriction, including without limitation the rights
//to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//copies of the Software, and to permit persons to whom the Software is
//furnished to do so, subject to the following conditions:
//
//The above copyright notice and this permission notice shall be included in
//all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//THE SOFTWARE.
#endregion

namespace MattMcLoughlinEventHubSample1
{
    class Program
    {
        static void Main(string[] args)
        {
            // Configuration data - modify this
            string azureStorageAccountName = ""; // get this from the Azure portal
            string azureStorageAccountKey = ""; // get this from the Azure portal
            string azureServiceBusConnectionString = ""; // get this from the Azure portal
            string azureEventHubName = ""; // must be lowercase

            if (string.IsNullOrWhiteSpace(azureStorageAccountKey) || string.IsNullOrWhiteSpace(azureStorageAccountKey) || string.IsNullOrWhiteSpace(azureServiceBusConnectionString) || string.IsNullOrWhiteSpace(azureEventHubName))
            {
                throw new ArgumentException("You must specify the basic configuration information.");
            }

            Console.WriteLine("--------------------------------------------------------------------");
            Console.WriteLine("Simple, Complete Event Hub Sample Application");
            Console.WriteLine("Written by Matt McLoughlin - http://blog.mattmcloughlin.com");
            Console.WriteLine("--------------------------------------------------------------------");
            Console.WriteLine("It is recommended to widen this console window for readability:");
            Console.WriteLine("Top-left icon -> Properties -> Layout -> Window Size -> width = 160");
            Console.WriteLine("--------------------------------------------------------------------");


            string eventProcessorHostName = Guid.NewGuid().ToString().Replace("-", ""); // must be unique for each instance of EventProcessorHost
            string storageConnectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", azureStorageAccountName, azureStorageAccountKey);

            MattMcLEventHub eventHub = new MattMcLEventHub();

            eventHub.SendTestEventHubMessages(azureEventHubName, azureServiceBusConnectionString);

            eventHub.StartEventProcessorHost(azureEventHubName, eventProcessorHostName, storageConnectionString, azureServiceBusConnectionString);

            Console.WriteLine("Waiting to async send/receive messages.  Press 'Enter' to quit at any time.");
            Console.ReadLine();

            eventHub.StopEventProcessorHost().Wait(); // this is CRITICAL to prevent 'stuck' partitions and message replay
        }
    }
    public class MattMcLEventHub
    {
        private EventProcessorHost host;
        public void StartEventProcessorHost(string eventHubName, string eventProcessorHostName, string storageConnectionString, string serviceBusConnectionString)
        {
            host = new EventProcessorHost(eventProcessorHostName, eventHubName, EventHubConsumerGroup.DefaultGroupName, serviceBusConnectionString, storageConnectionString);
            EventProcessorOptions options = new EventProcessorOptions();
            options.ExceptionReceived += OnExceptionReceived;
            host.RegisterEventProcessorAsync<MattMcLEventProcessor>(options);
        }

        public void SendTestEventHubMessages(string eventHubName, string serviceBusConnectionString, int numberOfTestMessagesToSend = 50)
        {
            var manager = NamespaceManager.CreateFromConnectionString(serviceBusConnectionString);
            var description = manager.CreateEventHubIfNotExists(eventHubName);
            var client = EventHubClient.CreateFromConnectionString(serviceBusConnectionString, description.Path);
            client.RetryPolicy = Microsoft.ServiceBus.RetryExponential.Default;
            for (int i = 1; i <= numberOfTestMessagesToSend; i++)
            {
                string textData = string.Format("Sent number: {0} Generated at: {1}", i, DateTime.Now);
                byte[] data = UTF8Encoding.UTF8.GetBytes(textData);
                EventData eventData = new EventData(data);
                client.SendAsync(eventData);
            }
        }
        public async Task StopEventProcessorHost()
        {
            if (host != null)
            {
                await host.UnregisterEventProcessorAsync();
            }
        }
        private void OnExceptionReceived(object sender, ExceptionReceivedEventArgs e)
        {
            Console.WriteLine("Error: " + e.Exception.Message);
        }
    }
    public class MattMcLEventProcessor : IEventProcessor
    {
        private int partitionMessagesReceivedCount = 0;
        Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            return Task.FromResult<object>(null);
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            return Task.FromResult<object>(null);
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            string partitionId = context.Lease.PartitionId;
            if (messages != null)
            {
                foreach (var message in messages)
                {
                    byte[] data = message.GetBytes();
                    string text = Encoding.UTF8.GetString(data);
                    int totalMessagesReceived = Interlocked.Increment(ref MessageCounter.TotalMessagesReceived);
                    Console.WriteLine("TotalMessageCount: {0} PartitionId: {1} PartitionMessageCount: {2} Message: [{3}]", totalMessagesReceived, partitionId, partitionMessagesReceivedCount, text);
                    partitionMessagesReceivedCount++;
                }
                await context.CheckpointAsync();
            }
        }
    }
    public static class MessageCounter
    {
        public static int TotalMessagesReceived = 0;
    }
}