using MassTransit;
using MongoDB.Driver;
using Shared;
using Shared.Events;
using Stock.API.Services;

namespace Stock.API.Consumers;

public class OrderCreatedEventConsumer(MongoDBService mongoDBService, ISendEndpointProvider sendEndpointProvider,IPublishEndpoint publishEndpoint) : IConsumer<OrderCreatedEvent>
{

    public async Task Consume(ConsumeContext<OrderCreatedEvent> context)
    {
        List<bool> stockResult = new();
        IMongoCollection<Stock.API.Models.Stock> collection = mongoDBService.GetCollection<Stock.API.Models.Stock>();

        foreach (var orderItem in context.Message.OrderItems)
        {
            stockResult.Add(await (await collection.FindAsync(s => s.ProductId == orderItem.ProductId.ToString() && s.Count >= orderItem.Count)).AnyAsync());
        }
        if (stockResult.TrueForAll(s => s.Equals(true)))
        {
            //Stock Güncellemesi
            foreach (var orderItem in context.Message.OrderItems)
            {
                Models.Stock stock = await (await collection.FindAsync(s => s.ProductId == orderItem.ProductId.ToString())).FirstOrDefaultAsync();

                stock.Count -= orderItem.Count;

                await collection.ReplaceOneAsync(s => s.ProductId == orderItem.ProductId.ToString(), stock);

            }
            //Payment i uyaracak event'in fırlatılması
            var sendEndpoint = await sendEndpointProvider.GetSendEndpoint(new Uri($"queue:{RabbitMQSettings.Payment_StockReservedEventQueue}"));
            await sendEndpoint.Send<StockReservedEvent>(new()
            {
                OrderId = context.Message.OrderId,
                BuyerId = context.Message.BuyerId,
                TotalPrice = context.Message.TotalPrice,
                OrderItems = context.Message.OrderItems
            });
        }
        else
        {
            //Stok yetersiz
            //Order i uyaracak event in fırlatılması
            StockNotReservedEvent stockNotReservedEvent = new()
            {
                OrderId = context.Message.OrderId,
                BuyerId = context.Message.BuyerId,
                Message = "Stock is not enough"
            };

            await publishEndpoint.Publish<StockNotReservedEvent>(stockNotReservedEvent);

        }

    }
}
