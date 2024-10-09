using MassTransit;
using Stock.API.Services;
using MongoDB.Driver;
using Stock.API.Consumers;
using Shared;

var builder = WebApplication.CreateBuilder(args);



builder.Services.AddMassTransit(configurator =>
{
    configurator.AddConsumer<OrderCreatedEventConsumer>();
    configurator.AddConsumer<PaymentFailedEventConsumer>();
    configurator.UsingRabbitMq((context, _configure) =>
    {
        _configure.Host(builder.Configuration.GetConnectionString("RabbitMQ"));

        _configure.ReceiveEndpoint(RabbitMQSettings.Stock_OrderCreatedEventQueue, e => e.ConfigureConsumer<OrderCreatedEventConsumer>(context));
        _configure.ReceiveEndpoint(RabbitMQSettings.Stock_PaymentFailedEventQueue, e => e.ConfigureConsumer<PaymentFailedEventConsumer>(context));
    });
});
builder.Services.AddSingleton<Stock.API.Services.MongoDBService>();

var app = builder.Build();


using IServiceScope scope = app.Services.CreateScope();
MongoDBService mongoDbService = scope.ServiceProvider.GetService<MongoDBService>();
var stockCollection = mongoDbService.GetCollection<Stock.API.Models.Stock>();
if (!stockCollection.FindSync(session => true).Any())
{
    await stockCollection.InsertOneAsync(new() { ProductId = Guid.NewGuid().ToString(), Count = 100 });
    await stockCollection.InsertOneAsync(new() { ProductId = Guid.NewGuid().ToString(), Count = 200 });
    await stockCollection.InsertOneAsync(new() { ProductId = Guid.NewGuid().ToString(), Count = 50 });
    await stockCollection.InsertOneAsync(new() { ProductId = Guid.NewGuid().ToString(), Count = 30 });
    await stockCollection.InsertOneAsync(new() { ProductId = Guid.NewGuid().ToString(), Count = 5 });
}


app.Run();
