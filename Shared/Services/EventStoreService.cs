using EventStore.Client;
using Shared.Services.Abstractions;
using System.Text.Json;

namespace Shared.Services
{
    public class EventStoreService : IEventStoreService
    {
        EventStoreClientSettings GetEventStoreClientSettings(string connectionString = "esdb://admin:changeit@localhost:2113?tls=false&tlsVerifyCert=false") =>
           EventStoreClientSettings.Create(connectionString);


        EventStoreClient Client { get => new(GetEventStoreClientSettings()); }

        /// <summary>
        /// Verilen olay verilerini (EventData koleksiyonu) belirli bir akışa (stream) ekler.
        /// Bu metot, olayları EventStore'da depolamak için kullanılır.
        /// </summary>
        /// <param name="streamName">Olayların ekleneceği akışın adı.</param>
        /// <param name="eventData">Eklenmek üzere olan olay verilerini içeren koleksiyon.</param>
        /// <returns>Asenkron bir Task döner. Olaylar başarılı bir şekilde akışa eklendiğinde bu görev tamamlanır.</returns>
        public async Task AppendToStreamAsync(string streamName, IEnumerable<EventData> eventData)
            => await Client.AppendToStreamAsync(
                streamName: streamName,
                eventData: eventData,
                expectedState: StreamState.Any);

        /// <summary>
        /// Verilen bir olay nesnesini (object @event) EventData formatına dönüştürür.
        /// EventData, EventStore'a eklenebilecek bir olayın temsilidir ve olayın
        /// benzersiz kimliğini (UUID), türünü (type) ve serileştirilmiş (serialized) veri içeriğini (data) içerir.
        /// </summary>
        /// <param name="event">Dönüştürülecek olan olay nesnesi. Herhangi bir türde olabilir.</param>
        /// <returns>Olay verilerini içeren EventData nesnesi döner.</returns>
        public EventData GenerateEventData(object @event)
            => new(
                    eventId: Uuid.NewUuid(),
                    type: @event.GetType().Name,
                    data: JsonSerializer.SerializeToUtf8Bytes(@event)
                );

        /// <summary>
        /// EventStore'da belirli bir akışa (stream) abone olmanızı sağlar. Bu, akışa yeni olaylar eklendiğinde belirli bir geri çağırma
        /// fonksiyonunun tetiklenmesini sağlar.
        /// </summary>
        /// <param name="streamName">Abone olunacak akışın adı.</param>
        /// <param name="eventAppeared">
        /// Akışta yeni bir olay göründüğünde çağrılacak olan geri çağırma fonksiyonu.
        /// Parametreleri:
        /// <list type="bullet">
        /// <item>
        /// <description>
        /// <c>StreamSubscription streamSubscription:</c> Abonenin kendisini temsil eder. Abonelik ile ilgili bilgiler içerir.
        /// </description>
        /// </item>
        /// <item>
        /// <description>
        /// <c>ResolvedEvent resolvedEvent:</c> Akışta bulunan bir olayı temsil eder. Olayın verilerini, metadata'sını ve konumunu içerir.
        /// </description>
        /// </item>
        /// <item>
        /// <description>
        /// <c>CancellationToken cancellationToken:</c> Aboneliğin iptali için kullanılan bir token. Abonelik sırasında iptal işlemlerini
        /// yönetmek için kullanılır.
        /// </description>
        /// </item>
        /// </list>
        /// </param>
        /// <param name="subscriptionDropped">
        /// Abonelik düştüğünde (bağlantı kesildiğinde veya abonelikte bir hata meydana geldiğinde) çağrılacak olan geri çağırma fonksiyonu.
        /// Parametreleri:
        /// <list type="bullet">
        /// <item>
        /// <description>
        /// <c>StreamSubscription streamSubscription:</c> Düşen aboneliği temsil eder.
        /// </description>
        /// </item>
        /// <item>
        /// <description>
        /// <c>SubscriptionDroppedReason subscriptionDroppedReason:</c> Aboneliğin düşme nedenini belirtir. (Örneğin, bağlantı kaybı, sunucu hatası vb.)
        /// </description>
        /// </item>
        /// <item>
        /// <description>
        /// <c>Exception exception:</c> Eğer varsa, düşme sebebiyle ilişkili istisnayı içerir.
        /// </description>
        /// </item>
        /// </list>
        /// </param>
        /// <returns>Asenkron bir <c>Task</c> döner.</returns>
        public async Task SubscribeToStreamAsync(string streamName, Func<StreamSubscription, ResolvedEvent, CancellationToken, Task> eventAppeared)
          => await Client.SubscribeToStreamAsync(
              streamName: streamName,
              start: FromStream.Start,
              eventAppeared: eventAppeared,
              subscriptionDropped: (streamSubscription, subscriptionDroppedReason, exception) => Console.WriteLine("Disconnected!") 
              );
    }
}

