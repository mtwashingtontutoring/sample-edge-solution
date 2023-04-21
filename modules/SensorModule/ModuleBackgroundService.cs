using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Client.Transport.Mqtt;
using System.Text;
using System.Text.Json;

namespace SensorModule;

class PositionMessage {
    public double latitude {get; set;}
    public double longitude {get; set;}
    public double height {get; set;}
    public double roll {get; set;}
    public double pitch {get; set;}
    public double heading {get; set;}
    public double gForceMagnitude {get; set;}
    public double east_offset {get; set;}
    public double north_offset {get; set;}
    public double total_offset {get; set;}
    public DateTime timestamp {get; set;}
    

    public PositionMessage(double latitude, double longitude,
        double height, double roll, double pitch, double heading, double gForceMagnitude,
        double east_offset, double north_offset, double total_offset, DateTime timestamp
    )  {
        this.latitude = latitude;
        this.longitude = longitude;
        this.height = height;
        this.roll = roll;
        this.pitch = pitch;
        this.heading = heading;
        this.gForceMagnitude = gForceMagnitude;
        this.east_offset = east_offset;
        this.north_offset = north_offset;
        this.total_offset = total_offset;
        this.timestamp = timestamp;
    }

    public async Task<Stream> toJSONStream() {
        var stream = new MemoryStream();
        await JsonSerializer.SerializeAsync<PositionMessage>(stream, this);
        stream.Position = 0;

        return stream;
    }

    public static PositionMessage fromJSONStream(Stream stream) {
        return null;
    }
}

internal class ModuleBackgroundService : BackgroundService {
    private int _counter;
    private ModuleClient? _moduleClient;
    private CancellationToken _cancellationToken;
    private readonly ILogger<ModuleBackgroundService> _logger;

    private static DateTime EPOCH_DATE_TIME = new DateTime(1970, 1, 1);

    private const int SEND_PERIOD_MS = 10000;
    private const double latitude1=29.10798914; //Marlin nominal latitude in degree
    private const double longitude1=-87.94334921; //Marlin nominal longitude in degree
    private const double rollCorrection=0.23; //roll initial value correction
    private const double pitchCorrection=0.65; //pitch initial value correction
    private const double headingCorrection=-3.0; //heading initial value correction
    private static double startTime = (DateTime.Now-EPOCH_DATE_TIME).TotalMilliseconds;    
    private double lastSentTime = 0;
    private long numMessagePacketsSent = 0;
    private List<PositionMessage> unsentPositions = new List<PositionMessage>();

    public ModuleBackgroundService(ILogger<ModuleBackgroundService> logger) => _logger = logger;
    
    protected override async Task ExecuteAsync(CancellationToken cancellationToken) {
        _cancellationToken = cancellationToken;
        MqttTransportSettings mqttSetting = new(TransportType.Mqtt_Tcp_Only);
        ITransportSettings[] settings = { mqttSetting };

        // Open a connection to the Edge runtime
        _moduleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);

        // Reconnect is not implemented because we'll let docker restart the process when the connection is lost
        _moduleClient.SetConnectionStatusChangesHandler((status, reason) => 
            _logger.LogWarning("Connection changed: Status: {status} Reason: {reason}", status, reason));

        await _moduleClient.OpenAsync(cancellationToken);

        _logger.LogInformation("IoT Hub module client initialized.");

        // Register callback to be called when a message is received by the module
        await _moduleClient.SetInputMessageHandlerAsync("input1", ProcessMessageAsync, null, cancellationToken);

        while(true) {
            try{
                await ReadDevicePackets();
            } catch(Exception) {}
            await Task.Delay(1000);
        }
    }
    
    private async Task<MessageResponse> ProcessMessageAsync(Message message, object userContext) {
        int counterValue = Interlocked.Increment(ref _counter);

        byte[] messageBytes = message.GetBytes();
        string messageString = Encoding.UTF8.GetString(messageBytes);
        _logger.LogInformation("Received message: {counterValue}, Body: [{messageString}]", counterValue, messageString);

        if (!string.IsNullOrEmpty(messageString))
        {
            using var pipeMessage = new Message(messageBytes);
            foreach (var prop in message.Properties)
            {
                pipeMessage.Properties.Add(prop.Key, prop.Value);
            }
            await _moduleClient!.SendEventAsync("output1", pipeMessage, _cancellationToken);

            _logger.LogInformation("Received message sent");
        }
        return MessageResponse.Completed;
    }
    private double GetDistance(double longitude, double latitude, double otherLongitude, double otherLatitude) {
        var d1 = latitude * (Math.PI / 180.0);
        var num1 = longitude * (Math.PI / 180.0);
        var d2 = otherLatitude * (Math.PI / 180.0);
        var num2 = otherLongitude * (Math.PI / 180.0) - num1;
        var d3 = Math.Pow(Math.Sin((d2 - d1) / 2.0), 2.0) + Math.Cos(d1) * Math.Cos(d2) * Math.Pow(Math.Sin(num2 / 2.0), 2.0);
        
        return 6376500.0 * (2.0 * Math.Atan2(Math.Sqrt(d3), Math.Sqrt(1.0 - d3))) * 3.28084; // Distance in feet
    }
    private async Task ReadDevicePackets() {

        double latitude = 0;          // degree
        double longitude = 0;         //  degree
        double height = 0;            //  meters
        double roll = 0;              // degree
        double pitch = 0;             // degree 
        double heading = 0;           // degree 
        double gForceMagnitude = 10;  // m/s^2
        
        double north_offset = -1*(GetDistance(longitude,latitude1,longitude,latitude) - 70.01); // 70.01 ft correction for the antenna location
        double east_offset = 1*(GetDistance(longitude1,latitude,longitude,latitude) -12.02); // 12.02 ft correction for the antenna location
        double total_offset = Math.Sqrt(east_offset*east_offset+north_offset*north_offset); // Total offset

        PositionMessage positionMessage= new PositionMessage(latitude,longitude,height,roll,
            pitch,heading, gForceMagnitude, east_offset, north_offset, total_offset, DateTime.Now.AddHours(-5)
        );

        await SendMessage(positionMessage);
    }
    private async Task SendMessage(PositionMessage positionMessage) {
        DateTime now = DateTime.Now;
        double now_ms = (now-EPOCH_DATE_TIME).TotalMilliseconds;

        unsentPositions.Add(positionMessage);

        if(_moduleClient != null && (now_ms-lastSentTime)>SEND_PERIOD_MS) { 
            try{       
                using(Message msg = new Message(Encoding.ASCII.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject(unsentPositions)))) {
                    msg.ContentType = "application/json";
                    msg.ContentEncoding = "utf-8";
                    Console.WriteLine($"{now} - sending ({unsentPositions.Count}) messages to positioning \"topic\" - packet # {numMessagePacketsSent++}");
                    await _moduleClient!.SendEventAsync("positioning", msg);
                    lastSentTime = now_ms;
                    unsentPositions.Clear();
                }
            } catch(Exception e) {
                Console.Error.WriteLine(e);
            }
        }
    }
}
