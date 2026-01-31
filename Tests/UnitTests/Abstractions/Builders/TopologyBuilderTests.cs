using FlySwattr.NATS.Abstractions;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Abstractions.Builders;

[Property("nTag", "Abstractions")]
public class TopologyBuilderTests
{
    private readonly TopologyBuilder<TestTopologySource> _sut;

    public TopologyBuilderTests()
    {
        _sut = new TopologyBuilder<TestTopologySource>();
    }

    #region MapConsumer Validation Tests

    [Test]
    public void MapConsumer_ShouldThrowArgumentException_WhenConsumerNameIsNull()
    {
        // Arrange
        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;

        // Act & Assert
        Should.Throw<ArgumentException>(() => _sut.MapConsumer<TestMessage>(null!, handler));
    }

    [Test]
    public void MapConsumer_ShouldThrowArgumentException_WhenConsumerNameIsEmpty()
    {
        // Arrange
        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;

        // Act & Assert
        Should.Throw<ArgumentException>(() => _sut.MapConsumer<TestMessage>("", handler));
    }

    [Test]
    public void MapConsumer_ShouldThrowArgumentException_WhenConsumerNameIsWhitespace()
    {
        // Arrange
        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;

        // Act & Assert
        Should.Throw<ArgumentException>(() => _sut.MapConsumer<TestMessage>("   ", handler));
    }

    [Test]
    public void MapConsumer_ShouldThrowArgumentNullException_WhenHandlerIsNull()
    {
        // Act & Assert
        Should.Throw<ArgumentNullException>(() => _sut.MapConsumer<TestMessage>("test-consumer", null!));
    }

    #endregion

    #region MapConsumer Registration Tests

    [Test]
    public void MapConsumer_ShouldRegisterHandler_WhenValidParameters()
    {
        // Arrange
        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;

        // Act
        _sut.MapConsumer("test-consumer", handler);

        // Assert
        _sut.HandlerMappings.ShouldContainKey("test-consumer");
        var registration = _sut.HandlerMappings["test-consumer"];
        registration.MessageType.ShouldBe(typeof(TestMessage));
        registration.ConsumerName.ShouldBe("test-consumer");
        registration.Handler.ShouldBe(handler);
    }

    [Test]
    public void MapConsumer_ShouldOverwriteHandler_WhenCalledWithSameConsumerName()
    {
        // Arrange
        Func<IJsMessageContext<TestMessage>, Task> handler1 = _ => Task.CompletedTask;
        Func<IJsMessageContext<TestMessage>, Task> handler2 = _ => Task.CompletedTask;

        // Act
        _sut.MapConsumer("test-consumer", handler1);
        _sut.MapConsumer("test-consumer", handler2);

        // Assert
        _sut.HandlerMappings.Count.ShouldBe(1);
        _sut.HandlerMappings["test-consumer"].Handler.ShouldBe(handler2);
    }

    [Test]
    public void MapConsumer_ShouldReturnBuilder_ForChaining()
    {
        // Arrange
        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;

        // Act
        var result = _sut.MapConsumer("test-consumer", handler);

        // Assert
        result.ShouldBeSameAs(_sut);
    }

    [Test]
    public void MapConsumer_ShouldAllowChaining_MultipleConsumers()
    {
        // Arrange
        Func<IJsMessageContext<TestMessage>, Task> handler1 = _ => Task.CompletedTask;
        Func<IJsMessageContext<OtherTestMessage>, Task> handler2 = _ => Task.CompletedTask;

        // Act
        _sut.MapConsumer("consumer-1", handler1)
            .MapConsumer("consumer-2", handler2);

        // Assert
        _sut.HandlerMappings.Count.ShouldBe(2);
        _sut.HandlerMappings.ShouldContainKey("consumer-1");
        _sut.HandlerMappings.ShouldContainKey("consumer-2");
    }

    #endregion

    #region MapConsumer with Options Tests

    [Test]
    public void MapConsumer_WithOptions_ShouldInvokeConfigureOptions()
    {
        // Arrange
        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;
        var optionsConfigured = false;

        // Act
        _sut.MapConsumer("test-consumer", handler, options =>
        {
            optionsConfigured = true;
            options.MaxConcurrency = 5;
        });

        // Assert
        optionsConfigured.ShouldBeTrue();
        _sut.HandlerMappings["test-consumer"].Options.MaxConcurrency.ShouldBe(5);
    }

    [Test]
    public void MapConsumer_WithOptions_ShouldAllowNullConfigureOptions()
    {
        // Arrange
        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;

        // Act & Assert - should not throw
        Should.NotThrow(() => _sut.MapConsumer("test-consumer", handler, null));

        _sut.HandlerMappings.ShouldContainKey("test-consumer");
    }

    [Test]
    public void MapConsumer_WithOptions_ShouldReturnBuilder_ForChaining()
    {
        // Arrange
        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;

        // Act
        var result = _sut.MapConsumer("test-consumer", handler, _ => { });

        // Assert
        result.ShouldBeSameAs(_sut);
    }

    #endregion

    #region HandlerMappings Tests

    [Test]
    public void HandlerMappings_ShouldBeEmptyInitially()
    {
        // Assert
        _sut.HandlerMappings.ShouldBeEmpty();
    }

    [Test]
    public void HandlerMappings_ShouldBeReadOnly()
    {
        // Assert
        _sut.HandlerMappings.ShouldBeAssignableTo<IReadOnlyDictionary<string, ConsumerHandlerRegistration>>();
    }

    #endregion

    #region TopologyConsumerOptions Tests

    [Test]
    public void TopologyConsumerOptions_ShouldHaveCorrectDefaults()
    {
        // Arrange
        var options = new TopologyConsumerOptions();

        // Assert
        options.MaxConcurrency.ShouldBeNull();
        options.EnableLoggingMiddleware.ShouldBeTrue();
        options.EnableValidationMiddleware.ShouldBeTrue();
        options.ResiliencePipelineKey.ShouldBeNull();
        options.MiddlewareTypes.ShouldBeEmpty();
    }

    [Test]
    public void TopologyConsumerOptions_AddMiddleware_ShouldAddType()
    {
        // Arrange
        var options = new TopologyConsumerOptions();

        // Act
        options.AddMiddleware<TestMiddleware>();

        // Assert
        options.MiddlewareTypes.ShouldContain(typeof(TestMiddleware));
    }

    [Test]
    public void TopologyConsumerOptions_AddMiddleware_ShouldReturnOptions_ForChaining()
    {
        // Arrange
        var options = new TopologyConsumerOptions();

        // Act
        var result = options.AddMiddleware<TestMiddleware>();

        // Assert
        result.ShouldBeSameAs(options);
    }

    [Test]
    public void TopologyConsumerOptions_AddMiddleware_ShouldAllowMultipleMiddlewares()
    {
        // Arrange
        var options = new TopologyConsumerOptions();

        // Act
        options.AddMiddleware<TestMiddleware>()
               .AddMiddleware<AnotherTestMiddleware>();

        // Assert
        options.MiddlewareTypes.Count.ShouldBe(2);
        options.MiddlewareTypes.ShouldContain(typeof(TestMiddleware));
        options.MiddlewareTypes.ShouldContain(typeof(AnotherTestMiddleware));
    }

    #endregion

    #region ConsumerHandlerRegistration Tests

    [Test]
    public void ConsumerHandlerRegistration_ShouldStoreAllProperties()
    {
        // Arrange
        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;
        var options = new TopologyConsumerOptions { MaxConcurrency = 10 };

        // Act
        var registration = new ConsumerHandlerRegistration(
            typeof(TestMessage),
            "test-consumer",
            handler,
            options);

        // Assert
        registration.MessageType.ShouldBe(typeof(TestMessage));
        registration.ConsumerName.ShouldBe("test-consumer");
        registration.Handler.ShouldBe(handler);
        registration.Options.ShouldBe(options);
    }

    [Test]
    public void ConsumerHandlerRegistration_ShouldUseDefaultOptions_WhenNullProvided()
    {
        // Arrange
        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;

        // Act
        var registration = new ConsumerHandlerRegistration(
            typeof(TestMessage),
            "test-consumer",
            handler);

        // Assert
        registration.Options.ShouldNotBeNull();
        registration.Options.EnableLoggingMiddleware.ShouldBeTrue();
        registration.Options.EnableValidationMiddleware.ShouldBeTrue();
    }

    #endregion

    #region Test Types

    private class TestTopologySource : ITopologySource
    {
        public IEnumerable<StreamSpec> GetStreams() => [];
        public IEnumerable<ConsumerSpec> GetConsumers() => [];
    }

    private class TestMessage
    {
        public string Data { get; set; } = string.Empty;
    }

    private class OtherTestMessage
    {
        public int Value { get; set; }
    }

    private class TestMiddleware;

    private class AnotherTestMiddleware;

    #endregion
}
