package ai.pipestream.confluence;

import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

/**
 * The HTTP transport of a scenario run: {@link RecordingTape} sends to the
 * real site and saves every exchange, {@link ReplayTape} answers from the
 * saved exchanges. Only the blocking {@code send} is served.
 */
abstract class ExchangeTape extends HttpClient {

    /** Starts the named scenario; later exchanges belong to it. */
    abstract void scenario(String name);

    /** Marks the exchanges made while on as one group that may arrive in any order. */
    abstract void unordered(boolean on);

    /** Waits for the real site to catch up; a replay does not wait. */
    abstract void pause(Duration time) throws InterruptedException;

    @Override
    public Optional<CookieHandler> cookieHandler() {
        return Optional.empty();
    }

    @Override
    public Optional<Duration> connectTimeout() {
        return Optional.empty();
    }

    @Override
    public Redirect followRedirects() {
        return Redirect.NEVER;
    }

    @Override
    public Optional<ProxySelector> proxy() {
        return Optional.empty();
    }

    @Override
    public SSLContext sslContext() {
        throw new UnsupportedOperationException("a tape has no TLS context");
    }

    @Override
    public SSLParameters sslParameters() {
        throw new UnsupportedOperationException("a tape has no TLS parameters");
    }

    @Override
    public Optional<Authenticator> authenticator() {
        return Optional.empty();
    }

    @Override
    public Version version() {
        return Version.HTTP_1_1;
    }

    @Override
    public Optional<Executor> executor() {
        return Optional.empty();
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
            HttpResponse.BodyHandler<T> handler) {
        throw new UnsupportedOperationException("a tape serves blocking sends only");
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
            HttpResponse.BodyHandler<T> handler, HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
        throw new UnsupportedOperationException("a tape serves blocking sends only");
    }
}
