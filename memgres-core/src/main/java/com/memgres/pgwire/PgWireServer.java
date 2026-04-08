package com.memgres.pgwire;

import com.memgres.engine.Database;
import com.memgres.engine.DatabaseRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * A Netty-based server that speaks the PostgreSQL wire protocol (v3).
 * This allows standard PostgreSQL JDBC drivers to connect.
 */
public class PgWireServer {

    private static final Logger LOG = LoggerFactory.getLogger(PgWireServer.class);

    private final DatabaseRegistry registry;
    private final CancelRegistry cancelRegistry = new CancelRegistry();
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public PgWireServer(DatabaseRegistry registry) {
        this.registry = registry;
    }

    public int start(int port, String bindAddress) {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new PgWireDecoder(cancelRegistry),
                                    new PgWireHandler(registry, cancelRegistry)
                            );
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(bindAddress, port).sync();
            serverChannel = f.channel();

            InetSocketAddress addr = (InetSocketAddress) serverChannel.localAddress();
            LOG.info("PgWire server listening on port {}", addr.getPort());
            return addr.getPort();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to start PgWire server", e);
        }
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }
}
